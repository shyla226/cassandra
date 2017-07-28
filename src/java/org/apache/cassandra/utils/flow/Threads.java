/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils.flow;

import java.util.EnumMap;
import java.util.concurrent.Callable;

import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCScheduler;

public class Threads
{
    static class RequestOnCore implements FlowSubscription, TaggedRunnable
    {
        final int coreId;
        final FlowSubscription source;
        final TPCTaskType stage;

        <T> RequestOnCore(FlowSubscriber<T> subscriber, int coreId, TPCTaskType stage, Flow<T> source) throws Exception
        {
            this.coreId = coreId;
            this.source = source.subscribe(subscriber);
            this.stage = stage;
        }

        public void request()
        {
            if (TPC.isOnCore(coreId))
                run();
            else
                TPC.getForCore(coreId).scheduleDirect(this);
        }

        public void close() throws Exception
        {
            // Close on the current thread to propagate exceptions
            source.close();
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            return source.addSubscriberChainFromSource(throwable);
        }

        public TPCTaskType getStage()
        {
            return stage;
        }

        public int scheduledOnCore()
        {
            return coreId;
        }

        public void run()
        {
            source.request();
        }
    }

    final static EnumMap<TPCTaskType, Flow.Operator[]> REQUEST_ON_CORE = new EnumMap<>(TPCTaskType.class);

    private static Flow.Operator<Object, Object> constructRequestOnCore(int coreId, TPCTaskType stage)
    {
        return (source, subscriber) -> new RequestOnCore(subscriber, coreId, stage, source);
    }

    /**
     * Returns an operator to perform each request() on the given flow on the specified core thread.
     * If execution is already on this thread, the request is called directly, otherwise it is given to the scheduler
     * for async execution.
     */
    public static <T> Flow.Operator<T, T> requestOnCore(int coreId, TPCTaskType stage)
    {
        Flow.Operator[] req = REQUEST_ON_CORE.computeIfAbsent(stage, t ->
        {
            Flow.Operator<?, ?>[] ops = new Flow.Operator[TPC.getNumCores()];
            for (int i = 0; i < ops.length; ++i)
                ops[i] = constructRequestOnCore(i, stage);
            return ops;
        });
        return (Flow.Operator<T, T>) req[coreId];
    }

    static class RequestOn implements FlowSubscription, TaggedRunnable
    {
        final Scheduler scheduler;
        final FlowSubscription source;
        final TPCTaskType stage;

        <T> RequestOn(FlowSubscriber<T> subscriber, Scheduler scheduler, TPCTaskType stage, Flow<T> source) throws Exception
        {
            this.scheduler = scheduler;
            this.source = source.subscribe(subscriber);
            this.stage = stage;
        }

        public void request()
        {
            // TODO: If blocking is not a concern, recognizing we are already on an IO thread could boost perf.
            scheduler.scheduleDirect(this);
        }

        public void close() throws Exception
        {
            source.close();
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            return source.addSubscriberChainFromSource(throwable);
        }

        public TPCTaskType getStage()
        {
            return stage;
        }

        public int scheduledOnCore()
        {
            return TPC.getNumCores();
        }

        public void run()
        {
            source.request();
        }
    }

    /**
     * Returns an operator to perform each request() on the given flow on the specified scheduler.
     * If we are already on that scheduler, whether the request is called directly or scheduled depends on the specific
     * scheduler.
     */
    public static <T> Flow.Operator<T, T> requestOn(Scheduler scheduler, TPCTaskType stage)
    {
        if (scheduler instanceof TPCScheduler)
            return requestOnCore(((TPCScheduler) scheduler).coreId(), stage);
        else if (scheduler == Schedulers.io())
            return requestOnIo(stage);
        else
            return createRequestOn(scheduler, stage);
    }

    private static <T> Flow.Operator<T, T> createRequestOn(Scheduler scheduler, TPCTaskType stage)
    {
        return (source, subscriber) -> new RequestOn(subscriber, scheduler, stage, source);
    }

    static final EnumMap<TPCTaskType, Flow.Operator<?, ?>> REQUEST_ON_IO = new EnumMap<>(TPCTaskType.class);

    /**
     * Returns an operator to perform each request() on the given flow on the IO scheduler.
     * Used for operations that can block (e.g. sync reads off disk).
     */
    public static <T> Flow.Operator<T, T> requestOnIo(TPCTaskType stage)
    {
        Flow.Operator<T, T> ret = (Flow.Operator<T, T>) REQUEST_ON_IO.get(stage);
        if (ret != null)
            return ret;

        synchronized (REQUEST_ON_IO)
        {
            return (Flow.Operator<T, T>) REQUEST_ON_IO.computeIfAbsent(stage, t -> createRequestOn(Schedulers.io(), t));
        }
    }

    static class EvaluateOn<T> implements FlowSubscription, TaggedRunnable
    {
        final FlowSubscriber<T> subscriber;
        final Callable<T> source;
        final TPCTaskType stage;
        final int coreId;

        private volatile int requested = 0;
        EvaluateOn(FlowSubscriber<T> subscriber, Callable<T> source, int coreId, TPCTaskType stage)
        {
            this.subscriber = subscriber;
            this.source = source;
            this.coreId = coreId;
            this.stage = stage;
        }

        public void request()
        {
            switch (requested++)
            {
            case 0:
                if (TPC.isOnCore(coreId))
                    run();
                else
                    TPC.getForCore(coreId).scheduleDirect(this);
                break;
            default:
                // Assuming no need to switch threads for no work.
                subscriber.onComplete();
            }
        }

        public void run()
        {
            try
            {
                T v = source.call();
                subscriber.onNext(v);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
            }
        }

        public void close()
        {
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            return Flow.wrapException(throwable, this);
        }

        public String toString()
        {
            return Flow.formatTrace("evaluateOn " + coreId + " stage " + stage, source, subscriber);
        }

        public TPCTaskType getStage()
        {
            return stage;
        }

        public int scheduledOnCore()
        {
            return coreId;
        }
    }

    /**
     * Returns a flow which represents the evaluation of the given callable on the specified core thread.
     * If execution is already on this thread, the evaluation is called directly, otherwise it is given to the scheduler
     * for async execution.
     */
    public static <T> Flow<T> evaluateOnCore(Callable<T> callable, int coreId, TPCTaskType stage)
    {
        return new Flow<T>()
        {
            public FlowSubscription subscribe(FlowSubscriber<T> subscriber)
            {
                return new EvaluateOn<T>(subscriber, callable, coreId, stage);
            }
        };
    }

    public static <T> Flow<T> deferOnCore(Callable<Flow<T>> source, int coreId, TPCTaskType stage)
    {
        return evaluateOnCore(source, coreId, stage).flatMap(x -> x);
    }
}
