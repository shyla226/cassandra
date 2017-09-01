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
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCScheduler;

public class Threads
{
    final static EnumMap<TPCTaskType, Flow.Operator[]> REQUEST_ON_CORE = new EnumMap<>(TPCTaskType.class);

    private static Flow.Operator<Object, Object> constructRequestOnCore(int coreId, TPCTaskType stage)
    {
        return (source, subscriber, subscriptionRecipient) -> new RequestOn(source, subscriber, subscriptionRecipient, TPC.getForCore(coreId), stage);
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

    static class RequestOn implements FlowSubscription, TaggedRunnable, FlowSubscriptionRecipient
    {
        final Scheduler scheduler;
        final TPCTaskType stage;
        FlowSubscription source;

        <T> RequestOn(Flow<T> source, FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient, Scheduler scheduler, TPCTaskType stage)
        {
            this.scheduler = scheduler;
            this.stage = stage;
            subscriptionRecipient.onSubscribe(this);

            if (TPC.isOnScheduler(scheduler))
                source.requestFirst(subscriber, this);
            else
                scheduler.scheduleDirect(new TaggedRunnable.Base(stage, scheduler)
                {
                    public void run()
                    {
                        source.requestFirst(subscriber, RequestOn.this);
                    }
                });
        }

        public void onSubscribe(FlowSubscription source)
        {
            this.source = source;
        }

        public void requestNext()
        {
            if (TPC.isOnScheduler(scheduler))
                run();
            else
                scheduler.scheduleDirect(this);
        }

        public void close() throws Exception
        {
            source.close();
        }

        public TPCTaskType getStage()
        {
            return stage;
        }

        public Scheduler scheduledOn()
        {
            return scheduler;
        }

        public void run()
        {
            source.requestNext();
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
        else if (scheduler == TPC.ioScheduler())
            return requestOnIo(stage);
        else
            return createRequestOn(scheduler, stage);
    }

    private static <T> Flow.Operator<T, T> createRequestOn(Scheduler scheduler, TPCTaskType stage)
    {
        return (source, subscriber, subscriptionRecipient) -> new RequestOn(source, subscriber, subscriptionRecipient, scheduler, stage);
    }

    static final EnumMap<TPCTaskType, Flow.Operator<?, ?>> REQUEST_ON_IO = new EnumMap<>(TPCTaskType.class);

    private static Flow.Operator<Object, Object> constructRequestOnIO(TPCTaskType stage)
    {
        return (source, subscriber, subscriptionRecipient) -> new RequestOn(source, subscriber, subscriptionRecipient, TPC.ioScheduler(), stage);
    }

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
            return (Flow.Operator<T, T>) REQUEST_ON_IO.computeIfAbsent(stage, t -> constructRequestOnIO(t));
        }
    }

    static class EvaluateOn<T> extends FlowSource<T> implements TaggedRunnable
    {
        final Callable<T> source;
        final TPCTaskType stage;
        final StagedScheduler scheduler;

        EvaluateOn(Callable<T> source, StagedScheduler scheduler, TPCTaskType stage)
        {
            this.source = source;
            this.scheduler = scheduler;
            this.stage = stage;
        }

        @Override
        public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
        {
            subscribe(subscriber, subscriptionRecipient);

            if (TPC.isOnScheduler(scheduler))
                run();
            else
                scheduler.execute(this, ExecutorLocals.create(), stage);
        }

        public void requestNext()
        {
            subscriber.onError(new AssertionError("requestNext called after onFinal"));
        }

        public void run()
        {
            try
            {
                T v = source.call();
                subscriber.onFinal(v);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
            }
        }

        public void close()
        {
        }

        public String toString()
        {
            return Flow.formatTrace("evaluateOn [" + scheduler + "] stage " + stage, source);
        }

        public TPCTaskType getStage()
        {
            return stage;
        }

        public Scheduler scheduledOn()
        {
            return scheduler;
        }
    }

    static class DeferOn<T> extends Flow<T> implements TaggedRunnable
    {
        final Callable<Flow<T>> flowSupplier;
        final TPCTaskType stage;
        final Scheduler scheduler;
        FlowSubscriber<T> subscriber;
        FlowSubscriptionRecipient subscriptionRecipient;
        Flow<T> sourceFlow;

        DeferOn(Callable<Flow<T>> source, Scheduler scheduler, TPCTaskType stage)
        {
            this.flowSupplier = source;
            this.scheduler = scheduler;
            this.stage = stage;
        }

        public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
        {
            this.subscriber = subscriber;
            this.subscriptionRecipient = subscriptionRecipient;

            if (TPC.isOnScheduler(scheduler))
                run();
            else
                scheduler.scheduleDirect(this);
        }

        public void run()
        {
            try
            {
                sourceFlow = flowSupplier.call();
            }
            catch (Throwable t)
            {
                subscriptionRecipient.onSubscribe(FlowSubscription.DONE);
                subscriber.onError(t);
                return;
            }

            sourceFlow.requestFirst(subscriber, subscriptionRecipient);
        }

        public String toString()
        {
            return Flow.formatTrace("deferOn [" + scheduler + "] stage " + stage, flowSupplier, sourceFlow);
        }

        public TPCTaskType getStage()
        {
            return stage;
        }

        public Scheduler scheduledOn()
        {
            return scheduler;
        }
    }

    /**
     * Returns a flow which represents the evaluation of the given callable on the specified core thread.
     * If execution is already on this thread, the evaluation is called directly, otherwise it is given to the scheduler
     * for async execution.
     */
    public static <T> Flow<T> evaluateOnCore(Callable<T> callable, int coreId, TPCTaskType stage)
    {
        return new EvaluateOn<T>(callable, TPC.getForCore(coreId), stage);
    }

    public static <T> Flow<T> evaluateOnIO(Callable<T> callable, TPCTaskType stage)
    {
        return new EvaluateOn<T>(callable, TPC.ioScheduler(), stage);
    }

    public static <T> Flow<T> deferOnCore(Callable<Flow<T>> source, int coreId, TPCTaskType stage)
    {
        return new DeferOn<T>(source, TPC.getForCore(coreId), stage);
    }

    public static <T> Flow<T> deferOnIO(Callable<Flow<T>> source, TPCTaskType stage)
    {
        return new DeferOn<>(source, TPC.ioScheduler(), stage);
    }

    /**
     * Op for applying any other subsequent operations/transformations on a (potentially) different scheduler.
     */
    static class SchedulingTransformer<I> extends FlowTransformNext<I, I>
    {
        final StagedScheduler scheduler;
        final TPCTaskType taskType;
        final ExecutorLocals locals = ExecutorLocals.create();

        public SchedulingTransformer(Flow<I> source, StagedScheduler scheduler, TPCTaskType taskType)
        {
            super(source);
            this.scheduler = scheduler;
            this.taskType = taskType;
        }

        @Override
        public void onNext(I next)
        {
            if (TPC.isOnScheduler(scheduler))
                subscriber.onNext(next);
            else
                scheduler.execute(() -> subscriber.onNext(next), locals, taskType);
        }


        @Override
        public void onFinal(I next)
        {
            if (TPC.isOnScheduler(scheduler))
                subscriber.onFinal(next);
            else
                scheduler.scheduleDirect(new TaggedRunnable.Base(taskType, scheduler)
                {
                    public void run()
                    {
                        subscriber.onFinal(next);
                    }
                });
        }

        public String toString()
        {
            return formatTrace(getClass().getSimpleName(), scheduler, sourceFlow);
        }
    }

    /**
     * Applies any subsequent transformations (i.e. map, reduce...) on the given scheduler
     * (similarly to RxJava's observeOn()).
     */
    public static <T> Flow<T> observeOn(Flow<T> source, StagedScheduler scheduler, TPCTaskType taskType)
    {
        return new SchedulingTransformer<>(source, scheduler, taskType);
    }
}
