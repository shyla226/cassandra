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

package org.apache.cassandra.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.reactivex.Flowable;
import io.reactivex.FlowableOperator;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.concurrent.NettyRxScheduler;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Created by blambov on 06.04.17.
 */
public class FlowableThreads
{
    static class RequestOnCore<T> implements Subscriber<T>, Subscription
    {
        final Subscriber<T> subscriber;
        final int coreId;
        final Runnable requestOne;
        Subscription source;

        RequestOnCore(Subscriber<T> subscriber, int coreId)
        {
            this.subscriber = subscriber;
            this.coreId = coreId;
            this.requestOne = () -> source.request(1);
        }

        public void onSubscribe(Subscription subscription)
        {
            this.source = subscription;
            subscriber.onSubscribe(subscription);
        }

        public void onNext(T t)
        {
            subscriber.onNext(t);
        }

        public void onError(Throwable throwable)
        {
            subscriber.onError(throwable);
        }

        public void onComplete()
        {
            subscriber.onComplete();
        }

        public void request(long l)
        {
            if (NettyRxScheduler.getCoreId() == coreId)
                source.request(l);
            else if (l == 1)
                NettyRxScheduler.getForCore(coreId).scheduleDirect(requestOne);
            else
                NettyRxScheduler.getForCore(coreId).scheduleDirect(() -> source.request(l));
        }

        public void cancel()
        {
            if (NettyRxScheduler.getCoreId() == coreId)
                source.cancel();
            else
                NettyRxScheduler.getForCore(coreId).scheduleDirect(source::cancel);

        }
    }

    final static FlowableOperator<?, ?> REQUEST_ON_CORE[] = new FlowableOperator[NettyRxScheduler.NUM_NETTY_THREADS];
    static
    {
        for (int i = 0; i < REQUEST_ON_CORE.length; ++i)
            REQUEST_ON_CORE[i] = constructRequestOnCore(i);
    }

    private static FlowableOperator<Object, Object> constructRequestOnCore(int coreId)
    {
        return subscriber -> new RequestOnCore<>(subscriber, coreId);
    }

    public static <T> FlowableOperator<T, T> requestOnCore(int coreId)
    {
        return (FlowableOperator<T, T>) REQUEST_ON_CORE[coreId];
    }

    static class RequestOn<T> implements Subscriber<T>, Subscription
    {
        final Subscriber<? super T> subscriber;
        final Scheduler scheduler;
        final Runnable requestOne;
        Subscription source;

        RequestOn(Subscriber<? super T> subscriber, Scheduler scheduler)
        {
            this.subscriber = subscriber;
            this.scheduler = scheduler;
            this.requestOne = () -> source.request(1);
        }

        public void onSubscribe(Subscription subscription)
        {
            this.source = subscription;
            subscriber.onSubscribe(this);
        }

        public void onNext(T t)
        {
            subscriber.onNext(t);
        }

        public void onError(Throwable throwable)
        {
            subscriber.onError(throwable);
        }

        public void onComplete()
        {
            subscriber.onComplete();
        }

        public void request(long l)
        {
            // TODO: If blocking is not a concern, recognizing we are already on an IO thread could boost perf.
            if (l == 1)
                scheduler.scheduleDirect(requestOne);
            else
                scheduler.scheduleDirect(() -> source.request(l));
        }

        public void cancel()
        {
           scheduler.scheduleDirect(source::cancel);
        }
    }

    public static <T> FlowableOperator<T, T> requestOn(Scheduler scheduler)
    {
        if (scheduler instanceof NettyRxScheduler)
            return requestOnCore(((NettyRxScheduler) scheduler).cpuId);
        else if (scheduler == Schedulers.io())
            return requestOnIo();
        else
            return createRequestOn(scheduler);
    }

    private static <T> FlowableOperator<T, T> createRequestOn(Scheduler scheduler)
    {
        return subscriber -> new RequestOn<T>(subscriber, scheduler);
    }

    static final FlowableOperator<?, ?> REQUEST_ON_IO = createRequestOn(Schedulers.io());

    public static <T> FlowableOperator<T, T> requestOnIo()
    {
        return (FlowableOperator<T, T>) REQUEST_ON_IO;
    }

    static class EvaluateOn<T> implements Subscription
    {
        final Subscriber<? super T> subscriber;
        final Callable<T> source;
        final int coreId;

        private volatile int requested = 0;
        private static AtomicIntegerFieldUpdater<EvaluateOn> requestedUpdater =
            AtomicIntegerFieldUpdater.newUpdater(EvaluateOn.class, "requested");

        EvaluateOn(Subscriber<? super T> subscriber, Callable<T> source, int coreId)
        {
            this.subscriber = subscriber;
            this.source = source;
            this.coreId = coreId;
        }

        public void request(long l)
        {
            if (requestedUpdater.compareAndSet(this, 0, 1))
            {
                if (NettyRxScheduler.getCoreId() == coreId)
                    evaluate();
                else
                    NettyRxScheduler.getForCore(coreId).scheduleDirect(this::evaluate);
            }
        }

        private void evaluate()
        {
            try
            {
                T v = source.call();
                subscriber.onNext(v);
                subscriber.onComplete();
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
            }
        }

        public void cancel()
        {
        }
    }

    public static <T> Flowable<T> evaluateOnCore(Callable<T> callable, int coreId)
    {
        return new Flowable<T>()
        {
            protected void subscribeActual(Subscriber<? super T> subscriber)
            {
                EvaluateOn<T> e = new EvaluateOn<T>(subscriber, callable, coreId);
                subscriber.onSubscribe(e);
            }
        };
    }

    // We may also need observeOnCore with the same semantics
}
