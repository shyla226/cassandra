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

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntSupplier;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import io.reactivex.Flowable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import static org.junit.Assert.assertEquals;

public class FlowableConcatLazyTest
{
    @Test
    public void testConcatLazy()
    {
        // Test with a mix of:
        // - immediate flowables
        // - delayed response flowables
        // - immediate response on another thread flowables

        // - onComplete response separated
        // - onComplete response with last onNext

        for (int typeSeed = 1; typeSeed < 10; ++typeSeed)
        {
            Random rand = new Random(typeSeed);
            for (int seed = 3; seed < 155; seed += 8)
            {
                Flowable<Integer> immediate = makeRecursive(() -> 0, seed);
                List<Integer> immValues = immediate.toList().blockingGet();
                System.out.println("Sequence length: " + immValues.size());
                Flowable<Integer> one = makeRecursive(rand::nextInt, seed);
                List<Integer> oneValues = one.toList().blockingGet();
                assertEquals(immValues, oneValues);
            }
        }
    }

    // TODO:
    // - onError thrown instead of onNext
    // - onError thrown after onNext

    // - onSubscribe delayed

    // - cancel call at random point from third thread

    Flowable<Integer> makeRecursive(IntSupplier typeRand, int seed)
    {
        Flowable<Integer> fl = make(typeRand, seed);
        switch (seed % 8)
        {
            case 3:
                return fl.lift(FlowableUtils.concatMapLazy(i -> makeRecursive(typeRand, i)));
            default:
                return fl;
        }
    }

    Flowable<? extends Object> makeTest(IntSupplier typeRand, int seed)
    {
        Flowable<Integer> fl = make(typeRand, seed);
        switch (seed % 8)
        {
            case 3:
                return fl.concatMap(i -> makeTest(typeRand, i).toList().toFlowable());
            default:
                return fl;
        }
    }

    Flowable<Integer> make(IntSupplier typeRand, int seed)
    {
        switch (typeRand.getAsInt() % 11)
        {
            case 2:
            case 3:
                return new Flowable<Integer>()
                {
                    protected void subscribeActual(Subscriber<? super Integer> subscriber)
                    {
                        subscriber.onSubscribe(new SpinningGenerator(subscriber, seed));
                    }
                };
            case 4:
                return new Flowable<Integer>()
                {
                    protected void subscribeActual(Subscriber<? super Integer> subscriber)
                    {
                        subscriber.onSubscribe(new YieldingGenerator(subscriber, seed));
                    }
                };
            case 5:
                return new Flowable<Integer>()
                {
                    protected void subscribeActual(Subscriber<? super Integer> subscriber)
                    {
                        subscriber.onSubscribe(new ParkingGenerator(subscriber, seed));
                    }
                };
            case 6:
                return new Flowable<Integer>()
                {
                    protected void subscribeActual(Subscriber<? super Integer> subscriber)
                    {
                        subscriber.onSubscribe(new SleepingGenerator(subscriber, seed));
                    }
                };
            default:
                return new Flowable<Integer>()
                {
                    protected void subscribeActual(Subscriber<? super Integer> subscriber)
                    {
                        subscriber.onSubscribe(new ImmediateGenerator(subscriber, seed));
                    }
                };
        }
    }

    static class ImmediateGenerator implements Subscription
    {
        final Subscriber<? super Integer> sub;
        final Random rand;
        volatile boolean cancelled = false;
        volatile boolean done = false;

        ImmediateGenerator(Subscriber<? super Integer> sub, int seed)
        {
            this.sub = sub;
            this.rand = new Random(seed);
        }

        public void request(long l)
        {
            for (long i = 0; !done && !cancelled && i < l; ++i)
            {
                switch (rand.nextInt(35))
                {
                    case 3:
                        perform(this::complete);
                        break;
                    case 4:
                        perform(this::nextAndComplete);
                        break;
                    default:
                        perform(this::next);
                        break;
                }
            }
        }

        void next()
        {
            sub.onNext(rand.nextInt());
        }

        void complete()
        {
            sub.onComplete();
            done = true;
        }

        void nextAndComplete()
        {
            next();
            complete();
        }

        // overridden in children
        void perform(Runnable call)
        {
            call.run();
        }

        public void cancel()
        {
            cancelled = true;
        }
    }

    static class SpinningGenerator extends ImmediateGenerator
    {
        final Thread spinner;
        volatile Runnable call = null;
        AtomicInteger iteration = new AtomicInteger(0);

        SpinningGenerator(Subscriber<? super Integer> sub, int seed)
        {
            super(sub, seed);
            spinner = new Thread()
            {
                public void run()
                {
                    while (!cancelled && !done)
                    {
                        if (call != null)
                        {
                            Runnable c = call;
                            call = null;
                            c.run();
                        }
                        else
                            iteration.getAndIncrement(); // to be replaced with Thread.onSpinWait() with Java 9
                    }
                }
            };
            spinner.start();
        }

        void perform(Runnable call)
        {
            while (this.call != null && !cancelled && !done)
                iteration.getAndIncrement(); // to be replaced with Thread.onSpinWait() with Java 9
            this.call = call;
        }
    }

    static class YieldingGenerator extends ImmediateGenerator
    {
        final Thread spinner;
        volatile Runnable call = null;

        YieldingGenerator(Subscriber<? super Integer> sub, int seed)
        {
            super(sub, seed);
            spinner = new Thread()
            {
                public void run()
                {
                    while (!cancelled && !done)
                    {
                        if (call != null)
                        {
                            Runnable c = call;
                            call = null;
                            c.run();
                        }
                        else
                            Thread.yield();
                    }
                }
            };
            spinner.start();
        }

        void perform(Runnable call)
        {
            while (this.call != null && !cancelled && !done)
                Thread.yield();
            this.call = call;
        }
    }

    static class ParkingGenerator extends ImmediateGenerator
    {
        final Thread spinner;
        volatile Runnable call = null;

        ParkingGenerator(Subscriber<? super Integer> sub, int seed)
        {
            super(sub, seed);
            spinner = new Thread()
            {
                public void run()
                {
                    while (!cancelled && !done)
                    {
                        if (call != null)
                        {
                            Runnable c = call;
                            call = null;
                            c.run();
                        }
                        else
                            LockSupport.park();
                    }
                }
            };
            spinner.start();
        }

        void perform(Runnable call)
        {
            while (this.call != null && !cancelled && !done)
                Thread.yield();
            this.call = call;
            LockSupport.unpark(spinner);
        }
    }

    static class SleepingGenerator extends ImmediateGenerator
    {
        final Thread spinner;
        volatile Runnable call = null;

        SleepingGenerator(Subscriber<? super Integer> sub, int seed)
        {
            super(sub, seed);
            spinner = new Thread()
            {
                public void run()
                {
                    while (!cancelled && !done)
                    {
                        if (call != null)
                        {
                            Runnable c = call;
                            call = null;
                            c.run();
                        }
                        else
                            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
                    }
                }
            };
            spinner.start();
        }

        void perform(Runnable call)
        {
            while (this.call != null && !cancelled && !done)
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
            this.call = call;
        }
    }

}
