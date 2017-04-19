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

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntSupplier;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FlatMapTest
{
    @Test
    public void testFlatMap() throws Exception
    {
        // Test with a mix of:
        // - immediate flowables
        // - delayed response flowables
        // - immediate response on another thread flowables

        for (int typeSeed = 1; typeSeed < 10; ++typeSeed)
        {
            Random rand = new Random(typeSeed);
            for (int seed = 3; seed < 155; seed += 8)
            {
                CsFlow<Integer> immediate = makeRecursive(() -> 0, seed);
                List<Integer> immValues = immediate.toList().blockingSingle();
                System.out.println("Sequence length: " + immValues.size());
                CsFlow<Integer> one = makeRecursive(rand::nextInt, seed);
                List<Integer> oneValues = one.toList().blockingSingle();
                assertEquals(immValues, oneValues);
            }
        }
    }

    // TODO:
    // - onError thrown instead of onNext

    CsFlow<Integer> makeRecursive(IntSupplier typeRand, int seed)
    {
        CsFlow<Integer> fl = make(typeRand, seed);
        switch (seed % 8)
        {
            case 3:
                return fl.flatMap(i -> makeRecursive(typeRand, i));
            default:
                return fl;
        }
    }

    CsFlow<? extends Object> makeTest(IntSupplier typeRand, int seed)
    {
        CsFlow<Integer> fl = make(typeRand, seed);
        switch (seed % 8)
        {
            case 3:
                return fl.flatMap(i -> makeTest(typeRand, i).toList());
            default:
                return fl;
        }
    }

    CsFlow<Integer> make(IntSupplier typeRand, int seed)
    {
        switch (typeRand.getAsInt() % 11)
        {
            case 2:
            case 3:
                return new CsFlow<Integer>()
                {
                    public CsSubscription subscribe(CsSubscriber<Integer> subscriber)
                    {
                        return new SpinningGenerator(subscriber, seed);
                    }
                };
            case 4:
                return new CsFlow<Integer>()
                {
                    public CsSubscription subscribe(CsSubscriber<Integer> subscriber)
                    {
                        return (new YieldingGenerator(subscriber, seed));
                    }
                };
            case 5:
                return new CsFlow<Integer>()
                {
                    public CsSubscription subscribe(CsSubscriber<Integer> subscriber)
                    {
                        return (new ParkingGenerator(subscriber, seed));
                    }
                };
            case 6:
                return new CsFlow<Integer>()
                {
                    public CsSubscription subscribe(CsSubscriber<Integer> subscriber)
                    {
                        return (new SleepingGenerator(subscriber, seed));
                    }
                };
            default:
                return new CsFlow<Integer>()
                {
                    public CsSubscription subscribe(CsSubscriber<Integer> subscriber)
                    {
                        return (new ImmediateGenerator(subscriber, seed));
                    }
                };
        }
    }

    static class ImmediateGenerator implements CsSubscription
    {
        final CsSubscriber<Integer> sub;
        final Random rand;
        volatile boolean closed = false;
        volatile boolean done = false;

        ImmediateGenerator(CsSubscriber<Integer> sub, int seed)
        {
            this.sub = sub;
            this.rand = new Random(seed);
        }

        public void request()
        {
            switch (rand.nextInt(35))
            {
                case 3:
                case 4:
                    perform(this::complete);
                    break;
                default:
                    perform(this::next);
                    break;
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

        // overridden in children
        void perform(Runnable call)
        {
            call.run();
        }

        public void close()
        {
            closed = true;
        }

        public void finalize()
        {
            if (!closed)
                System.err.println("Unclosed flow " + toString());
        }
    }

    static class SpinningGenerator extends ImmediateGenerator
    {
        final Thread spinner;
        volatile Runnable call = null;
        AtomicInteger iteration = new AtomicInteger(0);

        SpinningGenerator(CsSubscriber<Integer> sub, int seed)
        {
            super(sub, seed);
            spinner = new Thread()
            {
                public void run()
                {
                    while (!closed && !done)
                    {
                        while (call != null)
                        {
                            Runnable c = call;
                            call = null;
                            c.run();
                        }
                        iteration.getAndIncrement(); // to be replaced with Thread.onSpinWait() with Java 9
                    }
                }
            };
            spinner.start();
        }

        void perform(Runnable call)
        {
            while (this.call != null && !closed && !done)
                iteration.getAndIncrement(); // to be replaced with Thread.onSpinWait() with Java 9
            this.call = call;
        }
    }

    static class YieldingGenerator extends ImmediateGenerator
    {
        final Thread spinner;
        volatile Runnable call = null;

        YieldingGenerator(CsSubscriber<Integer> sub, int seed)
        {
            super(sub, seed);
            spinner = new Thread()
            {
                public void run()
                {
                    while (!closed && !done)
                    {
                        while (call != null)
                        {
                            Runnable c = call;
                            call = null;
                            c.run();
                        }
                        Thread.yield();
                    }
                }
            };
            spinner.start();
        }

        void perform(Runnable call)
        {
            while (this.call != null && !closed && !done)
                Thread.yield();
            this.call = call;
        }
    }

    static class ParkingGenerator extends ImmediateGenerator
    {
        final Thread spinner;
        volatile Runnable call = null;

        ParkingGenerator(CsSubscriber<Integer> sub, int seed)
        {
            super(sub, seed);
            spinner = new Thread()
            {
                public void run()
                {
                    while (!closed && !done)
                    {
                        while (call != null)
                        {
                            Runnable c = call;
                            call = null;
                            c.run();
                        }
                        LockSupport.park();
                    }
                }
            };
            spinner.start();
        }

        void perform(Runnable call)
        {
            while (this.call != null && !closed && !done)
                Thread.yield();
            this.call = call;
            LockSupport.unpark(spinner);
        }
    }

    static class SleepingGenerator extends ImmediateGenerator
    {
        final Thread spinner;
        volatile Runnable call = null;

        SleepingGenerator(CsSubscriber<Integer> sub, int seed)
        {
            super(sub, seed);
            spinner = new Thread()
            {
                public void run()
                {
                    while (!closed && !done)
                    {
                        while (call != null)
                        {
                            Runnable c = call;
                            call = null;
                            c.run();
                        }
                        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
                    }
                }
            };
            spinner.start();
        }

        void perform(Runnable call)
        {
            while (this.call != null && !closed && !done)
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
            this.call = call;
        }
    }

}
