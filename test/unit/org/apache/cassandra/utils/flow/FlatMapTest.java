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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntSupplier;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.BeforeClass;
import org.junit.Test;

import io.reactivex.CompletableObserver;
import org.apache.cassandra.utils.LineNumberInference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FlatMapTest
{
    @BeforeClass
    public static void init() throws Exception
    {
        LineNumberInference.init();
    }

    @Test
    public void testFlatMapNoFinal() throws Exception
    {
        // Test with a mix of:
        // - immediate flowables
        // - delayed response flowables
        // - immediate response on another thread flowables
        useFinal = false;

        for (int typeSeed = 1; typeSeed < 10; ++typeSeed)
        {
            Random rand = new Random(typeSeed);
            for (int seed = 3; seed < 155; seed += 8)
            {
                Flow<Integer> immediate = makeRecursive(() -> 0, seed);
                List<Integer> immValues = immediate.toList().blockingSingle();
                System.out.println("Sequence length: " + immValues.size());
                Flow<Integer> one = makeRecursive(rand::nextInt, seed);
                List<Integer> oneValues = one.toList().blockingSingle();
                assertEquals(immValues, oneValues);
            }
        }
    }

    @Test
    public void testFlatMapWithFinal() throws Exception
    {
        // Test with a mix of:
        // - immediate flowables
        // - delayed response flowables
        // - immediate response on another thread flowables
        useFinal = true;

        for (int typeSeed = 1; typeSeed < 10; ++typeSeed)
        {
            Random rand = new Random(typeSeed);
            for (int seed = 4; seed < 255; seed += 8)
            {
                Flow<Integer> immediate = makeRecursive(() -> 0, seed);
                List<Integer> immValues = immediate.toList().blockingSingle();
                System.out.println("Sequence length: " + immValues.size());
                Flow<Integer> one = makeRecursive(rand::nextInt, seed);
                List<Integer> oneValues = one.toList().blockingSingle();
                assertEquals(immValues, oneValues);
            }
        }
    }

    static boolean useFinal;

    // TODO:
    // - onError thrown instead of onNext

    Flow<Integer> makeRecursive(IntSupplier typeRand, int seed)
    {
        Flow<Integer> fl = make(typeRand, seed);
        switch (seed % 8)
        {
            case 3:
                return fl.flatMap(i -> makeRecursive(typeRand, i));
            default:
                return fl;
        }
    }

    Flow<? extends Object> makeTest(IntSupplier typeRand, int seed)
    {
        Flow<Integer> fl = make(typeRand, seed);
        switch (seed % 8)
        {
            case 3:
                return fl.flatMap(i -> makeTest(typeRand, i).toList());
            default:
                return fl;
        }
    }

    @Test
    public void testFlatMapCompletable() throws Exception
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
                List<Integer> immValues = makeListWithCompletable(() -> 0, seed);
                System.out.println("Sequence length: " + immValues.size());
                List<Integer> oneValues = makeListWithCompletable(rand::nextInt, seed);
                assertEquals(immValues, oneValues);
            }
        }
    }

    List<Integer> makeListWithCompletable(IntSupplier typeRand, int seed)
    {
        List<Integer> ret = new ArrayList<>();
        Flow<Integer> fl = make(typeRand, seed);
        fl.flatMapCompletable(i -> { ret.add(i); return CompletableObserver::onComplete; })
          .blockingAwait();
        return ret;
    }

    Flow<Integer> make(IntSupplier typeRand, int seed)
    {
        switch (typeRand.getAsInt() % 11)
        {
            case 2:
            case 3:
                return new SpinningGenerator(seed);
            case 4:
                return new YieldingGenerator(seed);
            case 5:
                return new ParkingGenerator(seed);
            case 6:
                return new SleepingGenerator(seed);
            default:
                return new ImmediateGenerator(seed);
        }
    }

    static class ImmediateGenerator extends FlowSource<Integer>
    {
        final Random rand;
        volatile boolean closed = false;
        volatile boolean done = false;

        ImmediateGenerator(int seed)
        {
            this.rand = new Random(seed);
        }

        public void requestNext()
        {
            switch (rand.nextInt(35))
            {
                case 3:
                    if (useFinal)
                    {
                        perform(this::nextAndComplete);
                        break;
                    } // else fall through
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
            subscriber.onNext(rand.nextInt());
        }

        void nextAndComplete()
        {
            subscriber.onFinal(rand.nextInt());
        }

        void complete()
        {
            subscriber.onComplete();
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

        public String toString()
        {
            return Flow.formatTrace(getClass().getSimpleName());
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

        SpinningGenerator(int seed)
        {
            super(seed);
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

        YieldingGenerator(int seed)
        {
            super(seed);
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

        ParkingGenerator(int seed)
        {
            super(seed);
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

        SleepingGenerator(int seed)
        {
            super(seed);
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

    @Test
    public void testFilterOpDoesntOverflowStack() throws Exception
    {
        final int size = 100000;
        final long result = Flow.fromIterable(() -> IntStream.range(0, size).iterator())
                                .flatMap(x -> Flow.<Integer>empty())
                                .countBlocking();

        assertEquals(0L, result);
    }
}
