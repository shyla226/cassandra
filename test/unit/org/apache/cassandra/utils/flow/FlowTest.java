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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Ordering;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.reactivex.functions.Function;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.LineNumberInference;
import org.apache.cassandra.utils.Reducer;

import static org.junit.Assert.fail;

public class FlowTest
{
    @BeforeClass
    public static void init() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
    }

    Function<Integer, Integer> inc = (i) -> i + 1;
    Function<Integer, Integer> multiplyByTwo = (i) -> i * 2;
    Function<Integer, Integer> multiplyByThree = (i) -> i * 3;
    Function<Integer, Integer> divideByZero = (i) -> i / 0;
    Flow.ReduceFunction<Integer, Integer> reduceToSum = (l, r) -> l + r;

    @Test
    public void reduceBlockingErrorHandlingTest() throws Exception
    {
        try
        {
            Flow.fromIterable(Arrays.asList(1, 2, 3, 4, 5))
                .map(inc)
                .map(multiplyByTwo)
                .map(divideByZero)
                .reduceBlocking(0, reduceToSum);
            fail("Failing operation should have resulted into the topology failure");
        }
        catch (Exception e)
        {
            assertStacktraceMessage(e.getSuppressed()[0].getMessage(), new Object[]{ inc, multiplyByTwo, divideByZero, reduceToSum });
        }
    }

    @Test
    public void reduceFutureErrorHandlingTest() throws Exception
    {
        try
        {
            Flow.fromIterable(Arrays.asList(1, 2, 3, 4, 5))
                .map(inc)
                .map(multiplyByTwo)
                .map(divideByZero)
                .reduceToFuture(0, reduceToSum)
                .get();
            fail("Failing operation should have resulted into the topology failure");
        }
        catch (Exception e)
        {
            assertStacktraceMessage(e.getCause().getSuppressed()[0].getMessage(),
                                    new Object[]{ inc, multiplyByTwo, divideByZero, reduceToSum });
        }
    }

    @Test
    public void flatMapErrorHandlingTest() throws Exception
    {
        // failure happening before flatmap
        try
        {
            Flow.fromIterable(range(5))
                .map(inc)
                .map(divideByZero)
                .flatMap((i) -> Flow.fromIterable(range(i)))
                .map(multiplyByTwo)
                .reduceBlocking(0, reduceToSum);
            fail("Failing operation should have resulted into the topology failure");
        }
        catch (Exception e)
        {
            assertStacktraceMessage(e.getSuppressed()[0].getMessage(), new Object[]{ inc, divideByZero, multiplyByTwo, reduceToSum });
        }

        // failure happening after flatmap
        try
        {
            Flow.fromIterable(range(5))
                .map(inc)
                .map(multiplyByTwo)
                .flatMap((i) -> Flow.fromIterable(range(i)))
                .map(divideByZero)
                .reduceBlocking(0, reduceToSum);
            fail("Failing operation should have resulted into the topology failure");
        }
        catch (Exception e)
        {
            assertStacktraceMessage(e.getSuppressed()[0].getMessage(), new Object[]{ inc, divideByZero, multiplyByTwo, reduceToSum });
        }

        // failure happening in flatmap operation itself
        try
        {
            Flow.fromIterable(range(5))
                .map(inc)
                .map(multiplyByTwo)
                .flatMap((i) -> {
                      if (true)
                          throw new RuntimeException();
                      return Flow.fromIterable(range(i));
                  })
                .map(divideByZero)
                .reduceBlocking(0, reduceToSum);
            fail("Failing operation should have resulted into the topology failure");
        }
        catch (Exception e)
        {
            assertStacktraceMessage(e.getSuppressed()[0].getMessage(), new Object[]{ inc, divideByZero, multiplyByTwo, reduceToSum });
        }
    }

    @Test
    public void groupErrorHandlingTest() throws Exception
    {
        GroupOp<Integer, Integer> failingGroupOp = new GroupOp<Integer, Integer>()
        {
            public boolean inSameGroup(Integer l, Integer r)
            {
                int a = r / 0; // Must fail
                return r < 6;
            }

            public Integer map(List<Integer> inputs)
            {
                return inputs.size();
            }
        };

        GroupOp<Integer, Integer> groupOp = new GroupOp<Integer, Integer>()
        {
            public boolean inSameGroup(Integer l, Integer r)
            {
                int a = r / 0; // Must fail
                return r < 6;
            }

            public Integer map(List<Integer> inputs)
            {
                return inputs.size();
            }
        };

        try
        {
            Flow.fromIterable(range(5))
                .map(inc)
                .group(failingGroupOp)
                .map(multiplyByTwo)
                .reduceBlocking(1, reduceToSum);
            fail("Failing operation should have resulted into the topology failure");
        }
        catch (Exception e)
        {
            assertStacktraceMessage(e.getSuppressed()[0].getMessage(), new Object[]{ inc, multiplyByTwo, reduceToSum, failingGroupOp });
        }

        try
        {
            Flow.fromIterable(range(5))
                .map(inc)
                .map(divideByZero)
                .group(failingGroupOp)
                .map(multiplyByTwo)
                .reduceBlocking(1, reduceToSum);
            fail("Failing operation should have resulted into the topology failure");
        }
        catch (Exception e)
        {
            assertStacktraceMessage(e.getSuppressed()[0].getMessage(), new Object[]{ inc, multiplyByTwo, reduceToSum, failingGroupOp, divideByZero });
        }

        try
        {
            Flow.fromIterable(range(5))
                .map(inc)
                .group(groupOp)
                .map(divideByZero)
                .map(multiplyByTwo)
                .reduceBlocking(1, reduceToSum);
            fail("Failing operation should have resulted into the topology failure");
        }
        catch (Exception e)
        {
            assertStacktraceMessage(e.getSuppressed()[0].getMessage(), new Object[]{ inc, multiplyByTwo, reduceToSum, groupOp, divideByZero });
        }
    }

    @Test
    public void mergeErrorHandlingTest() throws Exception
    {
        Reducer<Integer, Integer> reducer = new Reducer<Integer, Integer>()
        {
            Integer last = -1;
            public void reduce(int idx, Integer current)
            {
                last = current;
            }

            public Integer getReduced()
            {
                return last;
            }
        };

        try
        {
            Flow.merge(Arrays.asList(Flow.fromIterable(range(5))
                                         .map((i) -> i),
                                     Flow.fromIterable(range(5, 10))
                                         .map(multiplyByTwo)
                                         .map(divideByZero)),
                       Ordering.natural(),
                       reducer)
                .map(inc)
                .map(multiplyByThree)
                .reduceBlocking(0, reduceToSum);
            fail("Failing operation should have resulted into the topology failure");
        }
        catch (Exception e)
        {
            assertStacktraceMessage(e.getSuppressed()[0].getMessage(), new Object[]{ multiplyByTwo, divideByZero });
            assertStacktraceMessage(e.getSuppressed()[1].getMessage(), new Object[]{ inc, multiplyByThree, reduceToSum });
        }

        try
        {
            Flow.merge(Arrays.asList(Flow.fromIterable(range(5))
                                         .map((i) -> i),
                                     Flow.fromIterable(range(5, 10))
                                         .map(multiplyByTwo)
                                         .map(multiplyByThree)),
                       Ordering.natural(),
                       reducer)
                .map(inc)
                .map(divideByZero)
                .reduceBlocking(0, reduceToSum);
            fail("Failing operation should have resulted into the topology failure");
        }
        catch (Exception e)
        {
            assertStacktraceMessage(e.getSuppressed()[0].getMessage(), new Object[]{ inc, divideByZero, reduceToSum });
        }
    }

    @Test
    public void observeOnTest() throws Exception
    {
        AtomicReference<String> currentThread = new AtomicReference<>();
        AtomicReference<String> transformedThread1 = new AtomicReference<>();
        AtomicReference<String> transformedThread2 = new AtomicReference<>();

        Flow<String> flow;
        flow = Flow.fromIterable(Arrays.asList("ignored"));
        flow = flow.map(ignored ->
                        {
                            currentThread.set(Thread.currentThread().getName());
                            return ignored;
                        });

        flow = Threads.observeOn(flow, TPC.ioScheduler(), TPCTaskType.UNKNOWN);
        flow = flow.map(ignored ->
                        {
                            transformedThread1.set(Thread.currentThread().getName());
                            return ignored;
                        });

        flow = Threads.observeOn(flow, TPC.bestTPCScheduler(), TPCTaskType.UNKNOWN);

        flow.reduceBlocking("ignored", (i, o) ->
        {
            transformedThread2.set(Thread.currentThread().getName());
            return o;
        });

        Assert.assertNotEquals(currentThread.get(), transformedThread1.get());
        Assert.assertNotEquals(transformedThread1.get(), transformedThread2.get());
    }

    static void assertStacktraceMessage(String msg, Object[] tags)
    {
        for (Object tag : tags)
        {
            Assert.assertTrue(msg.contains(tag.toString()));
            LineNumberInference.Descriptor line = Flow.LINE_NUMBERS.getLine(tag.getClass());
            Assert.assertNotSame("Expected to have a defined source", line, LineNumberInference.UNKNOWN_SOURCE);
            Assert.assertTrue(msg.contains(line.source() + ":" + line.line()));
        }
    }

    static Iterable<Integer> range(final int max)
    {
        return range(0, max);
    }

    static Iterable<Integer> range(final int min, final int max)
    {
        return new Iterable<Integer>()
        {
            public Iterator<Integer> iterator()
            {
                return new Iterator<Integer> ()
                {
                    int current = min;
                    public boolean hasNext()
                    {
                        return current <= max;
                    }

                    public Integer next()
                    {
                        return current++;
                    }
                };
            }
        };
    }
}
