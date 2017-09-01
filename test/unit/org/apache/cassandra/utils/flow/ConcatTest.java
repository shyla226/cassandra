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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Test;

import io.reactivex.Completable;
import io.reactivex.functions.BiFunction;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class ConcatTest
{
    @Test
    public void testConcatFlows() throws Exception
    {
        testConcatFlows(this::range);
        testConcatFlows(this::rangeNoFinal);
    }

    public void testConcatFlows(BiFunction<Integer, Integer, Flow<Integer>> gen) throws Exception
    {
        Flow<Integer> flow1 = gen.apply(0, 10);
        Flow<Integer> flow2 = gen.apply(10, 20);
        Flow<Integer> flow3 = gen.apply(20, 30);

        long res = Flow.concat(flow1, flow2, flow3).countBlocking();
        assertEquals(30, res);

        // Reset flows.
        flow1 = gen.apply(0, 10);
        Flow<Integer> flow4 = gen.apply(10, 20);
        Flow<Integer> flow5 = gen.apply(20, 30);

        class MoreContents implements Callable<Flow<Integer>>
        {
            List<Flow<Integer>> flows = new ArrayList<>(Arrays.asList(flow4, flow5));

            public Flow<Integer> call()
            {
                return flows.isEmpty() ? null : flows.remove(0);
            }
        }

        res = flow1.concatWith(new MoreContents()).countBlocking();
        assertEquals(30, res);
    }

    @Test
    public void testConcatWithCompletables() throws Exception
    {
        testConcatWithCompletables(this::range);
        testConcatWithCompletables(this::rangeNoFinal);
    }

    public void testConcatWithCompletables(BiFunction<Integer, Integer, Flow<Integer>> gen) throws Exception
    {
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicInteger value = new AtomicInteger(-1);
        AtomicInteger completablePoint = new AtomicInteger(-1);
        Flow<Integer> flow1 = gen.apply(0, 10);
        Completable completable = Completable.fromRunnable(() ->
                                                           {
                                                               completablePoint.set(value.get());
                                                               assertTrue(completed.compareAndSet(false, true));
                                                           });

        long res = Flow.concat(completable, flow1)
                       .map(v ->
                            {
                                value.set(v);
                                return v;
                            })
                       .countBlocking();
        assertEquals(10, res);
        assertTrue(completed.get());
        assertEquals(9, value.get());
        assertEquals(-1, completablePoint.get());
        value.set(-1);
        completablePoint.set(-1);

        completed.set(false);
        flow1 = gen.apply(0, 10);
        res = Flow.concat(flow1, completable)
                  .map(v ->
                       {
                           value.set(v);
                           return v;
                       })
                  .countBlocking();
        assertEquals(10, res);
        assertTrue(completed.get());
        assertEquals(9, value.get());
        assertEquals(9, completablePoint.get());
    }

    Flow<Integer> range(int start, int end)
    {
        return new FlowSource()
        {
            int pos = start;

            public void requestNext()
            {
                if (pos + 1 < end)
                    subscriber.onNext(pos++);
                else
                    subscriber.onFinal(pos);
            }

            public void close() throws Exception
            {
            }
        };
    }

    Flow<Integer> rangeNoFinal(int start, int end)
    {
        return Flow.fromIterable(() -> IntStream.range(start, end).iterator());
    }
}
