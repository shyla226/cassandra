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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.Test;

import io.reactivex.Completable;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class ConcatTest
{
    @Test
    public void testConcatFlows() throws Exception
    {
        Flow<Integer> flow1 = Flow.fromIterable(() -> IntStream.range(0, 10).iterator());
        Flow<Integer> flow2 = Flow.fromIterable(() -> IntStream.range(10, 20).iterator());
        Flow<Integer> flow3 = Flow.fromIterable(() -> IntStream.range(20, 30).iterator());

        long res = Flow.concat(flow1, flow2, flow3).countBlocking();
        assertEquals(30, res);

        class MoreContents implements Supplier<Flow<Integer>>
        {
            List<Flow<Integer>> flows = new ArrayList<>(Arrays.asList(flow2, flow3));

            public Flow<Integer> get()
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
        AtomicBoolean completed = new AtomicBoolean(false);
        Flow<Integer> flow1 = Flow.fromIterable(() -> IntStream.range(0, 10).iterator());
        Completable completable = Completable.fromRunnable(() -> assertTrue(completed.compareAndSet(false, true)));

        long res = Flow.concat(completable, flow1).countBlocking();
        assertEquals(10, res);
        assertTrue(completed.get());

        completed.set(false);
        res = Flow.concat(flow1, completable).countBlocking();
        assertEquals(10, res);
        assertTrue(completed.get());
    }
}
