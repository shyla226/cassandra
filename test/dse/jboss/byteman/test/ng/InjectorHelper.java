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

package jboss.byteman.test.ng;


import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;

public class InjectorHelper extends Helper
{
    private static final ConcurrentMap<String, String> enabledRules = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, AtomicInteger> counters = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, CyclicBarrier> barriers = new ConcurrentHashMap<>();

    protected InjectorHelper(Rule rule)
    {
        super(rule);
    }

    public static void setEnabled(final String id, final boolean enabled)
    {
        if (enabled)
        {
            enabledRules.put(id, id);
        }
        else
        {
            enabledRules.remove(id);
        }
    }

    public static Integer readCounter(String name)
    {
        AtomicInteger counter = counters.get(name);
        return counter != null ? counter.get() : null;
    }

    public static void resetCounter(String name)
    {
        counters.remove(name);
    }

    public static void addBarrier(String name, int parties)
    {
        barriers.put(name, new CyclicBarrier(parties));
    }

    public static void awaitBarrier(String name) throws InterruptedException, BrokenBarrierException
    {
        barriers.get(name).await();
    }

    public static void resetBarrier(String name)
    {
        CyclicBarrier barrier = barriers.get(name);
        if (barriers.remove(name, barrier))
        {
            barrier.reset();
        }
    }

    public boolean isEnabled(String id)
    {
        return enabledRules.containsKey(id);
    }

    public void storeAndIncrementCounter(String name)
    {
        AtomicInteger newCounter = new AtomicInteger(0);
        AtomicInteger storedCounter = counters.putIfAbsent(name, newCounter);
        storedCounter = storedCounter == null ? newCounter : storedCounter;
        storedCounter.incrementAndGet();
    }

    public void awaitBarrierInternal(String name) throws InterruptedException
    {
        try
        {
            barriers.get(name).await();
        }
        catch (BrokenBarrierException ignored)
        {
            // We need to be lenient and ignore the exception here, as this exception is thrown during barrier removal.
        }
    }
}
