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

package org.apache.cassandra.metrics;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An counter metric that is composed exclusively by child counters,
 * so that the count returned is the sum of the child counters.
 */
class CompositeCounter extends Counter
{
    private final CopyOnWriteArrayList<Counter> childCounters;

    CompositeCounter()
    {
        this.childCounters = new CopyOnWriteArrayList<>();
    }

    @Override
    public void inc(long n)
    {
        throw new UnsupportedOperationException("Composite counters are read-only");
    }

    @Override
    public void dec(long n)
    {
        throw new UnsupportedOperationException("Composite counters are read-only");
    }

    @Override
    public long getCount()
    {
        return childCounters.stream().map(Counter::getCount).reduce(0L, Long::sum);
    }

    @Override
    public Type getType()
    {
        return Type.COMPOSITE;
    }

    @Override
    public void compose(Counter metric)
    {
        childCounters.add(metric);
    }
}
