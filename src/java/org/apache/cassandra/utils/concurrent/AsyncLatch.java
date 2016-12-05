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
package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * An aid similar to a standard {@code CountDownLatch} but that run a provided task when the count reaches zero.
 * <p>
 * If an {@link Executor} is provided in the constructor, the task is executed using that supplied executor. Otherwise,
 * the task is run on the thread whose call to {@code countDown()} makes the count reach 0.
 * <p>
 * The {@code countDown()} method can be called even after the {@code count} has reached 0 without error, but the task
 * is guaranteed to execute at most once (at most because it may not execute if the count never reaches 0).
 * <p>
 * Similarly to {@code CountDownLatch}, the count is set at creation and cannot be reset.
 */
public class AsyncLatch
{
    private final Runnable task;
    private final Executor executor;

    private volatile long count;
    private static final AtomicLongFieldUpdater<AsyncLatch> countUpdater = AtomicLongFieldUpdater.newUpdater(AsyncLatch.class, "count");

    public AsyncLatch(long count, Runnable task)
    {
        this(count, task, null);
    }

    public AsyncLatch(long count, Runnable task, Executor executor)
    {
        this.count = count;
        this.task = task;
        this.executor = executor;
    }

    public void countDown()
    {
        if (countUpdater.decrementAndGet(this) == 0)
        {
            if (executor == null)
                task.run();
            else
                executor.execute(task);
        }
    }

    public long getCount()
    {
        return count;
    }
}

