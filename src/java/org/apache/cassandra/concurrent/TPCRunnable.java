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

package org.apache.cassandra.concurrent;

import org.apache.cassandra.utils.flow.TaggedRunnable;

/**
 * Runnable wrapping TPC scheduled tasks.
 * Takes care of setting up local context (ExecutorLocals) and keeping track of scheduled and completed tasks for
 * metrics.
 */
public class TPCRunnable implements Runnable
{
    private final Runnable runnable;
    private final ExecutorLocals locals;
    private final TPCTaskType stage;
    private final int scheduledOn;

    public TPCRunnable(TaggedRunnable runnable)
    {
        this(runnable, ExecutorLocals.create(), runnable.getStage(), TaggedRunnable.scheduledOnCore(runnable));
    }

    public TPCRunnable(Runnable runnable, ExecutorLocals locals, TPCTaskType stage, int scheduledOn)
    {
        this.runnable = runnable;
        this.locals = locals;
        this.stage = stage;
        this.scheduledOn = scheduledOn;

        TPC.metrics(scheduledOn).scheduled(stage);
    }

    public void run()
    {
        TPC.metrics().starting(stage);

        ExecutorLocals.set(locals);

        try
        {
            // We don't expect this to throw, but we can't let the completed count miss any tasks because it has errored
            // out.
            runnable.run();
        }
        catch (Throwable t)
        {
            TPC.metrics().failed(stage, t);
            throw t;
        }
        finally
        {
            TPC.metrics().completed(stage);
        }
    }

    public void cancelled()
    {
        TPC.metrics(scheduledOn).cancelled(stage);
    }

    public void setPending()
    {
        TPC.metrics(scheduledOn).pending(stage, +1);
    }

    public void unsetPending()
    {
        TPC.metrics(scheduledOn).pending(stage, -1);
    }

    public boolean isPendable()
    {
        return stage.pendable;
    }

    public void blocked()
    {
        TPC.metrics(scheduledOn).blocked(stage);
    }

    public static TPCRunnable wrap(Runnable runnable)
    {
        return wrap(runnable, ExecutorLocals.create(), TPCTaskType.UNKNOWN, TPC.getNumCores());
    }

    public static TPCRunnable wrap(Runnable runnable, int defaultCore)
    {
        return wrap(runnable, ExecutorLocals.create(), TPCTaskType.UNKNOWN, defaultCore);
    }

    public static TPCRunnable wrap(Runnable runnable, TPCTaskType defaultStage, int defaultCore)
    {
        return wrap(runnable, ExecutorLocals.create(), defaultStage, defaultCore);
    }

    public static TPCRunnable wrap(Runnable runnable,
                                   ExecutorLocals locals,
                                   TPCTaskType defaultStage,
                                   StagedScheduler scheduler)
    {
        return wrap(runnable, locals, defaultStage, scheduler.metricsCoreId());
    }

    public static TPCRunnable wrap(Runnable runnable,
                                   ExecutorLocals locals,
                                   TPCTaskType defaultStage,
                                   int defaultCore)
    {
        if (runnable instanceof TPCRunnable)
            return (TPCRunnable) runnable;
        if (runnable instanceof TaggedRunnable)
            return new TPCRunnable((TaggedRunnable) runnable);

        return new TPCRunnable(runnable, locals, defaultStage, defaultCore);
    }
}
