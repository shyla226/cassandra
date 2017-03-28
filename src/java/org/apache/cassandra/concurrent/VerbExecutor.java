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

import javax.annotation.Nullable;

import io.reactivex.Scheduler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.tracing.TraceState;

/**
 * A thin wrapper around a {@link Stage} and a {@link io.reactivex.Scheduler} for executing
 * a verb request.
 *
 * When present, the rx scheduler is used, otherwise the stage is used. Eventually, stages will
 * be eliminated.
 */
public class VerbExecutor
{
    private final Stage stage;

    @Nullable
    private final Scheduler scheduler;

    public VerbExecutor(Stage stage)
    {
        this(stage, null);
    }

    private VerbExecutor(Stage stage, Scheduler scheduler)
    {
        this.stage = stage;
        this.scheduler = scheduler;
    }

    /**
     * Execute the specified runnable with the specified tracing state. This is typically called
     * when a message is received from a remote host.
     *
     * @param runnable - the runnable to execute, normally the verb handler with some wrapping
     * @param state - the tracing state received from a remote host
     */
    public void execute(Runnable runnable, TraceState state)
    {
        final ExecutorLocals locals = ExecutorLocals.create(state);
        if (scheduler != null)
            scheduler.scheduleDirect(new ExecutorLocals.WrappedRunnable(runnable, locals));
        else
            StageManager.getStage(stage).execute(runnable, locals);
    }


    /**
     * Execute the specified runnable, possibly immediately. This is typically called when a message has to be
     * delivered locally.
     *
     * @param runnable - the runnable to execute, normally the verb handler with some wrapping
     */
    public void maybeExecuteImmediately(Runnable runnable)
    {
        if (scheduler != null)
            scheduler.scheduleDirect(runnable);
        else
            StageManager.getStage(stage).maybeExecuteImmediately(runnable);
    }

    /**
     * Build an executor for a given request.
     *
     *@param <P> - the request payload, for example a ReadCommand for a ReadRequest etc.
     */
    public static final class Builder<P>
    {
        private final Stage stage;

        @Nullable
        private final SchedulerSupplier<P> schedulerSupplier;

        public Builder(Stage stage)
        {
            this(stage, null);
        }

        public Builder(Stage stage, SchedulerSupplier<P> schedulerSupplier)
        {
            assert stage != null : "stage cannot be null";

            this.stage = stage;
            this.schedulerSupplier = schedulerSupplier;
        }

        public VerbExecutor build(Message<P> message)
        {
            assert StageManager.getStage(stage) != null : stage.name() + "not found!";
            return message.isRequest()
                   ? new VerbExecutor(stage, schedulerSupplier == null ? null : schedulerSupplier.get(message.payload()))
                   : new VerbExecutor(message.group().isInternal() ? Stage.INTERNAL_RESPONSE : Stage.REQUEST_RESPONSE);
        }
    }
}
