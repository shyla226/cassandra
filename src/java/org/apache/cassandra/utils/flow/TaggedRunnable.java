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

import org.apache.cassandra.concurrent.TPCTaskType;

/**
 * Runnable extension that enables tagging a scheduled operation with information about the operation to be performed.
 * Used for metrics processing.
 */
public interface TaggedRunnable extends Runnable
{
    TPCTaskType getStage();
    int scheduledOnCore();

    public abstract static class Base implements TaggedRunnable
    {
        private final TPCTaskType stage;
        private final int core;

        public Base(TPCTaskType stage, int core)
        {
            this.stage = stage;
            this.core = core;
        }

        public TPCTaskType getStage()
        {
            return stage;
        }

        public int scheduledOnCore()
        {
            return core;
        }
    }

    static TPCTaskType stageOf(Runnable runnable)
    {
        return (runnable instanceof TaggedRunnable)
               ? ((TaggedRunnable) runnable).getStage()
               : TPCTaskType.UNKNOWN;
    }
}
