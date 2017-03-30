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

/**
 * A common interface for executing a runnable after having
 * installed the thread local values of the calling thread.
 */
public interface TracingAwareExecutor
{
    /**
     * Schedule the runnable for execution in a separate thread.
     * Before executing the runnable, the {@link ExecutorLocals#set(ExecutorLocals)}
     * will be invoked, so that the thread local values received in the locals
     * parameter will be available in the thread local of the executing thread.
     * <p>
     * This method is currently called by {@link org.apache.cassandra.net.MessagingService}
     * when a remote message is received.
     *
     * @param runnable - the runnable to execute
     * @param locals - the thread local value to set in the thread local of the executing thread
     */
    void execute(Runnable runnable, ExecutorLocals locals);

    /**
     * Either schedule the runnable in a remote thread or in the very same calling thread,
     * blocking the caller.
     * <p>
     * This method is currently called by {@link org.apache.cassandra.net.MessagingService}
     * when executing requests locally.
     *
     *  @param runnable - the runnable to execute
     */
    void maybeExecuteImmediately(Runnable runnable);
}
