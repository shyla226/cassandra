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

import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import com.google.common.annotations.VisibleForTesting;

/**
 * This class is a bridge between the Flow<T> that we return to clients and the one
 * that we receive when {@link #onSource(Flow)} is called. It converts
 * client requests into requests to the source flow.
 * <p>
 * If no source flow is available when the client subscribes, the request is recorded and
 * will be propagated to the source flow as soon as it is available.
 * <p>
 * See {@link org.apache.cassandra.service.ReadCallback#result} for an example on how this class
 * is used to set a source flow when a sufficient number of responses have been received.
 * <p>
 * The implementation is in {@link DeferredFlowImpl}.
 */
public abstract class DeferredFlow<T> extends Flow<T>
{
    /** Called when the source flow is available */
    public abstract boolean onSource(Flow<T> value);

    /** Indicates if the source flow is available */
    public abstract boolean hasSource();

    /**
     * Create a deferred flow that will throw a {@link TimeoutException}
     * after {@code timeoutNanos} nano seconds.
     *
     * @param timeoutNanos - the timeout in nano seconds
     * @param <T> - the type of flow items
     *
     * @return a deferred flow implementation
     */
    @VisibleForTesting
    static <T> DeferredFlow<T> createWithTimeout(long timeoutNanos)
    {
        return create(System.nanoTime() + timeoutNanos, () -> TPC.bestTPCScheduler(), () -> Flow.error(new TimeoutException()));
    }

    /**
     * Create a deferred flow that, if no source is provided before the given {@code deadlineNanos} expired, will use
     * the flow returned by the provided supplier as source.
     *
     * @param deadlineNanos - the deadline in nano seconds (corresponding to System.nanoTime)
     * @param scheduler - the scheduler to run timeout task
     * @param timeoutSupplier - a function that will supply a source when and if required.
     * @param <T> - the type of flow items
     *
     * @return a deferred flow implementation
     */
    public static <T> DeferredFlow<T> create(long deadlineNanos, Supplier<StagedScheduler> schedulerSupplier, Supplier<Flow<T>> timeoutSupplier)
    {
        return new DeferredFlowImpl<>(deadlineNanos, schedulerSupplier, timeoutSupplier);
    }
}
