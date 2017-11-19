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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.disposables.Disposable;

/**
 * Implementation of {@link DeferredFlow}.
 *
 * Record the source flow into an atomic reference and when, both subscriber and source flow
 * are available start requesting items, provided the subscriber has made a request. Once both
 * subscriber and source flow are available, this class is a simple bridge that subscribes the
 * subscriber directly to the source flow.
 * <p>
 * The complexity lies in coordinating the client requests with the availability of the source
 * flow, without relying on the synchronized keyword.
 * <p>
 * Two enums control the subscription and request status and they ensure that the first request,
 * or an initial close with no request, are performed exactly once.
 *
 * @param <T> - the type of the items for the flow
 */
class DeferredFlowImpl<T> extends DeferredFlow<T> implements FlowSubscriptionRecipient
{
    private static final Logger logger = LoggerFactory.getLogger(DeferredFlowImpl.class);

    private final AtomicReference<Flow<T>> source;
    private final long deadlineNanos;
    private final Supplier<Flow<T>> timeoutSupplier;
    private final Supplier<StagedScheduler> schedulerSupplier;

    private volatile FlowSubscriber<T> subscriber;
    private volatile FlowSubscriptionRecipient subscriptionRecipient;
    private volatile FlowSubscription subscription;
    private volatile Disposable timeoutTask;

    private final AtomicBoolean subscribed = new AtomicBoolean(false);

    DeferredFlowImpl(long deadlineNanos, Supplier<StagedScheduler> schedulerSupplier, Supplier<Flow<T>> timeoutSupplier)
    {
        assert schedulerSupplier != null;
        this.source = new AtomicReference<>(null);
        this.deadlineNanos = deadlineNanos;
        this.timeoutSupplier = timeoutSupplier;
        this.schedulerSupplier = schedulerSupplier;
    }

    public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
    {
        assert this.subscriber == null : "Only one subscriber is supported";
        this.subscriptionRecipient = subscriptionRecipient;
        this.subscriber = subscriber;

        if (source.get() == null)
            startTimeoutTask();

        maybeSubscribe();
    }

    public void onSubscribe(FlowSubscription source)
    {
        this.subscription = source;
    }

    /**
     * Called when we have received a sufficient number of requests to create a result or when the
     * timeout is expired. The source will contain any errors.
     * <p>
     * Disable the timer task if any, and try to subscribe unless already closed.
     *
     * @return true if the source was accepted, false otherwise (in case of a race)
     **/
    @Override
    public boolean onSource(Flow<T> value)
    {
        if (this.source.compareAndSet(null, value))
        {
            if (logger.isTraceEnabled())
                logger.trace("{} - got source", DeferredFlowImpl.this.hashCode());

            maybeSubscribe();
            return true;
        }
        else
            return false;
    }

    /**
     * Schedule a timeout task that will compete to set the source atomic reference
     */
    private void startTimeoutTask()
    {
        assert timeoutTask == null : "timeout task already running!";

        long timeoutNanos = this.deadlineNanos - System.nanoTime();
        if (timeoutNanos <= 0)
        { // deadline already passed
            onSource(timeoutSupplier.get());
            return;
        }

        timeoutTask = schedulerSupplier.get().scheduleDirect(() -> {
            timeoutTask = null;
            if (!hasSource()) // important to check because it is expensive to create an exception, due to the callstack
                onSource(timeoutSupplier.get());
        }, TPCTaskType.TIMED_TIMEOUT, timeoutNanos, TimeUnit.NANOSECONDS);
    }

    /** Dispose the timeout task, if available.
     * <p>
     * It's important to dispose of any pending timeout tasks in order to avoid keeping references to
     * the entire topology, which would cause the heap survivor and tenured areas to fill up and as a
     * consequence terrible performance due to high GC activity and long pauses
     */
    private void disposeTimeoutTask()
    {
        Disposable timeoutTask = this.timeoutTask;
        if (timeoutTask != null)
            timeoutTask.dispose();

        this.timeoutTask = null;
    }

    @Override
    public boolean hasSource()
    {
        return this.source.get() != null;
    }

    private void maybeSubscribe()
    {
        if (logger.isTraceEnabled())
            logger.trace("{} - maybeSubscribe {}/{}",
                         DeferredFlowImpl.this.hashCode(), source, subscription);

        if (subscriber == null || source.get() == null)
            return; // we need both subscriber and Flow before attempting to subscribe

        // One thread could be calling us for source, the other for subscriber and both can pass test above
        if (!subscribed.compareAndSet(false, true))
            return; // the other thread raced us

        disposeTimeoutTask();

        source.get().requestFirst(subscriber, subscriptionRecipient);
    }
}
