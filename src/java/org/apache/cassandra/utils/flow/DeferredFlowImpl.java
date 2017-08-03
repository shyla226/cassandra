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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.utils.JVMStabilityInspector;

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
class DeferredFlowImpl<T> extends DeferredFlow<T>
{
    private static final Logger logger = LoggerFactory.getLogger(DeferredFlowImpl.class);

    private final AtomicReference<Flow<T>> source;
    private final long deadlineNanos;
    private final Supplier<Flow<T>> timeoutSupplier;

    private volatile FlowSubscriber<T> subscriber;
    private volatile FlowSubscription subscription;
    private volatile Throwable subscriptionError;
    private volatile Disposable timeoutTask;

    private enum RequestStatus
    {
        NONE, ITEM, CLOSE
    }

    private enum SubscribeStatus
    {
        NONE, SUBSCRIBING, SUBSCRIBED
    }

    private final AtomicReference<RequestStatus> requestStatus = new AtomicReference<>(RequestStatus.NONE);
    private final AtomicReference<SubscribeStatus> subscribeStatus = new AtomicReference<>(SubscribeStatus.NONE);

    DeferredFlowImpl(long deadlineNanos, Supplier<Flow<T>> timeoutSupplier)
    {
        this.source = new AtomicReference<>(null);
        this.deadlineNanos = deadlineNanos;
        this.timeoutSupplier = timeoutSupplier;
    }

    public FlowSubscription subscribe(FlowSubscriber<T> subscriber) throws Exception
    {
        assert this.subscriber == null : "Only one subscriber is supported";
        this.subscriber = subscriber;

        if (source.get() == null)
            startTimeoutTask();

        maybeSubscribe();
        return new Subscription();
    }

    /**
     * Request to the source unless there was a subscription error, in which
     * case we report it. This method is guaranteed to be called when subscribeStatus
     * is already SUBSCRIBED, and therefore both subscriber and subscription are available.
     */
    private void realRequest()
    {
        if (subscriptionError != null)
            subscriber.onError(subscriptionError);
        else
            subscription.request();
    }

    private class Subscription implements FlowSubscription
    {
        public void request()
        {
            if (subscribeStatus.get() == SubscribeStatus.SUBSCRIBED)
            { // we already have a subscription to the source flow
                realRequest();
            }
            else
            { // no subscription yet, record the pending request
                boolean ret = requestStatus.compareAndSet(RequestStatus.NONE, RequestStatus.ITEM);
                if (!ret)
                {
                    subscriber.onError(new AssertionError("Unexpected request status: " + requestStatus.get()));
                    return;
                }

                // if in the meantime we raced with the subscribing thread, request the item only if
                // we can reset the request status
                if (subscribeStatus.get() == SubscribeStatus.SUBSCRIBED)
                {
                    if (requestStatus.compareAndSet(RequestStatus.ITEM, RequestStatus.NONE))
                        realRequest();
                }
            }
        }

        public void close() throws Exception
        {
            if (subscribeStatus.get() == SubscribeStatus.SUBSCRIBED)
            { // we already have a subscription to the source flow
                subscription.close();
            }
            else
            { // no subscription yet, record the pending close
                boolean ret = requestStatus.compareAndSet(RequestStatus.NONE, RequestStatus.CLOSE);
                assert ret : "Unexpected request status";

                // if in the meantime we raced with the subscribing thread, close only if
                // we can reset the request status
                if (subscribeStatus.get() == SubscribeStatus.SUBSCRIBED)
                {
                    if (requestStatus.compareAndSet(RequestStatus.CLOSE, RequestStatus.NONE))
                        subscription.close();
                }
            }
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            if (subscription != null)
                return subscription.addSubscriberChainFromSource(throwable);
            else
                return throwable;
        }
    }

    /**
     * Called when we have received a sufficient number of requests to create a result or when the
     * timeout is expired. The source will contain any errors.
     * <p>
     * Disable the timer task if any, and try to subscribe unless already closed.
     **/
    @Override
    public void onSource(Flow<T> value)
    {
        if (this.source.compareAndSet(null, value))
        {
            if (logger.isTraceEnabled())
                logger.trace("{} - got source", DeferredFlowImpl.this.hashCode());

            maybeSubscribe();
        }
        // else
        // {
        // TODO - Are we at risk of resource leaks if a timeout beats a real source? We shouldn't be, since Flow
        // is not closeable and therefore implementors should not allocate resources until there is a subscription
        // but we should probably review the code at some point and possibly merge Flow and FlowSubscription.
        // }
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

        timeoutTask = Schedulers.computation().scheduleDirect(() -> {

            timeoutTask = null;
            if (!hasSource()) // important to check because it is expensive to create an exception, due to the callstack
                onSource(timeoutSupplier.get());

        }, timeoutNanos, TimeUnit.NANOSECONDS);
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

        if (!subscribeStatus.compareAndSet(SubscribeStatus.NONE, SubscribeStatus.SUBSCRIBING))
            return; // the other thread raced us

        try
        {
            subscription = source.get().subscribe(subscriber);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            subscriptionError = t;
            logger.debug("Got error whilst subscribing to ReadCallback source, {}/{}", t.getClass(), t.getMessage());
        }

        // even if we got an error subscribing, we'll use that error, not the timeout and so dispose it if
        // there is one running
        disposeTimeoutTask();

        boolean ret = subscribeStatus.compareAndSet(SubscribeStatus.SUBSCRIBING, SubscribeStatus.SUBSCRIBED);
        assert ret : "Failed to set subscribe status, no other thread should have raced";

        if (requestStatus.compareAndSet(RequestStatus.ITEM, RequestStatus.NONE))
        {
            realRequest();
        }
        else if (requestStatus.compareAndSet(RequestStatus.CLOSE, RequestStatus.NONE))
        {
            try
            {
                subscription.close();
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.debug("Got error whilst closing subscription but downstream had already closed, so did not pass down error, {}/{}",
                                          t.getClass().getName(), t.getMessage());
            }
        }
    }
}
