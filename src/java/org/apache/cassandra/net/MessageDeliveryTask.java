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
package org.apache.cassandra.net;

import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.tracing.Tracing;

abstract class MessageDeliveryTask<T, M extends Message<T>> implements Runnable
{
    protected final M message;
    private final long enqueueTime;

    MessageDeliveryTask(M message)
    {
        this.message = message;
        this.enqueueTime = ApproximateTime.currentTimeMillis();
    }

    static <P, Q> RequestDeliveryTask<P, Q> forRequest(Request<P, Q> request)
    {
        return new RequestDeliveryTask<>(request);
    }

    static <Q> ResponseDeliveryTask<Q> forResponse(Response<Q> response)
    {
        return new ResponseDeliveryTask<>(response);
    }

    public void run()
    {
        long currentTimeMillis = ApproximateTime.currentTimeMillis();
        if (message.verb().droppedGroup() != null) // not one-way
            MessagingService.instance().metrics.addQueueWaitTime(message.verb().droppedGroup().toString(),
                                                             currentTimeMillis - enqueueTime);

        if (message.isTimedOut(currentTimeMillis))
        {
            Tracing.trace("Discarding unhandled but timed out message from {}", message.from());
            MessagingService.instance().incrementDroppedMessages(message);
            return;
        }

        deliverMessage(currentTimeMillis);
    }

    protected abstract void deliverMessage(long currentTimeMillis);

    private static class RequestDeliveryTask<P, Q> extends MessageDeliveryTask<P, Request<P, Q>>
    {
        private RequestDeliveryTask(Request<P, Q> request)
        {
            super(request);
        }

        protected void deliverMessage(long currentTimeMillis)
        {
            // First, deal with forwards if we have any
            for (ForwardRequest<?, ?> forward : message.forwardRequests())
                MessagingService.instance().forward(forward);

            if (message.verb().isOneWay())
                ((OneWayRequest<?>)message).execute();
            else
                message.execute(MessagingService.instance()::reply, this::onAborted);
        }

        private void onAborted()
        {
            Tracing.trace("Discarding partial response to {} (timed out)", message.from());
            MessagingService.instance().incrementDroppedMessages(message);
        }
    }

    private static class ResponseDeliveryTask<Q> extends MessageDeliveryTask<Q, Response<Q>>
    {
        private ResponseDeliveryTask(Response<Q> response)
        {
            super(response);
        }

        protected void deliverMessage(long currentTimeMillis)
        {
            CallbackInfo<Q> info = MessagingService.instance().getRegisteredCallback(message, true);
            // If the info have expired, ignore the response (we already logged in getRegisteredCallback)
            if (info == null)
                return;

            Tracing.trace("Processing response from {}", message.from());
            MessageCallback<Q> callback = info.callback;
            // TODO: should we maybe add the latency on failure too?
            if (!message.isFailure())
                MessagingService.instance().addLatency(message.verb(), message.from(), Math.max(currentTimeMillis - info.requestStartMillis, 0));

            message.deliverTo(callback);
            MessagingService.instance().updateBackPressureOnReceive(message.from(), message.verb(), false);
        }
    }
}
