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

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ExpiringMap;

class MessageDeliveryTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(MessageDeliveryTask.class);

    private final Message message;
    private final long enqueueTime;

    MessageDeliveryTask(Message message)
    {
        assert message != null;
        this.message = message;
        this.enqueueTime = ApproximateTime.currentTimeMillis();
    }

    public void run()
    {
        MessagingService.instance().metrics.addQueueWaitTime(message.verb().toString(),
                                                             ApproximateTime.currentTimeMillis() - enqueueTime);

        if (message.isTimedOut())
        {
            Tracing.trace("Discarding unhandled but timed out message from {}", message.from());
            MessagingService.instance().incrementDroppedMessages(message);
            return;
        }

        if (message.isRequest())
        {
            Request<?, ?> request = (Request)message;
            // First, deal with forwards if we have any
            for (ForwardRequest<?, ?> forward : request.forwardRequests())
                MessagingService.instance().forward(forward);

            if (request.verb().isOneWay())
                ((OneWayRequest<?>)request).execute();
            else
                request.execute(MessagingService.instance()::reply, onAborted(request));
        }
        else
        {
            handleResponse((Response<?>) message);
        }
    }

    private Runnable onAborted(Request<?, ?> request)
    {
        return () ->
        {
            Tracing.trace("Discarding partial response to {} (timed out)", request.from());
            MessagingService.instance().incrementDroppedMessages(request);
        };
    }

    private static <Q> void handleResponse(Response<Q> response)
    {
        Tracing.trace("Processing response from {}", response.from());

        ExpiringMap.CacheableObject<CallbackInfo<?>> cObj = MessagingService.instance().removeRegisteredCallback(response);
        if (cObj == null)
        {
            String msg = "Callback already removed for message {} from {}, ignoring response";
            logger.trace(msg, response.id(), response.from());
            Tracing.trace(msg, response.id(), response.from());
            return;
        }

        @SuppressWarnings("unchecked")
        MessageCallback<Q> callback = (MessageCallback<Q>)cObj.get().callback;
        // TODO: should we maybe add the latency on failure too?
        if (!response.isFailure())
            MessagingService.instance().addLatency(response.verb(), response.from(), cObj.lifetime(TimeUnit.MILLISECONDS));

        response.deliverTo(callback);
        MessagingService.instance().updateBackPressureOnReceive(response.from(), response.verb(), false);
    }
}
