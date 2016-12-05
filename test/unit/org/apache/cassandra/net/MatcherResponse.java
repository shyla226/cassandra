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

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Sends a response for an incoming message with a matching {@link Matcher}.
 * The actual behavior by any instance of this class can be inspected by
 * interacting with the returned {@link MockMessagingSpy}.
 */
public class MatcherResponse
{
    private final Matcher matcher;
    private final Set<Integer> sendResponses = new HashSet<>();
    private final MockMessagingSpy spy = new MockMessagingSpy();
    private final AtomicInteger limitCounter = new AtomicInteger(Integer.MAX_VALUE);
    private IMessageSink sink;

    MatcherResponse(Matcher matcher)
    {
        this.matcher = matcher;
    }

    /**
     * Do not create any responses for intercepted outbound messages.
     */
    public MockMessagingSpy dontReply()
    {
        return respond((Response<?>)null);
    }

    /**
     * Respond with provided message in reply to each intercepted outbound message.
     * @param message   the message to use as mock reply from the cluster
     */
    public MockMessagingSpy respond(Response<?> message)
    {
        return respondN(message, Integer.MAX_VALUE);
    }

    /**
     * Respond a limited number of times with the provided message in reply to each intercepted outbound message.
     * @param response  the message to use as mock reply from the cluster
     * @param limit     number of times to respond with message
     */
    public MockMessagingSpy respondN(final Response<?> response, int limit)
    {
        return respondN(in -> (Response)response, limit);
    }

    /**
     * Respond with the message created by the provided function that will be called with each intercepted outbound message.
     * @param fnResponse    function to call for creating reply based on intercepted message and target address
     */
    public <T, S> MockMessagingSpy respond(Function<Request<T, S>, Response<S>> fnResponse)
    {
        return respondN(fnResponse, Integer.MAX_VALUE);
    }

    /**
     * Respond with message wrapping the payload object created by provided function called for each intercepted outbound message.
     * The target address from the intercepted message will automatically be used as the created message's sender address.
     * @param fnResponse    function to call for creating payload object based on intercepted message and target address
     */
    public <T, S> MockMessagingSpy respondWithPayloadForEachReceiver(Function<Request<T, S>, S> fnResponse)
    {
        return respondNWithPayloadForEachReceiver(fnResponse, Integer.MAX_VALUE);
    }

    /**
     * Respond a limited number of times with message wrapping the payload object created by provided function called for
     * each intercepted outbound message. The target address from the intercepted message will automatically be used as the
     * created message's sender address.
     * @param fnResponse    function to call for creating payload object based on intercepted message and target address
     */
    public <T, S> MockMessagingSpy respondNWithPayloadForEachReceiver(Function<Request<T, S>, S> fnResponse, int limit)
    {
        return respondN((Request<T, S> r) -> {
                    S payload = fnResponse.apply(r);
                    if (payload == null)
                        return null;
                    else
                        return r.respond(payload);
                },
                limit);
    }

    /**
     * Responds to each intercepted outbound message by creating a response message wrapping the next element consumed
     * from the provided queue. No reply will be send when the queue has been exhausted.
     * @param cannedResponses   prepared payload messages to use for responses
     */
    public <T, S> MockMessagingSpy respondWithPayloadForEachReceiver(Queue<S> cannedResponses)
    {
        return respondWithPayloadForEachReceiver((Request<T, S> msg) -> cannedResponses.poll());
    }

    /**
     * Responds to each intercepted outbound message by creating a response message wrapping the next element consumed
     * from the provided queue. This method will block until queue elements are available.
     * @param cannedResponses   prepared payload messages to use for responses
     */
    public <T, S> MockMessagingSpy respondWithPayloadForEachReceiver(BlockingQueue<S> cannedResponses)
    {
        return respondWithPayloadForEachReceiver((Request<T, S> msg) -> {
            try
            {
                return cannedResponses.take();
            }
            catch (InterruptedException e)
            {
                return null;
            }
        });
    }

    /**
     * Respond a limited number of times with the message created by the provided function that will be called with
     * each intercepted outbound message.
     * @param fnResponse    function to call for creating reply based on intercepted message and target address
     */
    public <T, S> MockMessagingSpy respondN(Function<Request<T, S>, Response<S>> fnResponse, int limit)
    {
        limitCounter.set(limit);

        assert sink == null: "destroy() must be called first to register new response";

        sink = new IMessageSink()
        {
            @SuppressWarnings("unchecked")
            public boolean allowOutgoingMessage(Message message, MessageCallback<?> callback)
            {
                System.out.println("Caught " + message);

                if (!message.isRequest())
                    return true;

                Request<T, S> request = (Request<T, S>)message;

                // prevent outgoing message from being send in case matcher indicates a match
                // and instead send the mocked response
                if (matcher.matches(request))
                {
                    spy.matchingRequest(request);

                    if (limitCounter.decrementAndGet() < 0)
                        return false;

                    synchronized (sendResponses)
                    {
                        // I'm not sure about retry semantics regarding message/ID relationships, but I assume
                        // sending a message multiple times using the same ID shouldn't happen..
                        assert message.id() == -1 || !sendResponses.contains(message.id()) : "ID re-use for outgoing message";
                        sendResponses.add(message.id());
                    }

                    // create response asynchronously to match request/response communication execution behavior
                    new Thread(() -> {
                        Response<S> response = fnResponse.apply(request);
                        if (response != null)
                        {
                            if (callback != null)
                                ((MessageCallback<S>)callback).onResponse(response);
                            else
                                MessagingService.instance().receive(response);
                            spy.matchingResponse(response);
                        }
                    }).start();

                    return false;
                }
                return true;
            }

            public boolean allowIncomingMessage(Message message)
            {
                return true;
            }
        };
        MessagingService.instance().addMessageSink(sink);

        return spy;
    }

    /**
     * Stops currently registered response from being send.
     */
    public void destroy()
    {
        MessagingService.instance().removeMessageSink(sink);
    }
}
