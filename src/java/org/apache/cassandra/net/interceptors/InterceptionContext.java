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
package org.apache.cassandra.net.interceptors;

import java.util.List;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Response;

/**
 * The context of an interception.
 * <p>
 * This is used by interceptors ({@link Interceptor}) to both get more context on the the type of
 * interception this is (the {@link #direction} in particular) and decide what to do with the message. In practice,
 * interceptor can 2 main things when they intercept a message: they can pass a message down ({@link #passDown}) or
 * they can drop the message ({@link #drop}). If they drop the message, nothing further happen, no following
 * interceptors are called and the message is dropped on the floor. If the interceptor "pass-down", then whatever
 * message is give to {@link #passDown} is passed to the next interceptor in the pipeline, and if this is
 * the last interceptor of the pipeline, it is simply delivered normally. Note that the message given to
 * {@link #passDown} can be either the message intercepted, or a modified message (that needs to be of the same
 * verb and same "type" (request or response)). An interceptor that simply pass the intercepted message done is an
 * interceptor that is a no-op for this message.
 * <p>
 * Note that for an interceptor to call neither {@link #passDown} nor {@link #drop} is equivalent to dropping the
 * message, so {@link #drop} is about making intent explicit but is optional.
 */
public class InterceptionContext<M extends Message<?>>
{
    private static final Logger logger = LoggerFactory.getLogger(InterceptionContext.class);

    private final MessageDirection direction;
    private final Consumer<M> handler;
    @Nullable private final Consumer<Response<?>> responseCallback;
    private final List<Interceptor> pipeline;
    private int next; // index of the next interceptor to apply

    InterceptionContext(MessageDirection direction,
                        Consumer<M> handler,
                        Consumer<Response<?>> responseCallback,
                        List<Interceptor> pipeline)
    {
        this(direction, handler, responseCallback, pipeline, 0);
    }

    private InterceptionContext(MessageDirection direction,
                                Consumer<M> handler,
                                Consumer<Response<?>> responseCallback,
                                List<Interceptor> pipeline,
                                int next)
    {
        this.direction = direction;
        this.handler = handler;
        this.responseCallback = responseCallback;
        this.pipeline = pipeline;
        this.next = next;
    }

    public boolean isReceiving()
    {
        return direction() == MessageDirection.RECEIVING;
    }

    public boolean isSending()
    {
        return direction() == MessageDirection.SENDING;
    }

    /**
     * The direction that was intercepted, that is whether the interception this the context of is sending the message
     * or receiving the message.
     */
    public MessageDirection direction()
    {
        return direction;
    }

    /**
     * If the message intercepted is a two-way request being sent (locally or not), the callback for the responses of
     * those requests.
     *
     * @return the response callback for the intercepted message if it's a two-way request being sent, {@code null}
     * otherwise.
     */
    public Consumer<Response<?>> responseCallback()
    {
        return responseCallback;
    }

    /**
     * Pass-down the message to the next interceptor in the pipeline, or deliver the message normally if this was the
     * last/only interceptor.
     */
    public void passDown(M message)
    {
        if (next >= pipeline.size())
        {
            // We pass through the interceptor pipeline, pass down to the will consumer
            handler.accept(message);
        }
        else
        {
            pipeline.get(next++).intercept(message, this);
        }
    }

    /**
     * Drop the request on the floor.
     */
    public void drop(M message)
    {
        // This method is just about making things more implicit, but "dropping" the message is basically the default
        // if the message isn't explicitly passed down.
        logger.trace("Message {} dropped by interceptor", message);
    }

}
