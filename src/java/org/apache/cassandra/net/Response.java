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

import java.net.InetAddress;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.utils.FBUtilities;

/**
 * A response message for the inter-node messaging protocol.
 * <p>
 * As any request can fail (for known or unknown reasons), we have a special {@link FailureResponse} sub-class that
 * represent failure responses.
 */
public class Response<Q> extends Message<Q>
{
    private final Verb<?, Q> verb;

    /**
     * Package-protected because responses are not meant to be create manually outside of the 'net' package. Use
     * {@link Request#respond} instead.
     */
    Response(InetAddress from,
             InetAddress to,
             int id,
             Verb<?, Q> verb,
             Data<Q> data)
    {
        super(from, to, id, data);
        this.verb = verb;
    }

    public static <R> Response<R> local(Verb<?, R> verb, R payload, long createdAtMillis)
    {
        InetAddress local = FBUtilities.getLocalAddress();
        return new Response<>(local,
                              local,
                              -1,
                              verb,
                              // payload size is -1 because it's local, we're not supposed to use the payload size
                              new Data<>(payload, -1, createdAtMillis, Long.MAX_VALUE));
    }

    @VisibleForTesting
    public static <R> Response<R> testResponse(InetAddress from,
                                               InetAddress to,
                                               Verb<?, R> verb,
                                               R payload)
    {
        return new Response<>(from,
                              to,
                              -1,
                              verb,
                              new Data<>(payload));
    }

    @VisibleForTesting
    public static <R> Response<R> localTestResponse(Verb<?, R> verb, R payload)
    {
        InetAddress local = FBUtilities.getLocalAddress();
        return testResponse(local, local, verb, payload);
    }

    @VisibleForTesting
    public static Response<EmptyPayload> testLocalAck(Verb<?, EmptyPayload> verb)
    {
        return localTestResponse(verb, EmptyPayload.instance);
    }

    public boolean isRequest()
    {
        return false;
    }

    public Verb<?, Q> verb()
    {
        return verb;
    }

    public Response<Q> addParameters(MessageParameters parameters)
    {
        return new Response<>(from(), to(), id(), verb(), messageData.withAddedParameters(parameters));
    }

    /**
     * Whether this is a failure response or not.
     *
     * @return {@code true} if this is a failure response, in which case this can be safely casted into a
     * {@link FailureResponse} object. {@code false} if it is a successful response.
     */
    public boolean isFailure()
    {
        // Overrided by FailureResponse
        return false;
    }

    void deliverTo(MessageCallback<Q> callback)
    {
        callback.onResponse(this);
    }

    long payloadSerializedSize(MessagingVersion version)
    {
        return messageData.payloadSize >= 0 && version == MessagingService.current_version
               ? messageData.payloadSize
               : version.serializer(verb()).responseSerializer.serializedSize(payload());
    }
}
