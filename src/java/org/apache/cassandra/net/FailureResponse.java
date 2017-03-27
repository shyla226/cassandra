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

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A failure response, which comes with the reason of the failure (though that reason might be
 * {@link RequestFailureReason#UNKNOWN}).
 */
public class FailureResponse<Q> extends Response<Q>
{
    private final RequestFailureReason reason;

    /**
     * Package-protected because responses are not meant to be created manually outside of the 'net' package. Use
     * {@link Request#respondWithFailure} instead.
     */
    FailureResponse(InetAddress from,
                    InetAddress to,
                    int id,
                    Verb<?, Q> verb,
                    RequestFailureReason reason,
                    Data<Q> data)
    {
        super(from, to, id, verb, data);
        this.reason = reason;
    }

    public static <R> FailureResponse<R> local(Verb<?, R> verb,
                                               RequestFailureReason reason,
                                               long createdAtMillis)
    {
        InetAddress local = FBUtilities.getLocalAddress();
        return new FailureResponse<>(local,
                                     local,
                                     -1,
                                     verb,
                                     reason,
                                     new Data<>(null, -1, createdAtMillis, Long.MAX_VALUE));
    }

    public RequestFailureReason reason()
    {
        return reason;
    }

    public boolean isFailure()
    {
        return true;
    }

    @Override
    public FailureResponse<Q> addParameters(MessageParameters parameters)
    {
        return new FailureResponse<Q>(from(), to(), id(), verb(), reason, messageData.withAddedParameters(parameters));
    }

    @Override
    void deliverTo(MessageCallback<Q> callback)
    {
        callback.onFailure(this);
    }

    @Override
    long payloadSerializedSize(MessagingVersion version)
    {
        return 0;
    }

    @Override
    public String toString()
    {
        return String.format("[error]@%d: %s %s %s", id(), from(), isRequest() ? "->" : "<-", to());
    }
}
