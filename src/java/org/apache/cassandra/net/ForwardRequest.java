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

/**
 * A request that has been forwarded by an intermediate node. The request {@link #from} method will return the
 * intermediate node, but responses to the request will not be sent to that node, but rather to the original node
 * for which the intermediate one forwarded the request (see {@link #respond} which uses the {@link #replyTo} address).
 */
class ForwardRequest<P, Q> extends Request<P, Q>
{
    final InetAddress replyTo;

    ForwardRequest(InetAddress from,
                   InetAddress to,
                   InetAddress replyTo,
                   int id,
                   Verb<P, Q> type,
                   Data<P> data)
    {
        super(from, to, id, type, data);
        this.replyTo = replyTo;
    }

    @Override
    boolean isForwarded()
    {
        return true;
    }

    @Override
    public Request<P, Q> addParameters(MessageParameters parameters)
    {
        // We really shouldn't be modifying parameters on a forwarded request (we should do it on the original message)
        // so not allowing for now. If a genuine need for this ever arise, it'll be easy enough to change.
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Q> respond(Q payload)
    {
        return new Response<>(local, replyTo, id(), verb(), responseData(payload));
    }

    @Override
    public FailureResponse<Q> respondWithFailure(RequestFailureReason reason)
    {
        return new FailureResponse<>(local, replyTo, id(), verb(), reason, messageData.withPayload(null, -1));
    }
}
