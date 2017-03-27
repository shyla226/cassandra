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
import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.exceptions.InternalRequestExecutionException;

/**
 * A callback specialized for returning a value from a single target; that is, this is for messages
 * that we only send to one recipient.
 */
class SingleTargetCallback<P> extends CompletableFuture<P> implements MessageCallback<P>
{
    public void onResponse(Response<P> response)
    {
        complete(response.payload());
    }

    public void onFailure(FailureResponse response)
    {
        completeExceptionally(new InternalRequestExecutionException(response.reason(), "Got error back from " + response.from()));
    }

    public void onTimeout(InetAddress host)
    {
        completeExceptionally(new CallbackExpiredException());
    }
}
