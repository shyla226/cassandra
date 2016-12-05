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

/**
 * A callback called on the reception of responses to a message.
 * <p>
 * Implementors of MessageCallback need to make sure that any public methods are threadsafe with respect to
 * {@link #onResponse}, {@link #onFailure} and {@link #onTimeout} being called from the message service. In particular,
 * if any shared state is referenced, making those methods alone synchronized will not suffice.
 */
public interface MessageCallback<Q>
{
    /**
     * Called on reception of a successful response.
     *
     * @param response the response received.
     */
    void onResponse(Response<Q> response);

    /**
     * Called on reception of a failure response.
     *
     * @param response the failure response received.
     */
    void onFailure(FailureResponse<Q> response);

    /**
     * Called if no reponse is received before the timeout set by the message
     * type this is a callback of.
     *
     * @param host the host that didn't respond before the callback timeout.
     */
    default void onTimeout(InetAddress host)
    {
        // Most of the time, we give up on a callback after the timeout, so there is nothing to do when the callback
        // is basically expunged from the messaging service expiring map. This is overridden on writes however to write
        // hints.
    }
}
