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

import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.db.monitoring.AbortedOperationException;

/**
 * Interface for the handler of a particular verb.
 * <p>
 * A verb handler basically executes a particular request (potentially asynchronously, hence the future in the return
 * type) generating a response to that request.
 *
 * @param <P> the type of the payload of the requests this handler deals with.
 * @param <Q> the type of the payload of the responses this handler generates.
 */
public interface VerbHandler<P, Q>
{
    /**
     * Handles a particular request generating a response (potentially but not necessarily asynchronously).
     * <p>
     * Implementers <b>must</b> ensure that calls to this method never throw or return exceptionally with an exception
     * with the only exceptions of {@link AbortedOperationException} (in case the query is aborted by monitoring) and
     * {@link DroppingResponseException}. Any other exception must be turned into a {@link FailureResponse} instead.
     * Callers can thus rely on that fact.
     * <p>
     * Note that in practice, implementors should not implement this generic interface but rather implement one of the
     * sub-interface defined in {@link VerbHandlers} (which in practice they do because verbs and their handler is
     * created through {@link VerbGroup.RegistrationHelper} which uses those sub-interfaces). Those are both more
     * convenient and ensure proper handling of exceptions as described in the previous paragraph
     * (see {@link VerbHandlers#handleFailure}).
     *
     * @param request the request to handle.
     * @return a future on the response to {@code request}. This can be {@code null} for one-way verbs when no response
     * whatsoever needs to be sent.
     *
     * @throws AbortedOperationException if the operation this is a handler for is monitored and has been aborted
     * @throws DroppingResponseException if this is a two-way handler but for some reason no response should be sent back.
     *
     */
    CompletableFuture<Response<Q>> handle(Request<P, Q> request);
}
