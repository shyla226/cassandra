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

import org.apache.cassandra.net.Message;

/**
 * Interface for message "interceptor" that intercept inter-node messages for testing purposes.
 * <p>
 * Interceptors can set through the {@code -Ddse.net.interceptors} system property at startup.
 * When set this way, all message received or sent by the node will pass through the interceptor methods and the
 * interceptor may or may not do something with.
 * <p>
 * Interceptors exists for testing: they allow for things like having a node drop certain types of message on the floor
 * temporarily, delaying message delivery by some amount to simulate latency, ....
 * <p>
 * It is possible to set more than one interceptor at a time, for instance by passing:
 * {@code -Ddse.net.interceptors='DelayingInterceptor, DroppingInterceptor'}. In that case,
 * message will go through interceptors in the order the interceptors are defined.
 * <p>
 * Note that most interceptor have a number of additional options to control their behavior, some of them inherited from
 * {@link AbstractInterceptor}.
 *
 * @see DroppingInterceptor for an interceptor that drops certain message on the floor
 * @see DelayingInterceptor for an interceptor that delays message by a configurable amount before delivering them
 * @see FakeWriteInterceptor for an interceptor that drop but acknowledge write messages
 */
public interface Interceptor
{
    /**
     * Intercepts a message, deciding what should be done about it (the {@code context} is used to either pass down the
     * message, drop the message or any other type of action).
     *
     * @param message the message to (maybe) intercept.
     * @param context the interception context which provide both information on the interception running, and is used
     *                to pass the message down the interceptor pipeline.
     */
    public <M extends Message<?>> void intercept(M message, InterceptionContext<M> context);
}
