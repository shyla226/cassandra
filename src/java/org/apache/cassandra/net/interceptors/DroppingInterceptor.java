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

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.net.Message;

/**
 * An internode message interceptor that drops the messages it intercepts.
 * <p>
 * Note that when a message is intercepted, it is as if it was never received/sent by the node on which the interceptor
 * is active. In particular, this is different from dropping a message and no "dropped" metrics is updated.
 *
 * @see AbstractInterceptor for details on the options supported by this interceptor (including how to pick which
 * messages to drop, how to have message dropped only probabilistically, etc.)
 */
public class DroppingInterceptor extends AbstractInterceptor
{
    public DroppingInterceptor(String name)
    {
        super(name,
              ImmutableSet.of(), // Needs to be configured by the user (through JMX or runtime property)
              Message.Type.all(),
              MessageDirection.all(),
              Message.Locality.all());
    }

    protected <M extends Message<?>> void handleIntercepted(M message, InterceptionContext<M> context)
    {
        // Drop whatever is intercepted
        context.drop(message);
    }
}
