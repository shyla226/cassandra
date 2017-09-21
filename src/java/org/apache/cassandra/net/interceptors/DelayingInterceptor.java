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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.Message;

/**
 * An internode message interceptor that delays messages delivery.
 * <p>
 * This interceptor provides a simple way to simulate lag for a given node. A specific delay <b>must</b> be configured
 * through the {@code -Ddse.net.interceptors.message_delay_ms} and intercepted messages will be delivered with
 * that configured delay.
 *
 * @see AbstractInterceptor for details on the options supported by this interceptor (including how to pick which
 * messages to delay, how to have message delayed only probabilistically, etc.)
 */
public class DelayingInterceptor extends AbstractInterceptor
{
    private static final String DELAY_PROPERTY = "message_delay_ms";

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    private final int messageDelay;

    public DelayingInterceptor(String name)
    {
        super(name,
              ImmutableSet.of(), // Needs to be configured by the user (through JMX or runtime property)
              Sets.immutableEnumSet(Message.Type.REQUEST), // only delay requests by default. Here again, delaying both side would double the delay
              // in practice which is probably what we want.
              Sets.immutableEnumSet(MessageDirection.RECEIVING), // don't delay on both SENDING and RECEIVING or that'll double the delay.
              Sets.immutableEnumSet(Message.Locality.all()));
        this.messageDelay = messageDelay();
    }

    private int messageDelay()
    {
        String delayStr = getProperty(DELAY_PROPERTY);
        if (delayStr == null)
            throw new ConfigurationException(String.format("Missing -D%s%s property for delaying interceptor",
                                                           Interceptors.PROPERTY_PREFIX, DELAY_PROPERTY));

        return Integer.valueOf(delayStr);
    }

    protected <M extends Message<?>> void handleIntercepted(M message, InterceptionContext<M> context)
    {
        // We don't want to hold whatever thread is sending/delivering message, so we simply schedule the action  with
        // the added delay.
        executor.schedule(() -> context.passDown(message), messageDelay, TimeUnit.MILLISECONDS);
    }
}
