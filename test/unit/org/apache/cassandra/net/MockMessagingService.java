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
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.function.Predicate;

import com.google.common.collect.Iterables;

/**
 * Starting point for mocking {@link MessagingService} interactions. Outgoing messages can be
 * intercepted by first creating a {@link MatcherResponse} by calling {@link MockMessagingService#when(Matcher)}.
 * Alternatively {@link Matcher}s can be created by using helper methods such as {@link #to(InetAddress)},
 * {@link #verb(Verb)} or {@link #payload(Predicate)} and may also be
 * nested using {@link MockMessagingService#all(Matcher[])} or {@link MockMessagingService#any(Matcher[])}.
 * After each test, {@link MockMessagingService#cleanup()} must be called for free listeners registered
 * in {@link MessagingService}.
 */
public class MockMessagingService
{

    private MockMessagingService()
    {
    }

    /**
     * Creates a MatcherResponse based on specified matcher.
     */
    public static MatcherResponse when(Matcher matcher)
    {
        return new MatcherResponse(matcher);
    }

    /**
     * Unsubscribes any handlers added by calling {@link MessagingService#addMessageSink(IMessageSink)}.
     * This should be called after each test.
     */
    public static void cleanup()
    {
        MessagingService.instance().clearMessageSinks();
    }

    /**
     * Creates a matcher that will indicate if the target address of the outgoing message equals the
     * provided address.
     */
    public static Matcher to(String address)
    {
        try
        {
            return to(InetAddress.getByName(address));
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a matcher that will indicate if the target address of the outgoing message equals the
     * provided address.
     */
    public static Matcher to(InetAddress address)
    {
        return in -> in.to().equals(address);
    }

    /**
     * Creates a matcher that will indicate if the definition of the outgoing message equals the
     * provided value.
     */
    public static Matcher verb(Verb<?, ?> definition)
    {
        return in -> in.verb().equals(definition);
    }

    /**
     * Creates a matcher based on the result of the provided predicate called with the outgoing message.
     */
    public static Matcher message(Predicate<Request<?, ?>> fn)
    {
        return fn::test;
    }

    /**
     * Creates a matcher based on the result of the provided predicate called with the outgoing message's payload.
     */
    @SuppressWarnings("unchecked")
    public static <T> Matcher payload(Predicate<T> fn)
    {
        return msg -> fn.test((T)msg.payload());
    }

    /**
     * Inverts boolean result of wrapped matcher.
     */
    public static Matcher not(Matcher matcher)
    {
        return o -> !matcher.matches(o);
    }

    /**
     * Indicates true in case all provided matchers returned true.
     */
    public static Matcher all(Matcher... matchers)
    {
        return r -> Iterables.all(Arrays.asList(matchers), m -> m.matches(r));
    }

    /**
     * Indicates true in case at least a single provided matcher returned true.
     */
    public static Matcher any(Matcher... matchers)
    {
        return r -> Iterables.any(Arrays.asList(matchers), m -> m.matches(r));
    }
}