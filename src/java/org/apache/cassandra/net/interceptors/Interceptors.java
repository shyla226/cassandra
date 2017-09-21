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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Essentially a collection of interceptors that can be applied to messages to intercept them.
 */
public class Interceptors
{
    private static final String PROPERTY = "dse.net.interceptors";
    static final String PROPERTY_PREFIX = PROPERTY + '.';

    private static final Logger logger = LoggerFactory.getLogger(Interceptors.class);

    // Poor-man COWList. The only reason we don't reuse CopyOnWriteArrayList is that it feels like having
    // the true list snapshot in the Pipeline feels like it might prove more flexible than having a ListIterator
    // (allows to query the previous/next interceptors without modifying the iterator specifically). And it's not really
    // more complex since we don't do anything fancy.
    private final AtomicReference<List<Interceptor>> interceptors = new AtomicReference<>(Collections.emptyList());

    /**
     * Loads and adds to this instance the interceptors provided through the {@link #PROPERTY} runtime property.
     */
    public void load()
    {
        String names = System.getProperty(PROPERTY);
        if (names == null)
            return;
        names = names.trim();
        if (names.charAt(0) == '\'' || names.charAt(0) == '"')
            names = names.substring(1, names.length() - 1);
        load(names);
    }

    /**
     * Loads and adds to this instance the interceptors whose names are provided as argument.
     *
     * @param classNames the comma-separated list of names for the interceptors to load.
     */
    private void load(String classNames)
    {
        if (classNames == null || classNames.isEmpty())
            return;

        List<String> names = Splitter.on(",")
                                     .trimResults()
                                     .omitEmptyStrings()
                                     .splitToList(classNames);
        List<Interceptor> loaded = new ArrayList<>(names.size());
        for (String name : names)
        {
            if (!name.contains("."))
                name = "org.apache.cassandra.net.interceptors." + name;

            String jmxName;
            if (name.contains("="))
            {
                List<String> parts = Splitter.on("=").trimResults().splitToList(name);
                if (parts.size() != 2)
                    throw new ConfigurationException("Invalid interceptor name declaration: " + name);
                name = parts.get(0);
                jmxName = parts.get(1);
            }
            else
            {
                List<String> parts = Splitter.on(".").trimResults().splitToList(name);
                jmxName = parts.get(parts.size() - 1);
            }

            try
            {
                Class<Interceptor> cls = FBUtilities.classForName(name, "Interceptor");
                Constructor<Interceptor> ctor = cls.getConstructor(String.class);
                loaded.add(ctor.newInstance(jmxName));
                logger.info("Using class {} to intercept internode messages (You shouldn't see this unless this is running a test)", name);
            }
            catch (Exception e)
            {
                // Interceptor exists for testing and if the test sets interceptors, that's because it rely on it. So if we
                // fail creating any of the provided interceptor, just break loudly.
                logger.error("Error instantiating internode message interceptor class {}: {}", name, e.getMessage());
                throw new IllegalStateException(e);
            }
        }
        interceptors.set(loaded);
    }

    /**
     * Adds the provided interceptor to this instance.
     */
    public void add(Interceptor interceptor)
    {
        List<Interceptor> current, updated;
        do
        {
            current = interceptors.get();
            updated = new ArrayList<>(current.size() + 1);
            updated.addAll(current);
            updated.add(interceptor);
        } while (!interceptors.compareAndSet(current, updated));
    }

    /**
     * Removes the provided interceptor from this instance if it is present.
     */
    public void remove(Interceptor interceptor)
    {
        List<Interceptor> current, updated;
        do
        {
            current = interceptors.get();
            if (!current.contains(interceptor))
                return;
            
            updated = new ArrayList<>(current.size() - 1);
            Iterables.addAll(updated, Iterables.filter(current, i -> !i.equals(interceptor)));
        } while (!interceptors.compareAndSet(current, updated));
    }

    /**
     * Clear this instance, removing any interceptors.
     */
    public void clear()
    {
        interceptors.set(Collections.emptyList());
    }

    public boolean isEmpty()
    {
        return interceptors.get() == null;
    }

    /**
     * Intercept the provided request using the configured interceptors, or directly pass it down to the provided
     * consumer if no interceptors are configured.
     *
     * @param message the message to intercept.
     * @param handler the handler to which the message (or whatever the interceptors modified it to) is fed to once/if
     *                all interceptors pass it down (if any interceptor don't pass the message down, nothing further
     *                happens for that message, it is dropped).
     * @param responseCallback if {@code message} is a two-way request, the callback to use if the interceptor want to
     *                         short-circuit the message handling completely and feed back some response directly
     *                         instead. This can be {@code null} if the message is either not a request, or a one-way
     *                         request. Note that this isn't properly typed to keep simple (we use a wilcard for the
     *                         response type) but this should be in practice a callback for the type of response that
     *                         correspond to {@code message}.
     */
    public <M extends Message<?>> void intercept(M message,
                                                 Consumer<M> handler,
                                                 Consumer<Response<?>> responseCallback)
    {
        List<Interceptor> snapshot = interceptors.get();
        if (snapshot.isEmpty())
        {
            handler.accept(message);
        }
        else
        {
            if (message.isLocal())
            {
                // A local message is both send and received by the node, so chain both.
                doIntercept(message,
                            MessageDirection.SENDING,
                            snapshot,
                            msg -> doIntercept(msg, MessageDirection.RECEIVING, snapshot, handler, responseCallback),
                            responseCallback);
            }
            else
            {
                MessageDirection direction = message.from().equals(FBUtilities.getBroadcastAddress())
                                             ? MessageDirection.SENDING
                                             : MessageDirection.RECEIVING;
                doIntercept(message, direction, snapshot, handler, responseCallback);
            }
        }
    }

    /**
     * Same as {@link #intercept}, but with cleaner typing when we handle a request and have the {@link MessageCallback}
     * for that request "as response callback" (so we're on the sending side).
     *
     * @param request the request to intercept.
     * @param handler the handler for the request if it made it through the interceptors (see {@link #intercept} for details).
     * @param responseCallback the callback for responses to {@code request}.
     */
    @SuppressWarnings("unchecked")
    public <P, Q, M extends Request<P, Q>> void interceptRequest(M request,
                                                                 Consumer<M> handler,
                                                                 MessageCallback<Q> responseCallback)
    {
        intercept(request, handler, response -> ((Response<Q>)response).deliverTo(responseCallback));
    }

    private <M extends Message<?>> void doIntercept(M message,
                                                    MessageDirection direction,
                                                    List<Interceptor> pipeline,
                                                    Consumer<M> handler,
                                                    Consumer<Response<?>> responseCallback)
    {
        // Make sure we pass the response callback properly if it's a two-way request and we're on the sending side
        assert message.verb().isOneWay() || !(message instanceof Request) || direction != MessageDirection.SENDING || responseCallback != null;
        InterceptionContext<M> ctx = new InterceptionContext<>(direction, handler, responseCallback, pipeline);
        ctx.passDown(message);
    }
}
