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
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Iterables;

import org.apache.cassandra.exceptions.RequestFailureReason;

/**
 * A request for a one-way verb, that is a request for which no response is sent.
 */
public class OneWayRequest<P> extends Request<P, NoResponse>
{
    private static final Runnable NOOP = () -> {};

    OneWayRequest(InetAddress from,
                  InetAddress to,
                  Verb.OneWay<P> verb,
                  Message.Data<P> data)
    {
        this(from,
             to,
             verb,
             data,
             Collections.emptyList());
    }

    OneWayRequest(InetAddress from,
                  InetAddress to,
                  Verb.OneWay<P> verb,
                  Message.Data<P> data,
                  List<Forward> forwards)
    {
        super(from, to, -1, verb, data, forwards);
    }

    /**
     * Executes the request (using the handler of the verb this is a request of).
     * <p>
     * Note that this method mostly delegate the work to {@code verb().handler()} which decides if the method is
     * synchronous or not and so this method may or may not block.
     */
    void execute()
    {
        assert verb.isOneWay();
        execute(verb.EMPTY_RESPONSE_CONSUMER, NOOP);
    }

    @Override
    public Verb.OneWay<P> verb()
    {
        return (Verb.OneWay<P>)super.verb();
    }

    @Override
    public OneWayRequest<P> addParameters(MessageParameters parameters)
    {
        return new OneWayRequest<>(from(), to(), verb(), messageData.withAddedParameters(parameters));
    }

    @Override
    public Response<NoResponse> respond(NoResponse payload)
    {
        throw new UnsupportedOperationException();

    }

    @Override
    public FailureResponse<NoResponse> respondWithFailure(RequestFailureReason reason)
    {
        throw new UnsupportedOperationException();
    }

    static class Dispatcher<P> extends Request.Dispatcher<P, NoResponse>
    {
        Dispatcher(MessageTargets targets,
                   Verb.OneWay<P> verb,
                   Message.Data<P> messageData)
        {
            super(targets, verb, messageData);
        }

        Verb.OneWay<P> verb()
        {
            return (Verb.OneWay<P>) verb;
        }

        @Override
        Iterable<OneWayRequest<P>> remoteRequests()
        {
            Iterable<OneWayRequest<P>> withoutForwards = Iterables.transform(targets.nonForwardingRemotes(),
                                                                             to -> verb().newRequest(to, messageData));

            if (!targets.hasForwards())
                return withoutForwards;

            return Iterables.concat(withoutForwards,
                                    Iterables.<MessageTargets.WithForwards, OneWayRequest<P>>transform(targets.remotesWithForwards(), t -> verb().newRequestWithForwards(t.target, messageData, t.forwards)));
        }

        OneWayRequest<P> localRequest()
        {
            return verb().newRequest(local, messageData);
        }
    }
}
