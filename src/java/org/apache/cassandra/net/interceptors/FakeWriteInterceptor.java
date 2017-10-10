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
import com.google.common.collect.Sets;

import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;

/**
 * An internode message interceptor for writes that drop requests it intercepts but acknowledge them to the
 * sender node nonetheless.
 * <p>
 * Contrarily to {@link DroppingInterceptor}, messages intercepted by this interceptor will never yield timeout exceptions
 * on the sending node, nor trigger hinted-handoffs, since as far as the sending node is concerned, the write will be
 * successful. However, the write won't be applied nor persisted by the node on which this is on.
 * <p>
 * This is useful to simulate a node completely missing data but without having to shutdown the node nor having to
 * disable hinted handoffs and the like.
 * <p>
 * Note that this interceptor is limited to (non-LWT) writes as "faking" acknowledgement makes less sense/is less useful
 * for other message types. This does mean that while it extends {@link AbstractInterceptor}, this interceptor ignores
 * the {@code -Ddse.net.interceptors.intercepted*} properties.
 *
 * @see AbstractInterceptor for details on the options supported by this interceptor
 */
public class FakeWriteInterceptor extends AbstractInterceptor
{
    // Listing verbs manually so we're sure we include only AckedRequest verbs
    private static final ImmutableSet<Verb<?, ?>> intercepted = ImmutableSet.of(Verbs.WRITES.WRITE,
                                                                                Verbs.WRITES.VIEW_WRITE,
                                                                                Verbs.WRITES.BATCH_STORE,
                                                                                Verbs.WRITES.COUNTER_FORWARDING,
                                                                                Verbs.WRITES.READ_REPAIR,
                                                                                Verbs.HINTS.HINT);

    public FakeWriteInterceptor(String name)
    {
        super(name,
              intercepted,
              Sets.immutableEnumSet(Message.Type.REQUEST),
              Sets.immutableEnumSet(MessageDirection.RECEIVING),
              Sets.immutableEnumSet(Message.Locality.all()));
    }

    @Override
    protected boolean allowModifyingIntercepted()
    {
        return false;
    }

    protected <M extends Message<?>> void handleIntercepted(M message, InterceptionContext<M> context)
    {
        // We only intercept this and don't let users change it.
        assert message.isRequest()
               && intercepted.contains(message.verb())
               && context.direction() == MessageDirection.RECEIVING;

        @SuppressWarnings("unchecked")
        Request<?, EmptyPayload> request = (Request<?, EmptyPayload>) message;

        // We want to drop the message but respond with a "fake" ack instead
        context.drop(message);
        context.responseCallback().accept(request.respond(EmptyPayload.instance));
    }
}
