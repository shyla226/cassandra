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
package org.apache.cassandra.transport;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.reactivex.Single;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;

public class ServerConnection extends Connection
{
    private static final Logger logger = LoggerFactory.getLogger(ServerConnection.class);
    private enum State { UNINITIALIZED, AUTHENTICATION, READY }

    private volatile IAuthenticator.SaslNegotiator saslNegotiator;
    private final ClientState clientState;
    private volatile State state;
    private AtomicLong inFlightRequests;

    public ServerConnection(Channel channel, ProtocolVersion version, Connection.Tracker tracker)
    {
        super(channel, version, tracker);
        this.clientState = ClientState.forExternalCalls(channel.remoteAddress(), this);
        this.state = State.UNINITIALIZED;
        this.inFlightRequests = new AtomicLong(0L);
    }

    public Single<QueryState> validateNewMessage(Message.Request request, ProtocolVersion version)
    {
        Message.Type type = request.type;

        switch (state)
        {
            case UNINITIALIZED:
                if (type != Message.Type.STARTUP && type != Message.Type.OPTIONS)
                    throw new ProtocolException(String.format("Unexpected message %s, expecting STARTUP or OPTIONS", type));
                break;
            case AUTHENTICATION:
                if (type != Message.Type.AUTH_RESPONSE)
                    throw new ProtocolException(String.format("Unexpected message %s, expecting SASL_RESPONSE", type));
                break;
            case READY:
                if (type == Message.Type.STARTUP)
                    throw new ProtocolException("Unexpected message STARTUP, the connection is already initialized");
                break;
            default:
                throw new AssertionError();
        }

        if (clientState.getUser() == null)
            return Single.just(new QueryState(clientState, null));

        return DatabaseDescriptor.getAuthManager()
                                 .getUserRolesAndPermissions(clientState.getUser())
                                 .map(u -> new QueryState(clientState, request.getStreamId(), u));
    }

    public void applyStateTransition(Message.Type requestType, Message.Type responseType)
    {
        switch (state)
        {
            case UNINITIALIZED:
                if (requestType == Message.Type.STARTUP)
                {
                    if (responseType == Message.Type.AUTHENTICATE)
                        state = State.AUTHENTICATION;
                    else if (responseType == Message.Type.READY)
                        state = State.READY;
                }
                break;
            case AUTHENTICATION:
                assert requestType == Message.Type.AUTH_RESPONSE;

                if (responseType == Message.Type.AUTH_SUCCESS)
                {
                    state = State.READY;
                    // we won't use the authenticator again, null it so that it can be GC'd
                    saslNegotiator = null;
                }
                break;
            case READY:
                break;
            default:
                throw new AssertionError();
        }
    }

    public IAuthenticator.SaslNegotiator getSaslNegotiator()
    {
        if (saslNegotiator == null)
            saslNegotiator = DatabaseDescriptor.getAuthenticator().newSaslNegotiator(getClientAddress());
        return saslNegotiator;
    }

    /**
     * Increment the number of in-flight requests, i.e. the number of RX chains that have been
     * created to handle a client request. This is currently called just before subscribing, but
     * once we are sure the chain exists.
     */
    public void onNewRequest()
    {
        inFlightRequests.incrementAndGet();
    }

    /**
     * Decrement the number of in-flight requests, i.e. the number of RX chains servicing client
     * requests. This is currently called when the chain completes, either successfully or with
     * an error.
     */
    public void onRequestCompleted()
    {
        inFlightRequests.decrementAndGet();
    }

    protected InetSocketAddress getRemoteAddress()
    {
        return clientState.isInternal
             ? null
             : clientState.getRemoteAddress();
    }

    protected final InetAddress getClientAddress()
    {
        InetSocketAddress socketAddress = getRemoteAddress();
        return socketAddress == null ? null : socketAddress.getAddress();
    }

    /**
     * Return a future that will complete when the number of in flight requests is zero.
     * <p>
     * This method should be called after the channel has been closed so that there are no
     * more incoming client requests. It is currently used to ensure that no RX chains are still
     * active when draining a node, since such chains could cause errors if other services
     * are stopped, for example stopping messaging service or the mutation stages causes
     * execution rejected exceptions.
     *
     * @return a future that will complete when the number of in flight requests is zero.
     */
    public CompletableFuture<Void> waitForInFlightRequests()
    {
        if (logger.isTraceEnabled())
            logger.trace("Waiting for {} in flight requests to complete", inFlightRequests.get());

        if (inFlightRequests.get() == 0)
            return CompletableFuture.completedFuture(null);

        final CompletableFuture<Void> ret = new CompletableFuture<>();
        StageManager.getScheduler(Stage.REQUEST_RESPONSE).scheduleDirect(() -> checkInFlightRequests(ret), 1, TimeUnit.MILLISECONDS);
        return ret;
    }

    private void checkInFlightRequests(final CompletableFuture<Void> fut)
    {
        if (inFlightRequests.get() == 0)
            fut.complete(null);
        else
            StageManager.getScheduler(Stage.REQUEST_RESPONSE).scheduleDirect(() -> checkInFlightRequests(fut), 1, TimeUnit.MILLISECONDS);
    }
}
