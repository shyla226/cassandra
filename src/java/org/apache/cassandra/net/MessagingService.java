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


import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.*;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.net.ssl.SSLHandshakeException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.jctools.maps.NonBlockingHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.ILatencySubscriber;
import org.apache.cassandra.metrics.ConnectionMetrics;
import org.apache.cassandra.metrics.DroppedMessageMetrics;
import org.apache.cassandra.metrics.MessagingMetrics;
import org.apache.cassandra.net.interceptors.Interceptor;
import org.apache.cassandra.net.interceptors.Interceptors;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.*;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

public final class MessagingService implements MessagingServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(MessagingService.class);

    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=MessagingService";

    public static final MessagingVersion current_version = MessagingVersion.DSE_60;

    /**
     * We preface every connection handshake with this number so the recipient can validate
     * the sender is sane. For legacy OSS protocol, this is done for every message.
     */
    public static final int PROTOCOL_MAGIC = 0xCA552DFA;

    private static final AtomicInteger idGen = new AtomicInteger(0);

    /**
     * Message interceptors: either populated in the ctor through {@link Interceptors#load(String)}, or programmatically by
     * calling {@link #addInterceptor}/{@link #clearInterceptors()} for tests.
     */
    private final Interceptors messageInterceptors = new Interceptors();

    public final MessagingMetrics metrics = new MessagingMetrics();

    public static final long STARTUP_TIME = System.nanoTime();

    /* This records all the results mapped by message Id */
    private final ExpiringMap<Integer, CallbackInfo<?>> callbacks;

    @VisibleForTesting
    final ConcurrentMap<InetAddress, OutboundTcpConnectionPool> connectionManagers = new NonBlockingHashMap<>();

    private final List<SocketThread> socketThreads = Lists.newArrayList();
    private final SimpleCondition listenGate;

    private final DroppedMessages droppedMessages = new DroppedMessages();

    private final List<ILatencySubscriber> subscribers = new CopyOnWriteArrayList<>();

    // protocol versions of the other nodes in the cluster
    private final ConcurrentMap<InetAddress, MessagingVersion> versions = new NonBlockingHashMap<>();

    // back-pressure implementation
    private final BackPressureStrategy backPressure = DatabaseDescriptor.getBackPressureStrategy();

    private static class MSHandle
    {
        public static final MessagingService instance = new MessagingService(false);
    }

    public static MessagingService instance()
    {
        return MSHandle.instance;
    }

    private static class MSTestHandle
    {
        public static final MessagingService instance = new MessagingService(true);
    }

    static MessagingService test()
    {
        return MSTestHandle.instance;
    }

    private MessagingService(boolean testOnly)
    {
        listenGate = new SimpleCondition();

        if (!testOnly)
            droppedMessages.scheduleLogging();

        Consumer<Pair<Integer, ExpiringMap.ExpiringObject<CallbackInfo<?>>>> timeoutReporter = pair ->
        {
            CallbackInfo expiredCallbackInfo = pair.right.get();
            MessageCallback<?> callback = expiredCallbackInfo.callback;
            InetAddress target = expiredCallbackInfo.target;

            addLatency(expiredCallbackInfo.verb, target, pair.right.timeoutMillis());

            ConnectionMetrics.totalTimeouts.mark();
            OutboundTcpConnectionPool cp = getConnectionPool(expiredCallbackInfo.target).join();
            if (cp != null)
                cp.incrementTimeout();

            updateBackPressureOnReceive(target, expiredCallbackInfo.verb, true).join();
            expiredCallbackInfo.responseExecutor.execute(() -> callback.onTimeout(target), null);
        };
        callbacks = new ExpiringMap<>(DatabaseDescriptor.getMinRpcTimeout(), timeoutReporter);

        if (!testOnly)
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            try
            {
                mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        messageInterceptors.load();
    }

    public void addInterceptor(Interceptor interceptor)
    {
        this.messageInterceptors.add(interceptor);
    }

    public void removeInterceptor(Interceptor interceptor)
    {
        this.messageInterceptors.remove(interceptor);
    }

    public void clearInterceptors()
    {
        this.messageInterceptors.clear();
    }

    static int newMessageId()
    {
        return idGen.incrementAndGet();
    }

    /**
     * Sends a request message, setting the provided callback to be called with the response received.
     *
     * @param request the request to send.
     * @param callback the callback to set for the response to {@code request}.
     */
    public <Q> void send(Request<?, Q> request, MessageCallback<Q> callback)
    {
        if (request.isLocal())
        {
            deliverLocally(request, callback);
        }
        else
        {
            registerCallback(request, callback);
            ClientWarn.instance.storeForRequest(request.id());
            updateBackPressureOnSend(request);
            sendRequest(request, callback);
        }
    }

    /**
     * Sends the requests for a provided dispatcher, setting the provided callback to be called with received responses.
     *
     * @param dispatcher the dispatcher to use to generate requests to send.
     * @param callback the callback to set for responses to the request from {@code dispatcher}.
     */
    public <Q> void send(Request.Dispatcher<?, Q> dispatcher, MessageCallback<Q> callback)
    {
        assert callback != null && !dispatcher.verb().isOneWay();

        for (Request<?, Q> request : dispatcher.remoteRequests())
            send(request, callback);

        // Note that it's important that local delivery is done _after_ sending the other request, because if the are
        // already on the right thread, said local delivery will be done inline.
        if (dispatcher.hasLocalRequest())
            deliverLocally(dispatcher.localRequest(), callback);
    }

    private <P, Q> void deliverLocally(Request<P, Q> request, MessageCallback<Q> callback)
    {
        Consumer<Response<Q>> handleResponse = response ->
                                               request.responseExecutor()
                                                      .execute(() -> deliverLocalResponse(request, response, callback),
                                                               ExecutorLocals.create());

        Consumer<Response<Q>> onResponse = messageInterceptors.isEmpty()
                                           ? handleResponse
                                           : r -> messageInterceptors.intercept(r, handleResponse, null);

        Runnable onAborted = () ->
                             request.responseExecutor().execute(() -> {
                                 Tracing.trace("Discarding partial local response (timed out)");
                                 MessagingService.instance().incrementDroppedMessages(request);
                                 callback.onTimeout(FBUtilities.getBroadcastAddress());
                             }, ExecutorLocals.create());

        Consumer<Request<P, Q>> consumer = rq ->
        {
            if (rq.isTimedOut(ApproximateTime.currentTimeMillis()))
            {
                onAborted.run();
                return;
            }
            rq.execute(onResponse, onAborted);
        };
        deliverLocallyInternal(request, consumer, callback);
    }

    private <P, Q> void deliverLocalResponse(Request<P, Q> request, Response<Q> response, MessageCallback<Q> callback)
    {
        addLatency(request.verb(), request.to(), request.lifetimeMillis());
        response.deliverTo(callback);
    }

    private <P, Q> void registerCallback(Request<P, Q> request, MessageCallback<Q> callback)
    {
        long startTime = request.operationStartMillis();
        long timeout = request.timeoutMillis();
        TracingAwareExecutor executor = request.responseExecutor();

        for (Request.Forward forward : request.forwards())
            registerCallback(forward.id, forward.to, request.verb(), callback, startTime, timeout, executor);

        registerCallback(request.id(), request.to(), request.verb(), callback, startTime, timeout, executor);
    }

    private <Q> void registerCallback(int id,
                                      InetAddress to,
                                      Verb<?, Q> type,
                                      MessageCallback<Q> callback,
                                      long startTimeMillis,
                                      long timeout,
                                      TracingAwareExecutor executor)
    {
        timeout += DatabaseDescriptor.getEndpointSnitch().getCrossDcRttLatency(to);
        CallbackInfo previous = callbacks.put(id, new CallbackInfo<>(to, callback, type, executor, startTimeMillis), timeout);
        assert previous == null : String.format("Callback already exists for id %d! (%s)", id, previous);
    }

    /**
     * Sends a request and returns a future on the reception of the response.
     *
     * @param request the request to send.
     * @return a future on the reception of the response payload to {@code request}. The future may either complete
     * sucessfully, complete exceptionally with an {@link InternalRequestExecutionException} if destination sends back
     * an error, or complete exceptionally with a {@link CallbackExpiredException} if no responses is received before
     * the timeout on the message.
     */
    public <Q> CompletableFuture<Q> sendSingleTarget(Request<?, Q> request)
    {
        SingleTargetCallback<Q> callback = new SingleTargetCallback<>();
        send(request, callback);
        return callback;
    }

    /**
     * Forwards a request to its final destination.
     * <p>
     * This doesn't set a callback since the final destination will respond to initial sender (which has set the proper
     * callbacks).
     */
    void forward(ForwardRequest<?, ?> request)
    {
        // Note that we don't attempt to intercept requests for which we're just the forwarder. Those will be intercepted
        // on the final receiver if needs be.
        sendInternal(request);
    }

    /**
     * Sends a one-way request.
     * <p>
     * Note that this is a fire-and-forget type of method: the method returns as soon as the request has been set to
     * be send and there is no way to know if the request has been successfully delivered or not.
     *
     * @param request the request to send.
     */
    public void send(OneWayRequest<?> request)
    {
        if (request.isLocal())
            deliverLocallyOneWay(request);
        else
            sendRequest(request, null);
    }

    /**
     * Sends the one-way requests generated by the provided dispatcher.
     * <p>
     * Note that this is a fire-and-forget type of method: the method returns as soon as the requets have been set to
     * be send and there is no way to know if the requests have been successfully delivered or not.
     *
     * @param dispatcher the dispatcher to use to generate the one-way requests to send.
     */
    public void send(OneWayRequest.Dispatcher<?> dispatcher)
    {
        for (OneWayRequest<?> request : dispatcher.remoteRequests())
            sendRequest(request, null);

        // Note that it's important that local delivery is done _after_ sending the other request, because if the are
        // already on the right thread, said local delivery will be done inline.
        if (dispatcher.hasLocalRequest())
            deliverLocallyOneWay(dispatcher.localRequest());
    }

    private void deliverLocallyOneWay(OneWayRequest<?> request)
    {
        deliverLocallyInternal(request, r -> r.execute(r.verb().EMPTY_RESPONSE_CONSUMER, () -> {}), null);
    }

    /**
     * Sends a response message.
     */
    void reply(Response<?> response)
    {
        Tracing.trace("Enqueuing {} response to {}", response.verb(), response.from());
        sendResponse(response);
    }

    private <P, Q> void sendRequest(Request<P, Q> request, MessageCallback<Q> callback)
    {
        messageInterceptors.interceptRequest(request, this::sendInternal, callback);
    }

    private <Q> void sendResponse(Response<Q> response)
    {
        messageInterceptors.intercept(response, this::sendInternal, null);
    }

    private CompletableFuture<Void> sendInternal(Message<?> message)
    {
        if (logger.isTraceEnabled())
            logger.trace("Sending {}", message);

        return getConnection(message).thenAccept(cp -> cp.enqueue(message));
    }

    private <P, Q, M extends Request<P, Q>> void deliverLocallyInternal(M request,
                                                                        Consumer<M> consumer,
                                                                        MessageCallback<Q> callback)
    {
        try
        {
            Consumer<M> delivery = rq -> rq.requestExecutor().execute(() -> consumer.accept(rq), ExecutorLocals.create());
            messageInterceptors.interceptRequest(request, delivery, callback);
        }
        catch (Throwable t)
        {
            // For now the only case that we intentionally target with this is rejecting the MessageDeliveryTask by the
            // event loop. If anything else triggers this log, we should make sure that it didn't fall through the
            // cracks by chance while it was meant to be handled by another component.
            logger.error("Error while locally processing {} request:", request.verb(), t);
            callback.onFailure(request.respondWithFailure(RequestFailureReason.UNKNOWN));
        }
    }

    /**
     * Updates the back-pressure state on sending the provided message.
     *
     * @param request The request sent.
     */
    <Q> CompletableFuture<Void> updateBackPressureOnSend(Request<?, Q> request)
    {
        if (request.verb().supportsBackPressure() && DatabaseDescriptor.backPressureEnabled())
        {
            return getConnectionPool(request.to()).thenAccept(cp -> {
                if (cp != null)
                {
                    BackPressureState backPressureState = cp.getBackPressureState();
                    backPressureState.onRequestSent(request);
                }
            });
        }

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Updates the back-pressure state on reception from the given host if enabled and the given message callback supports it.
     *
     * @param host The replica host the back-pressure state refers to.
     * @param verb The message verb.
     * @param timeout True if updated following a timeout, false otherwise.
     */
    CompletableFuture<Void> updateBackPressureOnReceive(InetAddress host, Verb<?, ?> verb, boolean timeout)
    {
        if (verb.supportsBackPressure() && DatabaseDescriptor.backPressureEnabled())
        {
            return getConnectionPool(host).thenAccept(cp -> {
                if (cp != null)
                {
                    BackPressureState backPressureState = cp.getBackPressureState();
                    if (!timeout)
                        backPressureState.onResponseReceived();
                    else
                        backPressureState.onResponseTimeout();
                }
            });
        }

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Applies back-pressure for the given hosts, according to the configured strategy.
     *
     * If the local host is present, it is removed from the pool, as back-pressure is only applied
     * to remote hosts.
     *
     * @param hosts The hosts to apply back-pressure to.
     * @param timeoutInNanos The max back-pressure timeout.
     */
    @SuppressWarnings("unchecked")
    public CompletableFuture<Void> applyBackPressure(Iterable<InetAddress> hosts, long timeoutInNanos)
    {
        if (DatabaseDescriptor.backPressureEnabled())
        {
            Set<BackPressureState> states = new HashSet<BackPressureState>();
            CompletableFuture<Void> future = null;
            for (InetAddress host : hosts)
            {
                if (host.equals(FBUtilities.getBroadcastAddress()))
                    continue;

                CompletableFuture<Void> next = getConnectionPool(host).thenAccept(cp -> {
                    if (cp != null)
                        states.add(cp.getBackPressureState());
                });

                if (future == null)
                    future = next;
                else
                    future = future.thenAcceptBoth(next, (a,b) -> {});
            }

            if (future != null)
            {
                return future.thenCompose(ignored -> backPressure.apply(states, timeoutInNanos, TimeUnit.NANOSECONDS));
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Track latency information for the dynamic snitch.
     *
     * @param verb the verb for which we're adding latency information.
     * @param address the host that replied to the message for which we're adding latency.
     * @param latency the latency to record in milliseconds
     */
    void addLatency(Verb<?, ?> verb, InetAddress address, long latency)
    {
        for (ILatencySubscriber subscriber : subscribers)
            subscriber.receiveTiming(verb, address, latency);
    }

    /**
     * called from gossiper when it notices a node is not responding.
     */
    public CompletableFuture<Void> convict(InetAddress ep)
    {
        return getConnectionPool(ep).thenAccept(cp -> {
            if (cp != null)
            {
                logger.debug("Resetting pool for " + ep);
                cp.reset();
            }
            else
            {
                logger.debug("Not resetting pool for {} because internode authenticator said not to connect", ep);
            }
        });
    }

    public void listen()
    {
        callbacks.reset(); // hack to allow tests to stop/restart MS
        listen(FBUtilities.getLocalAddress());
        if (DatabaseDescriptor.shouldListenOnBroadcastAddress()
            && !FBUtilities.getLocalAddress().equals(FBUtilities.getBroadcastAddress()))
        {
            listen(FBUtilities.getBroadcastAddress());
        }
        listenGate.signalAll();
    }

    /**
     * Listen on the specified port.
     *
     * @param localEp InetAddress whose port to listen on.
     */
    private void listen(InetAddress localEp) throws ConfigurationException
    {
        for (ServerSocket ss : getServerSockets(localEp))
        {
            SocketThread th = new SocketThread(ss, "ACCEPT-" + localEp);
            th.start();
            socketThreads.add(th);
        }
    }

    @SuppressWarnings("resource")
    private List<ServerSocket> getServerSockets(InetAddress localEp) throws ConfigurationException
    {
        final List<ServerSocket> ss = new ArrayList<ServerSocket>(2);
        if (DatabaseDescriptor.getServerEncryptionOptions().internode_encryption != ServerEncryptionOptions.InternodeEncryption.none)
        {
            try
            {
                ss.add(SSLFactory.getServerSocket(DatabaseDescriptor.getServerEncryptionOptions(), localEp, DatabaseDescriptor.getSSLStoragePort()));
            }
            catch (IOException e)
            {
                throw new ConfigurationException("Unable to create ssl socket", e);
            }
            // setReuseAddress happens in the factory.
            logger.info("Starting Encrypted Messaging Service on SSL port {}", DatabaseDescriptor.getSSLStoragePort());
        }

        if (DatabaseDescriptor.getServerEncryptionOptions().internode_encryption != ServerEncryptionOptions.InternodeEncryption.all)
        {
            ServerSocketChannel serverChannel = null;
            try
            {
                serverChannel = ServerSocketChannel.open();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            ServerSocket socket = serverChannel.socket();
            try
            {
                socket.setReuseAddress(true);
            }
            catch (SocketException e)
            {
                FileUtils.closeQuietly(socket);
                throw new ConfigurationException("Insufficient permissions to setReuseAddress", e);
            }
            InetSocketAddress address = new InetSocketAddress(localEp, DatabaseDescriptor.getStoragePort());
            try
            {
                socket.bind(address,500);
            }
            catch (BindException e)
            {
                FileUtils.closeQuietly(socket);
                if (e.getMessage().contains("in use"))
                    throw new ConfigurationException(address + " is in use by another process.  Change listen_address:storage_port in cassandra.yaml to values that do not conflict with other services");
                else if (e.getMessage().contains("Cannot assign requested address"))
                    throw new ConfigurationException("Unable to bind to address " + address
                                                     + ". Set listen_address in cassandra.yaml to an interface you can bind to, e.g., your private IP address on EC2");
                else
                    throw new RuntimeException(e);
            }
            catch (IOException e)
            {
                FileUtils.closeQuietly(socket);
                throw new RuntimeException(e);
            }
            String nic = FBUtilities.getNetworkInterface(localEp);
            logger.info("Starting Messaging Service on {}:{}{}", localEp, DatabaseDescriptor.getStoragePort(),
                        nic == null? "" : String.format(" (%s)", nic));
            ss.add(socket);
        }
        return ss;
    }

    public void waitUntilListening()
    {
        try
        {
            listenGate.await();
        }
        catch (InterruptedException ie)
        {
            logger.trace("await interrupted");
        }
    }

    public boolean isListening()
    {
        return listenGate.isSignaled();
    }

    public void destroyConnectionPool(InetAddress to)
    {
        logger.trace("Destroy pool {}", to);
        OutboundTcpConnectionPool cp = connectionManagers.get(to);
        if (cp == null)
            return;
        cp.close();
        connectionManagers.remove(to);
    }

    /**
     * Get a connection pool to the specified endpoint. Constructs one if none exists.
     *
     * Can return null if the InternodeAuthenticator fails to authenticate the node.
     * @param to
     * @return The connection pool or null if internode authenticator says not to
     */
    public CompletableFuture<OutboundTcpConnectionPool> getConnectionPool(InetAddress to)
    {
        final OutboundTcpConnectionPool cp = connectionManagers.get(to);

        if (cp != null)
        {
            if (cp.isStarted())
                return CompletableFuture.completedFuture(cp);

            return CompletableFuture.supplyAsync(() -> {
                cp.waitForStarted();
                return cp;
            });
        }
        else
        {
            return CompletableFuture.supplyAsync(() -> {

                //Don't attempt to connect to nodes that won't (or shouldn't) authenticate anyways
                if (!DatabaseDescriptor.getInternodeAuthenticator().authenticate(to, OutboundTcpConnectionPool.portFor(to)))
                    return null;

                InetAddress preferredIp = SystemKeyspace.getPreferredIP(to);
                OutboundTcpConnectionPool np = new OutboundTcpConnectionPool(to, preferredIp, backPressure.newState(to));
                OutboundTcpConnectionPool existingPool = connectionManagers.putIfAbsent(to, np);
                if (existingPool != null)
                    np = existingPool;
                else
                    np.start();

                np.waitForStarted();

                return np;
            });
        }
    }

    public boolean hasValidIncomingConnections(InetAddress from, int minAgeInSeconds)
    {
        long now = System.nanoTime();

        for (SocketThread socketThread : socketThreads)
        {
            Collection<Closeable> sockets = socketThread.connections.get(from);
            if (sockets != null)
            {
                for (Closeable socket : sockets)
                {
                    if (socket instanceof IncomingTcpConnection)
                    {
                        //If the node just started up we should skip the conenction time check
                        if ((now - ((IncomingTcpConnection)socket).getConnectTime()) > TimeUnit.SECONDS.toNanos(minAgeInSeconds) ||
                            TimeUnit.NANOSECONDS.toSeconds(now - STARTUP_TIME) < minAgeInSeconds * 2)
                            return true;
                    }
                }
            }
        }

        return false;
    }

    public CompletableFuture<OutboundTcpConnection> getConnection(Message msg)
    {
        return getConnectionPool(msg.to()).thenApply(cp -> cp == null ? null : cp.getConnection(msg));
    }

    public void register(ILatencySubscriber subcriber)
    {
        subscribers.add(subcriber);
    }

    public void clearCallbacksUnsafe()
    {
        callbacks.reset();
    }

    /**
     * Wait for callbacks and don't allow any more to be created (since they could require writing hints)
     */
    public void shutdown()
    {
        logger.info("Waiting for messaging service to quiesce");

        // the important part
        if (!callbacks.shutdownBlocking(DatabaseDescriptor.getMinRpcTimeout() * 2))
            logger.warn("Failed to wait for messaging service callbacks shutdown");

        // attempt to humor tests that try to stop and restart MS
        try
        {
            for (SocketThread th : socketThreads)
                try
                {
                    th.close();
                }
                catch (IOException e)
                {
                    // see https://issues.apache.org/jira/browse/CASSANDRA-10545
                    handleIOExceptionOnClose(e);
                }
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public void receive(Message<?> message)
    {
        messageInterceptors.intercept(message, this::receiveInternal, this::reply);
    }

    private void receiveInternal(Message<?> message)
    {
        TraceState state = Tracing.instance.initializeFromMessage(message);
        if (state != null)
            state.trace("{} message received from {}", message.verb(), message.from());

        ExecutorLocals locals = ExecutorLocals.create(state, ClientWarn.instance.getForMessage(message.id()));
        try
        {
            if (message.isRequest())
                receiveRequestInternal((Request<?, ?>) message, locals);
            else
                receiveResponseInternal((Response<?>) message, locals);
        }
        // Ideally exceptions during remote message handling should be processed in the corresponding message verb
        // handler. This however doesn't happen if the exception happens even before the message got the chance to be
        // handled (e.g. if its task is rejected by its assigned event loop and the corresponding MessageDeliveryTask
        // is not run at all). In this case, if the remote message is a request-response, we can immediately reply
        // with a generic FailureResponse.
        catch (Throwable t)
        {
            // For now the only case that we intentionally target with this is rejecting the MessageDeliveryTask by the
            // event loop. If anything else triggers this log, we should make sure that it didn't fall through the
            // cracks by chance while it was meant to be handled by another component.
            logger.error("Error while receiving {} message from {} :", message.verb(), message.from(), t);
            if (message.isRequest() && !message.verb().isOneWay())
            {
                Request<?, ?> request = (Request<?, ?>) message;
                reply(request.respondWithFailure(RequestFailureReason.UNKNOWN));
            }
        }
    }

    private <P, Q> void receiveRequestInternal(Request<P, Q> request, ExecutorLocals locals)
    {
        request.requestExecutor().execute(  MessageDeliveryTask.forRequest(request), locals);
    }

    private <Q> void receiveResponseInternal(Response<Q> response, ExecutorLocals locals)
    {
        CallbackInfo<Q> info = getRegisteredCallback(response, false);

        // Ignore expired callback info (we already logged in getRegisteredCallback)
        if (info != null)
            info.responseExecutor.execute(MessageDeliveryTask.forResponse(response), locals);
    }

    // Only required by legacy serialization. Can inline in following method when we get rid of that.
    CallbackInfo<?> getRegisteredCallback(int id, boolean remove, InetAddress from)
    {
        ExpiringMap.ExpiringObject<CallbackInfo<?>> expiring = remove ? callbacks.remove(id) : callbacks.get(id);
        if (expiring == null)
        {
            String msg = "Callback already removed for message {} from {}, ignoring response";
            logger.trace(msg, id, from);
            Tracing.trace(msg, id, from);

            return null;
        }
        else
            return expiring.get();
    }

    @SuppressWarnings("unchecked")
    <Q> CallbackInfo<Q> getRegisteredCallback(Response<Q> response, boolean remove)
    {
        return (CallbackInfo<Q>) getRegisteredCallback(response.id(), remove, response.from());
    }

    public static void validateMagic(int magic) throws IOException
    {
        if (magic != PROTOCOL_MAGIC)
            throw new IOException("invalid protocol header");
    }

    public static int getBits(int packed, int start, int count)
    {
        return (packed >>> ((start + 1) - count)) & ~(-1 << count);
    }

    /**
     * @return the last version associated with address, or @param version if this is the first such version
     */
    public MessagingVersion setVersion(InetAddress endpoint, MessagingVersion version)
    {
        logger.trace("Setting version {} for {}", version, endpoint);

        MessagingVersion v = versions.put(endpoint, version);
        return v == null ? version : v;
    }

    public void resetVersion(InetAddress endpoint)
    {
        logger.trace("Resetting version for {}", endpoint);
        versions.remove(endpoint);
    }

    /**
     * Returns the messaging-version as announced by the given node but capped
     * to the min of the version as announced by the node and {@link #current_version}.
     */
    public MessagingVersion getVersion(InetAddress endpoint)
    {
        MessagingVersion v = versions.get(endpoint);
        if (v == null)
        {
            // we don't know the version. assume current. we'll know soon enough if that was incorrect.
            //logger.trace("Assuming current protocol version for {}", endpoint);
            return MessagingService.current_version;
        }
        else
            return MessagingVersion.min(v, MessagingService.current_version);
    }

    @Deprecated
    public int getVersion(String endpoint) throws UnknownHostException
    {
        return getVersion(InetAddress.getByName(endpoint)).protocolVersion().handshakeVersion;
    }

    /**
     * Returns the messaging-version exactly as announced by the given endpoint.
     */
    public MessagingVersion getRawVersion(InetAddress endpoint)
    {
        MessagingVersion v = versions.get(endpoint);
        if (v == null)
            throw new IllegalStateException("getRawVersion() was called without checking knowsVersion() result first");
        return v;
    }

    public boolean knowsVersion(InetAddress endpoint)
    {
        return versions.containsKey(endpoint);
    }

    void incrementDroppedMessages(Message<?> message)
    {
        Verb<?, ?> definition = message.verb();
        assert !definition.isOneWay() : "Shouldn't drop a one-way message";
        if (message.isRequest())
        {
            Object payload = message.payload();
            if (payload instanceof IMutation)
                updateDroppedMutationCount((IMutation) payload);
        }

        droppedMessages.onDroppedMessage(message);
    }


    private void updateDroppedMutationCount(IMutation mutation)
    {
        assert mutation != null : "Mutation should not be null when updating dropped mutations count";

        for (TableId tableId : mutation.getTableIds())
        {
            ColumnFamilyStore cfs = Keyspace.open(mutation.getKeyspaceName()).getColumnFamilyStore(tableId);
            if (cfs != null)
            {
                cfs.metric.droppedMutations.inc();
            }
        }
    }


    @VisibleForTesting
    public static class SocketThread extends Thread
    {
        private final ServerSocket server;
        @VisibleForTesting
        public final Multimap<InetAddress, Closeable> connections = Multimaps.synchronizedMultimap(HashMultimap.create());

        SocketThread(ServerSocket server, String name)
        {
            super(name);
            this.server = server;
        }

        @SuppressWarnings("resource")
        public void run()
        {
            while (!server.isClosed())
            {
                Socket socket = null;
                try
                {
                    socket = server.accept();
                    if (!authenticate(socket))
                    {
                        logger.trace("remote failed to authenticate");
                        socket.close();
                        continue;
                    }

                    socket.setKeepAlive(true);
                    socket.setSoTimeout(2 * OutboundTcpConnection.WAIT_FOR_VERSION_MAX_TIME);
                    // determine the connection type to decide whether to buffer
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    MessagingService.validateMagic(in.readInt());
                    int header = in.readInt();
                    boolean isStream = MessagingService.getBits(header, 3, 1) == 1;
                    ProtocolVersion version = ProtocolVersion.fromProtocolHeader(header);
                    logger.trace("Connection version {} from {}", version, socket.getInetAddress());
                    socket.setSoTimeout(0);

                    Thread thread = isStream
                                  ? new IncomingStreamingConnection(version, socket, connections)
                                  : new IncomingTcpConnection(version, MessagingService.getBits(header, 2, 1) == 1, socket, connections);
                    thread.start();

                    //The connections are removed from this collection when they are closed.
                    connections.put(socket.getInetAddress(), (Closeable) thread);
                }
                catch (AsynchronousCloseException e)
                {
                    // this happens when another thread calls close().
                    logger.trace("Asynchronous close seen by server thread");
                    break;
                }
                catch (ClosedChannelException e)
                {
                    logger.trace("MessagingService server thread already closed");
                    break;
                }
                catch (SSLHandshakeException e)
                {
                    logger.error("SSL handshake error for inbound connection from " + socket, e);
                    FileUtils.closeQuietly(socket);
                }
                catch (Throwable t)
                {
                    logger.trace("Error reading the socket {}", socket, t);
                    FileUtils.closeQuietly(socket);
                }
            }
            logger.info("MessagingService has terminated the accept() thread");
        }

        void close() throws IOException
        {
            logger.trace("Closing accept() thread");

            try
            {
                server.close();
            }
            catch (IOException e)
            {
                // see https://issues.apache.org/jira/browse/CASSANDRA-8220
                // see https://issues.apache.org/jira/browse/CASSANDRA-12513
                handleIOExceptionOnClose(e);
            }
            synchronized (connections)
            {
                // make a copy to avoid concurrent modification exceptions in close()
                for (Closeable connection : Lists.newArrayList(connections.values()))
                    connection.close();
            }
        }

        private boolean authenticate(Socket socket)
        {
            return DatabaseDescriptor.getInternodeAuthenticator().authenticate(socket.getInetAddress(), socket.getPort());
        }
    }

    private static void handleIOExceptionOnClose(IOException e) throws IOException
    {
        // dirty hack for clean shutdown on OSX w/ Java >= 1.8.0_20
        // see https://bugs.openjdk.java.net/browse/JDK-8050499;
        // also CASSANDRA-12513
        if (FBUtilities.isMacOSX)
        {
            switch (e.getMessage())
            {
                case "Unknown error: 316":
                case "No such file or directory":
                    return;
            }
        }

        throw e;
    }

    public Map<String, Integer> getLargeMessagePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<>(connectionManagers.size());
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            pendingTasks.put(entry.getKey().getHostAddress(), entry.getValue().large().getPendingMessages());
        return pendingTasks;
    }

    public Map<String, Long> getLargeMessageCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<>(connectionManagers.size());
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            completedTasks.put(entry.getKey().getHostAddress(), entry.getValue().large().getCompletedMesssages());
        return completedTasks;
    }

    public Map<String, Long> getLargeMessageDroppedTasks()
    {
        Map<String, Long> droppedTasks = new HashMap<>(connectionManagers.size());
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            droppedTasks.put(entry.getKey().getHostAddress(), entry.getValue().large().getDroppedMessages());
        return droppedTasks;
    }

    public Map<String, Integer> getSmallMessagePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<>(connectionManagers.size());
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            pendingTasks.put(entry.getKey().getHostAddress(), entry.getValue().small().getPendingMessages());
        return pendingTasks;
    }

    public Map<String, Long> getSmallMessageCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<>(connectionManagers.size());
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            completedTasks.put(entry.getKey().getHostAddress(), entry.getValue().small().getCompletedMesssages());
        return completedTasks;
    }

    public Map<String, Long> getSmallMessageDroppedTasks()
    {
        Map<String, Long> droppedTasks = new HashMap<>(connectionManagers.size());
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            droppedTasks.put(entry.getKey().getHostAddress(), entry.getValue().small().getDroppedMessages());
        return droppedTasks;
    }

    public Map<String, Integer> getGossipMessagePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<>(connectionManagers.size());
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            pendingTasks.put(entry.getKey().getHostAddress(), entry.getValue().gossip().getPendingMessages());
        return pendingTasks;
    }

    public Map<String, Long> getGossipMessageCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<>(connectionManagers.size());
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            completedTasks.put(entry.getKey().getHostAddress(), entry.getValue().gossip().getCompletedMesssages());
        return completedTasks;
    }

    public Map<String, Long> getGossipMessageDroppedTasks()
    {
        Map<String, Long> droppedTasks = new HashMap<>(connectionManagers.size());
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            droppedTasks.put(entry.getKey().getHostAddress(), entry.getValue().gossip().getDroppedMessages());
        return droppedTasks;
    }

    public Map<String, Integer> getDroppedMessages()
    {
        return droppedMessages.getSnapshot();
    }

    /**
     * @return Dropped message metrics by group with full access to all underlying metrics.
     * Used by DSE's DroppedMessagesWriter
     */
    public Map<DroppedMessages.Group, DroppedMessageMetrics> getDroppedMessagesWithAllMetrics()
    {
        return droppedMessages.getAllMetrics();
    }

    public long getTotalTimeouts()
    {
        return ConnectionMetrics.totalTimeouts.getCount();
    }

    public Map<String, Long> getTimeoutsPerHost()
    {
        Map<String, Long> result = new HashMap<String, Long>(connectionManagers.size());
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry: connectionManagers.entrySet())
        {
            String ip = entry.getKey().getHostAddress();
            long recent = entry.getValue().getTimeouts();
            result.put(ip, recent);
        }
        return result;
    }

    public Map<String, Double> getBackPressurePerHost()
    {
        Map<String, Double> map = new HashMap<>(connectionManagers.size());
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            map.put(entry.getKey().getHostAddress(), entry.getValue().getBackPressureState().getBackPressureRateLimit());

        return map;
    }

    @Override
    public void setBackPressureEnabled(boolean enabled)
    {
        DatabaseDescriptor.setBackPressureEnabled(enabled);
    }

    @Override
    public boolean isBackPressureEnabled()
    {
        return DatabaseDescriptor.backPressureEnabled();
    }

    public static IPartitioner globalPartitioner()
    {
        return StorageService.instance.getTokenMetadata().partitioner;
    }

    public static void validatePartitioner(Collection<? extends AbstractBounds<?>> allBounds)
    {
        for (AbstractBounds<?> bounds : allBounds)
            validatePartitioner(bounds);
    }

    public static void validatePartitioner(AbstractBounds<?> bounds)
    {
        if (globalPartitioner() != bounds.left.getPartitioner())
            throw new AssertionError(String.format("Partitioner in bounds serialization. Expected %s, was %s.",
                                                   globalPartitioner().getClass().getName(),
                                                   bounds.left.getPartitioner().getClass().getName()));
    }

    @VisibleForTesting
    public List<SocketThread> getSocketThreads()
    {
        return socketThreads;
    }
}
