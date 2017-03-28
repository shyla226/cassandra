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
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.net.ssl.SSLHandshakeException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.LocalAwareExecutorService;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.ILatencySubscriber;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.ConnectionMetrics;
import org.apache.cassandra.metrics.DroppedMessageMetrics;
import org.apache.cassandra.metrics.MessagingMetrics;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.*;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

public final class MessagingService implements MessagingServiceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=MessagingService";

    public static final MessagingVersion current_version = MessagingVersion.DSE_60;

    /**
     * We preface every connection handshake with this number so the recipient can validate
     * the sender is sane. For legacy OSS protocol, this is done for every message.
     */
    public static final int PROTOCOL_MAGIC = 0xCA552DFA;

    private static final AtomicInteger idGen = new AtomicInteger(0);

    public final MessagingMetrics metrics = new MessagingMetrics();

    /* This records all the results mapped by message Id */
    private final ExpiringMap<Integer, CallbackInfo<?>> callbacks;

    @VisibleForTesting
    final ConcurrentMap<InetAddress, OutboundTcpConnectionPool> connectionManagers = new NonBlockingHashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(MessagingService.class);
    private static final int LOG_DROPPED_INTERVAL_IN_MS = 5000;

    private final List<SocketThread> socketThreads = Lists.newArrayList();
    private final SimpleCondition listenGate;

    private static final class DroppedMessages
    {
        DroppedMessageMetrics metrics; // not final for testing reasons only
        final AtomicInteger droppedInternal;
        final AtomicInteger droppedCrossNode;

        DroppedMessages(Verb<?, ?> verb)
        {
            this(new DroppedMessageMetrics(verb));
        }

        DroppedMessages(DroppedMessageMetrics metrics)
        {
            this.metrics = metrics;
            this.droppedInternal = new AtomicInteger(0);
            this.droppedCrossNode = new AtomicInteger(0);
        }

        @VisibleForTesting
        void reset(String scope)
        {
            this.metrics = new DroppedMessageMetrics(metricName -> new CassandraMetricsRegistry.MetricName("DroppedMessages", metricName, scope));
            this.droppedInternal.getAndSet(0);
            this.droppedCrossNode.getAndSet(0);
        }
    }

    @VisibleForTesting
    public void resetDroppedMessagesMap(String scope)
    {
        for (DroppedMessages droppedMessages : droppedMessagesMap.values())
            droppedMessages.reset(scope);
    }

    // total dropped message counts for server lifetime
    private final Map<Verb<?, ?>, DroppedMessages> droppedMessagesMap;

    private final List<ILatencySubscriber> subscribers = new ArrayList<>();

    // protocol versions of the other nodes in the cluster
    private final ConcurrentMap<InetAddress, MessagingVersion> versions = new NonBlockingHashMap<>();

    // message sinks are a testing hook
    private final Set<IMessageSink> messageSinks = new CopyOnWriteArraySet<>();

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
        this.droppedMessagesMap = initializeDroppedMessageMap();

        listenGate = new SimpleCondition();

        if (!testOnly)
            ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::logDroppedMessages, LOG_DROPPED_INTERVAL_IN_MS, LOG_DROPPED_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);

        Consumer<Pair<Integer, ExpiringMap.CacheableObject<CallbackInfo<?>>>> timeoutReporter = pair ->
        {
            CallbackInfo expiredCallbackInfo = pair.right.get();
            MessageCallback<?> callback = expiredCallbackInfo.callback;
            InetAddress target = expiredCallbackInfo.target;

            addLatency(expiredCallbackInfo.verb, target, pair.right.timeoutMillis());

            ConnectionMetrics.totalTimeouts.mark();
            OutboundTcpConnectionPool cp = getConnectionPool(expiredCallbackInfo.target);
            if (cp != null)
                cp.incrementTimeout();

            updateBackPressureOnReceive(target, expiredCallbackInfo.verb, true);
            StageManager.getStage(Stage.INTERNAL_RESPONSE).submit(() -> callback.onTimeout(target));
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
    }

    private Map<Verb<?, ?>, DroppedMessages> initializeDroppedMessageMap()
    {
        ImmutableMap.Builder<Verb<?, ?>, DroppedMessages> builder = ImmutableMap.builder();
        for (VerbGroup<?> group : Verbs.allGroups())
        {
            for (Verb<?, ?> definition : group)
            {
                // One way messages don't timeout and are never dropped.
                if (!definition.isOneWay())
                    builder.put(definition, new DroppedMessages(definition));
            }
        }
        builder.put(Tracing.TRACE_MSG_DEF, new DroppedMessages(Tracing.TRACE_MSG_DEF));
        return builder.build();
    }

    public void addMessageSink(IMessageSink sink)
    {
        messageSinks.add(sink);
    }

    public void removeMessageSink(IMessageSink sink)
    {
        messageSinks.remove(sink);
    }

    public void clearMessageSinks()
    {
        messageSinks.clear();
    }

    static int newMessageId()
    {
        return idGen.incrementAndGet();
    }

    /**
     * Sends a request message, setting the provided callback to be called with the response received.
     * <p>
     * As for any other sending method of this class, if the request is local and the stage configured for this
     * request is a {@link LocalAwareExecutorService}, then this <b>must</b> be called on a {@link LocalAwareExecutorService}
     * worker since we'll call {@link LocalAwareExecutorService#maybeExecuteImmediately} and that method silently
     * assumes it is called on a worker.
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
            updateBackPressureOnSend(request);
            sendInternal(request, callback);
        }
    }

    /**
     * Sends the requests for a provided dispatcher, setting the provided callback to be called with received responses.
     * <p>
     * As for any other sending method of this class, if the dispatcher includes a local request and the stage configured
     * for this request is a {@link LocalAwareExecutorService}, then this <b>must</b> be called on a
     * {@link LocalAwareExecutorService} worker since we'll call {@link LocalAwareExecutorService#maybeExecuteImmediately}
     * and that method silently assumes it is called on a worker.
     *
     * @param dispatcher the dispatcher to use to generate requests to send.
     * @param callback the callback to set for responses to the request from {@code dispatcher}.
     */
    public <Q> void send(Request.Dispatcher<?, Q> dispatcher, MessageCallback<Q> callback)
    {
        assert callback != null && !dispatcher.verb().isOneWay();

        for (Request<?, Q> request : dispatcher.remoteRequests())
            send(request, callback);

        // It is important to deliver the local request last (after other have been sent) because we use
        // LocalAwareExecutorService.maybeExecuteImmediately() which might execute on the current thread and would
        // thus block other messages.
        if (dispatcher.hasLocalRequest())
            deliverLocally(dispatcher.localRequest(), callback);
    }

    private <P, Q> void deliverLocally(Request<P, Q> request, MessageCallback<Q> callback)
    {
        Consumer<Response<Q>> onResponse = response ->
        {
            addLatency(request.verb(), request.to(), request.lifetimeMillis());
            response.deliverTo(callback);
        };
        Runnable onAborted = () ->
        {
            Tracing.trace("Discarding partial local response (timed out)");
            MessagingService.instance().incrementDroppedMessages(request);
            callback.onTimeout(FBUtilities.getBroadcastAddress());
        };
        Runnable runnable = () ->
        {
            if (request.isTimedOut())
            {
                onAborted.run();
                return;
            }
            request.execute(onResponse, onAborted);
        };
        deliverLocallyInternal(request, callback, runnable);
    }

    private <P, Q> void registerCallback(Request<P, Q> request, MessageCallback<Q> callback)
    {
        long timeout = request.timeoutMillis();

        for (Request.Forward forward : request.forwards())
            registerCallback(forward.id, forward.to, request.verb(), callback, timeout);

        registerCallback(request.id(), request.to(), request.verb(), callback, timeout);
    }

    private <Q> void registerCallback(int id, InetAddress to, Verb<?, Q> type, MessageCallback<Q> callback, long timeout)
    {
        CallbackInfo previous = callbacks.put(id, new CallbackInfo<>(to, callback, type), timeout);
        assert previous == null : String.format("Callback already exists for id %d! (%s)", id, previous);
    }

    /**
     * Sends a request and returns a future on the reception of the response.
     * <p>
     * As for any other sending method of this class, if the request is local and the stage configured for this
     * request is a {@link LocalAwareExecutorService}, then this <b>must</b> be called on a {@link LocalAwareExecutorService}
     * worker since we'll call {@link LocalAwareExecutorService#maybeExecuteImmediately} and that method silently
     * assumes it is called on a worker.
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
        sendInternal(request, null);
    }

    /**
     * Sends a one-way request.
     * <p>
     * Note that this is a fire-and-forget type of method: the method returns as soon as the request has been set to
     * be send and there is no way to know if the request has been successfully delivered or not.
     * <p>
     * As for any other sending method of this class, if the request is local and the stage configured for this
     * request is a {@link LocalAwareExecutorService}, then this <b>must</b> be called on a {@link LocalAwareExecutorService}
     * worker since we'll call {@link LocalAwareExecutorService#maybeExecuteImmediately} and that method silently
     * assumes it is called on a worker.
     *
     * @param request the request to send.
     */
    public void send(OneWayRequest<?> request)
    {
        if (request.isLocal())
            deliverLocallyOneWay(request);
        else
            sendInternal(request, null);
    }

    /**
     * Sends the one-way requests generated by the provided dispatcher.
     * <p>
     * Note that this is a fire-and-forget type of method: the method returns as soon as the requets have been set to
     * be send and there is no way to know if the requests have been successfully delivered or not.
     * <p>
     * As for any other sending method of this class, if the dispatcher includes a local request and the stage configured
     * for this request is a {@link LocalAwareExecutorService}, then this <b>must</b> be called on a
     * {@link LocalAwareExecutorService} worker since we'll call {@link LocalAwareExecutorService#maybeExecuteImmediately}
     * and that method silently assumes it is called on a worker.
     *
     * @param dispatcher the dispatcher to use to generate the one-way requests to send.
     */
    public void send(OneWayRequest.Dispatcher<?> dispatcher)
    {
        for (OneWayRequest<?> request : dispatcher.remoteRequests())
            send(request);

        // It is important to deliver the local request last (after other have been sent) because we use
        // LocalAwareExecutorService.maybeExecuteImmediately() which might execute on the current thread and would
        // thus block other messages.
        if (dispatcher.hasLocalRequest())
            deliverLocallyOneWay(dispatcher.localRequest());
    }

    private void deliverLocallyOneWay(OneWayRequest<?> request)
    {
        deliverLocallyInternal(request, null, request::execute);
    }

    /**
     * Sends a response message.
     */
    void reply(Response<?> response)
    {
        Tracing.trace("Enqueuing {} response to {}", response.verb(), response.from());
        sendInternal(response, null);
    }

    private void sendInternal(Message message, MessageCallback<?> callback)
    {
        if (logger.isTraceEnabled())
            logger.trace("Sending {}", message);

        // message sinks are a testing hook
        for (IMessageSink ms : messageSinks)
            if (!ms.allowOutgoingMessage(message, callback))
                return;

        getConnection(message).enqueue(message);
    }

    private void deliverLocallyInternal(Request<?, ?> request, MessageCallback<?> callback, Runnable runnable)
    {
        // message sinks are a testing hook
        for (IMessageSink ms : messageSinks)
            if (!ms.allowOutgoingMessage(request, callback))
                return;

        StageManager.getStage(request.stage()).maybeExecuteImmediately(runnable);
    }


    /**
     * Updates the back-pressure state on sending the provided message.
     *
     * @param request The request sent.
     */
    <Q> void updateBackPressureOnSend(Request<?, Q> request)
    {
        if (request.verb().supportsBackPressure() && DatabaseDescriptor.backPressureEnabled())
        {
            OutboundTcpConnectionPool cp = getConnectionPool(request.to());
            if (cp != null)
            {
                BackPressureState backPressureState = cp.getBackPressureState();
                backPressureState.onRequestSent(request);
            }
        }
    }

    /**
     * Updates the back-pressure state on reception from the given host if enabled and the given message callback supports it.
     *
     * @param host The replica host the back-pressure state refers to.
     * @param verb The message verb.
     * @param timeout True if updated following a timeout, false otherwise.
     */
    void updateBackPressureOnReceive(InetAddress host, Verb<?, ?> verb, boolean timeout)
    {
        if (verb.supportsBackPressure() && DatabaseDescriptor.backPressureEnabled())
        {
            OutboundTcpConnectionPool cp = getConnectionPool(host);
            if (cp != null)
            {
                BackPressureState backPressureState = cp.getBackPressureState();
                if (!timeout)
                    backPressureState.onResponseReceived();
                else
                    backPressureState.onResponseTimeout();
            }
        }
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
    public void applyBackPressure(Iterable<InetAddress> hosts, long timeoutInNanos)
    {
        if (DatabaseDescriptor.backPressureEnabled())
        {
            Set<BackPressureState> states = new HashSet<BackPressureState>();
            for (InetAddress host : hosts)
            {
                if (host.equals(FBUtilities.getBroadcastAddress()))
                    continue;
                OutboundTcpConnectionPool cp = getConnectionPool(host);
                if (cp != null)
                    states.add(cp.getBackPressureState());
            }
            backPressure.apply(states, timeoutInNanos, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Track latency information for the dynamic snitch.
     *
     * @param verb the verb for which we're adding latency information.
     * @param address the host that replied to the message for which we're adding latency.
     * @param latency the latentcy to record in milliseconds
     */
    void addLatency(Verb<?, ?> verb, InetAddress address, long latency)
    {
        for (ILatencySubscriber subscriber : subscribers)
            subscriber.receiveTiming(verb, address, latency);
    }

    /**
     * called from gossiper when it notices a node is not responding.
     */
    public void convict(InetAddress ep)
    {
        OutboundTcpConnectionPool cp = getConnectionPool(ep);
        if (cp != null)
        {
            logger.trace("Resetting pool for {}", ep);
            cp.reset();
        }
        else
        {
            logger.debug("Not resetting pool for {} because internode authenticator said not to connect", ep);
        }
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
    public OutboundTcpConnectionPool getConnectionPool(InetAddress to)
    {
        OutboundTcpConnectionPool cp = connectionManagers.get(to);
        if (cp == null)
        {
            //Don't attempt to connect to nodes that won't (or shouldn't) authenticate anyways
            if (!DatabaseDescriptor.getInternodeAuthenticator().authenticate(to, OutboundTcpConnectionPool.portFor(to)))
                return null;

            cp = new OutboundTcpConnectionPool(to, backPressure.newState(to));
            OutboundTcpConnectionPool existingPool = connectionManagers.putIfAbsent(to, cp);
            if (existingPool != null)
                cp = existingPool;
            else
                cp.start();
        }
        cp.waitForStarted();
        return cp;
    }

    public OutboundTcpConnection getConnection(Message msg)
    {
        OutboundTcpConnectionPool cp = getConnectionPool(msg.to());
        return cp == null ? null : cp.getConnection(msg);
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
        // We may need to schedule hints on the mutation stage, so it's erroneous to shut down the mutation stage first
        assert !StageManager.getStage(Stage.MUTATION).isShutdown();

        // the important part
        if (!callbacks.shutdownBlocking())
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

    public void receive(Message message)
    {
        TraceState state = Tracing.instance.initializeFromMessage(message);
        if (state != null)
            state.trace("{} message received from {}", message.verb(), message.from());

        // message sinks are a testing hook
        for (IMessageSink ms : messageSinks)
            if (!ms.allowIncomingMessage(message))
                return;

        StageManager.getStage(message.stage()).execute(new MessageDeliveryTask(message),
                                                       ExecutorLocals.create(state));
    }

    // Only required by legacy serialization. Can inline in previous call when we get rid of that.
    CallbackInfo<?> getRegisteredCallback(int id)
    {
        return callbacks.get(id).get();
    }

    ExpiringMap.CacheableObject<CallbackInfo<?>> removeRegisteredCallback(Response<?> response)
    {
        return callbacks.remove(response.id());
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
        // We can't talk to someone from the future
        version = MessagingVersion.min(version, current_version);
        logger.trace("Setting version {} for {}", version, endpoint);

        MessagingVersion v = versions.put(endpoint, version);
        return v == null ? version : v;
    }

    public void resetVersion(InetAddress endpoint)
    {
        logger.trace("Resetting version for {}", endpoint);
        versions.remove(endpoint);
    }

    public MessagingVersion getVersion(InetAddress endpoint)
    {
        MessagingVersion v = versions.get(endpoint);
        if (v == null)
        {
            // we don't know the version. assume current. we'll know soon enough if that was incorrect.
            logger.trace("Assuming current protocol version for {}", endpoint);
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

    public void incrementDroppedMessages(Message message)
    {
        Verb<?, ?> definition = message.verb();
        assert !definition.isOneWay() : "Shouldn't drop a one-way message";
        if (message.isRequest())
        {
            Object payload = ((Request<?, ?>)message).payload();
            if (payload instanceof IMutation)
                updateDroppedMutationCount((IMutation) payload);
        }

        incrementDroppedMessages(definition, message.lifetimeMillis(), message.from().equals(message.to()));
    }

    @VisibleForTesting
    void incrementDroppedMessages(Verb<?, ?> definition, long timeTaken, boolean hasCrossedNode)
    {
        DroppedMessages droppedMessages = droppedMessagesMap.get(definition);
        if (droppedMessages == null)
        {
            // This really shouldn't happen or is a bug, but throwing don't feel necessary either
            NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 5, TimeUnit.MINUTES, "Cannot increment dropped message for unregistered message {}", definition);
            return;
        }

        incrementDroppedMessages(droppedMessages, timeTaken, hasCrossedNode);
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

    private void incrementDroppedMessages(DroppedMessages droppedMessages, long timeTaken, boolean hasCrossedNode)
    {
        DroppedMessageMetrics metrics = droppedMessages.metrics;
        metrics.dropped.mark();
        if (hasCrossedNode)
        {
            droppedMessages.droppedCrossNode.incrementAndGet();
            metrics.crossNodeDroppedLatency.update(timeTaken, TimeUnit.MILLISECONDS);
        }
        else
        {
            droppedMessages.droppedInternal.incrementAndGet();
            metrics.internalDroppedLatency.update(timeTaken, TimeUnit.MILLISECONDS);
        }
    }

    private void logDroppedMessages()
    {
        List<String> logs = getDroppedMessagesLogs();
        for (String log : logs)
            logger.info(log);

        if (logs.size() > 0)
            StatusLogger.log();
    }

    @VisibleForTesting
    List<String> getDroppedMessagesLogs()
    {
        List<String> ret = new ArrayList<>();
        for (Map.Entry<Verb<?, ?>, DroppedMessages> entry : droppedMessagesMap.entrySet())
        {
            Verb<?, ?> definition = entry.getKey();
            DroppedMessages droppedMessages = entry.getValue();

            int droppedInternal = droppedMessages.droppedInternal.getAndSet(0);
            int droppedCrossNode = droppedMessages.droppedCrossNode.getAndSet(0);
            if (droppedInternal > 0 || droppedCrossNode > 0)
            {
                ret.add(String.format("%s messages were dropped in last %d ms: %d internal and %d cross node."
                                     + " Mean internal dropped latency: %d ms and Mean cross-node dropped latency: %d ms",
                                     definition,
                                     LOG_DROPPED_INTERVAL_IN_MS,
                                     droppedInternal,
                                     droppedCrossNode,
                                     TimeUnit.NANOSECONDS.toMillis((long)droppedMessages.metrics.internalDroppedLatency.getSnapshot().getMean()),
                                     TimeUnit.NANOSECONDS.toMillis((long)droppedMessages.metrics.crossNodeDroppedLatency.getSnapshot().getMean())));
            }
        }
        return ret;
    }

    @VisibleForTesting
    public static class SocketThread extends Thread
    {
        private final ServerSocket server;
        @VisibleForTesting
        public final Set<Closeable> connections = Sets.newConcurrentHashSet();

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
                    connections.add((Closeable) thread);
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
            for (Closeable connection : connections)
            {
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
        if ("Mac OS X".equals(System.getProperty("os.name")))
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
        Map<String, Integer> map = new HashMap<>(droppedMessagesMap.size());
        for (Map.Entry<Verb<?, ?>, DroppedMessages> entry : droppedMessagesMap.entrySet())
            map.put(entry.getKey().toString(), (int) entry.getValue().metrics.dropped.getCount());
        return map;
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
