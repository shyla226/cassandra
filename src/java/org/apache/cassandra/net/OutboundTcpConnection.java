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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.Checksum;

import javax.net.ssl.SSLHandshakeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocalThread;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

import org.apache.cassandra.concurrent.ParkedThreadsMonitor;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.Coalescable;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.WrappedBoolean;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscGrowableArrayQueue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;

public class OutboundTcpConnection extends FastThreadLocalThread implements ParkedThreadsMonitor.MonitorableThread
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundTcpConnection.class);
    private static final NoSpamLogger nospamLogger = NoSpamLogger.getLogger(logger, 10, TimeUnit.SECONDS);

    private static final String PREFIX = Config.PROPERTY_PREFIX;

    /*
     * Enabled/disable TCP_NODELAY for intradc connections. Defaults to enabled.
     */
    private static final String INTRADC_TCP_NODELAY_PROPERTY = PREFIX + "otc_intradc_tcp_nodelay";
    private static final boolean INTRADC_TCP_NODELAY = Boolean.parseBoolean(System.getProperty(INTRADC_TCP_NODELAY_PROPERTY, "true"));

    /*
     * Size of buffer in output stream
     */
    private static final String BUFFER_SIZE_PROPERTY = PREFIX + "otc_buffer_size";
    private static final int BUFFER_SIZE = Integer.getInteger(BUFFER_SIZE_PROPERTY, 1024 * 64);

    public static final int MAX_COALESCED_MESSAGES = 128;

    private static CoalescingStrategy newCoalescingStrategy(String displayName, OutboundTcpConnection owner)
    {
        return CoalescingStrategies.newCoalescingStrategy(DatabaseDescriptor.getOtcCoalescingStrategy(),
                                                          DatabaseDescriptor.getOtcCoalescingWindow(),
                                                          owner,
                                                          logger,
                                                          displayName);
    }

    static
    {
        String strategy = DatabaseDescriptor.getOtcCoalescingStrategy();
        switch (strategy)
        {
        case "TIMEHORIZON":
            break;
        case "MOVINGAVERAGE":
        case "FIXED":
        case "DISABLED":
            logger.info("OutboundTcpConnection using coalescing strategy {}", strategy);
            break;
            default:
                //Check that it can be loaded
                newCoalescingStrategy("dummy", null);
        }

        int coalescingWindow = DatabaseDescriptor.getOtcCoalescingWindow();
        if (coalescingWindow != Config.otc_coalescing_window_us_default)
            logger.info("OutboundTcpConnection coalescing window set to {}μs", coalescingWindow);

        if (coalescingWindow < 0)
            throw new ExceptionInInitializerError(
                    "Value provided for coalescing window must be greater than 0: " + coalescingWindow);

        int otc_backlog_expiration_interval_in_ms = DatabaseDescriptor.getOtcBacklogExpirationInterval();
        if (otc_backlog_expiration_interval_in_ms != Config.otc_backlog_expiration_interval_ms_default)
            logger.info("OutboundTcpConnection backlog expiration interval set to to {}ms", otc_backlog_expiration_interval_in_ms);
    }

    private volatile boolean isStopped = false;
    private volatile ThreadState state = ThreadState.WORKING;
    private Thread thread;

    private static final int OPEN_RETRY_DELAY = 100; // ms between retries
    public static final int WAIT_FOR_VERSION_MAX_TIME = 5000;

    static final int LZ4_HASH_SEED = 0x9747b28c;

    private final MessagePassingQueue<QueuedMessage> backlog = new MpscGrowableArrayQueue<>(4096, 1 << 30);
    private static final String BACKLOG_PURGE_SIZE_PROPERTY = PREFIX + "otc_backlog_purge_size";
    @VisibleForTesting
    static final int BACKLOG_PURGE_SIZE = Integer.getInteger(BACKLOG_PURGE_SIZE_PROPERTY, 1024);
    private final AtomicBoolean backlogExpirationActive = new AtomicBoolean(false);
    private volatile long backlogNextExpirationTime;

    private final OutboundTcpConnectionPool poolReference;

    private final CoalescingStrategy cs;
    private DataOutputStreamPlus out;
    private Socket socket;
    private volatile long completed;
    private final AtomicLong dropped = new AtomicLong();
    private volatile int currentMsgBufferCount = 0;
    private final boolean isGossip;

    private volatile MessagingVersion targetVersion;
    private volatile Message.Serializer messageSerializer;

    public OutboundTcpConnection(OutboundTcpConnectionPool pool, String name)
    {
        this(pool, name, false);
    }

    public OutboundTcpConnection(OutboundTcpConnectionPool pool, String name, boolean isGossip)
    {
        super("MessagingService-Outgoing-" + pool.endPoint() + "-" + name);
        this.poolReference = pool;
        this.isGossip = isGossip;
        cs = newCoalescingStrategy(pool.endPoint().getHostAddress(), this);

        // We want to use the most precise version we know because while there is version detection on connect(),
        // the target version might be accessed by the pool (in getConnection()) before we actually connect (as we
        // connect when the first message is submitted). Note however that the only case where we'll connect
        // without knowing the true version of a node is if that node is a seed (otherwise, we can't know a node
        // unless it has been gossiped to us or it has connected to us and in both case this sets the version) and
        // in that case we won't rely on that targetVersion before we're actually connected and so the version
        // detection in connect() will do its job.
        targetVersion = MessagingService.instance().getVersion(pool.endPoint());
    }

    private static boolean isLocalDC(InetAddress targetHost)
    {
        return isLocalDC(DatabaseDescriptor.getEndpointSnitch().getDatacenter(targetHost));
    }

    private static boolean isLocalDC(String remoteDC)
    {
        String localDC = DatabaseDescriptor.getLocalDataCenter();

        // When we don't know the DC default to local
        return remoteDC.equals(localDC) || DatabaseDescriptor.getEndpointSnitch().isDefaultDC(remoteDC);
    }

    public void enqueue(Message message)
    {
        boolean success = backlog.relaxedOffer(new QueuedMessage(message));
        assert success;
    }

    /**
     * This is a helper method for unit testing. Disclaimer: Do not use this method outside unit tests, as
     * this method is iterating the queue which can be an expensive operation (CPU time, queue locking).
     *
     * @return true, if the queue contains at least one expired element
     */
    @VisibleForTesting // (otherwise = VisibleForTesting.NONE)
    boolean backlogContainsExpiredMessages(long currentTimeMillis)
    {
        WrappedBoolean hasExpired = new WrappedBoolean(false);
        backlog.drain(entry -> {
            if (entry.message.isTimedOut(currentTimeMillis))
                hasExpired.set(true);

            backlog.offer(entry);
        }, backlog.size());

        return hasExpired.get();
    }

    void closeSocket(boolean destroyThread)
    {
        logger.debug("Enqueuing socket close for {}", poolReference.endPoint());
        isStopped = destroyThread; // Exit loop to stop the thread
        backlog.clear();
        // in the "destroyThread = true" case, enqueuing the sentinel is important mostly to unblock the backlog.take()
        // (via the CoalescingStrategy) in case there's a data race between this method enqueuing the sentinel
        // and run() clearing the backlog on connection failure.
        enqueue(Message.CLOSE_SENTINEL);
    }

    void softCloseSocket()
    {
        enqueue(Message.CLOSE_SENTINEL);
    }


    public ThreadState getThreadState()
    {
        return state;
    }

    public void park()
    {
        state = ThreadState.PARKED;
        LockSupport.park();
    }

    public void unpark()
    {
        assert thread != null;
        state = ThreadState.WORKING;
        LockSupport.unpark(thread);
    }

    public boolean shouldUnpark(long now)
    {
        return state == ThreadState.PARKED && !backlog.isEmpty();
    }

    public void run()
    {
        if (thread == null)
            thread = Thread.currentThread();

        //Register self with Monitor
        ParkedThreadsMonitor.instance.get().addThreadToMonitor(this);

        final int drainedMessageSize = MAX_COALESCED_MESSAGES;
        // keeping list (batch) size small for now; that way we don't have an unbounded array (that we never resize)
        final List<QueuedMessage> drainedMessages = new ArrayList<>(drainedMessageSize);

        outer:
        while (!isStopped)
        {
            try
            {
                cs.coalesce(backlog, drainedMessages, drainedMessageSize);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }

            int count = currentMsgBufferCount = drainedMessages.size();

            //The timestamp of the first message has already been provided to the coalescing strategy
            //so skip logging it.
            inner:
            for (QueuedMessage qm : drainedMessages)
            {
                try
                {
                    Message m = qm.message;
                    if (m == Message.CLOSE_SENTINEL)
                    {
                        logger.trace("Disconnecting because CLOSE_SENTINEL detected");
                        disconnect();
                        if (isStopped)
                            break outer;
                        continue;
                    }

                    if (m.isTimedOut(ApproximateTime.millisTime()))
                        dropped.incrementAndGet();
                    else if (socket != null || connect())
                        writeConnected(qm, count == 1 && backlog.isEmpty());
                    else
                    {
                        // Not connected! Clear out the queue, else gossip messages back up. Update dropped
                        // statistics accordingly. Hint: The statistics may be slightly too low, if messages
                        // are added between the calls of backlog.size() and backlog.clear()
                        dropped.addAndGet(backlog.size());
                        backlog.clear();
                        break inner;
                    }
                }
                catch (InternodeAuthFailed e)
                {
                    logger.warn("Internode auth failed connecting to {}", poolReference.endPoint());
                    //Remove the connection pool and other thread so messages aren't queued
                    MessagingService.instance().destroyConnectionPool(poolReference.endPoint());
                }
                catch (Exception e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    // really shouldn't get here, as exception handling in writeConnected() is reasonably robust
                    // but we want to catch anything bad we don't drop the messages in the current batch
                    logger.error("error processing a message intended for {}", poolReference.endPoint(), e);
                }
                currentMsgBufferCount = --count;
            }
            // Update dropped statistics by the number of unprocessed drainedMessages
            dropped.addAndGet(currentMsgBufferCount);
            drainedMessages.clear();
        }

        //Register self with Monitor
        ParkedThreadsMonitor.instance.get().removeThreadToMonitor(this);
    }

    public int getPendingMessages()
    {
        return backlog.size() + currentMsgBufferCount;
    }

    public long getCompletedMesssages()
    {
        return completed;
    }

    public long getDroppedMessages()
    {
        return dropped.get();
    }

    private static boolean shouldCompressConnection(InetAddress endpoint)
    {
        switch (DatabaseDescriptor.internodeCompression())
        {
            case none:
                return false;
            case all:
                return true;
            case dc:
                return !isLocalDC(endpoint);
        }
        throw new AssertionError("internode-compression " + DatabaseDescriptor.internodeCompression());
    }

    public static boolean shouldCompressConnection(String dc)
    {
        switch (DatabaseDescriptor.internodeCompression())
        {
            case none:
                return false;
            case all:
                return true;
            case dc:
                return !isLocalDC(dc);
        }
        throw new AssertionError("internode-compression " + DatabaseDescriptor.internodeCompression());
    }

    private void writeConnected(QueuedMessage qm, boolean flush)
    {
        try
        {
            long serializedSize = messageSerializer.serializedSize(qm.message);
            assert serializedSize <= Integer.MAX_VALUE : "Invalid message, too large: " + serializedSize;
            int messageSize = (int)serializedSize;

            Tracing.instance.onMessageSend(qm.message, messageSize);

            messageSerializer.writeSerializedSize(messageSize, out);
            messageSerializer.serialize(qm.message, out);

            completed++;
            if (flush || qm.message.verb() == Verbs.GOSSIP.ECHO)
                out.flush();
        }
        catch (Throwable e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            disconnect();
            if (e instanceof IOException || e.getCause() instanceof IOException)
            {
                logger.debug("Error writing to {}", poolReference.endPoint(), e);

                // If we haven't retried this message yet, put it back on the queue to retry after re-connecting.
                // See CASSANDRA-5393 and CASSANDRA-12192.
                if (qm.shouldRetry())
                {
                    boolean accepted = backlog.relaxedOffer(new RetriedQueuedMessage(qm));
                    assert accepted;
                }
            }
            else
            {
                // Non IO exceptions are likely a programming error so let's not silence them
                logger.error("error writing to {}", poolReference.endPoint(), e);
            }
        }
    }

    public boolean isSocketOpen()
    {
        return socket != null && socket.isConnected();
    }

    private void disconnect()
    {
        if (socket != null)
        {
            try
            {
                if (out != null)
		    out.flush();
                socket.close();
                logger.debug("Socket to {} closed", poolReference.endPoint());
            }
            catch (IOException e)
            {
                logger.debug("Exception closing connection to {}", poolReference.endPoint(), e);
            }
            out = null;
            socket = null;
        }
    }

    @SuppressWarnings("resource")
    private boolean connect() throws InternodeAuthFailed
    {
        InetAddress endpoint = poolReference.endPoint();
        if (!DatabaseDescriptor.getInternodeAuthenticator().authenticate(endpoint, poolReference.portFor(endpoint)))
        {
            throw new InternodeAuthFailed();
        }

        logger.debug("Attempting to connect to {}", endpoint);


        long start = System.nanoTime();
        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getRpcTimeout());
        while (System.nanoTime() - start < timeout)
        {
            targetVersion = MessagingService.instance().getVersion(endpoint);
            boolean success = false;
            try
            {
                socket = poolReference.newSocket();
                socket.setKeepAlive(true);

                if (isLocalDC(endpoint) || isGossip)
                {
                    socket.setTcpNoDelay(INTRADC_TCP_NODELAY);
                }
                else
                {
                    socket.setTcpNoDelay(DatabaseDescriptor.getInterDCTcpNoDelay());
                }

                if (DatabaseDescriptor.getInternodeSendBufferSize() > 0)
                {
                    try
                    {
                        socket.setSendBufferSize(DatabaseDescriptor.getInternodeSendBufferSize());
                    }
                    catch (SocketException se)
                    {
                        logger.warn("Failed to set send buffer size on internode socket.", se);
                    }
                }

                // SocketChannel may be null when using SSL
                WritableByteChannel ch = socket.getChannel();
                out = new BufferedDataOutputStreamPlus(ch != null ? ch : Channels.newChannel(socket.getOutputStream()), BUFFER_SIZE);

                ProtocolVersion targetProtocolVersion = targetVersion.protocolVersion();
                boolean compress = shouldCompressConnection(poolReference.endPoint());

                out.writeInt(MessagingService.PROTOCOL_MAGIC);
                out.writeInt(targetProtocolVersion.makeProtocolHeader(compress, false));
                out.flush();

                DataInputStream in = new DataInputStream(socket.getInputStream());
                ProtocolVersion maxTargetVersion = handshakeVersion(socket, in);
                if (maxTargetVersion == null)
                {
                    // no version is returned, so disconnect an try again
                    logger.trace("Target max version is {}; no version information yet, will retry", maxTargetVersion);
                    disconnect();
                    continue;
                }
                else
                {
                    MessagingService.instance().setVersion(endpoint,
                                                           MessagingVersion.from(maxTargetVersion));
                }

                if (targetProtocolVersion.compareTo(maxTargetVersion) > 0)
                {
                    logger.trace("Target max version is {}; will reconnect with that version", maxTargetVersion);
                    try
                    {
                        if (DatabaseDescriptor.getSeeds().contains(endpoint))
                            logger.warn("Seed gossip version is {}; will not connect with that version", maxTargetVersion);
                    }
                    catch (Throwable e)
                    {
                        // If invalid yaml has been added to the config since startup, getSeeds() will throw an AssertionError
                        // Additionally, third party seed providers may throw exceptions if network is flakey
                        // Regardless of what's thrown, we must catch it, disconnect, and try again
                        JVMStabilityInspector.inspectThrowable(e);
                        logger.warn("Configuration error prevented outbound connection: {}", e.getLocalizedMessage());
                    }
                    return false;
                }

                if (targetProtocolVersion.compareTo(maxTargetVersion) < 0 && targetVersion != MessagingService.current_version)
                {
                    logger.trace("Detected higher max version {} (using {}); will reconnect when queued messages are done",
                                 maxTargetVersion, targetProtocolVersion);
                    softCloseSocket();
                }

                // We've agreed on targetVersion as our communication version new
                long baseTimestampMillis = System.currentTimeMillis();
                messageSerializer = Message.createSerializer(targetVersion, baseTimestampMillis);

                out.writeInt(MessagingService.current_version.protocolVersion().handshakeVersion);
                CompactEndpointSerializationHelper.serialize(FBUtilities.getBroadcastAddress(), out);

                // Writes connection parameters
                if (targetVersion.isDSE())
                {
                    MessageParameters connectionParameters = MessageParameters.builder()
                                                                              .putLong(MessageSerializer.BASE_TIMESTAMP_KEY, baseTimestampMillis)
                                                                              .build();
                    connectionParameters.serializer().serialize(connectionParameters, out);
                }

                if (compress)
                {
                    out.flush();
                    logger.trace("Upgrading OutputStream to {} to be compressed", endpoint);

                    // TODO: custom LZ4 OS that supports BB write methods
                    LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
                    Checksum checksum = XXHashFactory.fastestInstance().newStreamingHash32(LZ4_HASH_SEED).asChecksum();
                    out = new WrappedDataOutputStreamPlus(new LZ4BlockOutputStream(socket.getOutputStream(),
                                                                        1 << 14,  // 16k block size
                                                                        compressor,
                                                                        checksum,
                                                                        true)); // no async flushing
                }
                logger.debug("Done connecting to {}", endpoint);
                success = true;
                return true;
            }
            catch (SSLHandshakeException e)
            {
                logger.error("SSL handshake error for outbound connection to " + socket, e);
                disconnect();
                // SSL errors won't be recoverable within timeout period so we'll just abort
                return false;
            }
            catch (ConnectException e)
            {
                disconnect();
                nospamLogger.debug(String.format("Unable to connect to %s (%s)", poolReference.endPoint(), e.toString()));
                Uninterruptibles.sleepUninterruptibly(OPEN_RETRY_DELAY, TimeUnit.MILLISECONDS);
            }
            catch (IOException e)
            {
                disconnect();
                logger.debug("unable to connect to " + poolReference.endPoint(), e);
                Uninterruptibles.sleepUninterruptibly(OPEN_RETRY_DELAY, TimeUnit.MILLISECONDS);
            }
            finally
            {
                if (!success)
                    disconnect();
            }
        }
        return false;
    }

    private ProtocolVersion handshakeVersion(Socket socket, DataInputStream in) throws IOException
    {
        int oldTimeout = socket.getSoTimeout();
        try
        {
            socket.setSoTimeout(WAIT_FOR_VERSION_MAX_TIME);
            return ProtocolVersion.fromHandshakeVersion(in.readInt());
        }
        finally
        {
            socket.setSoTimeout(oldTimeout);
        }
    }

    /**
     * Expire elements from the queue if the queue is pretty full and expiration is not already in progress.
     * This method will only remove droppable expired entries. If no such element exists, nothing is removed from the queue.
     */
    @VisibleForTesting
    void expireMessages(long currentTimeMillis)
    {
        if (backlog.size() <= BACKLOG_PURGE_SIZE)
            return; // Plenty of space

        if (backlogNextExpirationTime > currentTimeMillis)
            return; // Expiration is not due.

        /**
         * Expiration is an expensive process. Iterating the queue locks the queue for both writes and
         * reads during iter.next() and iter.remove(). Thus letting only a single Thread do expiration.
         */
        if (backlogExpirationActive.compareAndSet(false, true))
        {
            try
            {
                backlog.drain(entry -> {
                    if (!entry.message.isTimedOut(currentTimeMillis))
                        backlog.relaxedOffer(entry);
                    else
                        dropped.incrementAndGet();
                }, backlog.size());
            }
            finally
            {
                long backlogExpirationInterval = DatabaseDescriptor.getOtcBacklogExpirationInterval();
                backlogNextExpirationTime = currentTimeMillis + backlogExpirationInterval;
                backlogExpirationActive.set(false);
            }
        }
    }

    /** messages that have not been retried yet */
    private static class QueuedMessage implements Coalescable
    {
        final Message message;
        final long timestampNanos;

        QueuedMessage(Message message)
        {
            this.message = message;
            this.timestampNanos = ApproximateTime.nanoTime();
        }

        boolean shouldRetry()
        {
            // retry all messages once
            return true;
        }

        public long timestampNanos()
        {
            return timestampNanos;
        }
    }

    private static class RetriedQueuedMessage extends QueuedMessage
    {
        RetriedQueuedMessage(QueuedMessage msg)
        {
            super(msg.message);
        }

        boolean shouldRetry()
        {
            return false;
        }
    }

    private static class InternodeAuthFailed extends Exception {}
}
