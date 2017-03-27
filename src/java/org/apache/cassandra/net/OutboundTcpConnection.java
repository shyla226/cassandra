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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Checksum;

import javax.net.ssl.SSLHandshakeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocalThread;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
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

import com.google.common.util.concurrent.Uninterruptibles;

public class OutboundTcpConnection extends FastThreadLocalThread
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

    private static CoalescingStrategy newCoalescingStrategy(String displayName)
    {
        return CoalescingStrategies.newCoalescingStrategy(DatabaseDescriptor.getOtcCoalescingStrategy(),
                                                          DatabaseDescriptor.getOtcCoalescingWindow(),
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
                newCoalescingStrategy("dummy");
        }

        int coalescingWindow = DatabaseDescriptor.getOtcCoalescingWindow();
        if (coalescingWindow != Config.otc_coalescing_window_us_default)
            logger.info("OutboundTcpConnection coalescing window set to {}μs", coalescingWindow);

        if (coalescingWindow < 0)
            throw new ExceptionInInitializerError(
                    "Value provided for coalescing window must be greater than 0: " + coalescingWindow);
    }

    private volatile boolean isStopped = false;

    private static final int OPEN_RETRY_DELAY = 100; // ms between retries
    public static final int WAIT_FOR_VERSION_MAX_TIME = 5000;

    static final int LZ4_HASH_SEED = 0x9747b28c;

    private final BlockingQueue<QueuedMessage> backlog = new LinkedBlockingQueue<>();

    private final OutboundTcpConnectionPool poolReference;

    private final CoalescingStrategy cs;
    private DataOutputStreamPlus out;
    private Socket socket;
    private volatile long completed;
    private final AtomicLong dropped = new AtomicLong();
    private volatile int currentMsgBufferCount = 0;

    private volatile MessagingVersion targetVersion;
    private volatile Message.Serializer messageSerializer;

    public OutboundTcpConnection(OutboundTcpConnectionPool pool, String name)
    {
        super("MessagingService-Outgoing-" + pool.endPoint() + "-" + name);
        this.poolReference = pool;
        cs = newCoalescingStrategy(pool.endPoint().getHostAddress());

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
        String remoteDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(targetHost);
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
        return remoteDC.equals(localDC);
    }

    public void enqueue(Message message)
    {
        if (backlog.size() > 1024)
            expireMessages();
        try
        {
            backlog.put(new QueuedMessage(message));
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
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

    public MessagingVersion getTargetVersion()
    {
        return targetVersion;
    }

    public void run()
    {
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

            currentMsgBufferCount = drainedMessages.size();

            int count = drainedMessages.size();
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
                        disconnect();
                        if (isStopped)
                            break outer;
                        continue;
                    }

                    if (m.isTimedOut())
                        dropped.incrementAndGet();
                    else if (socket != null || connect())
                        writeConnected(qm, count == 1 && backlog.isEmpty());
                    else
                    {
                        // clear out the queue, else gossip messages back up.
                        drainedMessages.clear();
                        backlog.clear();
                        break inner;
                    }
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
            drainedMessages.clear();
        }
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

    private boolean shouldCompressConnection()
    {
        // assumes version >= 1.2
        return DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.all
               || (DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.dc && !isLocalDC(poolReference.endPoint()));
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
            if (flush)
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
                    try
                    {
                        backlog.put(new RetriedQueuedMessage(qm));
                    }
                    catch (InterruptedException e1)
                    {
                        throw new AssertionError(e1);
                    }
                }
            }
            else
            {
                // Non IO exceptions are likely a programming error so let's not silence them
                logger.error("error writing to {}", poolReference.endPoint(), e);
            }
        }
    }

    private void disconnect()
    {
        if (socket != null)
        {
            try
            {
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
    private boolean connect()
    {
        logger.debug("Attempting to connect to {}", poolReference.endPoint());

        long start = System.nanoTime();
        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getRpcTimeout());
        while (System.nanoTime() - start < timeout)
        {
            targetVersion = MessagingService.instance().getVersion(poolReference.endPoint());
            try
            {
                socket = poolReference.newSocket();
                socket.setKeepAlive(true);
                if (isLocalDC(poolReference.endPoint()))
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

                out.writeInt(MessagingService.PROTOCOL_MAGIC);
                out.writeInt(targetProtocolVersion.makeProtocolHeader(shouldCompressConnection(), false));
                out.flush();

                DataInputStream in = new DataInputStream(socket.getInputStream());
                ProtocolVersion maxTargetVersion = handshakeVersion(in);
                if (maxTargetVersion == null)
                {
                    // no version is returned, so disconnect an try again
                    logger.trace("Target max version is {}; no version information yet, will retry", maxTargetVersion);
                    disconnect();
                    continue;
                }
                else
                {
                    MessagingService.instance().setVersion(poolReference.endPoint(),
                                                           MessagingVersion.from(maxTargetVersion));
                }

                if (targetProtocolVersion.compareTo(maxTargetVersion) > 0)
                {
                    logger.trace("Target max version is {}; will reconnect with that version", maxTargetVersion);
                    try
                    {
                        if (DatabaseDescriptor.getSeeds().contains(poolReference.endPoint()))
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
                    finally
                    {
                        disconnect();
                        return false;
                    }
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

                if (shouldCompressConnection())
                {
                    out.flush();
                    logger.trace("Upgrading OutputStream to {} to be compressed", poolReference.endPoint());

                    // TODO: custom LZ4 OS that supports BB write methods
                    LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
                    Checksum checksum = XXHashFactory.fastestInstance().newStreamingHash32(LZ4_HASH_SEED).asChecksum();
                    out = new WrappedDataOutputStreamPlus(new LZ4BlockOutputStream(socket.getOutputStream(),
                                                                        1 << 14,  // 16k block size
                                                                        compressor,
                                                                        checksum,
                                                                        true)); // no async flushing
                }
                logger.debug("Done connecting to {}", poolReference.endPoint());
                return true;
            }
            catch (SSLHandshakeException e)
            {
                logger.error("SSL handshake error for outbound connection to " + socket, e);
                socket = null;
                // SSL errors won't be recoverable within timeout period so we'll just abort
                return false;
            }
            catch (ConnectException e)
            {
                socket = null;
                nospamLogger.debug(String.format("Unable to connect to %s (%s)", poolReference.endPoint(), e.toString()));
                Uninterruptibles.sleepUninterruptibly(OPEN_RETRY_DELAY, TimeUnit.MILLISECONDS);
            }
            catch (IOException e)
            {
                socket = null;
                logger.debug("Unable to connect to {}", poolReference.endPoint(), e);
                Uninterruptibles.sleepUninterruptibly(OPEN_RETRY_DELAY, TimeUnit.MILLISECONDS);
            }
        }
        return false;
    }

    private ProtocolVersion handshakeVersion(final DataInputStream inputStream)
    {
        final AtomicReference<ProtocolVersion> version = new AtomicReference<>();
        final CountDownLatch versionLatch = new CountDownLatch(1);
        NamedThreadFactory.createThread(() ->
        {
            try
            {
                logger.info("Handshaking version with {}", poolReference.endPoint());
                version.set(ProtocolVersion.fromHandshakeVersion(inputStream.readInt()));
            }
            catch (IOException ex)
            {
                final String msg = "Cannot handshake version with " + poolReference.endPoint();
                if (logger.isTraceEnabled())
                    logger.trace(msg, ex);
                else
                    logger.info(msg);
            }
            finally
            {
                //unblock the waiting thread on either success or fail
                versionLatch.countDown();
            }
        }, "HANDSHAKE-" + poolReference.endPoint()).start();

        try
        {
            versionLatch.await(WAIT_FOR_VERSION_MAX_TIME, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }
        return version.get();
    }

    private void expireMessages()
    {
        Iterator<QueuedMessage> iter = backlog.iterator();
        while (iter.hasNext())
        {
            if (iter.next().message.isTimedOut())
            {
                iter.remove();
                dropped.incrementAndGet();
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
            this.timestampNanos = System.nanoTime();
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
}
