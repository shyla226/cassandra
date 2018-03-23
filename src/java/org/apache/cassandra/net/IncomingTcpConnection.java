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
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.zip.Checksum;

import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocalThread;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.io.util.NIODataInputStream;
import org.apache.cassandra.io.util.TrackedDataInputPlus;

public class IncomingTcpConnection extends FastThreadLocalThread implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingTcpConnection.class);

    private static final int BUFFER_SIZE = Integer.getInteger(Config.PROPERTY_PREFIX + ".itc_buffer_size", 1024 * 64);

    private long connectTime = 0;
    private final ProtocolVersion protocolVersion;
    private final boolean compressed;
    private final Socket socket;
    private final Multimap<InetAddress, Closeable> group;
    public final InetAddress socketFrom;
    private Message.Serializer messageSerializer;

    public InetAddress snitchFrom;

    public IncomingTcpConnection(ProtocolVersion version, boolean compressed, Socket socket, Multimap<InetAddress, Closeable> group)
    {
        super("MessagingService-Incoming-" + socket.getInetAddress());
        this.protocolVersion = version;
        this.compressed = compressed;
        this.socket = socket;
        this.socketFrom = socket.getInetAddress();
        this.group = group;
        if (DatabaseDescriptor.getInternodeRecvBufferSize() > 0)
        {
            try
            {
                this.socket.setReceiveBufferSize(DatabaseDescriptor.getInternodeRecvBufferSize());
            }
            catch (SocketException se)
            {
                logger.warn("Failed to set receive buffer size on internode socket.", se);
            }
        }
    }

    /**
     * A new connection will either stream or message for its entire lifetime: because streaming
     * bypasses the InputStream implementations to use sendFile, we cannot begin buffering until
     * we've determined the type of the connection.
     */
    @Override
    public void run()
    {
        try
        {
            if (MessagingVersion.from(protocolVersion) == null)
                throw new UnsupportedOperationException(String.format("Unable to read obsolete message version %s; "
                                                                      + "The earliest version supported is %s",
                                                                      protocolVersion.handshakeVersion, MessagingVersion.values()[0]));
            if (MessagingVersion.from(protocolVersion) == MessagingVersion.OSS_40)
                throw new UnsupportedOperationException(String.format("Received messaging version %d (OSS C* 4.0) is not supported. Terminating connection.",
                                                                      protocolVersion.handshakeVersion));

            receiveMessages();
        }
        catch (EOFException e)
        {
            logger.trace("eof reading from socket; closing", e);
            // connection will be reset so no need to throw an exception.
        }
        catch (UnknownTableException e)
        {
            logger.warn("UnknownTableException reading from socket; closing", e);
        }
        catch (IOException e)
        {
            logger.trace("IOException reading from socket; closing", e);
        }
        finally
        {
            close();
        }
    }

    @Override
    public void close()
    {
        try
        {
            if (logger.isTraceEnabled())
                logger.trace("Closing socket {} - isclosed: {}", socket, socket.isClosed());
            if (!socket.isClosed())
            {
                socket.close();
            }
        }
        catch (IOException e)
        {
            logger.trace("Error closing socket", e);
        }
        finally
        {
            group.remove(socketFrom, this);
            if (snitchFrom != null)
                group.remove(snitchFrom, this);
        }
    }

    @SuppressWarnings("resource") // Not closing constructed DataInputPlus's as the stream needs to remain open.
    private void receiveMessages() throws IOException
    {
        // handshake (true) endpoint versions
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        // if this version is < the MS version the other node is trying
        // to connect with, the other node will disconnect
        out.writeInt(MessagingService.current_version.protocolVersion().handshakeVersion);
        out.flush();
        DataInputPlus in = new DataInputStreamPlus(socket.getInputStream());
        ProtocolVersion maxVersion = ProtocolVersion.fromHandshakeVersion(in.readInt());
        // outbound side will reconnect if necessary to upgrade version
        assert protocolVersion.compareTo(MessagingService.current_version.protocolVersion()) <= 0;
        snitchFrom = CompactEndpointSerializationHelper.deserialize(in);
        // record the (true) version of the endpoint
        MessagingService.instance().setVersion(snitchFrom, MessagingVersion.from(maxVersion));
        logger.trace("Set version for {} to {} (will use {})", snitchFrom, maxVersion, MessagingService.instance().getVersion(snitchFrom));

        //Save the connection under snitch address
        group.put(snitchFrom, this);


        MessagingVersion version = MessagingVersion.from(protocolVersion);

        // Read connection parameters
        long baseTimestampMillis = -1; // Unused and unset on pre-4.0
        if (version.isDSE())
        {
            MessageParameters connectionParameters = MessageParameters.serializer().deserialize(in);
            assert connectionParameters.has(MessageSerializer.BASE_TIMESTAMP_KEY);
            baseTimestampMillis = connectionParameters.getLong(MessageSerializer.BASE_TIMESTAMP_KEY);
        }
        messageSerializer = Message.createSerializer(version, baseTimestampMillis);

        if (compressed)
        {
            logger.trace("Upgrading incoming connection to be compressed");
            LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();
            Checksum checksum = XXHashFactory.fastestInstance().newStreamingHash32(OutboundTcpConnection.LZ4_HASH_SEED).asChecksum();
            in = new DataInputStreamPlus(new LZ4BlockInputStream(socket.getInputStream(),
                                                             decompressor,
                                                             checksum));
        }
        else
        {
            ReadableByteChannel channel = socket.getChannel();
            in = new NIODataInputStream(channel != null ? channel : Channels.newChannel(socket.getInputStream()), BUFFER_SIZE);
        }

        this.connectTime = System.nanoTime();

        while (true)
        {
            receiveMessage(new TrackedDataInputPlus(in));
        }
    }

    private void receiveMessage(TrackedDataInputPlus input) throws IOException
    {
        int size = messageSerializer.readSerializedSize(input);
        try
        {
            Message message = messageSerializer.deserialize(input, size, snitchFrom);
            if (message == null)
            {
                // callback expired; nothing to do
                return;
            }

            MessagingService.instance().receive(message);
        }
        catch (MessageDeserializationException e)
        {
            if (e.isRecoverable())
                e.recover(input);
            else
                throw e;
        }
    }

    public long getConnectTime()
    {
        return connectTime;
    }

}
