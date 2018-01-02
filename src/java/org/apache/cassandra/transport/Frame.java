
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

import java.io.IOException;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.Attribute;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.TPCEventLoop;
import org.apache.cassandra.concurrent.TPCRunnable;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.transport.Frame.Header.HeaderFlag;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.utils.Flags;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeSource;
import org.jctools.queues.SpscArrayQueue;

public class Frame
{
    public static final byte PROTOCOL_VERSION_MASK = 0x7f;

    public final Header header;
    public final ByteBuf body;

    /**
     * An on-wire frame consists of a header and a body.
     *
     * The header is defined the following way in native protocol version 3 and later:
     *
     *   0         8        16        24        32         40
     *   +---------+---------+---------+---------+---------+
     *   | version |  flags  |      stream       | opcode  |
     *   +---------+---------+---------+---------+---------+
     *   |                length                 |
     *   +---------+---------+---------+---------+
     */
    private Frame(Header header, ByteBuf body)
    {
        this.header = header;
        this.body = body;
    }

    public void retain()
    {
        body.retain();
    }

    public boolean release()
    {
        return body.release();
    }

    public static Frame create(TimeSource timeSource, Message.Type type, int streamId, ProtocolVersion version, int flags, ByteBuf body)
    {
        Header header = new Header(version, flags, streamId, type, timeSource.nanoTime());
        return new Frame(header, body);
    }

    public static class Header
    {
        // 9 bytes in protocol version 3 and later
        public static final int LENGTH = 9;

        public static final int BODY_LENGTH_SIZE = 4;

        public final ProtocolVersion version;
        public int flags;
        public final int streamId;
        public final Message.Type type;
        public final long queryStartNanoTime;

        private Header(ProtocolVersion version, int flags, int streamId, Message.Type type, long queryStartNanoTime)
        {
            this.version = version;
            this.flags = flags;
            this.streamId = streamId;
            this.type = type;
            this.queryStartNanoTime = queryStartNanoTime;
        }

        public interface HeaderFlag
        {
            static final int NONE           = 0;

            static final int COMPRESSED     = 1 << 0;
            static final int TRACING        = 1 << 1;
            static final int CUSTOM_PAYLOAD = 1 << 2;
            static final int WARNING        = 1 << 3;
            static final int USE_BETA       = 1 << 4;
        }
    }

    public Frame with(ByteBuf newBody)
    {
        return new Frame(header, newBody);
    }

    public static class Decoder extends ByteToMessageDecoder
    {
        private static final int MAX_FRAME_LENGTH = DatabaseDescriptor.getNativeTransportMaxFrameSize();

        private boolean discardingTooLongFrame;
        private long tooLongFrameLength;
        private long bytesToDiscard;
        private int tooLongStreamId;

        private final TimeSource timeSource;
        private final Connection.Factory factory;
        private final AsyncProcessor processor;

        public Decoder(TimeSource timeSource, Connection.Factory factory)
        {
            this(timeSource, factory, null);
        }

        public Decoder(TimeSource timeSource, Connection.Factory factory, AsyncProcessor processor)
        {
            this.timeSource = timeSource;
            this.factory = factory;
            this.processor = processor;
        }

        @VisibleForTesting
        Frame decodeFrame(ByteBuf buffer)
        throws Exception
        {
            if (discardingTooLongFrame)
            {
                bytesToDiscard = discard(buffer, bytesToDiscard);
                // If we have discarded everything, throw the exception
                if (bytesToDiscard <= 0)
                    fail();
                return null;
            }

            int readableBytes = buffer.readableBytes();
            if (readableBytes == 0)
                return null;

            int idx = buffer.readerIndex();

            // Check the first byte for the protocol version before we wait for a complete header.  Protocol versions
            // 1 and 2 use a shorter header, so we may never have a complete header's worth of bytes.
            int firstByte = buffer.getByte(idx++);
            Message.Direction direction = Message.Direction.extractFromVersion(firstByte);
            int versionNum = firstByte & PROTOCOL_VERSION_MASK;
            ProtocolVersion version = ProtocolVersion.decode(versionNum);

            // Wait until we have the complete header
            if (readableBytes < Header.LENGTH)
                return null;

            int flags = buffer.getByte(idx++);

            if (version.isBeta() && !Flags.contains(flags, HeaderFlag.USE_BETA))
                throw new ProtocolException(String.format("Beta version of the protocol used (%s), but USE_BETA flag is unset", version),
                                            version);

            int streamId = buffer.getShort(idx);
            idx += 2;

            // This throws a protocol exceptions if the opcode is unknown
            Message.Type type;
            try
            {
                type = Message.Type.fromOpcode(buffer.getUnsignedByte(idx++), direction);
            }
            catch (ProtocolException e)
            {
                throw ErrorMessage.wrap(e, streamId);
            }

            long bodyLength = buffer.getUnsignedInt(idx);
            idx += Header.BODY_LENGTH_SIZE;

            long frameLength = bodyLength + Header.LENGTH;
            if (frameLength > MAX_FRAME_LENGTH)
            {
                // Enter the discard mode and discard everything received so far.
                discardingTooLongFrame = true;
                tooLongStreamId = streamId;
                tooLongFrameLength = frameLength;
                bytesToDiscard = discard(buffer, frameLength);
                if (bytesToDiscard <= 0)
                    fail();
                return null;
            }

            if (buffer.readableBytes() < frameLength)
                return null;

            // extract body
            ByteBuf body = buffer.slice(idx, (int) bodyLength);

            idx += bodyLength;
            buffer.readerIndex(idx);

            return new Frame(new Header(version, flags, streamId, type, timeSource.nanoTime()), body.retain());
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> results)
        throws Exception
        {
            Frame frame = null;
            try
            {
                frame = decodeFrame(buffer);
                if (frame == null)
                    return;

                Attribute<Connection> attrConn = ctx.channel().attr(Connection.attributeKey);
                Connection connection = attrConn.get();
                if (connection == null)
                {
                    // First message seen on this channel, attach the connection object
                    connection = factory.newConnection(ctx.channel(), frame.header.version);
                    attrConn.set(connection);
                }
                else if (connection.getVersion() != frame.header.version)
                {
                    throw ErrorMessage.wrap(
                    new ProtocolException(String.format(
                    "Invalid message version. Got %s but previous messages on this connection had version %s",
                    frame.header.version, connection.getVersion())),
                    frame.header.streamId);
                }

                if (processor != null)
                    processor.maybeDoAsync(ctx, frame, results);
                else
                    results.add(frame);
            }
            catch (Throwable t)
            {
                if (frame != null)
                    frame.release();
                throw t;
            }
        }

        private void fail()
        {
            // Reset to the initial state and throw the exception
            long tooLongFrameLength = this.tooLongFrameLength;
            this.tooLongFrameLength = 0;
            discardingTooLongFrame = false;
            String msg = String.format("Request is too big: length %d exceeds maximum allowed length %d.", tooLongFrameLength,  MAX_FRAME_LENGTH);
            throw ErrorMessage.wrap(new InvalidRequestException(msg), tooLongStreamId);
        }
    }

    // How much remains to be discarded
    private static long discard(ByteBuf buffer, long remainingToDiscard)
    {
        int availableToDiscard = (int) Math.min(remainingToDiscard, buffer.readableBytes());
        buffer.skipBytes(availableToDiscard);
        return remainingToDiscard - availableToDiscard;
    }

    @ChannelHandler.Sharable
    public static class Encoder extends MessageToMessageEncoder<Frame>
    {
        public void encode(ChannelHandlerContext ctx, Frame frame, List<Object> results)
        throws IOException
        {
            ByteBuf header = CBUtil.allocator.buffer(Header.LENGTH);

            Message.Type type = frame.header.type;
            header.writeByte(type.direction.addToVersion(frame.header.version.asInt()));
            header.writeByte(frame.header.flags);

            // Continue to support writing pre-v3 headers so that we can give proper error messages to drivers that
            // connect with the v1/v2 protocol. See CASSANDRA-11464.
            if (frame.header.version.isGreaterOrEqualTo(ProtocolVersion.V3))
                header.writeShort(frame.header.streamId);
            else
                header.writeByte(frame.header.streamId);

            header.writeByte(type.opcode);
            header.writeInt(frame.body.readableBytes());

            results.add(header);
            results.add(frame.body);
        }
    }

    @ChannelHandler.Sharable
    public static class Decompressor extends MessageToMessageDecoder<Frame>
    {
        public void decode(ChannelHandlerContext ctx, Frame frame, List<Object> results)
        throws IOException
        {
            Connection connection = ctx.channel().attr(Connection.attributeKey).get();

            if (!Flags.contains(frame.header.flags, HeaderFlag.COMPRESSED) || connection == null)
            {
                results.add(frame);
                return;
            }

            FrameCompressor compressor = connection.getCompressor();
            if (compressor == null)
            {
                results.add(frame);
                return;
            }

            results.add(compressor.decompress(frame));
        }
    }

    @ChannelHandler.Sharable
    public static class Compressor extends MessageToMessageEncoder<Frame>
    {
        public void encode(ChannelHandlerContext ctx, Frame frame, List<Object> results)
        throws IOException
        {
            Connection connection = ctx.channel().attr(Connection.attributeKey).get();

            // Never compress STARTUP messages
            if (frame.header.type == Message.Type.STARTUP || connection == null)
            {
                results.add(frame);
                return;
            }

            FrameCompressor compressor = connection.getCompressor();
            if (compressor == null)
            {
                results.add(frame);
                return;
            }

            frame.header.flags = Flags.add(frame.header.flags, HeaderFlag.COMPRESSED);
            results.add(compressor.compress(frame));
        }
    }

    /**
     * The async frame processor schedules frames for async decoding if the event loop asks to backpressure, in order
     * to give more CPU power to inflight requests and in particular reduce heap usage (and related GC activity).
     * <p>
     * Please note actual async processing only works with Epoll event loops ans tasks supporting
     * {@link TPCTaskType#backpressured()}, otherwise decoding will be always processed synchronously.
     */
    public static class AsyncProcessor implements Runnable
    {
        private final TPCEventLoop eventLoop;
        private final SpscArrayQueue<Pair<ChannelHandlerContext, Frame>> frames;

        public AsyncProcessor(TPCEventLoop eventLoop)
        {
            this.eventLoop = eventLoop;
            this.frames = new SpscArrayQueue<>(1 << 16);
        }

        public void maybeDoAsync(ChannelHandlerContext context, Frame frame, List<Object> out)
        {
            // If backpressure is not supported by the message type, or the queue is empty and we shouldn't backpressure
            // (due to not hitting the limit), execute synchronously by directly adding the frame to the output list:
            if (!frame.header.type.supportsBackpressure || (!eventLoop.shouldBackpressure() && frames.isEmpty()))
                out.add(frame);
            // Otherwise execute async:
            else
            {
                // If the queue is empty, this is the first async execution, so schedule it: we want to schedule only
                // once, because when it will execute we will try to consume the whole queue.
                if (frames.isEmpty())
                    schedule();
                // Otherwise try offering and fail if unable to:
                if (!frames.offer(Pair.create(context, frame)))
                    throw new OverloadedException("Too many pending client requests, dropping the current request.");
            }
        }

        @Override
        public void run()
        {
            // Consume frames and pass them to the next handler in the channel pipeline:
            while (!frames.isEmpty() && !eventLoop.shouldBackpressure())
            {
                Pair<ChannelHandlerContext, Frame> contextAndFrame = frames.poll();
                if (contextAndFrame.left.channel().isActive())
                    contextAndFrame.left.fireChannelRead(contextAndFrame.right);
                else
                    contextAndFrame.right.release();
            }
            // If we didn't drain the whole queue, reschedule:
            if (!frames.isEmpty())
                schedule();
        }

        /**
         * Schedule frame decoding on the back of this event loop.
         */
        private void schedule()
        {
            eventLoop.execute(new TPCRunnable(
                this,
                ExecutorLocals.create(),
                TPCTaskType.FRAME_DECODE,
                eventLoop.coreId()));
        }
    }
}
