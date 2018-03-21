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

import io.netty.buffer.ByteBuf;

import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import org.junit.BeforeClass;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.cassandra.concurrent.TPCEventLoop;
import org.apache.cassandra.concurrent.TPCRunnable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.TestTimeSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class FrameAsyncProcessorTest
{
    private static final ByteBuf BUFFER = CBUtil.allocator.buffer();

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testSyncProcessingIfEventLoopShouldNotBackpressure()
    {
        TPCEventLoop eventLoop = Mockito.mock(TPCEventLoop.class);
        Mockito.when(eventLoop.shouldBackpressure(false)).thenReturn(false);
        Frame.AsyncProcessor processor = new Frame.AsyncProcessor(eventLoop);

        // Use a type supporting backpressure and verify sync execution due to the event loop not supporting it:
        Frame frame = createFrame(Message.Type.QUERY);
        List out = new LinkedList();
        processor.maybeDoAsync(Mockito.mock(ChannelHandlerContext.class), frame, out);
        Assert.assertEquals(1, out.size());
    }

    @Test
    public void testSyncProcessingIfNotSupportedByType()
    {
        TPCEventLoop eventLoop = Mockito.mock(TPCEventLoop.class);
        Mockito.when(eventLoop.shouldBackpressure(false)).thenReturn(true);
        Frame.AsyncProcessor processor = new Frame.AsyncProcessor(eventLoop);

        // Use a type not supporting backpressure and verify sync execution even if the event loop supports it:
        Frame frame = createFrame(Message.Type.AUTHENTICATE);
        List out = new LinkedList();
        processor.maybeDoAsync(Mockito.mock(ChannelHandlerContext.class), frame, out);
        Assert.assertEquals(1, out.size());
    }

    @Test
    public void testAsyncProcessing()
    {
        TPCEventLoop eventLoop = Mockito.mock(TPCEventLoop.class);
        Mockito.when(eventLoop.shouldBackpressure(false)).thenReturn(true);
        Frame.AsyncProcessor processor = new Frame.AsyncProcessor(eventLoop);

        // Verify async processing:
        Frame frame = createFrame(Message.Type.QUERY);
        List out = new LinkedList();
        processor.maybeDoAsync(Mockito.mock(ChannelHandlerContext.class), frame, out);
        Assert.assertTrue(out.isEmpty());
        Mockito.verify(eventLoop).execute(Mockito.any());

        // Reset the event loop mock to later verify we don't schedule again: I know, test smell...
        Mockito.reset(eventLoop);

        // Now make the event loop return false: it should still do async because the frames queue is not empty:
        Mockito.when(eventLoop.shouldBackpressure(false)).thenReturn(false);
        frame = createFrame(Message.Type.QUERY);
        out = new LinkedList();
        processor.maybeDoAsync(Mockito.mock(ChannelHandlerContext.class), frame, out);
        Assert.assertTrue(out.isEmpty());
        // But no execution of the schedule task this time as the queue was not empty:
        Mockito.verify(eventLoop, Mockito.never()).execute(Mockito.any());

        // Finally verify if the message type doesn't support backpressure we do sync regardless if we did async before:
        Mockito.when(eventLoop.shouldBackpressure(false)).thenReturn(true);
        frame = createFrame(Message.Type.AUTHENTICATE);
        out = new LinkedList();
        processor.maybeDoAsync(Mockito.mock(ChannelHandlerContext.class), frame, out);
        Assert.assertEquals(1, out.size());
    }

    @Test
    public void testFrameIsProcessed()
    {
        ChannelHandlerContext context = Mockito.mock(ChannelHandlerContext.class);
        Channel channel = Mockito.mock(Channel.class);
        Mockito.when(context.channel()).thenReturn(channel);
        Mockito.when(channel.isActive()).thenReturn(true);

        TPCEventLoop eventLoop = Mockito.mock(TPCEventLoop.class);
        Frame.AsyncProcessor processor = new Frame.AsyncProcessor(eventLoop);

        ArgumentCaptor<TPCRunnable> capturedTask = ArgumentCaptor.forClass(TPCRunnable.class);

        // Async processing will schedule a task on the event loop, which we capure here:
        Mockito.when(eventLoop.shouldBackpressure(false)).thenReturn(true);
        Frame frame = createFrame(Message.Type.QUERY);
        List out = new LinkedList();
        processor.maybeDoAsync(context, frame, out);
        Assert.assertTrue(out.isEmpty());
        Mockito.verify(eventLoop).execute(capturedTask.capture());

        // Get the captured task and run it: this will consume the queue and pass the contained frame to the context.
        Mockito.when(eventLoop.shouldBackpressure(false)).thenReturn(false);
        capturedTask.getValue().run();
        Mockito.verify(context).fireChannelRead(Mockito.same(frame));
    }

    @Test
    public void testFrameIsNotProcessedDueToInactiveChannel()
    {
        ChannelHandlerContext context = Mockito.mock(ChannelHandlerContext.class);
        Channel channel = Mockito.mock(Channel.class);
        Mockito.when(context.channel()).thenReturn(channel);
        Mockito.when(channel.isActive()).thenReturn(false);

        TPCEventLoop eventLoop = Mockito.mock(TPCEventLoop.class);
        Frame.AsyncProcessor processor = new Frame.AsyncProcessor(eventLoop);

        ArgumentCaptor<TPCRunnable> capturedTask = ArgumentCaptor.forClass(TPCRunnable.class);

        // Async processing will schedule a task on the event loop, which we capure here:
        Mockito.when(eventLoop.shouldBackpressure(false)).thenReturn(true);
        Frame frame = createFrame(Message.Type.QUERY);
        List out = new LinkedList();
        processor.maybeDoAsync(context, frame, out);
        Assert.assertTrue(out.isEmpty());
        Mockito.verify(eventLoop).execute(capturedTask.capture());

        // Get the captured task and run it: this will consume the queue but not pass the contained frame to the context.
        Mockito.when(eventLoop.shouldBackpressure(false)).thenReturn(false);
        capturedTask.getValue().run();
        Mockito.verify(eventLoop, Mockito.times(1)).execute(Mockito.anyObject());
        Mockito.verify(context, Mockito.never()).fireChannelRead(Mockito.anyObject());
    }

    @Test
    public void testFrameIsNotProcessedDueToBackpressure()
    {
        ChannelHandlerContext context = Mockito.mock(ChannelHandlerContext.class);
        Channel channel = Mockito.mock(Channel.class);
        Mockito.when(context.channel()).thenReturn(channel);
        Mockito.when(channel.isActive()).thenReturn(false);

        TPCEventLoop eventLoop = Mockito.mock(TPCEventLoop.class);
        Frame.AsyncProcessor processor = new Frame.AsyncProcessor(eventLoop);

        ArgumentCaptor<TPCRunnable> capturedTask = ArgumentCaptor.forClass(TPCRunnable.class);

        // Async processing will schedule a task on the event loop, which we capure here:
        Mockito.when(eventLoop.shouldBackpressure(false)).thenReturn(true);
        Frame frame = createFrame(Message.Type.QUERY);
        List out = new LinkedList();
        processor.maybeDoAsync(context, frame, out);
        Assert.assertTrue(out.isEmpty());
        Mockito.verify(eventLoop).execute(capturedTask.capture());

        // Get the captured task and run it: this will not consume the queue due to backpressure still on:
        Mockito.when(eventLoop.shouldBackpressure(false)).thenReturn(true);
        capturedTask.getValue().run();
        Mockito.verify(eventLoop, Mockito.times(2)).execute(Mockito.anyObject());
        Mockito.verify(context, Mockito.never()).fireChannelRead(Mockito.anyObject());
    }

    private Frame createFrame(Message.Type type)
    {
        return Frame.create(new TestTimeSource(), type, 0, ProtocolVersion.V5, 0, BUFFER);
    }
}
