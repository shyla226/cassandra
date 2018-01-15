package org.apache.cassandra.cql3.continuous.paging;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;

public class ContinuousPageWriterTest
{
    @Test
    public void testSendPage()
    {
        testSendPage(true);
        testSendPage(false);
    }

    private void testSendPage(boolean inEventLoop)
    {
        ContinuousPagingTestStubs.RecordingEventLoop eventLoop = new ContinuousPagingTestStubs.RecordingEventLoop(inEventLoop);
        ContinuousPagingTestStubs.RecordingChannel channel = new ContinuousPagingTestStubs.RecordingChannel(eventLoop);
        int pages = 2;

        ContinuousPageWriter writer = new ContinuousPageWriter(() -> channel, Integer.MAX_VALUE, pages);

        for (int i = 0; i < pages; i++)
        {
            writer.sendPage(makeFrame(Message.Type.QUERY), true);
        }

        eventLoop.runAll();

        Assert.assertEquals(pages, channel.writeCalls);
        Assert.assertEquals(pages, channel.flushCalls);
    }

    @Test
    public void testSendErrorDiscardsPages()
    {
        ContinuousPagingTestStubs.RecordingEventLoop eventLoop = new ContinuousPagingTestStubs.RecordingEventLoop();
        ContinuousPagingTestStubs.RecordingChannel channel = new ContinuousPagingTestStubs.RecordingChannel(eventLoop);
        int pages = 2;

        ContinuousPageWriter writer = new ContinuousPageWriter(() -> channel, Integer.MAX_VALUE, pages);

        for (int i = 0; i < pages; i++)
        {
            writer.sendPage(makeFrame(Message.Type.QUERY), true);
        }
        writer.sendError(makeFrame(Message.Type.ERROR));

        eventLoop.runAll();

        Assert.assertEquals(1, channel.writeCalls);
        Assert.assertEquals(1, channel.flushCalls);
        Assert.assertEquals(Message.Type.ERROR, ((Frame) channel.writeObjects.get(0)).header.type);
    }

    @Test
    public void testCancelDiscardsPages()
    {
        ContinuousPagingTestStubs.RecordingEventLoop eventLoop = new ContinuousPagingTestStubs.RecordingEventLoop();
        ContinuousPagingTestStubs.RecordingChannel channel = new ContinuousPagingTestStubs.RecordingChannel(eventLoop);
        int pages = 2;

        ContinuousPageWriter writer = new ContinuousPageWriter(() -> channel, Integer.MAX_VALUE, pages);

        for (int i = 0; i < pages; i++)
        {
            writer.sendPage(makeFrame(Message.Type.QUERY), true);
        }
        writer.cancel();

        eventLoop.runAll();

        Assert.assertEquals(0, channel.writeCalls);
        Assert.assertEquals(0, channel.flushCalls);
    }

    @Test
    public void testWriterRunsOutOfSpace()
    {
        ContinuousPagingTestStubs.RecordingEventLoop eventLoop = new ContinuousPagingTestStubs.RecordingEventLoop();
        ContinuousPagingTestStubs.RecordingChannel channel = new ContinuousPagingTestStubs.RecordingChannel(eventLoop);
        int pages = 2;

        ContinuousPageWriter writer = new ContinuousPageWriter(() -> channel, Integer.MAX_VALUE, pages);

        Assert.assertTrue(writer.hasSpace());

        writer.sendPage(makeFrame(Message.Type.QUERY), true);

        Assert.assertTrue(writer.hasSpace());

        writer.sendPage(makeFrame(Message.Type.QUERY), true);

        Assert.assertFalse(writer.hasSpace());
    }

    private Frame makeFrame(Message.Type type)
    {
        return Frame.create(type, 1, ProtocolVersion.DSE_V2, Frame.Header.HeaderFlag.NONE, CBUtil.allocator.buffer());
    }
}
