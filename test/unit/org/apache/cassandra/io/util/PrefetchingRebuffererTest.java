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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.mockito.Mockito;

import static org.junit.Assert.*;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.psjava.util.AssertStatus.assertTrue;

public class PrefetchingRebuffererTest
{
    final int PAGE_SIZE = 4096;
    final int READ_AHEAD_KB = PAGE_SIZE * 2;
    final double READ_AHEAD_WINDOW = 0.5;
    AsynchronousChannelProxy channel;

    final Map<Long, TestBufferHolder> buffers = new HashMap();

    Rebufferer source;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp() throws IOException
    {
        channel = Mockito.mock(AsynchronousChannelProxy.class);
        source = Mockito.mock(Rebufferer.class);
        when(source.rebufferSize()).thenReturn(PAGE_SIZE);
        when(source.channel()).thenReturn(channel);
        when(source.fileLength()).thenReturn(Long.MAX_VALUE);

        buffers.clear();
    }

    Rebufferer.BufferHolder makeBuffer(long offset)
    {
        TestBufferHolder ret = buffers.computeIfAbsent(offset, o -> new TestBufferHolder(ByteBuffer.allocate(PAGE_SIZE), o));
        ret.numRequested++;
        return ret;
    }


    @Test
    public void testPrefetchSamePage()
    {
        PrefetchingRebufferer rebufferer = new PrefetchingRebufferer(source, channel, READ_AHEAD_KB, READ_AHEAD_WINDOW);
        assertNotNull(rebufferer);

        when(source.rebufferAsync(anyLong())).thenAnswer(mock -> CompletableFuture.completedFuture(makeBuffer((long)mock.getArguments()[0])));

        for (int i = 0; i < PAGE_SIZE; i++)
        {
            Rebufferer.BufferHolder buf = rebufferer.rebuffer(i);
            assertNotNull(buf);
            assertEquals(0L, buf.offset());
            assertEquals(PAGE_SIZE, buf.buffer().capacity());
            buf.release();
        }

        rebufferer.close();

        assertEquals(3, buffers.size()); // 1 requested and 2 prefetched
        assertTrue(buffers.containsKey(0L));
        assertTrue(buffers.containsKey((long)PAGE_SIZE));
        assertTrue(buffers.containsKey((long)PAGE_SIZE * 2));
        assertFalse(buffers.containsKey((long)PAGE_SIZE * 3)); // never prefetched

        assertEquals(PAGE_SIZE, buffers.get(0L).numRequested); // requested many times
        assertEquals(1, buffers.get((long)PAGE_SIZE).numRequested); // prefetched only once
        assertEquals(1, buffers.get((long)PAGE_SIZE * 2).numRequested); // prefetched only once

        for (TestBufferHolder buffer : buffers.values())
            assertTrue(buffer.released); // make sure close is effective in releasing prefetched buffers

        verify(source, times(1)).close();
    }

    @Test
    public void testPrefetchAtPageBoundaries()
    {
        PrefetchingRebufferer.metrics.reset();

        final int prefetchSize = 8;
        final int windowSize = 4;
        final int numPages = 10;
        PrefetchingRebufferer rebufferer = new PrefetchingRebufferer(source,
                                                                     channel,
                                                                     prefetchSize * PAGE_SIZE,
                                                                     (double)windowSize / prefetchSize);
        assertNotNull(rebufferer);

        when(source.rebufferAsync(anyLong())).thenAnswer(mock -> CompletableFuture.completedFuture(makeBuffer((long)mock.getArguments()[0])));

        for (int i = 0; i < numPages; i++)
        {
            Rebufferer.BufferHolder buf = rebufferer.rebuffer(i * PAGE_SIZE);
            assertNotNull(buf);
            assertEquals(i * PAGE_SIZE, buf.offset());
            assertEquals(PAGE_SIZE, buf.buffer().capacity());
            buf.release();

            for (int k = i; k <= i + windowSize; k ++)
            {
                assertTrue(buffers.containsKey((long)k * PAGE_SIZE));
                assertEquals(1, buffers.get((long)k * PAGE_SIZE).numRequested);
            }
        }

        rebufferer.close();

        assertTrue(numPages + windowSize <= PrefetchingRebufferer.metrics.prefetched.getCount(),
                   String.format("We should have prefetched at least num pages + window size (%d + %d = %d), but instead got %d",
                                 numPages, windowSize, numPages + windowSize, PrefetchingRebufferer.metrics.prefetched.getCount()));

        assertTrue(windowSize <= PrefetchingRebufferer.metrics.unused.getCount(),
                   String.format("We should have not used at least window size (%d), not %d",
                   windowSize, PrefetchingRebufferer.metrics.unused.getCount()));
    }

    @Test
    public void testPrefetchInCacheOnly() throws InterruptedException
    {
        final CountDownLatch completed = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>(null);
        final TPCScheduler scheduler =  TPC.bestTPCScheduler();

        PrefetchingRebufferer rebufferer = new PrefetchingRebufferer(source, channel, READ_AHEAD_KB, READ_AHEAD_WINDOW);
        assertNotNull(rebufferer);

        final CompletableFuture<Rebufferer.BufferHolder> bufferFuture = new CompletableFuture<>();
        when(source.rebufferAsync(anyLong())).thenAnswer(mock -> bufferFuture);

        try
        {
            rebufferer.rebuffer(0L, Rebufferer.ReaderConstraint.ASYNC);
            fail("Should have thrown a NotInCacheException");
        }
        catch (Rebufferer.NotInCacheException ex)
        {
            ex.accept(() -> completed.countDown(),
                      throwable -> { completed.countDown(); error.set(throwable); return null; },
                      scheduler);
        }

        bufferFuture.complete(makeBuffer(0L));

        scheduler.getExecutor().execute(() -> {}); // a no-op on the same scheduler to ensure the callback has executed

        completed.await(10, TimeUnit.SECONDS);
        assertNull(error.get());

        // this passes because it is pre-fetched (actually it's the same future just completed)
        Rebufferer.BufferHolder buf = rebufferer.rebuffer(PAGE_SIZE, Rebufferer.ReaderConstraint.ASYNC);
        assertNotNull(buf);
        buf.release();

        rebufferer.close();
    }

    @Test
    public void testNonSequentialAccess()
    {
        PrefetchingRebufferer.metrics.reset();

        PrefetchingRebufferer rebufferer = new PrefetchingRebufferer(source, channel, READ_AHEAD_KB, READ_AHEAD_WINDOW);
        assertNotNull(rebufferer);

        when(source.rebufferAsync(anyLong())).thenAnswer(mock -> CompletableFuture.completedFuture(makeBuffer((long)mock.getArguments()[0])));

        long[] offsets = new long[] { 0, PAGE_SIZE * 2, PAGE_SIZE };

        for (long offset : offsets)
        {
            Rebufferer.BufferHolder buf = rebufferer.rebuffer(offset);
            assertNotNull(buf);
            assertEquals(offset, buf.offset());
            assertEquals(PAGE_SIZE, buf.buffer().capacity());
            buf.release();
        }

        rebufferer.close();
        assertEquals(5, buffers.size());

        // requesting PAGE_SIZE after PAGE_SIZE * 2 will not trigger any PF
        assertEquals(1, PrefetchingRebufferer.metrics.skipped.getCount());

        for (int i = 0; i < 5; i++)
        {
            TestBufferHolder buffer = buffers.get((long) i * PAGE_SIZE);
            assertNotNull(buffer);
            // i == 1 means offset = PAGE_SIZE, which is requested a second time because no longer in the queue
            assertEquals(i == 1 ? 2 : 1, buffer.numRequested);
            assertTrue(buffer.released);
        }
    }

    @Test
    public void testSkippingBuffers()
    {
        PrefetchingRebufferer.metrics.reset();

        final int prefetchSize = 8;
        final int windowSize = 4;
        final int numPages = 24;
        PrefetchingRebufferer rebufferer = new PrefetchingRebufferer(source,
                                                                     channel,
                                                                     prefetchSize * PAGE_SIZE,
                                                                     (double)windowSize / prefetchSize);
        assertNotNull(rebufferer);

        when(source.rebufferAsync(anyLong())).thenAnswer(mock -> CompletableFuture.completedFuture(makeBuffer((long)mock.getArguments()[0])));

        for (int i = 0; i < numPages; i+=2)
        {
            Rebufferer.BufferHolder buf = rebufferer.rebuffer(i * PAGE_SIZE);
            assertNotNull(buf);
            assertEquals(i * PAGE_SIZE, buf.offset());
            assertEquals(PAGE_SIZE, buf.buffer().capacity());
            buf.release();

            for (int k = i; k <= i + windowSize; k ++)
            {
                assertTrue(buffers.containsKey((long)k * PAGE_SIZE));
                assertEquals(1, buffers.get((long)k * PAGE_SIZE).numRequested);
            }
        }

        rebufferer.close();

        // we skip every other page so we requested this many buffers
        int numBuffersRequested = numPages / 2;

        assertTrue(numBuffersRequested + windowSize <= PrefetchingRebufferer.metrics.prefetched.getCount(),
                   String.format("We should have prefetched at least num pages + window size (%d + %d = %d), but instead got %d",
                                 numPages, windowSize, numPages + windowSize, PrefetchingRebufferer.metrics.prefetched.getCount()));

        // each request discards 1 buffer
        assertTrue(numBuffersRequested + windowSize <= PrefetchingRebufferer.metrics.unused.getCount(),
                   String.format("We should have not used at least window size (%d), not %d",
                                 windowSize, PrefetchingRebufferer.metrics.unused.getCount()));
    }

    @Test
    public void testBatchesAreSubmitted()
    {
        PrefetchingRebufferer.metrics.reset();

        PrefetchingRebufferer rebufferer = new PrefetchingRebufferer(source, channel, READ_AHEAD_KB, READ_AHEAD_WINDOW);
        assertNotNull(rebufferer);

        final long offset = PAGE_SIZE;
        final CompletableFuture<Rebufferer.BufferHolder> bufferFuture = new CompletableFuture<>();
        when(source.rebufferAsync(anyLong())).thenAnswer(mock -> bufferFuture);

        CompletableFuture<Rebufferer.BufferHolder> ret = rebufferer.rebufferAsync(offset);
        bufferFuture.complete(makeBuffer(offset));

        ret.join();

        verify(channel, times(2)).submitBatch(); // one call for the requested buffer and one for read-ahead
    }


    @Test(expected = CorruptSSTableException.class)
    public void testExceptionInSourceRebuffer()
    {
        PrefetchingRebufferer rebufferer = new PrefetchingRebufferer(source, channel, READ_AHEAD_KB, READ_AHEAD_WINDOW);
        assertNotNull(rebufferer);

        when(source.rebufferAsync(anyLong())).thenThrow(CorruptSSTableException.class);

        rebufferer.rebufferAsync(0);
    }

    @Test(expected = CorruptSSTableException.class)
    public void testReadChunkReturnsFutureCompletedExceptionally()
    {
        CompletableFuture<Rebufferer.BufferHolder> bufferFuture = new CompletableFuture<>();
        bufferFuture.completeExceptionally(new CorruptSSTableException(null, ""));

        PrefetchingRebufferer rebufferer = new PrefetchingRebufferer(source, channel, READ_AHEAD_KB, READ_AHEAD_WINDOW);
        assertNotNull(rebufferer);

        when(source.rebufferAsync(anyLong())).thenReturn(bufferFuture);

        CompletableFuture<Rebufferer.BufferHolder> res = rebufferer.rebufferAsync(0);
        assertNotNull(res);
        assertTrue(res.isCompletedExceptionally());

        try
        {
            rebufferer.rebuffer(0); // with throw a CorruptSSTableException
        }
        finally
        {
            rebufferer.close();
        }
    }

    private static class TestBufferHolder implements Rebufferer.BufferHolder
    {
        final ByteBuffer buffer;
        boolean released;
        long offset;
        int numRequested;

        TestBufferHolder(ByteBuffer buffer, long offset)
        {
            this.buffer = buffer;
            this.released = false;
            this.offset = offset;
        }

        public ByteBuffer buffer()
        {
            return buffer.duplicate();
        }

        public long offset()
        {
            return offset;
        }

        public void release()
        {
            released = true;
        }

    }
}
