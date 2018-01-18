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

package org.apache.cassandra.cache;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.AsynchronousChannelProxy;
import org.apache.cassandra.io.util.ChunkReader;
import org.apache.cassandra.io.util.FileAccessType;
import org.apache.cassandra.io.util.Rebufferer;
import org.mockito.Matchers;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.psjava.util.AssertStatus.assertTrue;

public class CachingRebuffererTest
{
    final int PAGE_SIZE = 4096;
    File file;
    AsynchronousChannelProxy channel;
    ChunkReader chunkReader;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp() throws IOException
    {
        file = File.createTempFile("CachingRebuffererTest", "");
        file.deleteOnExit();

        channel = new AsynchronousChannelProxy(file.getPath(), false);
        chunkReader = Mockito.mock(ChunkReader.class);

        when(chunkReader.chunkSize()).thenReturn(PAGE_SIZE);
        when(chunkReader.channel()).thenReturn(channel);

        ChunkCache.instance.invalidateFile(file.getPath());
    }

    // Helper test to estimate the memory overhead caused by buffer cache
    @Ignore
    @Test
    public void calculateMemoryOverhead() throws InterruptedException
    {
        class EmptyAllocatingChunkReader implements ChunkReader
        {
            public CompletableFuture<ByteBuffer> readChunk(long position, ByteBuffer buffer)
            {
                return CompletableFuture.completedFuture(ByteBuffer.allocateDirect(PAGE_SIZE));
            }

            public int chunkSize()
            {
                return PAGE_SIZE;
            }

            public BufferType preferredBufferType()
            {
                return BufferType.OFF_HEAP;
            }

            public boolean isMmap()
            {
                return false;
            }

            public ChunkReader withChannel(AsynchronousChannelProxy channel)
            {
                return this;
            }

            public Rebufferer instantiateRebufferer(FileAccessType accessType)
            {
                return null;
            }

            public void close()
            {

            }

            public AsynchronousChannelProxy channel()
            {
                return channel;
            }

            public long fileLength()
            {
                return 0;
            }

            public double getCrcCheckChance()
            {
                return 0;
            }
        }

        Rebufferer rebufferer = ChunkCache.instance.maybeWrap(new EmptyAllocatingChunkReader()).instantiateRebufferer();

        // Ask the runtime to GC
        for (int i = 0; i < 10; i++)
            Runtime.getRuntime().gc();

        long mem = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();

        // Allocate 1,5M items
        long count = 1_500_000;

        // Cache them
        for (long i = 0; i < count; i++)
            rebufferer.rebufferAsync(i * PAGE_SIZE);

        // Ask the runtime to GC again
        for (int i = 0; i < 10; i++)
            Runtime.getRuntime().gc();

        // Sleep a bit to be more sure GC is actually triggered
        Thread.sleep(10000);

        long memAfter = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        System.out.println("(memAfter - mem) = " + ((memAfter - mem) / count));
    }

    @Test
    public void testRebufferInSamePage()
    {
        when(chunkReader.readChunk(anyInt(), Matchers.any(ByteBuffer.class)))
        .thenReturn(CompletableFuture.completedFuture(ByteBuffer.allocate(PAGE_SIZE)));
        when(chunkReader.chunkSize()).thenReturn(PAGE_SIZE);

        Rebufferer rebufferer = ChunkCache.instance.maybeWrap(chunkReader).instantiateRebufferer();
        assertNotNull(rebufferer);
        assertEquals(PAGE_SIZE, rebufferer.rebufferSize());

        for (int i = 0; i < PAGE_SIZE; i++)
        {
            Rebufferer.BufferHolder buffer = rebufferer.rebuffer(i);
            assertNotNull(buffer);
            assertEquals(PAGE_SIZE, buffer.buffer().capacity());
            assertEquals(0, buffer.offset());
            buffer.release();
        }

        verify(chunkReader, times(1)).readChunk(anyInt(), Matchers.any(ByteBuffer.class));
    }

    @Test
    public void testRebufferSeveralPages()
    {
        when(chunkReader.readChunk(anyInt(), Matchers.any(ByteBuffer.class)))
        .thenReturn(CompletableFuture.completedFuture(ByteBuffer.allocate(PAGE_SIZE)));

        Rebufferer rebufferer = ChunkCache.instance.maybeWrap(chunkReader).instantiateRebufferer();
        assertNotNull(rebufferer);

        final int numPages = 10;

        for (int j = 0; j < numPages; j++)
        {
            for (int i = j * PAGE_SIZE; i < (j + 1) * PAGE_SIZE; i ++)
            {
                Rebufferer.BufferHolder buffer = rebufferer.rebuffer(i);
                assertNotNull(buffer);
                assertEquals(PAGE_SIZE, buffer.buffer().capacity());
                assertEquals(j * PAGE_SIZE, buffer.offset());
                buffer.release();
            }
        }

        verify(chunkReader, times(numPages)).readChunk(anyInt(), Matchers.any(ByteBuffer.class));
    }

    @Test
    public void testRebufferInCacheOnly() throws InterruptedException
    {
        final CountDownLatch completed = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>(null);

        final CompletableFuture<ByteBuffer> bufferFuture = new CompletableFuture<>();
        when(chunkReader.readChunk(anyInt(), Matchers.any(ByteBuffer.class))).thenReturn(bufferFuture);

        Rebufferer rebufferer = ChunkCache.instance.maybeWrap(chunkReader).instantiateRebufferer();
        assertNotNull(rebufferer);

        TPCScheduler scheduler =  TPC.bestTPCScheduler();

        try
        {
            rebufferer.rebuffer(0, Rebufferer.ReaderConstraint.ASYNC);
            fail("Should have thrown a NotInCacheException");
        }
        catch (Rebufferer.NotInCacheException ex)
        {
            ex.accept(() -> completed.countDown(),
                      throwable -> { completed.countDown(); error.set(throwable); return null; },
                      scheduler);
        }

        bufferFuture.complete(ByteBuffer.allocate(PAGE_SIZE));

        scheduler.getExecutor().execute(() -> {}); // a no-op on the same scheduler to ensure the callback has executed

        completed.await(10, TimeUnit.SECONDS);
        assertNull(error.get());
    }

    @Test
    public void testRebufferContendedPage() throws InterruptedException
    {
        final int numThreads = 15;
        final int numAttempts = 1024;
        final CountDownLatch doneLatch = new CountDownLatch(numThreads);
        final AtomicReference<Throwable> error = new AtomicReference<>(null);
        final AtomicInteger numBuffers = new AtomicInteger(0);

        when(chunkReader.readChunk(anyInt(), Matchers.any(ByteBuffer.class)))
        .then(invocationOnMock -> {
            numBuffers.incrementAndGet();
            return CompletableFuture.completedFuture(ByteBuffer.allocate(PAGE_SIZE));
        });

        Rebufferer rebufferer = ChunkCache.instance.maybeWrap(chunkReader).instantiateRebufferer();
        assertNotNull(rebufferer);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (int j = 0; j < numThreads; j++)
        {
            executor.submit(() -> {
                try
                {
                    for (int i = 0; i < numAttempts; i++)
                    {
                        Rebufferer.BufferHolder buffer = rebufferer.rebuffer(0);
                        assertNotNull(buffer);
                        assertEquals(PAGE_SIZE, buffer.buffer().capacity());
                        assertEquals(0, buffer.offset());
                        buffer.release();

                        // removes the buffer from the cache, other threads should still be able to create a new one and
                        // insert it into the cache thanks to the ref. counting mechanism
                        ((ChunkCache.CachingRebufferer) rebufferer).invalidate(0);
                    }
                }
                catch (Throwable t)
                {
                    error.set(t);
                }
                finally
                {
                    doneLatch.countDown();
                }
            });
        }

        doneLatch.await(1, TimeUnit.MINUTES);
        assertNull(error.get());
        assertTrue(numBuffers.get() > 1); // there should be several buffers created, in the thousands, up to numThreads * numAttempts
    }

    @Test(expected = CorruptSSTableException.class)
    public void testExceptionInReadChunk()
    {
        when(chunkReader.readChunk(anyInt(), Matchers.any(ByteBuffer.class)))
        .thenThrow(CorruptSSTableException.class);

        Rebufferer rebufferer = ChunkCache.instance.maybeWrap(chunkReader).instantiateRebufferer();
        assertNotNull(rebufferer);

        rebufferer.rebufferAsync(0);
    }

    @Test(expected = CorruptSSTableException.class)
    public void testReadChunkReturnsFutureCompletedExceptionally()
    {
        CompletableFuture<ByteBuffer> bufferFuture = new CompletableFuture<>();
        bufferFuture.completeExceptionally(new CorruptSSTableException(null, file.getPath()));

        when(chunkReader.readChunk(anyInt(), Matchers.any(ByteBuffer.class))).thenReturn(bufferFuture);

        Rebufferer rebufferer = ChunkCache.instance.maybeWrap(chunkReader).instantiateRebufferer();
        assertNotNull(rebufferer);

        CompletableFuture<Rebufferer.BufferHolder> res = rebufferer.rebufferAsync(0);
        assertNotNull(res);
        assertTrue(res.isCompletedExceptionally());

        rebufferer.rebuffer(0); // with throw a CorruptSSTableException
    }
}
