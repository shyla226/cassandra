/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.mos;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MemoryOnlyStatusTest
{
    private MemoryOnlyStatus memoryOnlyStatus;
    private static String tmpDirectory;

    @BeforeClass
    public static void setUp() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        tmpDirectory = Files.createTempDirectory("memory-only-").toString();
    }

    @AfterClass
    public static void tearDown()
    {
        FileUtils.deleteRecursive(new File(tmpDirectory));
    }

    private MappedByteBuffer createMappedBuffer(int length)
    {
        MappedByteBuffer out = null;
        try
        {
            Path filename = Paths.get(tmpDirectory, UUID.randomUUID().toString());
            out = new RandomAccessFile(filename.toString(), "rw")
                  .getChannel().map(FileChannel.MapMode.READ_ONLY, 0, length);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return out;
    }

    @Test
    public void testSequentialLocking()
    {
        final long maxToLock = 64 * 1024;
        memoryOnlyStatus = new MemoryOnlyStatus(maxToLock);
        assertEquals(maxToLock, memoryOnlyStatus.getMaxAvailableBytes());
        assertEquals(0, memoryOnlyStatus.getMemoryOnlyPercentUsed(), Math.ulp(0.0));

        // check that the totals are available and zero
        MemoryOnlyStatusMBean.TotalInfo info = memoryOnlyStatus.getMemoryOnlyTotals();
        assertNotNull(info);
        assertEquals(maxToLock, info.getMaxMemoryToLock());
        assertEquals(0, info.getUsed());
        assertEquals(0, info.getNotAbleToLock());

        // lock 4 buffers of 32k, only the first 2 should be locked
        MemoryLockedBuffer[] buffers = new MemoryLockedBuffer[4];
        for (int i = 0; i < buffers.length; i++)
            buffers[i] = lockBuffer(createMappedBuffer(32 * 1024), i < 2);

        assertEquals(1.0, memoryOnlyStatus.getMemoryOnlyPercentUsed(), Math.ulp(1.0));

        // unlock all buffers, succeeded or not
        for (MemoryLockedBuffer buffer : buffers)
            memoryOnlyStatus.unlock(buffer);

        // check that the totals are zero again
        info = memoryOnlyStatus.getMemoryOnlyTotals();
        assertNotNull(info);
        assertEquals(maxToLock, info.getMaxMemoryToLock());
        assertEquals(0, info.getUsed());
        assertEquals(0, info.getNotAbleToLock());

    }

    @Test
    public void testParallelLocking() throws InterruptedException
    {
        final long maxToLock = 256 * 1024;
        memoryOnlyStatus = new MemoryOnlyStatus(maxToLock);

        final int numTasks = 32;
        final long sizeToLock = maxToLock / numTasks;
        final ExecutorService executor = Executors.newFixedThreadPool(numTasks / 4);
        final BlockingQueue<MemoryLockedBuffer> lockedBuffers = new ArrayBlockingQueue<>(numTasks);

        {
            final CountDownLatch countDownLatch = new CountDownLatch(numTasks);
            final AtomicInteger errors = new AtomicInteger(0);

            for (int i = 0; i < numTasks; i++)
            {
                executor.submit(() ->
                                {
                                    try
                                    {
                                        MemoryLockedBuffer lockedBuffer = memoryOnlyStatus.lock(createMappedBuffer(Math.toIntExact(sizeToLock)));
                                        assertNotNull(lockedBuffer);
                                        assertTrue(lockedBuffer.succeeded);
                                        assertEquals(sizeToLock, lockedBuffer.amount);

                                        assertTrue(lockedBuffers.offer(lockedBuffer));
                                    }
                                    catch (Throwable t)
                                    {
                                        t.printStackTrace();
                                        errors.incrementAndGet();
                                    }
                                    finally
                                    {
                                        countDownLatch.countDown();
                                    }
                                });
            }

            countDownLatch.await(1, TimeUnit.MINUTES);
            assertEquals(0, errors.get());

            MemoryOnlyStatusMBean.TotalInfo info = memoryOnlyStatus.getMemoryOnlyTotals();
            assertNotNull(info);
            assertEquals(maxToLock, info.getMaxMemoryToLock());
            assertEquals(maxToLock, info.getUsed());
            assertEquals(0, info.getNotAbleToLock());
        }

        {
            final CountDownLatch countDownLatch = new CountDownLatch(numTasks);
            final AtomicInteger errors = new AtomicInteger(0);

            for (int i = 0; i < numTasks; i++)
            {
                executor.submit(() ->
                                {
                                    try
                                    {
                                        MemoryLockedBuffer buffer = lockedBuffers.poll();
                                        if (buffer != null)
                                            memoryOnlyStatus.unlock(buffer);
                                    }
                                    catch (Throwable t)
                                    {
                                        t.printStackTrace();
                                        errors.incrementAndGet();
                                    }
                                    finally
                                    {
                                        countDownLatch.countDown();
                                    }
                                });
            }

            countDownLatch.await(1, TimeUnit.MINUTES);
            assertEquals(0, errors.get());

            MemoryOnlyStatusMBean.TotalInfo info = memoryOnlyStatus.getMemoryOnlyTotals();
            assertNotNull(info);
            assertEquals(maxToLock, info.getMaxMemoryToLock());
            assertEquals(0, info.getUsed());
            assertEquals(0, info.getNotAbleToLock());
        }
    }

    private MemoryLockedBuffer lockBuffer(MappedByteBuffer buffer, boolean shouldSucceed)
    {
        MemoryOnlyStatusMBean.TotalInfo before = memoryOnlyStatus.getMemoryOnlyTotals();

        MemoryLockedBuffer lockedBuffer = memoryOnlyStatus.lock(buffer);
        assertNotNull(lockedBuffer);
        assertEquals(shouldSucceed, lockedBuffer.succeeded);
        assertEquals(roundTo4K(buffer.capacity()), lockedBuffer.amount);
        assertEquals(shouldSucceed, buffer.isLoaded());

        MemoryOnlyStatusMBean.TotalInfo after = memoryOnlyStatus.getMemoryOnlyTotals();
        if (shouldSucceed)
            assertEquals(before.getUsed() + lockedBuffer.amount, after.getUsed());
        else
            assertEquals(before.getNotAbleToLock() + lockedBuffer.amount, after.getNotAbleToLock());

        return lockedBuffer;
    }

    private long roundTo4K(long length)
    {
        return FBUtilities.align(length, 4096);
    }
}
