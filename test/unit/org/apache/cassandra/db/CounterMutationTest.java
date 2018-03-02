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
package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.WrappedRunnable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CounterMutationTest
{
    private static final String KEYSPACE1 = "CounterMutationTest";
    private static final String CF1 = "Counter1";
    private static final String CF2 = "Counter2";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.counterCFMD(KEYSPACE1, CF1),
                                    SchemaLoader.counterCFMD(KEYSPACE1, CF2));
    }

    @Test
    public void testSingleCell() throws WriteTimeoutException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();

        // Do the initial update (+1)
        addAndCheck(cfs, 1, 1);

        // Make another increment (+2)
        addAndCheck(cfs, 2, 3);

        // Decrement to 0 (-3)
        addAndCheck(cfs, -3, 0);
    }

    private void addAndCheck(ColumnFamilyStore cfs, long toAdd, long expected)
    {
        ColumnMetadata cDef = cfs.metadata().getColumn(ByteBufferUtil.bytes("val"));
        Mutation m = new RowUpdateBuilder(cfs.metadata(), 5, "key1").clustering("cc").add("val", toAdd).build();
        new CounterMutation(m, ConsistencyLevel.ONE).apply();

        Row row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val").build());
        assertEquals(expected, CounterContext.instance().total(row.getCell(cDef).value()));
    }

    @Test
    public void testTwoCells() throws WriteTimeoutException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();

        // Do the initial update (+1, -1)
        addTwoAndCheck(cfs, 1L, 1L, -1L, -1L);

        // Make another increment (+2, -2)
        addTwoAndCheck(cfs, 2L, 3L, -2L, -3L);

        // Decrement to 0 (-3, +3)
        addTwoAndCheck(cfs, -3L, 0L, 3L, 0L);
    }

    private void addTwoAndCheck(ColumnFamilyStore cfs, long addOne, long expectedOne, long addTwo, long expectedTwo)
    {
        ColumnMetadata cDefOne = cfs.metadata().getColumn(ByteBufferUtil.bytes("val"));
        ColumnMetadata cDefTwo = cfs.metadata().getColumn(ByteBufferUtil.bytes("val2"));

        Mutation m = new RowUpdateBuilder(cfs.metadata(), 5, "key1")
            .clustering("cc")
            .add("val", addOne)
            .add("val2", addTwo)
            .build();
        new CounterMutation(m, ConsistencyLevel.ONE).apply();

        Row row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val", "val2").build());
        assertEquals(expectedOne, CounterContext.instance().total(row.getCell(cDefOne).value()));
        assertEquals(expectedTwo, CounterContext.instance().total(row.getCell(cDefTwo).value()));
    }

    @Test
    public void testBatch() throws WriteTimeoutException
    {
        ColumnFamilyStore cfsOne = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        ColumnFamilyStore cfsTwo = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF2);

        cfsOne.truncateBlocking();
        cfsTwo.truncateBlocking();

        // Do the update (+1, -1), (+2, -2)
        Mutation batch = new Mutation(KEYSPACE1, Util.dk("key1"));
        batch.add(new RowUpdateBuilder(cfsOne.metadata(), 5, "key1")
            .clustering("cc")
            .add("val", 1L)
            .add("val2", -1L)
            .build().get(cfsOne.metadata()));

        batch.add(new RowUpdateBuilder(cfsTwo.metadata(), 5, "key1")
            .clustering("cc")
            .add("val", 2L)
            .add("val2", -2L)
            .build().get(cfsTwo.metadata()));

        new CounterMutation(batch, ConsistencyLevel.ONE).apply();

        ColumnMetadata c1cfs1 = cfsOne.metadata().getColumn(ByteBufferUtil.bytes("val"));
        ColumnMetadata c2cfs1 = cfsOne.metadata().getColumn(ByteBufferUtil.bytes("val2"));

        Row row = Util.getOnlyRow(Util.cmd(cfsOne).includeRow("cc").columns("val", "val2").build());
        assertEquals(1L, CounterContext.instance().total(row.getCell(c1cfs1).value()));
        assertEquals(-1L, CounterContext.instance().total(row.getCell(c2cfs1).value()));

        ColumnMetadata c1cfs2 = cfsTwo.metadata().getColumn(ByteBufferUtil.bytes("val"));
        ColumnMetadata c2cfs2 = cfsTwo.metadata().getColumn(ByteBufferUtil.bytes("val2"));
        row = Util.getOnlyRow(Util.cmd(cfsTwo).includeRow("cc").columns("val", "val2").build());
        assertEquals(2L, CounterContext.instance().total(row.getCell(c1cfs2).value()));
        assertEquals(-2L, CounterContext.instance().total(row.getCell(c2cfs2).value()));

        // Check the caches, separately
        CBuilder cb = CBuilder.create(cfsOne.metadata().comparator);
        cb.add("cc");

        assertEquals(1L, cfsOne.getCachedCounter(Util.dk("key1").getKey(), cb.build(), c1cfs1, null).count);
        assertEquals(-1L, cfsOne.getCachedCounter(Util.dk("key1").getKey(), cb.build(), c2cfs1, null).count);

        assertEquals(2L, cfsTwo.getCachedCounter(Util.dk("key1").getKey(), cb.build(), c1cfs2, null).count);
        assertEquals(-2L, cfsTwo.getCachedCounter(Util.dk("key1").getKey(), cb.build(), c2cfs2, null).count);
    }

    @Test
    public void testDeletes() throws WriteTimeoutException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();
        ColumnMetadata cOne = cfs.metadata().getColumn(ByteBufferUtil.bytes("val"));
        ColumnMetadata cTwo = cfs.metadata().getColumn(ByteBufferUtil.bytes("val2"));

        // Do the initial update (+1, -1)
        new CounterMutation(
            new RowUpdateBuilder(cfs.metadata(), 5, "key1")
                .clustering("cc")
                .add("val", 1L)
                .add("val2", -1L)
                .build(),
            ConsistencyLevel.ONE).apply();

        Row row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val", "val2").build());
        assertEquals(1L, CounterContext.instance().total(row.getCell(cOne).value()));
        assertEquals(-1L, CounterContext.instance().total(row.getCell(cTwo).value()));

        // Remove the first counter, increment the second counter
        new CounterMutation(
            new RowUpdateBuilder(cfs.metadata(), 5, "key1")
                .clustering("cc")
                .delete(cOne)
                .add("val2", -5L)
                .build(),
            ConsistencyLevel.ONE).apply();

        row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val", "val2").build());
        assertEquals(null, row.getCell(cOne));
        assertEquals(-6L, CounterContext.instance().total(row.getCell(cTwo).value()));

        // Increment the first counter, make sure it's still shadowed by the tombstone
        new CounterMutation(
            new RowUpdateBuilder(cfs.metadata(), 5, "key1")
                .clustering("cc")
                .add("val", 1L)
                .build(),
            ConsistencyLevel.ONE).apply();
        row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val", "val2").build());
        assertEquals(null, row.getCell(cOne));

        // Get rid of the complete partition
        RowUpdateBuilder.deleteRow(cfs.metadata(), 6, "key1", "cc").applyUnsafe();
        Util.assertEmpty(Util.cmd(cfs).includeRow("cc").columns("val", "val2").build());

        // Increment both counters, ensure that both stay dead
        new CounterMutation(
            new RowUpdateBuilder(cfs.metadata(), 6, "key1")
                .clustering("cc")
                .add("val", 1L)
                .add("val2", 1L)
                .build(),
            ConsistencyLevel.ONE).apply();
        Util.assertEmpty(Util.cmd(cfs).includeRow("cc").columns("val", "val2").build());
    }

    @Test
    public void testParallelWritersSameKeyWithCache() throws Throwable
    {
        testParallelWritersSameKey(true);
    }

    @Test
    public void testParallelWritersSameKeyWithoutCache() throws Throwable
    {
        testParallelWritersSameKey(false);
    }

    private void testParallelWritersSameKey(boolean withCache) throws Throwable
    {
        final int writers = 25;
        final int insertsPerWriter = 50;
        final Map<Integer, Throwable> failedWrites = new ConcurrentHashMap<>();

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();

        CyclicBarrier semaphore = new CyclicBarrier(writers);

        if (!withCache)
            CacheService.instance.setCounterCacheCapacityInMB(0);

        try
        {
            Thread[] threads = new Thread[writers];
            for (int i = 0; i < writers; i++)
            {
                final int writer = i;
                Thread t = NamedThreadFactory.createThread(new WrappedRunnable()
                {
                    public void runMayThrow()
                    {
                        try
                        {
                            final int writerOffset = writer * insertsPerWriter;
                            semaphore.await();
                            for (int i = 0; i < insertsPerWriter; i++)
                            {
                                try
                                {
                                    Mutation m = new RowUpdateBuilder(cfs.metadata(), writerOffset + i, "key1")
                                                 .clustering("cc")
                                                 .add("val", 1L)
                                                 .build(); // + 1
                                    new CounterMutation(m, ConsistencyLevel.ONE).apply();
                                }
                                catch (Throwable t)
                                {
                                    failedWrites.put(i + writerOffset, t);
                                }
                            }
                        }
                        catch (Throwable e)
                        {
                            throw new RuntimeException(e);
                        }
                    }
                });
                t.start();
                threads[i] = t;
            }

            for (int i = 0; i < writers; i++)
                threads[i].join();

            assertTrue(failedWrites.toString(), failedWrites.isEmpty());

            ColumnMetadata cDef = cfs.metadata().getColumn(ByteBufferUtil.bytes("val"));
            Row row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val").build());
            assertEquals(writers * insertsPerWriter, CounterContext.instance().total(row.getCell(cDef).value()));
        }
        finally
        {
            CacheService.instance.setCounterCacheCapacityInMB(DatabaseDescriptor.getCounterCacheSizeInMB());
        }
    }

    @Test
    public void testParallelWritersDifferentKeysWithCache() throws Throwable
    {
        testParallelWritersDifferentKeys(true);
    }

    @Test
    public void testParallelWritersDifferentKeysWithoutCache() throws Throwable
    {
        testParallelWritersDifferentKeys(false);
    }


    private void testParallelWritersDifferentKeys(boolean withCache) throws Throwable
    {
        final int writers = 100;
        final int insertsPerWriter = 200;
        final Map<Integer, Throwable> failedWrites = new ConcurrentHashMap<>();

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();

        CyclicBarrier semaphore = new CyclicBarrier(writers);

        if (!withCache)
            CacheService.instance.setCounterCacheCapacityInMB(0);

        try
        {
            Thread[] threads = new Thread[writers];
            for (int i = 0; i < writers; i++)
            {
                final int writer = i;
                Thread t = NamedThreadFactory.createThread(new WrappedRunnable()
                {
                    public void runMayThrow()
                    {
                        try
                        {
                            final int writerOffset = writer * insertsPerWriter;
                            semaphore.await();
                            for (int i = 0; i < insertsPerWriter; i++)
                            {
                                try
                                {
                                    Mutation m = new RowUpdateBuilder(cfs.metadata(), i, "key_" + Integer.toString(writer))
                                                 .clustering("cc")
                                                 .add("val", 1L)
                                                 .build(); // + 1
                                    new CounterMutation(m, ConsistencyLevel.ONE).apply();
                                }
                                catch (Throwable t)
                                {
                                    t.printStackTrace();
                                    failedWrites.put(i + writerOffset, t);
                                }
                            }
                        }
                        catch (Throwable e)
                        {
                            throw new RuntimeException(e);
                        }
                    }
                });
                t.start();
                threads[i] = t;
            }

            for (int i = 0; i < writers; i++)
                threads[i].join();

            assertTrue(failedWrites.toString(), failedWrites.isEmpty());

            ColumnMetadata cDef = cfs.metadata().getColumn(ByteBufferUtil.bytes("val"));

            for (int i = 0; i < writers; i++)
            {
                Row row = Util.getOnlyRow(Util.cmd(cfs, "key_" + Integer.toString(i)).includeRow("cc").columns("val").build());
                assertEquals(insertsPerWriter, CounterContext.instance().total(row.getCell(cDef).value()));
            }
        }
        finally
        {
            CacheService.instance.setCounterCacheCapacityInMB(DatabaseDescriptor.getCounterCacheSizeInMB());
        }
    }

    @Test
    public void testParallelWritesWithTimeouts() throws Throwable
    {
        final int writers = 100;
        final ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();

        final CyclicBarrier semaphore = new CyclicBarrier(writers);
        final List<Throwable> failedWrites = new ArrayList<>(writers - 1);

        long defaultTimeout = DatabaseDescriptor.getCounterWriteRpcTimeout();
        DatabaseDescriptor.setCounterWriteRpcTimeout(1);
        try
        {
            Thread[] threads = new Thread[writers];
            for (int i = 0; i < writers; i++)
            {
                Thread t = NamedThreadFactory.createThread(new WrappedRunnable()
                {
                    public void runMayThrow()
                    {
                    try
                    {
                        semaphore.await();
                        Mutation m = new RowUpdateBuilder(cfs.metadata(), 5, "key1")
                                     .clustering("cc")
                                     .add("val", 1L)
                                     .build(); // + 1
                        new CounterMutation(m, ConsistencyLevel.ONE).apply();
                    }
                    catch (Throwable t)
                    {
                        failedWrites.add(t);
                    }
                    }
                });
                t.start();
                threads[i] = t;
            }

            for (int i = 0; i < writers; i++)
                threads[i].join();

            // most of writers should have failed since the wait timeout was 1 ms
            assertTrue(String.format("At most one writer should have succeeded but %d failed", failedWrites.size()),
                       (failedWrites.size() > writers / 3) && (failedWrites.size() < writers));

            ColumnMetadata cDef = cfs.metadata().getColumn(ByteBufferUtil.bytes("val"));
            Row row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val").build());
            assertTrue("At leat one writer should have succeeded but the counter value is less than one",
                       CounterContext.instance().total(row.getCell(cDef).value()) >= 1);
        }
        finally
        {
            DatabaseDescriptor.setCounterWriteRpcTimeout(defaultTimeout);
        }
    }
}
