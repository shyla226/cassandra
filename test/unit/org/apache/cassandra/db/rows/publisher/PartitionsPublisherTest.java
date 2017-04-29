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

package org.apache.cassandra.db.rows.publisher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.PartitionTrait;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PartitionsPublisherTest extends CQLTester
{
    @Test
    public void testConversionToIterator() throws Throwable
    {
        createTable("CREATE TABLE %s (p text, c text, v text, s text static, PRIMARY KEY (p, c))");

        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");
        execute("INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2");

        List<List<String>> expectedRows = new ArrayList<>(2);
        expectedRows.add(Arrays.asList("p1", "k1", "v1", "sv1"));
        expectedRows.add(Arrays.asList("p1", "k2", "v2", "sv1"));

        ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore(), Util.dk(ByteBufferUtil.bytes("p1"))).build();
        List<ImmutableBTreePartition> results = new ArrayList<>();
        try (UnfilteredPartitionIterator iterator = cmd.executeLocally().toIterator())
        {
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    results.add(ImmutableBTreePartition.create(partition));
                }
            }
        }

        assertEquals(1, results.size());
        assertEquals(2, results.get(0).rowCount());
    }

    @Test
    public void testEmptyPartition() throws Throwable
    {
        createTable("CREATE TABLE %s (p text, c text, v text, s text static, PRIMARY KEY (p, c))");

        ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore(), Util.dk(ByteBufferUtil.bytes("p1"))).build();
        List<ImmutableBTreePartition> results = new ArrayList<>();
        try (UnfilteredPartitionIterator iterator = cmd.executeLocally().toIterator())
        {
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    results.add(ImmutableBTreePartition.create(partition));
                }
            }
        }

        assertEquals(0, results.size());
    }

    @Test
    public void testIndexQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    " k1 int," +
                    " c1 int," +
                    " v1 int," +
                    "PRIMARY KEY (k1, c1))");
        createIndex("CREATE INDEX ON %s(v1)");

        int partitions = 3;
        int rowCount = 3;
        for (int i=0; i<partitions; i++)
            for (int j=0; j<rowCount; j++)
                execute("INSERT INTO %s (k1, c1, v1) VALUES (?, ?, ?)", i, j, 0);

        assertRowCount(execute("SELECT * FROM %s WHERE k1=0 AND c1>=0 AND c1<=3 AND v1=0"), rowCount);
    }

    @Test
    public void testMultiPartitionQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    " k1 int," +
                    " c1 int," +
                    " v1 int," +
                    "PRIMARY KEY (k1, c1))");

        int partitions = 10;
        int rowCount = 10;
        for (int i=0; i<partitions; i++)
            for (int j=0; j<rowCount; j++)
                execute("INSERT INTO %s (k1, c1, v1) VALUES (?, ?, ?)", i, j, 0);

        assertRowCount(execute("SELECT * FROM %s WHERE k1 = 0"), rowCount);
        assertRowCount(execute("SELECT * FROM %s"), partitions * rowCount);
        assertRowCount(execute("SELECT * FROM %s WHERE k1 IN (0, 3, 6)"), 3 * rowCount);
    }

    private void createData() throws Throwable
    {
        createTable("CREATE TABLE %s (p text, c text, v text, s text static, PRIMARY KEY (p, c))");

        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");

        execute("INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2");

        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p3", "k1", "v1", "sv3");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p3", "k2", "v2");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p3", "k3", "v3");
    }

    @Test
    public void testReduce() throws Throwable
    {
        createData();

        ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore()).build();
        List<ImmutableBTreePartition> results = ImmutableBTreePartition.create(cmd.executeLocally()).blockingGet();

        assertEquals(3, results.size());
    }

    @Test
    public void testCancelFullPartition() throws Throwable
    {
        createData();

        final CountDownLatch latch = new CountDownLatch(1);

        class Subscriber implements PartitionsSubscriber<Unfiltered>
        {
            volatile int numRows = 0;
            volatile boolean firstPartReceived;
            volatile Throwable error;
            PartitionsSubscription subscription;

           public void onSubscribe(PartitionsSubscription subscription)
           {
               this.subscription = subscription;
           }

            public void onNextPartition(PartitionTrait partition)
            {
                if (!firstPartReceived)
                {
                    firstPartReceived = true;
                }
                else
                {
                    subscription.close();
                }
            }

            public void onNext(Unfiltered item)
            {
                numRows++;
            }

            public void onError(Throwable error)
            {
                this.error = error;
            }

            public void onComplete()
            {
                latch.countDown();
            }
        };

        final ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore()).build();
        final Subscriber subscriber = new Subscriber();
        cmd.executeLocally().subscribe(subscriber);

        boolean completed = latch.await(15, TimeUnit.SECONDS);
        assertTrue("Subscriber was not completed", completed);

        assertNull(subscriber.error);
        assertEquals(2, subscriber.numRows); // only two rows in the first partition
    }

    @Test
    public void testCancelHalfPartition() throws Throwable
    {
        createData();

        final CountDownLatch latch = new CountDownLatch(1);
        final int numExpectedRows = 4; // first row of third partition, see createData()

        class Subscriber implements PartitionsSubscriber<Unfiltered>
        {
            volatile int numRows = 0;
            volatile Throwable error;
            PartitionsSubscription subscription;

            public void onSubscribe(PartitionsSubscription subscription)
            {
                this.subscription = subscription;
            }

            public void onNextPartition(PartitionTrait partition)
            {
            }

            public void onNext(Unfiltered item)
            {
                numRows++;
                if (numRows == numExpectedRows)
                    subscription.close();
            }

            public void onError(Throwable error)
            {
                this.error = error;
            }

            public void onComplete()
            {
                latch.countDown();
            }
        };

        final ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore()).build();
        final Subscriber subscriber = new Subscriber();
        cmd.executeLocally().subscribe(subscriber);

        boolean completed = latch.await(15, TimeUnit.SECONDS);
        assertTrue("Subscriber was not completed", completed);

        assertNull(subscriber.error);
        assertEquals(numExpectedRows, subscriber.numRows);
    }

    @Test
    public void testExceptionInOnNext() throws Throwable
    {
        createData();

        final CountDownLatch latch = new CountDownLatch(1);
        final RuntimeException exc = new RuntimeException("Test exception");

        class Subscriber implements PartitionsSubscriber<Unfiltered>
        {
            volatile Throwable error;
            volatile boolean completed;
            PartitionsSubscription subscription;

            public void onSubscribe(PartitionsSubscription subscription)
            {
                this.subscription = subscription;
            }

            public void onNextPartition(PartitionTrait partition)
            {
            }

            public void onNext(Unfiltered item)
            {
                throw exc;
            }

            public void onError(Throwable error)
            {
                this.error = error;
                latch.countDown();
            }

            public void onComplete()
            {
                completed = true;
            }
        };

        final ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore()).build();
        final Subscriber subscriber = new Subscriber();
        cmd.executeLocally().subscribe(subscriber);

        boolean errorReceived = latch.await(15, TimeUnit.SECONDS);
        assertTrue("Error was not received", errorReceived);

        assertEquals("Unexpected error received: " + subscriber.error, exc, subscriber.error);
        assertFalse("onComplete should not have been called", subscriber.completed);
    }

    @Test
    public void testExceptionInOnComplete() throws Throwable
    {
        createData();

        final CountDownLatch latch = new CountDownLatch(1);
        final RuntimeException exc = new RuntimeException("Test exception");

        class Subscriber implements PartitionsSubscriber<Unfiltered>
        {
            volatile Throwable error;
            PartitionsSubscription subscription;

            public void onSubscribe(PartitionsSubscription subscription)
            {
                this.subscription = subscription;
            }

            public void onNextPartition(PartitionTrait partition)
            {
            }

            public void onNext(Unfiltered item)
            {
            }

            public void onError(Throwable error)
            {
                this.error = error;
                latch.countDown();
            }

            public void onComplete()
            {
                throw exc;
            }
        };

        final ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore()).build();
        final Subscriber subscriber = new Subscriber();
        cmd.executeLocally().subscribe(subscriber);

        boolean errorReceived = latch.await(15, TimeUnit.SECONDS);
        assertTrue("Error was not received", errorReceived);

        assertEquals("Unexpected error received: " + subscriber.error, exc, subscriber.error);
    }
}
