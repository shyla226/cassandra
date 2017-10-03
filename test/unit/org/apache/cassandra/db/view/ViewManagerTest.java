/**
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

package org.apache.cassandra.db.view;

import org.apache.cassandra.db.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.*;

public class ViewManagerTest extends CQLTester
{
    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testUpdateWithLocks() throws Throwable
    {
        TableMetadata[] tables = new TableMetadata[10];
        for (int i = 0; i < 10; i++)
        {
            createTable(KEYSPACE_PER_TEST, "CREATE TABLE %s (a text PRIMARY KEY, b int)");
            tables[i] = tableMetadata(KEYSPACE_PER_TEST, currentTable());
        }

        ViewManager viewManager = new ViewManager(Keyspace.open(KEYSPACE_PER_TEST));

        AtomicInteger result = new AtomicInteger();

        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(8));
        List<ListenableFuture<?>> futures = new LinkedList<>();
        for (int i = 0; i < 10000; i++)
        {
            futures.add(executor.submit(() ->
            {
                try
                {
                    String key = "k";
                    Mutation m = new Mutation(KEYSPACE_PER_TEST, Util.dk(key));
                    for (int j = 0; j < 10; j++)
                    {
                        m.add(new RowUpdateBuilder(tables[j], System.currentTimeMillis(), key).buildUpdate());
                    }

                    viewManager.updateWithLocks(
                        m,
                        () -> CompletableFuture.completedFuture(null).thenRun(() ->
                        {
                            int get = result.get();
                            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
                            result.compareAndSet(get, get + 1);
                        }), true).get();
                }
                catch (Exception ex)
                {
                    throw new RuntimeException(ex);
                }
            }));
        }

        ListenableFuture all = Futures.allAsList(futures);
        all.get(1, TimeUnit.MINUTES);
        assertEquals(10000, result.get());

        Keyspace ks = Keyspace.open(KEYSPACE_PER_TEST);
        for (int i = 0; i < 10; i++)
            assertTrue(ks.getColumnFamilyStore(tables[i].id).metric.viewLockAcquireTime.getCount() > 0);
    }
}
