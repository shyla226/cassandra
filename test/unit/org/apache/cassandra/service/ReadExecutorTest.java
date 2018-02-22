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

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ReadExecutorTest
{
    private static Keyspace ks;
    private static ColumnFamilyStore cfs;
    private static List<InetAddress> targets;
    private long rpcTimeout;

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace("Foo", KeyspaceParams.simple(3), SchemaLoader.standardCFMD("Foo", "Bar"));
        ks = Keyspace.open("Foo");
        cfs = ks.getColumnFamilyStore("Bar");
        targets = ImmutableList.of(InetAddress.getByName("127.0.0.255"), InetAddress.getByName("127.0.0.254"), InetAddress.getByName("127.0.0.253"));
        cfs.sampleLatencyNanos = 0;
    }

    @Before
    public void resetCounters() throws Throwable
    {
        rpcTimeout = DatabaseDescriptor.getReadRpcTimeout();
        cfs.metric.speculativeInsufficientReplicas.dec(cfs.metric.speculativeInsufficientReplicas.getCount());
        cfs.metric.speculativeRetries.dec(cfs.metric.speculativeRetries.getCount());
        cfs.metric.speculativeFailedRetries.dec(cfs.metric.speculativeFailedRetries.getCount());
    }

    @After
    public void tearDown() throws Throwable
    {
        DatabaseDescriptor.setReadRpcTimeout(rpcTimeout);
    }

    private ReadContext ctx(ReadCommand command, ConsistencyLevel cl)
    {
        return ReadContext.builder(command, cl).build(System.nanoTime());
    }

    /**
     * If speculation would have been beneficial but could not be attempted due to lack of replicas
     * count that it occurred
     */
    @Test
    public void testUnableToSpeculate() throws Throwable
    {
        assertEquals(0, cfs.metric.speculativeInsufficientReplicas.getCount());
        assertEquals(0, ks.metric.speculativeInsufficientReplicas.getCount());
        ReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfs.metadata(), 0, Util.dk("ry@n_luvs_teh_y@nk33z"));
        AbstractReadExecutor executor = new AbstractReadExecutor.NeverSpeculatingReadExecutor(cfs, command, targets, ctx(command, ConsistencyLevel.LOCAL_QUORUM), true);
        executor.executeAsync().subscribe();
        executor.maybeTryAdditionalReplicas().blockingAwait();

        try
        {
            executor.result().blockingSingle();
            fail();
        }
        catch (ReadTimeoutException e)
        {
            //expected
        }

        // when executor.maybeTryAdditionalReplicas() completes, it will have scheduled an event on the TPCTimer
        // to increment the metrics. Schedule another event at a later time and wait for it to complete so we are
        // sure that the metrics have been updated
        final CompletableFuture complete = new CompletableFuture();
        TPC.bestTPCTimer().onTimeout(() -> complete.complete(null), cfs.sampleLatencyNanos + 100, TimeUnit.MILLISECONDS);
        complete.get();

        assertEquals(1, cfs.metric.speculativeInsufficientReplicas.getCount());
        assertEquals(1, ks.metric.speculativeInsufficientReplicas.getCount());

        //Shouldn't increment
        command = SinglePartitionReadCommand.fullPartitionRead(cfs.metadata(), 0, Util.dk("ry@n_luvs_teh_y@nk33z"));
        executor = new AbstractReadExecutor.NeverSpeculatingReadExecutor(cfs, command, targets, ctx(command, ConsistencyLevel.LOCAL_QUORUM), false);
        try
        {
            executor.result().blockingSingle();
            fail();
        }
        catch (ReadTimeoutException e)
        {
            //expected
        }

        assertEquals(1, cfs.metric.speculativeInsufficientReplicas.getCount());
        assertEquals(1, ks.metric.speculativeInsufficientReplicas.getCount());
    }

    /**
     *  Test that speculation when it is attempted is counted, and when it succeed
     *  no failure is counted.
     */
    @Test
    public void testSpeculateSucceeded() throws Throwable
    {
        assertEquals(0, cfs.metric.speculativeRetries.getCount());
        assertEquals(0, cfs.metric.speculativeFailedRetries.getCount());
        assertEquals(0, ks.metric.speculativeRetries.getCount());
        assertEquals(0, ks.metric.speculativeFailedRetries.getCount());
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfs.metadata(), 0, Util.dk("ry@n_luvs_teh_y@nk33z"));
        AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command, targets, ctx(command, ConsistencyLevel.LOCAL_QUORUM));
        DatabaseDescriptor.setReadRpcTimeout(TimeUnit.DAYS.toMillis(365));
        executor.maybeTryAdditionalReplicas().blockingAwait();

        // make sure that we send the failures after having executed the task scheduled in AbstractReadExecutor.SpeculatingReadExecutor.maybeTryAdditionalReplicas(),
        // which being executed on the TPCTimer have a latency of at least 100ms, hence the cfs.sampleLatencyNanos + 100
        // milliseconds delay
        command.getScheduler().schedule(() -> {
            //Failures end the read promptly but don't require mock data to be supplied
            Request<SinglePartitionReadCommand, ReadResponse> request0 = Request.fakeTestRequest(targets.get(0), -1, Verbs.READS.SINGLE_READ, command);
            executor.handler.onFailure(request0.respondWithFailure(RequestFailureReason.READ_TOO_MANY_TOMBSTONES));
            Request<SinglePartitionReadCommand, ReadResponse> request1 = Request.fakeTestRequest(targets.get(1), -1, Verbs.READS.SINGLE_READ, command);
            executor.handler.onFailure(request1.respondWithFailure(RequestFailureReason.READ_TOO_MANY_TOMBSTONES));
        }, TPCTaskType.UNKNOWN, cfs.sampleLatencyNanos + 100, TimeUnit.MILLISECONDS); // see comment above

        try
        {
            executor.result().blockingSingle();
            fail();
        }
        catch (ReadFailureException e)
        {
            //expected
        }

        assertEquals(1, cfs.metric.speculativeRetries.getCount());
        assertEquals(0, cfs.metric.speculativeFailedRetries.getCount());
        assertEquals(1, ks.metric.speculativeRetries.getCount());
        assertEquals(0, ks.metric.speculativeFailedRetries.getCount());
    }

    /**
     * Test that speculation failure statistics are incremented if speculation occurs
     * and the read still times out.
     */
    @Test
    public void testSpeculateFailed() throws Throwable
    {
        assertEquals(0, cfs.metric.speculativeRetries.getCount());
        assertEquals(0, cfs.metric.speculativeFailedRetries.getCount());
        assertEquals(0, ks.metric.speculativeRetries.getCount());
        assertEquals(0, ks.metric.speculativeFailedRetries.getCount());
        ReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfs.metadata(), 0, Util.dk("ry@n_luvs_teh_y@nk33z"));
        AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command, targets, ctx(command, ConsistencyLevel.LOCAL_QUORUM));
        executor.maybeTryAdditionalReplicas().blockingAwait();
        try
        {
            executor.result().blockingSingle();
            fail();
        }
        catch (ReadTimeoutException e)
        {
            //expected
        }

        // when executor.maybeTryAdditionalReplicas() completes, it will have scheduled an event on the command scheduler
        // to increment the metrics. Schedule another event on the same scheduler and wait for it to complete so we are
        // sure that the metrics have been updated

        assertEquals(1, cfs.metric.speculativeRetries.getCount());
        assertEquals(1, cfs.metric.speculativeFailedRetries.getCount());
        assertEquals(1, ks.metric.speculativeRetries.getCount());
        assertEquals(1, ks.metric.speculativeFailedRetries.getCount());
    }
}
