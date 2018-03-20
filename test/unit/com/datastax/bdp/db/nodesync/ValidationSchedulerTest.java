/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.db.nodesync;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BytemanUtil;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

@RunWith(BMUnitRunner.class)
public class ValidationSchedulerTest extends CQLTester
{
    static
    {
        BytemanUtil.randomizeBytemanPort();
    }

    @BeforeClass
    public static void setup()
    {
        System.setProperty(NodeSyncService.MIN_VALIDATION_INTERVAL_PROP_NAME, "-1");
    }

    @Test
    public void testTableDrop()
    {
        String ks = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }");
        String table = createTable(ks, "CREATE TABLE %s (k int PRIMARY KEY) WITH nodesync = { 'enabled' : 'true' }");
        TableMetadata tableMetadata = tableMetadata(ks, table);
        NodeSyncService service = new NodeSyncService();
        NodeSyncState state = new NodeSyncState(service);

        ValidationScheduler scheduler = new ValidationScheduler(state);
        scheduler.addAllContinuous(Stream.of(tableMetadata)).join();
        Schema.instance.registerListener(scheduler);
        MigrationManager.announceTableDrop(ks, table, true).blockingAwait();
        scheduler.getNextValidation(true);
        Assert.assertNull(scheduler.getNextValidation(false));
    }

    static volatile boolean hasUpdatedTableState = false;
    @Test
    @BMRule(name = "Allow us to detect when the executor has updated local ranges",
    targetClass = "TableState",
    targetMethod = "update(Collection)",
    targetLocation = "AT EXIT",
    action = "com.datastax.bdp.db.nodesync.ValidationSchedulerTest.hasUpdatedTableState = true;")
    public void testRecalculateOnAlterRf() throws Throwable
    {
        // Create 3 node local ring
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new Murmur3Partitioner();
        ArrayList<Token> endpointTokens = new ArrayList<>();
        ArrayList<Token> keyTokens = new ArrayList<>();
        List<InetAddress> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 3);

        // Create RF2 keyspace, a table in this keyspace, and a ValidationScheduler providing continuous validation
        // of this table
        String ks = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }");
        String table = createTable(ks, "CREATE TABLE %s (k int PRIMARY KEY) WITH nodesync = { 'enabled' : 'true' }");
        TableMetadata tableMetadata = tableMetadata(ks, table);
        NodeSyncService service = new NodeSyncService();
        NodeSyncState state = new NodeSyncState(service);

        ValidationScheduler scheduler = new ValidationScheduler(state);
        scheduler.addAllContinuous(Stream.of(tableMetadata)).join();
        Schema.instance.registerListener(scheduler);

        TableState tableState = scheduler.getContinuousProposer(Keyspace.open(ks).getMetadata().getTableOrViewNullable(table)).state;
        Collection<Range<Token>> localRanges = new HashSet<>();
        localRanges.add(new Range<>(endpointTokens.get(2), endpointTokens.get(0)));
        localRanges.add(new Range<>(endpointTokens.get(1), endpointTokens.get(2)));
        Assert.assertEquals(localRanges, tableState.localRanges());

        localRanges.add(new Range<>(endpointTokens.get(0), endpointTokens.get(1)));
        execute("ALTER KEYSPACE " + ks + " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
        // Updating segments on the TableState is async, so we need to spin on the flag
        // set by byteman
        Util.spinAssertEquals(true, () -> hasUpdatedTableState, 10);
        // We should now consider every range local, since RF is 3 and there are 3 nodes in the cluster,
        // each with one token
        Assert.assertEquals(localRanges, tableState.localRanges());
    }
}
