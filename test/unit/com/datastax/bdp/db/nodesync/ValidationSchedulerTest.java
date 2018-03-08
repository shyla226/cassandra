/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.db.nodesync;

import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

public class ValidationSchedulerTest extends CQLTester
{
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
}
