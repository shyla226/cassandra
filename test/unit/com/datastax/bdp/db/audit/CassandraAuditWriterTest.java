/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.util.*;

import org.junit.Before;
import org.junit.Test;

import com.datastax.bdp.db.upgrade.VersionDependentFeature;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.utils.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static com.datastax.bdp.db.audit.CassandraAuditWriter.BatchingOptions;
import static com.datastax.bdp.db.audit.CoreAuditableEventType.INSERT;
import static org.junit.Assert.*;

public class CassandraAuditWriterTest extends CQLTester
{
    @Before
    public void setUp()
    {
        if (Schema.instance.getKeyspaceMetadata(CassandraAuditKeyspace.NAME) != null)
            MigrationManager.announceKeyspaceDrop("cassandra_audit", false);

        CassandraAuditKeyspace.maybeConfigure();
        Gossiper.instance.clusterVersionBarrier.removeAllListeners();
        Gossiper.instance.clusterVersionBarrier.onLocalNodeReady();

        QueryProcessor.processBlocking(
                String.format("TRUNCATE %s.%s", CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG),
                ConsistencyLevel.ONE);

        assertEquals(0, getLoggedEventCount());
    }

    private static UntypedResultSet query(String q) throws RequestExecutionException
    {
        return QueryProcessor.processBlocking(String.format(q, CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG), ConsistencyLevel.ONE);
    }

    @Test
    public void testEventLoggingSuccess() throws Exception
    {
        CassandraAuditWriter logger = new CassandraAuditWriter(0, ConsistencyLevel.ONE, BatchingOptions.SYNC);
        logger.setUp();
        AuditableEvent event = newAuditEvent(INSERT,
                                             UUIDGen.getTimeUUID(),
                                             "ks",
                                             "tbl",
                                             UUID.randomUUID(),
                                             ConsistencyLevel.SERIAL);

        logger.recordEvent(event).blockingAwait();

        UntypedResultSet rows = query("SELECT * FROM %s.%s");
        assertEquals(1, rows.size());
        UntypedResultSet.Row row = rows.one();

        // check correct data was saved
        DateTime eventTime = new DateTime(event.getTimestamp(), DateTimeZone.UTC);
        assertEquals(eventTime.toDateMidnight().toDate(), row.getTimestamp("date"));
        assertEquals(FBUtilities.getBroadcastAddress(), row.getInetAddress("node"));
        assertEquals(eventTime.getHourOfDay() * 60 * 60, row.getInt("day_partition"));
        assertEquals(event.getTimestamp(), UUIDGen.unixTimestamp(row.getUUID("event_time")));
        assertEquals(event.getBatchId(), row.getUUID("batch_id"));
        assertEquals(event.getType().getCategory().toString(), row.getString("category"));
        assertEquals(event.getKeyspace(), row.getString("keyspace_name"));
        assertEquals(event.getOperation(), row.getString("operation"));
        assertEquals(event.getSource(), row.getString("source"));
        assertEquals(event.getColumnFamily(), row.getString("table_name"));
        assertEquals(event.getType().toString(), row.getString("type"));
        assertEquals(event.getUser(), row.getString("username"));
        assertEquals(event.getConsistencyLevel().toString(), row.getString("consistency"));


        rows = query("SELECT ttl(type) as ttltype FROM %s.%s");
        assertEquals(1, rows.size());
        row = rows.one();
        assertFalse(row.has("ttltype"));
    }

    @Test
    public void testNoClEventLoggingSuccess() throws Exception
    {
        CassandraAuditWriter logger = new CassandraAuditWriter(0, ConsistencyLevel.ONE, BatchingOptions.SYNC);
        logger.setUp();
        AuditableEvent event = newAuditEvent(INSERT,
                                             UUIDGen.getTimeUUID(),
                                             "ks",
                                             "tbl",
                                             UUID.randomUUID(),
                                             null);


        logger.recordEvent(event).blockingAwait();

        UntypedResultSet rows = query("SELECT * FROM %s.%s");
        assertEquals(1, rows.size());
        UntypedResultSet.Row row = rows.one();

        // check correct data was saved
        DateTime eventTime = new DateTime(event.getTimestamp(), DateTimeZone.UTC);
        assertEquals(eventTime.toDateMidnight().toDate(), row.getTimestamp("date"));
        assertEquals(FBUtilities.getBroadcastAddress(), row.getInetAddress("node"));
        assertEquals(eventTime.getHourOfDay() * 60 * 60, row.getInt("day_partition"));
        assertEquals(event.getTimestamp(), UUIDGen.unixTimestamp(row.getUUID("event_time")));
        assertEquals(event.getBatchId(), row.getUUID("batch_id"));
        assertEquals(event.getType().getCategory().toString(), row.getString("category"));
        assertEquals(event.getKeyspace(), row.getString("keyspace_name"));
        assertEquals(event.getOperation(), row.getString("operation"));
        assertEquals(event.getSource(), row.getString("source"));
        assertEquals(event.getColumnFamily(), row.getString("table_name"));
        assertEquals(event.getType().toString(), row.getString("type"));
        assertEquals(event.getUser(), row.getString("username"));

        assertFalse(row.has("consistency"));
        rows = query("SELECT ttl(type) as ttltype FROM %s.%s");
        assertEquals(1, rows.size());
        row = rows.one();
        assertFalse(row.has("ttltype"));
    }

    @Test
    public void testRetention() throws Exception
    {
        CassandraAuditWriter logger = new CassandraAuditWriter(1, ConsistencyLevel.ONE, BatchingOptions.SYNC);
        logger.setUp();
        AuditableEvent event = newAuditEvent(INSERT,
                                             UUIDGen.getTimeUUID(),
                                             "ks",
                                             "tbl",
                                             null,
                                             null);

        logger.recordEvent(event).blockingAwait();

        UntypedResultSet rows = query("SELECT * FROM %s.%s");
        assertEquals(1, rows.size());

        rows = query("SELECT ttl(type) as ttltype FROM %s.%s");
        assertEquals(1, rows.size());
        UntypedResultSet.Row row = rows.one();
        assertTrue(row.has("ttltype"));
        assertTrue(row.getInt("ttltype") <= 60 * 60);
    }

    @Test
    public void testBatchedEvents() throws Exception
    {
        // Logging synchronously to avoid sporadic timeouts (DSP-12301)
        CassandraAuditWriter logger = new CassandraAuditWriter(0, ConsistencyLevel.ONE, BatchingOptions.SYNC);
        logger.setUp();
        AuditableEvent event = newAuditEvent(INSERT,
                                             UUIDGen.getTimeUUID(),
                                             "ks",
                                             "tbl",
                                             null,
                                             null);


        logger.recordEvent(event).blockingAwait();

        waitForEventsToBeWritten(1, 2000);
    }

    @Test
    public void checkEventsArentSavedUntilFlushTimeIsMet() throws Exception
    {
        final int flushPeriod = 100;
        final int batchSize = 2;
        BatchingOptions options = new BatchingOptions(batchSize, flushPeriod, 100);

        CassandraAuditWriter logger = new CassandraAuditWriter(0, ConsistencyLevel.ONE, options);
        logger.setUp();

        AuditableEvent event = newAuditEvent(INSERT,
                                             UUIDGen.getTimeUUID(),
                                             "ks",
                                             "tbl",
                                             null,
                                             null);

        logger.recordEvent(event).blockingAwait();
        // no records should be written until the timer says the flush period has been exceeded
        assertEquals(0, getLoggedEventCount());

        waitForEventsToBeWritten(1, 2000);
    }

    @Test
    public void checkNullKeyspace() throws Exception
    {
        CassandraAuditWriter logger = new CassandraAuditWriter(0, ConsistencyLevel.ONE, BatchingOptions.SYNC);
        logger.setUp();
        AuditableEvent event = newAuditEvent(INSERT,
                                             UUIDGen.getTimeUUID(),
                                             null,
                                             "tbl",
                                             null,
                                             null);

        logger.recordEvent(event).blockingAwait();
    }


    /**
     * Check that multiple events created close together don't
     * overwrite each other because of unsafe timeuuid creation
     */
    @Test
    public void uuidCollisions() throws Exception
    {
        List<AuditableEvent> events = new ArrayList<>(10);
        for (int i = 0; i < 10; i++)
        {
            events.add(newAuditEvent(INSERT,
                                     UUIDGen.getTimeUUID(),
                                     "ks",
                                     "tbl",
                                     null,
                                     null));
        }

        Set<Long> uniqueTimes = new HashSet<>();
        CassandraAuditWriter logger = new CassandraAuditWriter(0, ConsistencyLevel.ONE, BatchingOptions.SYNC);
        logger.setUp();
        for (AuditableEvent event : events)
        {
            logger.recordEvent(event).blockingAwait();
            uniqueTimes.add(event.getTimestamp());
        }

        assertTrue("It took longer that 1ms to instantiate each event. Get a faster computer", uniqueTimes.size() < 10);

        UntypedResultSet rows = query("SELECT * FROM %s.%s");
        assertEquals(10, rows.size());
    }

    /**
     * A node starting up with the DSE 5.0 format of the dse_audit.audit_log table update it.
     *
     * Also covers the case when dse_audit.audit_log has been manually recreated and therefore
     * gets a different table-ID.
     */
    @Test
    public void schemaUpgrade() throws Exception
    {
        try
        {
            QueryProcessor.executeInternal(String.format("DROP TABLE %s.%s", CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG));

            // create the table as it was defined in DSE 5.0
            String oldSchema = String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s (" +
            "date timestamp," +
            "node inet," +
            "day_partition int," +
            "event_time timeuuid," +
            "batch_id uuid," +
            "category text," +
            "keyspace_name text," +
            "operation text," +
            "source text," +
            "table_name text," +
            "type text," +
            "username text," +
            "PRIMARY KEY ((date, node, day_partition), event_time)) ",
            CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG
            );
            QueryProcessor.executeInternal(oldSchema);
            TableId createdTableId = Schema.instance
                                           .getTableMetadata(CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG)
                                           .id;
            assertNotEquals(CassandraAuditKeyspace.tableId(CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG), createdTableId);

            CassandraAuditWriter logger = new CassandraAuditWriter(0, ConsistencyLevel.ONE, BatchingOptions.SYNC);
            logger.setUp();
            assertEquals(VersionDependentFeature.FeatureStatus.ACTIVATING, logger.feature.getStatus());

            TableMetadata cfm;
            cfm = Schema.instance.getTableMetadata(CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG);
            assertNull(cfm.getColumn(ByteBufferUtil.bytes("consistency")));
            assertNull(cfm.getColumn(ByteBufferUtil.bytes("authenticated")));

            // triggers callbacks to the VersionDependentFeature in CassandraAuditWriter (no gossip in place)
            for (int i = 0; i < 5_000; i++)
            {
                Thread.sleep(1L);
                if (VersionDependentFeature.FeatureStatus.ACTIVATED == logger.feature.getStatus())
                    break;
            }

            assertEquals(VersionDependentFeature.FeatureStatus.ACTIVATED, logger.feature.getStatus());

            cfm = Schema.instance.getTableMetadata(CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG);
            ColumnMetadata column = cfm.getColumn(ByteBufferUtil.bytes("authenticated"));
            assertNotNull(column);
            assertEquals(UTF8Type.instance, column.type);
            column = cfm.getColumn(ByteBufferUtil.bytes("consistency"));
            assertNotNull(column);
            assertEquals(UTF8Type.instance, column.type);

            assertEquals(cfm.id, createdTableId);
        }
        finally
        {
            // do not screw up other unit tests - if this unit test fails and leaves the audit-log table without
            // the native columns, the upgrade code won't get any callbacks and the table will be left as not upgraded
            // (missing columns)
            QueryProcessor.executeInternal(String.format("DROP TABLE %s.%s", CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG));
        }
    }

    /**
     * A node starting up without the dse_audit.audit_log table should create it.
     */
    @Test
    public void schemaRecreate()
    {
        try
        {
            QueryProcessor.executeInternal(String.format("DROP TABLE %s.%s", CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG));

            CassandraAuditWriter logger = new CassandraAuditWriter(0, ConsistencyLevel.ONE, BatchingOptions.SYNC);
            logger.setUp();
            assertEquals(VersionDependentFeature.FeatureStatus.ACTIVATED, logger.feature.getStatus());

            TableMetadata cfm = Schema.instance.getTableMetadata(CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG);
            ColumnMetadata column = cfm.getColumn(ByteBufferUtil.bytes("authenticated"));
            assertNotNull(column);
            assertEquals(UTF8Type.instance, column.type);
            column = cfm.getColumn(ByteBufferUtil.bytes("consistency"));
            assertNotNull(column);
            assertEquals(UTF8Type.instance, column.type);
        }
        finally
        {
            // do not screw up other unit tests - if this unit test fails and leaves the audit-log table without
            // the native columns, the upgrade code won't get any callbacks and the table will be left as not upgraded
            // (missing columns)
            QueryProcessor.executeInternal(String.format("DROP TABLE %s.%s", CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG));
        }
    }

    /**
     * In case there are more columns than expected, leave those untouched. This can happen when a newer DSE
     * version adds new columns.
     */
    @Test
    public void schemaMoreColumns()
    {
        try
        {
            QueryProcessor.executeInternal(String.format("ALTER TABLE %s.%s ADD (foo text, bar int, baz set<text>)",
                                                         CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG));

            CassandraAuditWriter logger = new CassandraAuditWriter(0, ConsistencyLevel.ONE, BatchingOptions.SYNC);
            logger.setUp();
            assertEquals(VersionDependentFeature.FeatureStatus.ACTIVATED, logger.feature.getStatus());

            TableMetadata cfm = Schema.instance.getTableMetadata(CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG);
            assertEquals(CassandraAuditWriter.NUM_FIELDS + 3, cfm.columns().size());
            assertNotNull(cfm.getColumn(ByteBufferUtil.bytes("authenticated")));
            assertNotNull(cfm.getColumn(ByteBufferUtil.bytes("consistency")));
            assertNotNull(cfm.getColumn(ByteBufferUtil.bytes("foo")));
            assertNotNull(cfm.getColumn(ByteBufferUtil.bytes("bar")));
            assertNotNull(cfm.getColumn(ByteBufferUtil.bytes("baz")));
        }
        finally
        {
            // do not screw up other unit tests - if this unit test fails and leaves the audit-log table without
            // the native columns, the upgrade code won't get any callbacks and the table will be left as not upgraded
            // (missing columns)
            QueryProcessor.executeInternal(String.format("DROP TABLE %s.%s", CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG));
        }
    }

    /**
     * When the day partition changes, separate batches should be executed for
     * each partition to prevent a warning appearing in customer logs
     */
    @Test
    public void batchPartitionSplit() throws Exception
    {
        BatchingOptions options = new BatchingOptions(20, 100, 100);

        final Set<CassandraAuditWriter.EventBatch> batches = new HashSet<>();
        CassandraAuditWriter logger = new CassandraAuditWriter(0, ConsistencyLevel.ONE, options) {
            @Override
            protected void executeBatches(Collection<EventBatch> b)
            {
                batches.addAll(b);
                super.executeBatches(b);
            }
        };

        logger.setUp();

        assertEquals(0, batches.size());

        AuditableEvent event = newAuditEvent(INSERT,
                                             UUIDGen.getTimeUUID(new DateTime(2016, 1, 1, 1, 59, 0, 0).getMillis()),
                                             "ks",
                                             "tbl",
                                             null,
                                             null);

        logger.recordEvent(event);

        assertEquals(0, batches.size());

        event = newAuditEvent(INSERT,
                              UUIDGen.getTimeUUID(new DateTime(2016, 1, 1, 2, 0, 0, 0).getMillis()),
                              "ks",
                              "tbl",
                              null,
                              null);


        logger.recordEvent(event);

        waitForEventsToBeWritten(2, 2000);
        assertEquals(2, batches.size());
    }

    private void waitForEventsToBeWritten(int eventCount, long timeout) throws RequestExecutionException
    {
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < timeout)
        {
            if (getLoggedEventCount() == eventCount)
                break;
            Thread.yield();
        }
        assertEquals(String.format("Expected %s events to be written, but condition not met after %s ms", eventCount, timeout),
                     eventCount, getLoggedEventCount());
    }

    private int getLoggedEventCount() throws RequestExecutionException
    {
        int c = 0;
        for (UntypedResultSet.Row row: query ("SELECT * FROM %s.%s"))
        {
            if (row.getString("keyspace_name").equals("ks"))
            {
                c++;
            }
        }
        return c;
    }

    private AuditableEvent newAuditEvent(AuditableEventType type,
                                         UUID uid,
                                         String keyspace,
                                         String table,
                                         UUID batchId,
                                         ConsistencyLevel cl) throws Exception
    {
        return new AuditableEvent(UserRolesAndPermissions.ANONYMOUS,
                                  type,
                                  "127.0.0.1",
                                  uid,
                                  batchId,
                                  keyspace,
                                  table,
                                  "...cql statement...",
                                  cl);
    }
}
