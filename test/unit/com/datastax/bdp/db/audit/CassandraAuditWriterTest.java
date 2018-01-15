/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static com.datastax.bdp.db.audit.CassandraAuditWriter.BatchingOptions;
import static com.datastax.bdp.db.audit.CoreAuditableEventType.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CassandraAuditWriterTest extends CQLTester
{
    @Before
    public void setUp() throws Exception
    {
        if (Schema.instance.getKeyspaceMetadata(CassandraAuditKeyspace.NAME) != null)
            MigrationManager.announceKeyspaceDrop("cassandra_audit", false);

        CassandraAuditKeyspace.maybeConfigure();

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
        Assert.assertFalse(row.has("ttltype"));
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
        Assert.assertFalse(row.has("ttltype"));
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
        Assert.assertTrue(row.has("ttltype"));
        Assert.assertTrue(row.getInt("ttltype") <= 60 * 60);
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

    @Test
    public void schemaUpgrade() throws Exception
    {
        QueryProcessor.executeInternal(String.format("DROP TABLE %s.%s", CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG));
        String olsSchema = String.format(
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
                        "PRIMARY KEY ((date, node, day_partition), event_time))",
                CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG
        );
        QueryProcessor.executeInternal(olsSchema);

        TableMetadata cfm;
        cfm = Schema.instance.getTableMetadata(CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG);
        Assert.assertNull(cfm.getColumn(ByteBufferUtil.bytes("consistency")));

        CassandraAuditKeyspace.maybeConfigure();

        cfm = Schema.instance.getTableMetadata(CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG);
        ColumnMetadata column = cfm.getColumn(ByteBufferUtil.bytes("consistency"));
        Assert.assertNotNull(column);
        Assert.assertEquals(UTF8Type.instance, column.type);
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

        Assert.assertEquals(0, batches.size());

        AuditableEvent event = newAuditEvent(INSERT,
                                             UUIDGen.getTimeUUID(new DateTime(2016, 1, 1, 1, 59, 0, 0).getMillis()),
                                             "ks",
                                             "tbl",
                                             null,
                                             null);

        logger.recordEvent(event);

        Assert.assertEquals(0, batches.size());

        event = newAuditEvent(INSERT,
                              UUIDGen.getTimeUUID(new DateTime(2016, 1, 1, 2, 0, 0, 0).getMillis()),
                              "ks",
                              "tbl",
                              null,
                              null);


        logger.recordEvent(event);

        waitForEventsToBeWritten(2, 2000);
        Assert.assertEquals(2, batches.size());
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
