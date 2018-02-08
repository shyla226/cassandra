/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;

import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class CassandraAuditKeyspace
{
    public static final String NAME = "dse_audit";
    public static final String AUDIT_LOG = "audit_log";

    /**
     * Table partitions rows by node/date/day_partition. Currently, day partition is updated
     * once an hour, but is meant to work as an additional performance tuning knob, if table
     * logging is being used in an environment with a ton of requests. In that case, even when
     * partitioned by the hour, rows could still become huge.
     *
     * audit_log is only created, if it does not exist.
     * For upgrade scenarios, when the table already exists, the columns 'authenticated'
     * and 'consistency', introduced in DSE 5.1, are added by the VersionDependentFeature+SchemaUpgrade
     * instances defined in CassandraAuditWriter.
     */
    private static final TableMetadata AuditLog = compile(
            NAME, AUDIT_LOG,
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
                    "authenticated text," +
                    "consistency text," +
                    "PRIMARY KEY ((date, node, day_partition), event_time))" +
                    " WITH COMPACTION={'class':'TimeWindowCompactionStrategy'}"
    );

    public static final KeyspaceMetadata metadata = KeyspaceMetadata.create(NAME,
                                                                            KeyspaceParams.simple(1),
                                                                            Tables.of());

    public static KeyspaceMetadata metadata()
    {
        return metadata;
    }

    public static List<TableMetadata> tablesIfNotExist()
    {
        return Collections.singletonList(AuditLog);
    }

    public static void maybeConfigure()
    {
        // Uses the current timestamp for schema mutations
        TPCUtils.blockingAwait(StorageService.instance.maybeAddOrUpdateKeyspace(metadata(), tablesIfNotExist(), FBUtilities.timestampMicros()));
    }

    private static TableMetadata compile(String keyspaceName, String tableName, String schema)
    {
        return CreateTableStatement.parse(String.format(schema, keyspaceName, tableName),
                                          keyspaceName)
                                   .id(tableId(keyspaceName, tableName))
                                   .dcLocalReadRepairChance(0)
                                   .memtableFlushPeriod((int) TimeUnit.HOURS.toMillis(1))
                                   .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(90))
                                   .build();

    }

    static TableId tableId(String keyspace, String table)
    {
        byte[] bytes = ArrayUtils.addAll(keyspace.getBytes(), table.getBytes());
        return TableId.fromUUID(UUID.nameUUIDFromBytes(bytes));
    }

    /**
     * Returns the keyspace used to store the audit logs
     * @return the keyspace used to store the audit logs
     */
    public static Keyspace getKeyspace()
    {
        return Keyspace.open(NAME);
    }

    /**
     * Returns the partitioner of the audit log table.
     * @return the partitioner of the audit log table.
     */
    public static IPartitioner getAuditLogPartitioner()
    {
        return AuditLog.partitioner;
    }
}
