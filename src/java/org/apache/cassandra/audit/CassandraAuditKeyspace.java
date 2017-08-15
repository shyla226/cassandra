/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.cql3.AuditUtils;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

public class CassandraAuditKeyspace
{

    public static final String NAME = "cassandra_audit";
    public static final String AUDIT_LOG = "audit_log";

    public static volatile boolean isRecordingConsistency;

    public static final CassandraVersion MIN_LOG_CONSISTENCY_VERSION = new CassandraVersion("5.0.0");

    /**
     * Table partitions rows by node/date/day_partition. Currently, day partition is updated
     * once an hour, but is meant to work as an additional performance tuning knob, if table
     * logging is being used in an environment with a ton of requests. In that case, even when
     * partitioned by the hour, rows could still become huge.
     */
    private static final TableMetadata AuditLog = AuditUtils.compile(
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

    private static final Logger logger = LoggerFactory.getLogger(CassandraAuditKeyspace.class);

    private static Tables tables()
    {
        return Tables.of(AuditLog);
    }

    public static void maybeConfigure()
    {
        // fire away without waiting for an upgrade as updates are propagated between 5.1 and 5.0
        AuditUtils.maybeCreateOrUpdateKeyspace(metadata(),
                                               FBUtilities.timestampMicros());
        AuditUtils.maybeAddNewColumn(NAME, AUDIT_LOG, "consistency",
                                     String.format("ALTER TABLE %s.%s ADD consistency text",
                                                   NAME, AUDIT_LOG));
        AuditUtils.maybeAddNewColumn(NAME, AUDIT_LOG, "authenticated",
                                     String.format("ALTER TABLE %s.%s ADD authenticated text",
                                                   NAME, AUDIT_LOG));
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(NAME, KeyspaceParams.simple(1), tables());
    }
}
