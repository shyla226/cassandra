/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.reactivex.Completable;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.ArrayUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.AlterTableStatement;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class CassandraAuditKeyspace
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraAuditKeyspace.class);

    public static final String NAME = "dse_audit";
    public static final String AUDIT_LOG = "audit_log";

    /**
     * Table partitions rows by node/date/day_partition. Currently, day partition is updated
     * once an hour, but is meant to work as an additional performance tuning knob, if table
     * logging is being used in an environment with a ton of requests. In that case, even when
     * partitioned by the hour, rows could still become huge.
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

    private static Tables tables()
    {
        return Tables.of(AuditLog);
    }

    public static void maybeConfigure()
    {
        // fire away without waiting for an upgrade as updates are propagated between 5.1 and 5.0
        maybeCreateOrUpdateKeyspace(metadata(), FBUtilities.timestampMicros());
        maybeAddNewColumn(NAME, AUDIT_LOG, "consistency",
                          String.format("ALTER TABLE %s.%s ADD consistency text", NAME, AUDIT_LOG));
        maybeAddNewColumn(NAME, AUDIT_LOG, "authenticated",
                          String.format("ALTER TABLE %s.%s ADD authenticated text", NAME, AUDIT_LOG));
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(NAME, KeyspaceParams.simple(1), tables());
    }

    public static TableMetadata compile(String keyspaceName, String tableName, String schema)
    {

        return CreateTableStatement.parse(String.format(schema, keyspaceName, tableName),
                                          keyspaceName)
                                   .id(tableId(keyspaceName, tableName))
                                   .dcLocalReadRepairChance(0)
                                   .memtableFlushPeriod((int) TimeUnit.HOURS.toMillis(1))
                                   .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(90))
                                   .build();

    }

    public static TableId tableId(String keyspace, String table)
    {
        byte[] bytes = ArrayUtils.addAll(keyspace.getBytes(), table.getBytes());
        return TableId.fromUUID(UUID.nameUUIDFromBytes(bytes));
    }
    
    /***
     * This is to ensure that newly added columns are correctly added after e.g. running an upgrade.
     *
     * @param ks The name of the keyspace containing the table to be altered
     * @param table The name of the table to be altered
     * @param columnName The column to add
     * @param cql The cql statement that actually does add the column to the specified table,
     * e.g. <b>alter table ks.tbl add col_name int;</b>.
     */
    private static void maybeAddNewColumn(String ks, String table, String columnName, String cql)
    {
        ColumnMetadata cd = Schema.instance.getTableMetadata(ks, table).getColumn(ByteBufferUtil.bytes(columnName));
        if (null == cd)
        {
            try
            {
                maybeAlterTable(ks, table, cql);
            }
            catch (org.apache.cassandra.exceptions.InvalidRequestException e)
            {
                logger.debug(String.format("Caught InvalidRequestException; probably this is just a race with another node attempting to add the column %s.", columnName), e);
            }
        }
    }

    /**
     * Alter a table at startup if it exists.
     *
     * It will do nothing if the table does not exist.
     *
     * @param keyspace The name of the keyspace containing the table to be altered
     * @param table    The name of the table to alter
     *
     */
    private static void maybeAlterTable(String keyspace, String table, String cql) throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        if (Schema.instance.getTableMetadata(keyspace, table) != null)
        {
            try
            {
                CFStatement parsed = (CFStatement)QueryProcessor.parseStatement(cql);
                parsed.prepareKeyspace(keyspace);
                AlterTableStatement statement = (AlterTableStatement) parsed.prepare().statement;
                statement.announceMigration(QueryState.forInternalCalls(), false).blockingGet();
            }
            catch (org.apache.cassandra.exceptions.InvalidRequestException e)
            {
                // DSP-6093: Pass IRE up the stack and catch everything else.  IRE can result from a race condition
                // when two nodes try to add the same column, and there isn't really a good way to avoid this.  It
                // would be possible to catch and silently dump this here, but then other IREs would drop as well.
                throw e;
            }
            catch (Exception e)
            {
                throw new AssertionError(e);
            }
        }
    }

    /**
     * Create a keyspace at startup if it doesn't already exist
     *
     * It will do nothing if the keyspace already exists and contains tables
     * equal to tables contained in expected keyspace. It does not update keyspace tables
     * if keyspace already exists and it's tables differ from tables contained
     * in expected keyspace (this will work once DSP-11736 is implemented).
     * It only creates the tables if they don't exist yet.
     * @param expected keyspace metadata to be created.
     * @param timestamp The timestamp to use for the new keyspace
     */
    private static void maybeCreateOrUpdateKeyspace(KeyspaceMetadata expected, long timestamp)
    {
        Completable migration;
        if (getKeyspaceMetadata(expected.name) == null)
        {
            try
            {
                migration = MigrationManager.announceNewKeyspace(expected, timestamp, false);
            }
            catch (AlreadyExistsException e)
            {
                logger.debug("Attempted to create new keyspace {}, but it already exists", expected.name);
                migration = Completable.complete();
            }
        }
        else
        {
            migration = Completable.complete();
        }

        TPCUtils.blockingAwait(migration.andThen(Completable.defer( () -> {
            KeyspaceMetadata defined = getKeyspaceMetadata(expected.name);
            Preconditions.checkNotNull(defined, String.format("Creating keyspace %s failed", expected.name));

            // While the keyspace exists, it might miss table or have outdated one
            // There is also the potential for a race, as schema migrations add the bare
            // keyspace into Schema.instance before adding its tables, so double check that
            // all the expected tables are present
            List<Completable> migrations = new ArrayList<>();
            // existing schema may be missing some types
            for (UserType expectedType : expected.types)
            {
                UserType definedType = defined.types.get(expectedType.name).orElse(null);
                if (definedType == null)
                {
                    migrations.add(MigrationManager.announceNewType(expectedType, false));
                }
                else if (!expectedType.equals(definedType))
                {
                    migrations.add(MigrationManager.announceTypeUpdate(expectedType, false));
                }
            }

            for (TableMetadata expectedTable : expected.tables)
            {
                TableMetadata definedTable = defined.tables.get(expectedTable.name).orElse(null);
                if (definedTable == null || !definedTable.equals(expectedTable))
                {
                    migrations.add(MigrationManager.forceAnnounceNewTable(expectedTable));
                }
            }

            return migrations.isEmpty() ? Completable.complete() : Completable.merge(migrations);
        })));
    }

    /**
     * Retrieves the requested keyspace object or null if it does not exist.
     *
     * @param keyspace a keyspace name
     * @return The keyspace definition or null if not found.
     */
    private static KeyspaceMetadata getKeyspaceMetadata(String keyspace)
    {
        return keyspace == null ? null : Schema.instance.getKeyspaceMetadata(keyspace);
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
