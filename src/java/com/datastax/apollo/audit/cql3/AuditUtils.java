package com.datastax.apollo.audit.cql3;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Completable;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.AlterKeyspaceStatement;
import org.apache.cassandra.cql3.statements.AlterTableStatement;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.DropIndexStatement;
import org.apache.cassandra.cql3.statements.DropKeyspaceStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.UseStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;

public class AuditUtils
{
    private static final Logger logger = LoggerFactory.getLogger(AuditUtils.class);

    public static String getKeyspace(CQLStatement stmt)
    {
        // Retrieving the keyspace via reflection is presumably quite expensive,
        // but this *should* be a relatively rare and non-time critical operation
        // High frequency/throughput operations like SELECT/UPDATES don't rely on
        // reflection
        if (stmt instanceof DropKeyspaceStatement
            || stmt instanceof UseStatement)
        {
            Field field = FBUtilities.getProtectedField(stmt.getClass(), "keyspace");
            return (getKeyspaceFromSchemaStatement(field, stmt));
        }
        else if (stmt instanceof CreateKeyspaceStatement)
        {
            Field field = FBUtilities.getProtectedField(stmt.getClass(), "name");
            return (getKeyspaceFromSchemaStatement(field, stmt));
        }
        else if (stmt instanceof CFStatement)
        {
            return getKeyspaceOrColumnFamilyFromCFStatement((CFStatement) stmt, true);
        }
        else if (stmt instanceof ModificationStatement)
        {
            return ((ModificationStatement) stmt).keyspace();
        }
        else if (stmt instanceof SelectStatement)
        {
            return ((SelectStatement) stmt).table.keyspace;
        }
        else
        {
            return null;
        }
    }

    public static String getKeyspaceFromSchemaStatement(Field field, Object statement)
    {
        try
        {
            return (String) field.get(statement);
        }
        catch (Exception e)
        {
            logger.info("Unable to retrieve keyspace from schema altering statement", e);
            return null;
        }
    }

    private static String getKeyspaceOrColumnFamilyFromCFStatement(CFStatement stmt, boolean retrieveKeyspace)
    {
        try
        {
            return retrieveKeyspace ? stmt.keyspace() : stmt.columnFamily();
        }
        catch (NullPointerException e)
        {
            // a NPE can happen if 'cfName' in CFStatement is null. However, retrieving the 'cfName' field
            // using Reflection & doing a null check will be wrong here, because some subclasses
            // override the methods 'keyspace()' / 'columnFamily()' and therefore won't suffer from a NPE.
            // For example, CreateTypeStatement overrides 'keyspace()' but doesn't have a table name and
            // calling 'columnFamily()' will throw a NPE, whereas 'keyspace()' won't.
            // Therefore we catch the NPE and just return null.
            return null;
        }
    }

    public static String getColumnFamily(CQLStatement stmt)
    {
        if (stmt instanceof DropKeyspaceStatement
            || stmt instanceof CreateKeyspaceStatement
            || stmt instanceof AlterKeyspaceStatement)
        {
            return null;
        }
        else if (stmt instanceof DropIndexStatement)
        {
            try
            {
                return ((DropIndexStatement) stmt).columnFamily();
            }
            catch (RuntimeException e)
            {
                logger.info("Couldn't get column family name from DropIndexStatement: ", null == e.getCause() ? e : e.getCause());

                if (e.getCause() instanceof InvalidRequestException || e instanceof InvalidRequestException)
                {
                    // we return null because we probably couldn't get the CF Name because the index was already deleted
                    return null;
                }

                throw e;
            }
        }

        else if (stmt instanceof CFStatement)
        {
            return getKeyspaceOrColumnFamilyFromCFStatement((CFStatement) stmt, false);
        }
        else if (stmt instanceof ModificationStatement)
        {
            return ((ModificationStatement) stmt).columnFamily();
        }
        else if (stmt instanceof SelectStatement)
        {
            return ((SelectStatement) stmt).table.name;
        }
        else
        {
            return null;
        }
    }

    /**
     * Processes a batch statement and awaits its completion.
     *
     * @param statement - the prepared statement to process
     * @param cl - the consistency level
     * @param values - the list of values to bind to the prepared statement
     */
    public static void processBatchBlocking(BatchStatement statement, ConsistencyLevel cl, List<List<ByteBuffer>> values)
    {
        BatchQueryOptions options =
        BatchQueryOptions.withPerStatementVariables(QueryOptions.forInternalCalls(cl, Collections.<ByteBuffer>emptyList()),
                                                    values,
                                                    Collections.emptyList());

        TPCUtils.blockingGet(QueryProcessor.instance.processBatch(statement, QueryState.forInternalCalls(), options, System.nanoTime()));
    }

    public static TableMetadata compile(String keyspaceName, String tableName, String schema)
    {

        return CreateTableStatement.parse(String.format(schema, keyspaceName, tableName),
                                          keyspaceName)
                                   .id(tableIdForDseSystemTable(keyspaceName,
                                                                tableName))
                                   .dcLocalReadRepairChance(0)
                                   .memtableFlushPeriod((int) TimeUnit.HOURS.toMillis(1))
                                   .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(90))
                                   .build();

    }

    public static TableId tableIdForDseSystemTable(String keyspace, String table)
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
    public static void maybeAddNewColumn(String ks, String table, String columnName, String cql)
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
     * THIS METHOD MUST BE USED FOR ANY TABLE BEING ALTERED DURING THE
     * STARTUP OF A DSE NODE
     *
     * THIS METHOD MUST NOT BE USED FOR ANY USER TABLES BEING ALTERED DURING
     * THE NORMAL RUNNING OF A DSE NODE
     *
     * @param keyspace The name of the keyspace containing the table to be altered
     * @param table    The name of the table to alter
     *
     */
    public static void maybeAlterTable(String keyspace, String table, String cql) throws org.apache.cassandra.exceptions.InvalidRequestException
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
     *
     * THIS METHOD MUST BE USED FOR ANY KEYSPACE BEING CREATED DURING THE
     * STARTUP OF A DSE NODE.
     *
     * THIS METHOD MUST NOT BE USED FOR USER KEYSPACES BEING CREATED DURING
     * THE NORMAL RUNNING OF A DSE NODE
     *
     * @param expected keyspace metadata to be created.
     * @param timestamp The timestamp to use for the new keyspace
     */
    public static void maybeCreateOrUpdateKeyspace(KeyspaceMetadata expected, long timestamp)
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
    public static KeyspaceMetadata getKeyspaceMetadata(String keyspace)
    {
        return keyspace == null ? null : Schema.instance.getKeyspaceMetadata(keyspace);
    }

    public static CQLStatement prepareStatement(String cql, QueryState queryState, String errorMessage)
    {
        try
        {
            ParsedStatement.Prepared stmt = null;
            while (stmt == null)
            {
                MD5Digest stmtId = TPCUtils.blockingGet(QueryProcessor.instance.prepare(cql, queryState)).statementId;
                stmt = QueryProcessor.instance.getPrepared(stmtId);
            }
            return stmt.statement;

        } catch (RequestValidationException e)
        {
            throw new RuntimeException(errorMessage, e);
        }
    }

}
