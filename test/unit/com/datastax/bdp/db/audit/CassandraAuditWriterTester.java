/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.FunctionExecutionException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.exceptions.UnauthorizedException;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class CassandraAuditWriterTester extends CQLTester
{
    @Before
    public void setUp() throws Throwable
    {
        execute("TRUNCATE TABLE " + CassandraAuditKeyspace.NAME + "." + CassandraAuditKeyspace.AUDIT_LOG);
    }

    @Test
    public void testEventLoggingWithSimpleQueries() throws Throwable
    {
        useSuperUser();

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value int)");
        String f = createFunction(KEYSPACE,
                                  "",
                                  "CREATE OR REPLACE FUNCTION %s() " +
                                  "CALLED ON NULL INPUT " +
                                  "RETURNS int " +
                                  "LANGUAGE java " +
                                  "AS 'throw new RuntimeException();';");

        executeNet("CREATE USER blake WITH PASSWORD 'pass1'");
        executeNet("GRANT SELECT, MODIFY ON KEYSPACE " + KEYSPACE + " TO blake");
        executeNet("GRANT EXECUTE ON FUNCTION " + f + "() TO blake");

        useUser("blake", "pass1");

        executeNet("INSERT INTO %s (pk, value) VALUES (1, 1)");

        executeNet(new SimpleStatement("BEGIN BATCH "
                                       + "INSERT INTO " + KEYSPACE + "." + currentTable() + " (pk, value) VALUES (2, 2); "
                                       + "INSERT INTO " + KEYSPACE + "." + currentTable() + " (pk, value) VALUES (3, 3); "
                                       + "APPLY BATCH"));

        executeInvalidQuery(new SimpleStatement("SELECT * FROM " + CassandraAuditKeyspace.NAME + "." + CassandraAuditKeyspace.AUDIT_LOG),
                            UnauthorizedException.class);

        executeInvalidQuery("SELECT pk, value, " + f + "() FROM %s", FunctionExecutionException.class);

        executeInvalidQuery("SELECT * FORM %s", SyntaxError.class);

        waitForLogging(10);

        assertRows(fetchAuditEvents(),
                   row("CREATE_ROLE", null, null, "cassandra", "CREATE USER blake WITH PASSWORD '*****'"),
                   row("GRANT", null, null, "cassandra", "GRANT SELECT, MODIFY ON KEYSPACE " + KEYSPACE + " TO blake"),
                   row("GRANT", null, null, "cassandra", "GRANT EXECUTE ON FUNCTION " + f + "() TO blake"),
                   row("CQL_UPDATE", KEYSPACE, currentTable(), "blake", "INSERT INTO " + KEYSPACE + "." + currentTable() + " (pk, value) VALUES (1, 1)"),
                   row("CQL_UPDATE", KEYSPACE, currentTable(), "blake", "INSERT INTO " + KEYSPACE + "." + currentTable() + " ( pk , value ) VALUES ( 2 , 2 ) ;"),
                   row("CQL_UPDATE", KEYSPACE, currentTable(), "blake", "INSERT INTO " + KEYSPACE + "." + currentTable() + " ( pk , value ) VALUES ( 3 , 3 ) ;"),
                   row("UNAUTHORIZED_ATTEMPT", CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG, "blake", "User blake has no SELECT permission on <table " + CassandraAuditKeyspace.NAME + "." + CassandraAuditKeyspace.AUDIT_LOG + "> or any of its parents SELECT * FROM " + CassandraAuditKeyspace.NAME + "." + CassandraAuditKeyspace.AUDIT_LOG),
                   row("CQL_SELECT", KEYSPACE, currentTable(), "blake", "SELECT pk, value, " + f + "() FROM " + KEYSPACE + "." + currentTable()),
                   row("REQUEST_FAILURE", KEYSPACE, currentTable(), "blake", "execution of '" + f + "[]' failed: java.lang.RuntimeException SELECT pk, value, " + f + "() FROM " + KEYSPACE + "." + currentTable()),
                   row("REQUEST_FAILURE", null, null, "blake", "line 1:9 mismatched input 'FORM' expecting K_FROM (SELECT * [FORM]...) SELECT * FORM " + KEYSPACE + "." + currentTable()));
    }

    @Test
    public void testEventLoggingWithPreparedQueries() throws Throwable
    {
        useSuperUser();

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value int)");
        String f = createFunction(KEYSPACE,
                                  "",
                                  "CREATE OR REPLACE FUNCTION %s() " +
                                  "CALLED ON NULL INPUT " +
                                  "RETURNS int " +
                                  "LANGUAGE java " +
                                  "AS 'throw new RuntimeException();';");

        executeNet("CREATE USER benjamin WITH PASSWORD 'pass1'");
        executeNet("GRANT SELECT, MODIFY ON KEYSPACE " + KEYSPACE + " TO benjamin");
        executeNet("GRANT EXECUTE ON FUNCTION " + f + "() TO benjamin");

        useUser("benjamin", "pass1");

        PreparedStatement stmt = prepareNet(formatQuery("INSERT INTO %s (pk, value) VALUES (?, ?)"));
        executeNet(stmt.bind(1, 1));

        stmt = prepareNet("BEGIN BATCH "
                           + "INSERT INTO " + KEYSPACE + "." + currentTable() + " (pk, value) VALUES (?, ?); "
                           + "INSERT INTO " + KEYSPACE + "." + currentTable() + " (pk, value) VALUES (?, ?); "
                           + "APPLY BATCH");
        executeNet(stmt.bind(2, 2, 3, 3));

        prepareAndExecuteInvalidQuery("SELECT * FROM " + CassandraAuditKeyspace.NAME + "." + CassandraAuditKeyspace.AUDIT_LOG,
                                      UnauthorizedException.class);

        prepareAndExecuteInvalidQuery("SELECT pk, value, " + f + "() FROM " + KEYSPACE + "." + currentTable(),
                                      FunctionExecutionException.class);

        String query = invalidQuery();

        prepareInvalidQuery(query, InvalidQueryException.class);

        prepareInvalidQuery("SELECT * FORM " + KEYSPACE + "." + currentTable(), SyntaxError.class);

        waitForLogging(16);

        assertRows(fetchAuditEvents(),
                   row("CREATE_ROLE", null, null, "cassandra", "CREATE USER benjamin WITH PASSWORD '*****'"),
                   row("GRANT", null, null, "cassandra", "GRANT SELECT, MODIFY ON KEYSPACE " + KEYSPACE + " TO benjamin"),
                   row("GRANT", null, null, "cassandra", "GRANT EXECUTE ON FUNCTION " + f + "() TO benjamin"),
                   row("CQL_PREPARE_STATEMENT", KEYSPACE, currentTable(), "benjamin", "INSERT INTO " + KEYSPACE + "." + currentTable() + " (pk, value) VALUES (?, ?)"),
                   row("CQL_UPDATE", KEYSPACE, currentTable(), "benjamin", "INSERT INTO " + KEYSPACE + "." + currentTable() + " (pk, value) VALUES (?, ?) [pk=1,value=1]"),
                   row("CQL_PREPARE_STATEMENT", KEYSPACE, currentTable(), "benjamin", "INSERT INTO " + KEYSPACE + "." + currentTable() + " ( pk , value ) VALUES ( ? , ? ) ;"),
                   row("CQL_PREPARE_STATEMENT", KEYSPACE, currentTable(), "benjamin", "INSERT INTO " + KEYSPACE + "." + currentTable() + " ( pk , value ) VALUES ( ? , ? ) ;"),
                   row("CQL_UPDATE", KEYSPACE, currentTable(), "benjamin", "INSERT INTO " + KEYSPACE + "." + currentTable() + " ( pk , value ) VALUES ( ? , ? ) ; [pk=2,value=2]"),
                   row("CQL_UPDATE", KEYSPACE, currentTable(), "benjamin", "INSERT INTO " + KEYSPACE + "." + currentTable() + " ( pk , value ) VALUES ( ? , ? ) ; [pk=3,value=3]"),
                   row("CQL_PREPARE_STATEMENT", CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG, "benjamin", "SELECT * FROM " + CassandraAuditKeyspace.NAME + "." + CassandraAuditKeyspace.AUDIT_LOG),
                   row("UNAUTHORIZED_ATTEMPT", CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG, "benjamin", "User benjamin has no SELECT permission on <table " + CassandraAuditKeyspace.NAME + "." + CassandraAuditKeyspace.AUDIT_LOG + "> or any of its parents SELECT * FROM " + CassandraAuditKeyspace.NAME + "." + CassandraAuditKeyspace.AUDIT_LOG),
                   row("CQL_PREPARE_STATEMENT", KEYSPACE, currentTable(), "benjamin", "SELECT pk, value, " + f + "() FROM " + KEYSPACE + "." + currentTable()),
                   row("CQL_SELECT", KEYSPACE, currentTable(), "benjamin", "SELECT pk, value, " + f + "() FROM " + KEYSPACE + "." + currentTable()),
                   row("REQUEST_FAILURE", KEYSPACE, currentTable(), "benjamin", "execution of '" + f + "[]' failed: java.lang.RuntimeException SELECT pk, value, " + f + "() FROM " + KEYSPACE + "." + currentTable()),
                   row("REQUEST_FAILURE", null, null, "benjamin", "Too many markers(?). 65536 markers exceed the allowed maximum of 65535 " + query),
                   row("REQUEST_FAILURE", null, null, "benjamin", "line 1:9 mismatched input 'FORM' expecting K_FROM (SELECT * [FORM]...) SELECT * FORM " + KEYSPACE + "." + currentTable()));
    }

    @Test
    public void testEventLoggingWithBatch() throws Throwable
    {
        useSuperUser();

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value int)");
        String f = createFunction(KEYSPACE,
                                  "",
                                  "CREATE OR REPLACE FUNCTION %s() " +
                                  "CALLED ON NULL INPUT " +
                                  "RETURNS int " +
                                  "LANGUAGE java " +
                                  "AS 'throw new RuntimeException();';");

        executeNet("CREATE USER sam WITH PASSWORD 'pass1'");
        executeNet("GRANT SELECT, MODIFY ON KEYSPACE " + KEYSPACE + " TO sam");
        executeNet("GRANT EXECUTE ON FUNCTION " + f + "() TO sam");

        useUser("sam", "pass1");

        PreparedStatement preparedStmt = prepareNet(formatQuery("DELETE FROM %s WHERE pk = ?"));

        BatchStatement stmt = new BatchStatement();
        stmt.add(new SimpleStatement(formatQuery("INSERT INTO %s (pk, value) VALUES (1, 2)")));
        stmt.add(preparedStmt.bind(2));
        executeNet(stmt);

        stmt = new BatchStatement();
        stmt.add(new SimpleStatement(formatQuery("INSERT INTO %s (pk, value) VALUES (1, 2)")));
        stmt.add(new SimpleStatement(formatQuery("ISERT INTO %s (pk, value) VALUES (1, 2)")));

        executeInvalidQuery(stmt, SyntaxError.class);

        stmt = new BatchStatement();
        stmt.add(new SimpleStatement(formatQuery("INSERT INTO %s (pk, value) VALUES (1, " + f + "())")));
        stmt.add(new SimpleStatement(formatQuery("INSERT INTO %s (pk, value) VALUES (1, 2)")));

        executeInvalidQuery(stmt, FunctionExecutionException.class);

        waitForLogging(11);

        assertRows(fetchAuditEvents(),
                   row("CREATE_ROLE", null, null, "cassandra", "CREATE USER sam WITH PASSWORD '*****'"),
                   row("GRANT", null, null, "cassandra", "GRANT SELECT, MODIFY ON KEYSPACE " + KEYSPACE + " TO sam"),
                   row("GRANT", null, null, "cassandra", "GRANT EXECUTE ON FUNCTION " + f + "() TO sam"),
                   row("CQL_PREPARE_STATEMENT", KEYSPACE, currentTable(), "sam", "DELETE FROM " + KEYSPACE + "." + currentTable() + " WHERE pk = ?"),
                   row("CQL_UPDATE", KEYSPACE, currentTable(), "sam", "INSERT INTO " + KEYSPACE + "." + currentTable() + " (pk, value) VALUES (1, 2)"),
                   row("CQL_DELETE", KEYSPACE, currentTable(), "sam", "DELETE FROM " + KEYSPACE + "." + currentTable() + " WHERE pk = ? [pk=2]"),
                   row("REQUEST_FAILURE", null, null, "sam", "line 1:0 no viable alternative at input 'ISERT' ([ISERT]...) ISERT INTO " + KEYSPACE + "." + currentTable() + " (pk, value) VALUES (1, 2)"),
                   row("CQL_UPDATE", KEYSPACE, currentTable(), "sam", "INSERT INTO " + KEYSPACE + "." + currentTable() + " (pk, value) VALUES (1, " + f + "())"),
                   row("CQL_UPDATE", KEYSPACE, currentTable(), "sam", "INSERT INTO " + KEYSPACE + "." + currentTable() + " (pk, value) VALUES (1, 2)"),
                   row("REQUEST_FAILURE", KEYSPACE, currentTable(), "sam", "execution of '" + f + "[]' failed: java.lang.RuntimeException INSERT INTO " + KEYSPACE + "." + currentTable() + " (pk, value) VALUES (1, " + f + "())"),
                   row("REQUEST_FAILURE", KEYSPACE, currentTable(), "sam", "execution of '" + f + "[]' failed: java.lang.RuntimeException INSERT INTO " + KEYSPACE + "." + currentTable() + " (pk, value) VALUES (1, 2)"));
    }

    /**
     * Waits for the specific amount of queries to be logged.
     * @param numberOfQueries the number of queries to be logged.
     */
    protected abstract void waitForLogging(int numberOfQueries) throws Throwable;

    private String invalidQuery()
    {
        StringBuilder builder = new StringBuilder("UPDATE " + KEYSPACE + "." + currentTable() + " SET value = 2 WHERE pk IN (");
        for (int i = 0; i < FBUtilities.MAX_UNSIGNED_SHORT + 1; i++)
        {
            if (i != 0)
                builder.append(", ");
            builder.append("?");
        }
        builder.append(')');

        String query = builder.toString();
        return query;
    }

    private void prepareInvalidQuery(String query, Class<? extends Throwable> clazz)
    {
        try
        {
            prepareNet(query);
            fail("The query should have trigger an error");
        }
        catch (Throwable t)
        {
            assertTrue(clazz.isAssignableFrom(t.getClass()));
        }
    }

    private void prepareAndExecuteInvalidQuery(String query, Class<? extends Throwable> clazz)
    {
        try
        {
            PreparedStatement stmt = prepareNet(query);
            executeNet(stmt.bind());
            fail("The query should have trigger an error");
        }
        catch (Throwable t)
        {
            assertTrue(clazz.isAssignableFrom(t.getClass()));
        }
    }

    private void executeInvalidQuery(String query, Class<? extends Throwable> clazz)
    {
        executeInvalidQuery(new SimpleStatement(formatQuery(query)), clazz);
    }

    private void executeInvalidQuery(Statement stmt, Class<? extends Throwable> clazz)
    {
        try
        {
            executeNet(stmt);
            fail("The query should have trigger an error");
        }
        catch (Throwable t)
        {
            assertTrue(clazz.isAssignableFrom(t.getClass()));
        }
    }

    protected final UntypedResultSet fetchAuditEvents() throws Throwable
    {
        return execute("SELECT type, keyspace_name, table_name, username, operation FROM " + CassandraAuditKeyspace.NAME + "." + CassandraAuditKeyspace.AUDIT_LOG);
    }
}
