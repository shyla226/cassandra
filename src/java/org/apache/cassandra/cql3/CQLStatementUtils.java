/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.cql3;

import org.apache.cassandra.cql3.statements.KeyspaceStatement;
import org.apache.cassandra.cql3.statements.TableStatement;

/**
 * Utility methods for working with {@code CQLStatement}s.
 */
public final class CQLStatementUtils
{
    /**
     * Returns the keyspace on which operate the specified statement.
     * @param stmt the statement
     * @return a keyspace name or {@code null} if the statement does not operate on a keyspace (e.g. auth statements).
     */
    public static String getKeyspace(CQLStatement stmt)
    {
        return stmt instanceof KeyspaceStatement ? ((KeyspaceStatement) stmt).keyspace() : null;
    }

    /**
     * Returns the table on which operate the specified statement.
     * @param stmt the statement
     * @return a table name or {@code null} if the statement does not operate on a table.
     */
    public static String getTable(CQLStatement stmt)
    {
        return stmt instanceof TableStatement ? ((TableStatement) stmt).columnFamily() : null;
    }

    private CQLStatementUtils()
    {
    }
}
