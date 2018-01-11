/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.cql3.statements;

/**
 * Statement performing an operation on a table
 */
public interface TableStatement extends KeyspaceStatement
{
    /**
     * Returns the table on which the statement is executed.
     * @return the table on which the the statement is executed
     */
    String columnFamily();
}
