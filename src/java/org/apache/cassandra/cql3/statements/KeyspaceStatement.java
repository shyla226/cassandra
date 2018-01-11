package org.apache.cassandra.cql3.statements;

/**
 * Statement performing an operation within the scope of a Keyspace
 */
public interface KeyspaceStatement
{
    /**
     * Returns the keyspace within which the statement is executed.
     * @return the keyspace within which the statement is executed
     */
    String keyspace();
}
