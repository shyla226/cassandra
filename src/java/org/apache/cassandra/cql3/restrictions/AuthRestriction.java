/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.cql3.restrictions;

import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.service.QueryState;

/**
 * Special restriction use to prevent access to some row level information.
 */
public interface AuthRestriction
{
    /**
     * Adds to the specified row filter the expressions used to control the row access.
     * @param filter the row filter to add expressions to
     * @param state the query state
     */
    public void addRowFilterTo(RowFilter filter, QueryState state);
}
