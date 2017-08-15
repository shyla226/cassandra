/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.cql3;

import java.util.Collections;
import java.util.List;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.cassandra.utils.Pair;

public class PreparedStatementCache
{
    // Holds the original query string & bound terms definitions
    // for prepared statements.
    //
    // Values are inserted during preparation & retrieved at
    // execution time by the OnPrepared & PostExecution hooks.
    // The cache uses weak keys, so when the prepared CQLStatement
    // is evicted from C*'s prepared statement cache the mapping
    // will also be evicted here.
    //
    // Note, getQueryString is only called *after* a prepared stmt
    // has been executed, so if we find ourselves in the position
    // where a statement is present in C*'s cache, but it has been
    // evicted here, we are pretty unlucky
    private final Cache<CQLStatement, Pair<String, List<ColumnSpecification>>> preparedQueryInfo =
                    CacheBuilder.newBuilder().weakKeys().build();

    private static final Pair<String, List<ColumnSpecification>> MISSING_QUERY_DEFAULT =
            Pair.create("Query string not found in prepared statement cache", Collections.<ColumnSpecification>emptyList());
    public static final PreparedStatementCache instance = new PreparedStatementCache();

    public void addQueryInfo(CQLStatement statement, String queryString, List<ColumnSpecification> boundNames)
    {
        preparedQueryInfo.put(statement, Pair.create(queryString, boundNames));
    }

    public Pair<String, List<ColumnSpecification>> getQueryInfo(CQLStatement statement)
    {
        Pair<String, List<ColumnSpecification>> queryInfo = preparedQueryInfo.getIfPresent(statement);
        return queryInfo != null ? queryInfo : MISSING_QUERY_DEFAULT;
    }
}
