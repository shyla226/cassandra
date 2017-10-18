/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import static com.datastax.bdp.db.nodesync.NodeSyncTestTools.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;

// Note: it's called Tester and not Test so JUnit don't pick it up and complain it has no runnable methods
class AbstractValidationProposerTester extends CQLTester
{
    static Function<String, Collection<Range<Token>>> TEST_RANGES = k -> Arrays.asList(range(0, 100),
                                                                                       range(200, 300),
                                                                                       range(400, 500));

    TableMetadata createDummyTable(String ks, boolean nodeSyncEnabled)
    {
        String name = createTable(ks, "CREATE TABLE %s (k int PRIMARY KEY) WITH nodesync = { 'enabled' : '" + nodeSyncEnabled + "' }");
        return tableMetadata(ks, name);
    }

    static Integer mb(int value)
    {
        return value * 1024 * 1024;
    }

}
