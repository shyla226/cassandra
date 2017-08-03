/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.apollo.nodesync;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;

import static com.datastax.apollo.nodesync.NodeSyncTestTools.*;

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
