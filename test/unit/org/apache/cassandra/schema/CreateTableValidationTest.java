/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.schema;

import java.util.UUID;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.UUIDGen;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class CreateTableValidationTest extends CQLTester
{
    @Test
    public void testInvalidBloomFilterFPRatio()
    {
        try
        {
            createTableMayThrow("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.0000001");
            fail("Expected an fp chance of 0.0000001 to be rejected");
        }
        catch (ConfigurationException exc) { }

        try
        {
            createTableMayThrow("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 1.1");
            fail("Expected an fp chance of 1.1 to be rejected");
        }
        catch (ConfigurationException exc) { }

        // sanity check
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1");
    }

    @Test
    public void testDuplicateCfId()
    {
        UUID id = UUIDGen.getTimeUUID();
        String table = createTable(String.format("CREATE TABLE %%s (a int PRIMARY KEY, b int) WITH ID = %s", id));

        String t = createTableName();
        try
        {
            QueryProcessor.executeOnceInternal(String.format("CREATE TABLE %s.%s (a int PRIMARY KEY, b int) WITH ID = %s",
                                                             KEYSPACE_PER_TEST,
                                                             t,
                                                             id))
                          .blockingGet();
            fail("CREATE TABLE with existing cf-id must fail");
        }
        catch (AlreadyExistsException exc) {
            assertEquals(KEYSPACE_PER_TEST, exc.ksName);
            assertEquals(currentTable(), exc.cfName);
            assertEquals(String.format("ID %s used in CREATE TABLE statement is already used by table %s.%s",
                                       id, KEYSPACE, table), exc.getMessage());
        }

        // duplicate cf-id with IF NOT EXISTS should do nothing
        String table2 = createTableMayThrow(String.format("CREATE TABLE IF NOT EXISTS %%s (a int PRIMARY KEY, b int) WITH ID = %s", id));
        assertNull(Schema.instance.getTableMetadata(KEYSPACE, table2));
    }
}
