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
package org.apache.cassandra.index.sai.cql;

import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;

public class SimpleTest extends SAITester
{
    @BeforeClass
    public static void setupVersionBarrier()
    {
        requireNetwork();
    }

    @Test
    public void test() throws Throwable
    {
        createTable("CREATE TABLE %s (key int PRIMARY KEY, value1 int, value2 text)");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(value1) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(value2) USING '%s'", StorageAttachedIndex.class.getName()));
        waitForIndexQueryable();

        execute("INSERT INTO %s (key, value1, value2) VALUES (0, 0, 'aaa')");
        execute("INSERT INTO %s (key, value1, value2) VALUES (1, 1, 'bbb')");

        flush();

        execute("INSERT INTO %s (key, value1, value2) VALUES (2, 0, 'aaa')");
        execute("INSERT INTO %s (key, value1, value2) VALUES (3, 1, 'bbb')");
        execute("INSERT INTO %s (key, value1, value2) VALUES (4, 1, 'ccc')");

        UntypedResultSet results = execute("SELECT * FROM %s WHERE value1 = 1 AND value2 = 'bbb'");
        Iterator<UntypedResultSet.Row> rowsIterator = results.iterator();
        while (rowsIterator.hasNext())
        {
            UntypedResultSet.Row row = rowsIterator.next();
            System.out.println("row key="+row.getInt("key")+" value1="+row.getInt("value1")+" value2=" + row.getString("value2"));
        }

        flush();

        results = execute("SELECT * FROM %s WHERE value1 = 1 AND value2 = 'bbb'");
        rowsIterator = results.iterator();
        while (rowsIterator.hasNext())
        {
            UntypedResultSet.Row row = rowsIterator.next();
            System.out.println("row key2="+row.getInt("key")+" value1="+row.getInt("value1")+" value2=" + row.getString("value2"));
        }
    }
}
