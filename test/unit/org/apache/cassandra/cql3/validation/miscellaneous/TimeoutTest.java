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

package org.apache.cassandra.cql3.validation.miscellaneous;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.junit.Assert.fail;

public class TimeoutTest extends CQLTester
{

    static
    {
        System.setProperty("cassandra.test.read_iteration_delay_ms", "5000");
    }


    @Test
    public void testTimeout() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b int, c int)");

        executeNet(ProtocolVersion.CURRENT,"INSERT INTO %s (a, b, c) VALUES (1, 2, 4)");
        getCurrentColumnFamilyStore().forceBlockingFlush();

        executeNet(ProtocolVersion.CURRENT,"INSERT INTO %s (a, b, c) VALUES (1, 2, 4)");
        getCurrentColumnFamilyStore().forceBlockingFlush();

        executeNet(ProtocolVersion.CURRENT,"INSERT INTO %s (a, b, c) VALUES (1, 2, 4)");
        getCurrentColumnFamilyStore().forceBlockingFlush();

        executeNet(ProtocolVersion.CURRENT,"INSERT INTO %s (a, b, c) VALUES (1, 2, 4)");
        getCurrentColumnFamilyStore().forceBlockingFlush();

        getCurrentColumnFamilyStore().forceMajorCompaction();

        try
        {
            executeNet(ProtocolVersion.CURRENT, formatQuery("select * from %s where a = 1"));
            fail("Should have timed out");
        }
        catch (Throwable t)
        {

        }

        executeNet(ProtocolVersion.CURRENT, "INSERT INTO %s (a, b, c) VALUES (2, 1, 6)");
        executeNet(ProtocolVersion.CURRENT, "INSERT INTO %s (a, b, c) VALUES (3, 2, 4)");

        getCurrentColumnFamilyStore().forceBlockingFlush();

    }

}
