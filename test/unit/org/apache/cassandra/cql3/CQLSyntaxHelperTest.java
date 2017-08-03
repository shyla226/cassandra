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
package org.apache.cassandra.cql3;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.apache.cassandra.cql3.CQLSyntaxHelper.*;


public class CQLSyntaxHelperTest
{
    @Test
    public void testToCQLMap() throws Exception
    {
        assertEquals("{}", toCQLMap(ImmutableMap.of()));
        assertEquals("{'foo': 'bar'}", toCQLMap(ImmutableMap.of("foo", "bar")));
        assertEquals("{'foo1': 'bar1', 'foo2': 'bar2'}", toCQLMap(ImmutableMap.of("foo1", "bar1", "foo2", "bar2")));
    }

    @Test
    public void testToCQLString() throws Exception
    {
        assertEquals("'foo'", toCQLString("foo"));
        assertEquals("'foo''bar'", toCQLString("foo'bar"));
        assertEquals("'foo''''bar''foo'", toCQLString("foo''bar'foo"));
    }
}