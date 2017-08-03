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
package org.apache.cassandra.utils.units;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.*;

public class TimeValueTest
{
    @Test
    public void testToString() throws Exception
    {
        assertEquals("10ms", TimeValue.of(10L, TimeUnit.MILLISECONDS).toString());
        assertEquals("1s", TimeValue.of(1000L, TimeUnit.MILLISECONDS).toString());
        assertEquals("1.2s", TimeValue.of(1200L, TimeUnit.MILLISECONDS).toString());
        assertEquals("42s", TimeValue.of(42_324L, TimeUnit.MILLISECONDS).toString());
        assertEquals("1m", TimeValue.of(60_000L, TimeUnit.MILLISECONDS).toString());
        assertEquals("4.2m", TimeValue.of(250_000L, TimeUnit.MILLISECONDS).toString());
        assertEquals("1h", TimeValue.of(3_600_000L, TimeUnit.MILLISECONDS).toString());
        assertEquals("2.8d", TimeValue.of(24 * 10_200_000L, TimeUnit.MILLISECONDS).toString());
    }
}