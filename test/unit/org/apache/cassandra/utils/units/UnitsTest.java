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

import org.junit.Test;

import static org.junit.Assert.*;

public class UnitsTest
{
    @Test
    public void testFormatValue() throws Exception
    {
        // No comma
        assertEquals("0", Units.formatValue(0L));
        assertEquals("1", Units.formatValue(1L));
        assertEquals("-1", Units.formatValue(-1L));
        assertEquals("10", Units.formatValue(10L));
        assertEquals("-10", Units.formatValue(-10L));
        assertEquals("999", Units.formatValue(999L));
        assertEquals("-999", Units.formatValue(-999L));

        // One comma
        assertEquals("1,000", Units.formatValue(1_000L));
        assertEquals("-1,000", Units.formatValue(-1_000L));
        assertEquals("12,345", Units.formatValue(12_345L));
        assertEquals("-12,345", Units.formatValue(-12_345L));
        assertEquals("999,999", Units.formatValue(999_999L));
        assertEquals("-999,999", Units.formatValue(-999_999L));

        // Two comma
        assertEquals("1,000,000", Units.formatValue(1_000_000L));
        assertEquals("-1,000,000", Units.formatValue(-1_000_000L));
        assertEquals("999,999,999", Units.formatValue(999_999_999L));
        assertEquals("-999,999,999", Units.formatValue(-999_999_999L));

        // Lots of comma
        assertEquals("123,456,789,123,456,789", Units.formatValue(123_456_789_123_456_789L));
        assertEquals("-123,456,789,123,456,789", Units.formatValue(-123_456_789_123_456_789L));
    }
}