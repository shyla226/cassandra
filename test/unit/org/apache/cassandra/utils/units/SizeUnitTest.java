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

import static org.apache.cassandra.utils.units.SizeUnit.*;

public class SizeUnitTest
{
    @Test
    public void testConvert() throws Exception
    {
        // We know convert delegates to the other methods, so we don't go overboard on testing it with all units. We
        // just use that test for a few random conversions.
        assertEquals(0, GIGABYTES.convert(100, BYTES));
        assertEquals(100, GIGABYTES.convert(100 * C3, BYTES));
        assertEquals(100 * C1, GIGABYTES.convert(100 * C4, BYTES));
        assertEquals(Long.MAX_VALUE, BYTES.convert(100 * C4, GIGABYTES));
    }

    @Test
    public void testToBytes() throws Exception
    {
        testToBytes(BYTES, C0);
        testToBytes(KILOBYTES, C1);
        testToBytes(MEGABYTES, C2);
        testToBytes(GIGABYTES, C3);
        testToBytes(TERABYTES, C4);

        // Test overflow
        assertEquals(Long.MAX_VALUE, TERABYTES.toBytes(Long.MAX_VALUE / 10));
    }

    private void testToBytes(SizeUnit unit, long constant)
    {
        assertEquals(1    * constant, unit.toBytes(1));
        assertEquals(1023 * constant, unit.toBytes(1023));
        assertEquals(1024 * constant, unit.toBytes(1024));
        assertEquals(2049 * constant, unit.toBytes(2049));
    }

    @Test
    public void testToKiloBytes() throws Exception
    {
        assertEquals(0, BYTES.toKiloBytes(1));
        assertEquals(0, BYTES.toKiloBytes(1023));
        assertEquals(1, BYTES.toKiloBytes(1024));
        assertEquals(2, BYTES.toKiloBytes(2049));

        testToKiloBytes(KILOBYTES, C0);
        testToKiloBytes(MEGABYTES, C1);
        testToKiloBytes(GIGABYTES, C2);
        testToKiloBytes(TERABYTES, C3);
    }

    private void testToKiloBytes(SizeUnit unit, long constant)
    {
        assertEquals(1    * constant, unit.toKiloBytes(1));
        assertEquals(1023 * constant, unit.toKiloBytes(1023));
        assertEquals(1024 * constant, unit.toKiloBytes(1024));
        assertEquals(2049 * constant, unit.toKiloBytes(2049));
    }

    @Test
    public void testToMegaBytes() throws Exception
    {
        testToMegaBytes(BYTES, 0);
        
        assertEquals(0, KILOBYTES.toMegaBytes(1));
        assertEquals(0, KILOBYTES.toMegaBytes(1023));
        assertEquals(1, KILOBYTES.toMegaBytes(1024));
        assertEquals(2, KILOBYTES.toMegaBytes(2049));

        testToMegaBytes(MEGABYTES, C0);
        testToMegaBytes(GIGABYTES, C1);
        testToMegaBytes(TERABYTES, C2);
    }

    private void testToMegaBytes(SizeUnit unit, long constant)
    {
        assertEquals(1    * constant, unit.toMegaBytes(1));
        assertEquals(1023 * constant, unit.toMegaBytes(1023));
        assertEquals(1024 * constant, unit.toMegaBytes(1024));
        assertEquals(2049 * constant, unit.toMegaBytes(2049));
    }

    @Test
    public void testToGigaBytes() throws Exception
    {
        testToGigaBytes(BYTES, 0);
        testToGigaBytes(KILOBYTES, 0);

        assertEquals(0, MEGABYTES.toGigaBytes(1));
        assertEquals(0, MEGABYTES.toGigaBytes(1023));
        assertEquals(1, MEGABYTES.toGigaBytes(1024));
        assertEquals(2, MEGABYTES.toGigaBytes(2049));

        testToGigaBytes(GIGABYTES, C0);
        testToGigaBytes(TERABYTES, C1);
    }

    private void testToGigaBytes(SizeUnit unit, long constant)
    {
        assertEquals(1    * constant, unit.toGigaBytes(1));
        assertEquals(1023 * constant, unit.toGigaBytes(1023));
        assertEquals(1024 * constant, unit.toGigaBytes(1024));
        assertEquals(2049 * constant, unit.toGigaBytes(2049));
    }

    @Test
    public void testToTeraBytes() throws Exception
    {
        testToTeraBytes(BYTES, 0);
        testToTeraBytes(KILOBYTES, 0);
        testToTeraBytes(MEGABYTES, 0);

        assertEquals(0, GIGABYTES.toTeraBytes(1));
        assertEquals(0, GIGABYTES.toTeraBytes(1023));
        assertEquals(1, GIGABYTES.toTeraBytes(1024));
        assertEquals(2, GIGABYTES.toTeraBytes(2049));

        testToTeraBytes(TERABYTES, C0);
    }

    private void testToTeraBytes(SizeUnit unit, long constant)
    {
        assertEquals(1    * constant, unit.toTeraBytes(1));
        assertEquals(1023 * constant, unit.toTeraBytes(1023));
        assertEquals(1024 * constant, unit.toTeraBytes(1024));
        assertEquals(2049 * constant, unit.toTeraBytes(2049));
    }
}