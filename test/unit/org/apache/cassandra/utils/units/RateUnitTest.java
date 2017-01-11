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

public class RateUnitTest
{
    @Test
    public void testConvert() throws Exception
    {
        assertEquals(10 * 1024, RateUnit.B_S.convert(10L, RateUnit.KB_S));
        assertEquals(10 * 1024 * 1024, RateUnit.B_S.convert(10L, RateUnit.MB_S));
        assertEquals(10 * 1024, RateUnit.MB_S.convert(10L, RateUnit.GB_S));
        assertEquals(0, RateUnit.MB_S.convert(10L, RateUnit.B_S));

        RateUnit B_MS = RateUnit.of(SizeUnit.BYTES, TimeUnit.MILLISECONDS);
        // 10 kB/s == 10,240 B/s == 10 kB/ms
        assertEquals(10L, B_MS.convert(10L, RateUnit.KB_S));

        RateUnit GB_D = RateUnit.of(SizeUnit.GIGABYTES, TimeUnit.DAYS);
        // 10 MB/s == 10 * 3600 MB/h == 36,000 MB/h == 864,000 MB/days == 843 GB/days
        assertEquals(843L, GB_D.convert(10L, RateUnit.MB_S));

        RateUnit GB_MS = RateUnit.of(SizeUnit.GIGABYTES, TimeUnit.MILLISECONDS);
        // 10 MB/s == 0 GB/ms
        assertEquals(0, GB_MS.convert(10L, RateUnit.MB_S));

        RateUnit B_H = RateUnit.of(SizeUnit.BYTES, TimeUnit.HOURS);
        // 10 MB/s == 10 * 1024 * 1024 B/s = 10 * 1024 * 1024 * 3600 B/hours
        assertEquals(10L * 1024 * 1024 * 3600, B_H.convert(10L, RateUnit.MB_S));
    }

    @Test
    public void testToHRString() throws Exception
    {
        assertEquals("10B/s", RateUnit.B_S.toHRString(10));
        assertEquals("1kB/s", RateUnit.B_S.toHRString(1024L));
        assertEquals("1.1kB/s", RateUnit.B_S.toHRString(1150L));
        assertEquals("4MB/s", RateUnit.B_S.toHRString(4 * 1024 * 1024L));
        assertEquals("2,600TB/s", RateUnit.GB_S.toHRString(2600 * 1024L));
    }

    @Test
    public void testCompare() throws Exception
    {
        assertTrue(RateUnit.MB_S.compareTo(RateUnit.MB_S) == 0);

        assertTrue(RateUnit.MB_S.compareTo(RateUnit.GB_S) < 0);
        assertTrue(RateUnit.MB_S.compareTo(RateUnit.TB_S) < 0);
        
        assertTrue(RateUnit.MB_S.compareTo(RateUnit.KB_S) > 0);
        assertTrue(RateUnit.MB_S.compareTo(RateUnit.B_S) > 0);

        RateUnit MB_MS = RateUnit.of(SizeUnit.MEGABYTES, TimeUnit.MILLISECONDS);
        RateUnit MB_NS = RateUnit.of(SizeUnit.MEGABYTES, TimeUnit.NANOSECONDS);

        assertTrue(RateUnit.MB_S.compareTo(MB_MS) < 0);
        assertTrue(RateUnit.MB_S.compareTo(MB_NS) < 0);

        RateUnit KB_MS = RateUnit.of(SizeUnit.KILOBYTES, TimeUnit.MILLISECONDS);
        RateUnit KB_NS = RateUnit.of(SizeUnit.KILOBYTES, TimeUnit.NANOSECONDS);

        // 1 MB/s = 1024 kB/s > 1000 kB/s = 1kB/ms
        assertTrue(RateUnit.MB_S.compareTo(KB_MS) > 0);
        // 1 MB/s = 1024 kB/s < 1000 * 1000 kB/s = 1kB/ns
        assertTrue(RateUnit.MB_S.compareTo(KB_NS) < 0);

        RateUnit MB_M = RateUnit.of(SizeUnit.MEGABYTES, TimeUnit.MINUTES);
        RateUnit GB_D = RateUnit.of(SizeUnit.GIGABYTES, TimeUnit.DAYS);
        // 1 MB/m = 1440 MB/d > 1024 MB/d = 1 GB/d
        assertTrue(MB_M.compareTo(GB_D) > 0);

        RateUnit GB_MS = RateUnit.of(SizeUnit.GIGABYTES, TimeUnit.MILLISECONDS);
        // 1 MB/s < 1 GB/ms
        assertTrue(RateUnit.MB_S.compareTo(GB_MS) < 0);
    }

    @Test
    public void testSmallestRepresentation() throws Exception
    {
        // The smallest unit
        RateUnit B_D = RateUnit.of(SizeUnit.BYTES, TimeUnit.DAYS);

        // A few simple case that all resolve to the smallest unit
        assertEquals(B_D, B_D.smallestRepresentableUnit(1));
        assertEquals(B_D, B_D.smallestRepresentableUnit(Long.MAX_VALUE));
        assertEquals(B_D, RateUnit.B_S.smallestRepresentableUnit(1));
        assertEquals(B_D, RateUnit.KB_S.smallestRepresentableUnit(1));

        assertEquals(RateUnit.MB_S, RateUnit.MB_S.smallestRepresentableUnit(Long.MAX_VALUE - 10));
        assertEquals(RateUnit.KB_S, RateUnit.MB_S.smallestRepresentableUnit((Long.MAX_VALUE / 1024) - 10));

        // Slightly more subtle cases
        long v1 = (Long.MAX_VALUE-1) / 1000;
        long v2 = (Long.MAX_VALUE-1) / 1024;
        long v3 = (Long.MAX_VALUE-1) / (1000 * 60);

        RateUnit MB_MS = RateUnit.of(SizeUnit.MEGABYTES, TimeUnit.MILLISECONDS);
        RateUnit KB_MS = RateUnit.of(SizeUnit.KILOBYTES, TimeUnit.MILLISECONDS);
        RateUnit MB_M = RateUnit.of(SizeUnit.MEGABYTES, TimeUnit.MINUTES);
        assertEquals(RateUnit.MB_S, MB_MS.smallestRepresentableUnit(v1));
        assertEquals(KB_MS, MB_MS.smallestRepresentableUnit(v2));
        assertEquals(MB_M, MB_MS.smallestRepresentableUnit(v3));

    }
}