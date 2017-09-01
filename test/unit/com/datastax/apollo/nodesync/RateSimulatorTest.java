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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.datastax.apollo.nodesync.RateSimulator.Info;
import com.datastax.apollo.nodesync.RateSimulator.TableInfo;
import org.apache.cassandra.utils.units.RateUnit;
import org.apache.cassandra.utils.units.RateValue;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.SizeValue;
import org.apache.cassandra.utils.units.TimeValue;

import static org.junit.Assert.*;

import static com.datastax.apollo.nodesync.RateSimulator.TableInfo.Property.*;

public class RateSimulatorTest
{
    // Fix the keyspace name for those test. Note that in reality, we'd need multiple keyspace to test different
    // replication factor, but we can fake different replication for the same keyspace as far as those tests go.
    private static final String KS_NAME = "ks";

    private static SizeValue bytes(long value)
    {
        return SizeValue.of(value, SizeUnit.BYTES);
    }

    private static SizeValue mb(long value)
    {
        return SizeValue.of(value, SizeUnit.MEGABYTES);
    }

    private static SizeValue gb(long value)
    {
        return SizeValue.of(value, SizeUnit.GIGABYTES);
    }

    private static TimeValue days(long value)
    {
        return TimeValue.of(value, TimeUnit.DAYS);
    }

    private static TimeValue hours(long value)
    {
        return TimeValue.of(value, TimeUnit.HOURS);
    }

    private static TimeValue sec(long value)
    {
        return TimeValue.of(value, TimeUnit.SECONDS);
    }

    private static TableInfo tableInfo(String table, int rf, SizeValue size, TimeValue deadline)
    {
        return new TableInfo(KS_NAME, table, rf, true, size, deadline);
    }

    @Test
    public void testToAndFromJMX()
    {
        Info info = Info.from(tableInfo("t1", 2, mb(100), days(20)),
                              tableInfo("t2", 4, gb(1), hours(2)));

        List<Map<String, String>> jmx = info.toJMX();
        Iterator<TableInfo> infoIter = info.tables().iterator();
        Iterator<Map<String, String>> jmxIter = jmx.iterator();

        // Test each individual info convert properly to JMX and back
        while (infoIter.hasNext() && jmxIter.hasNext())
            testToAndFromJMX(infoIter.next(), jmxIter.next());

        assertFalse(infoIter.hasNext());
        assertFalse(jmxIter.hasNext());

        // Lastly, make sure rebuilding from jmx gives us identity
        assertEquals(info, Info.fromJMX(jmx));
    }

    private void testToAndFromJMX(TableInfo info, Map<String, String> m)
    {
        // More a sanity check that we're provided proper arguments
        assertEquals(info.toStringMap(), m);

        assertEquals(info.keyspace, m.get(KEYSPACE.toString()));
        assertEquals(info.table, m.get(TABLE.toString()));
        assertEquals(info.replicationFactor, Integer.parseInt(m.get(REPLICATION_FACTOR.toString())));
        assertEquals(info.dataSize, SizeValue.of(Long.parseLong(m.get(DATA_SIZE.toString())), SizeUnit.BYTES));
        assertEquals(info.deadlineTarget, TimeValue.of(Long.parseLong(m.get(DEADLINE_TARGET.toString())), TimeUnit.SECONDS));

        // Make sure we don't have anything else we don't know about
        assertEquals(TableInfo.Property.values().length, m.size());

        assertEquals(info, TableInfo.fromStringMap(m));
    }

    @Test
    public void testTheoreticalMinimum()
    {
        testComputeRate(RateSimulator.Parameters.THEORETICAL_MINIMUM);
    }

    @Test
    public void testMinimumRecommend()
    {
        testComputeRate(RateSimulator.Parameters.MINIMUM_RECOMMENDED);
    }

    @Test
    public void testRecommended()
    {
        testComputeRate(RateSimulator.Parameters.RECOMMENDED);
    }

    private void testComputeRate(RateSimulator.Parameters params)
    {
        // single table has 200 bytes to validate at RF=2, so 100 bytes (plus adjustment), and that within a 10 sec
        // (minus adjustment).
        assertRate(params.adjustedSize(100)/params.adjustedDeadline(10),
                   params,
                   tableInfo("t", 2, bytes(200), sec(10)));

        // The min rate is obviously driven by the 2nd table (the 1st can go with a ridiculously slow rate).
        // For that 2nd table, it to validate 256Mb (1Gb but RF=4) in 2 hours, which is 256 * 1024 * 1024 bytes in
        // 2 * 3600 seconds. All this with adjustment.
        assertRate(params.adjustedSize(256L * 1024 * 1024) / params.adjustedDeadline(2 * 3600),
                   params,
                   tableInfo("t1", 2, mb(100), days(20)),
                   tableInfo("t2", 4, gb(1), hours(2)));


        // A single table with 100Gb to validate (300Gb with RF=3) in 10 days (mostly here to make the next case easier to read).
        // Note that the direct computation overflows, but it's trivial to just divide everything by 10, i.e. we need
        // the same rate than to validate 10Gb in 1 day.
        long expectedRate = params.adjustedSize(10L * 1024 * 1024 * 1024) / params.adjustedDeadline(24 * 3600);
        assertRate(expectedRate,
                   params,
                   tableInfo("t", 3, gb(300), days(10)));

        // Now, we have 3 of the same tables than above. Each can be validated in time with 'expectedRate', but we
        // really need all 3 to be validated with the 10 days deadline to respect it for all, so we need 3 times the
        // rate (not unexpectedly, we have 3 times as much data after all; mainly testing here that the simulator is
        // handling this right).
        assertRate(3 * expectedRate,
                   params,
                   tableInfo("t1", 3, gb(300), days(10)),
                   tableInfo("t2", 3, gb(300), days(10)),
                   tableInfo("t3", 3, gb(300), days(10)));
    }

    private void assertRate(long expectedRate, RateSimulator.Parameters params, TableInfo... tables)
    {
        RateValue computed = new RateSimulator(Info.from(tables), params).computeRate();
        assertRate(params.adjustedRate(expectedRate), computed);
    }

    // Most of our test computation are done in bytes-per-sec to keep it simple, so this the unit of expectedRate here.
    private void assertRate(long expectedRate, RateValue actualRate)
    {
        assertRate(RateValue.of(expectedRate, RateUnit.B_S), actualRate);
    }

    private void assertRate(RateValue expected, RateValue actual)
    {
        // We do some basic rate computation in the test to ensure the simulator is computing proper values. There
        // is float to integer conversions involved in both cases (our test computation, and the simulator ones), but
        // we don't want the test to be too fragile to those, because it really doesn't matter in context. So we simply
        // compare that the received rates are within 2% of the smaller value, which is enough to ensure the simulator
        // does what it should, without being overly annoying.
        long expectedRate = expected.in(RateUnit.B_S);
        long actualRate = actual.in(RateUnit.B_S);

        long epsilon = (long)(0.2 * Math.min(expectedRate, actualRate));
        assertTrue(String.format("Expected rate of %s but got %s", expected, actual),
                   Math.abs(expectedRate - actualRate) <= epsilon);
    }
}