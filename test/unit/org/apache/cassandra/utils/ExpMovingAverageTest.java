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

package org.apache.cassandra.utils;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ExpMovingAverageTest
{
    private static final double epsilon = 0.0001;

    @Test
    public void testUpdates() throws InterruptedException
    {
        TimeSource timeSource = new TestTimeSource();
        ExpMovingAverage average = ExpMovingAverage.oneMinute(timeSource);
        assertNotNull(average.toString());

        assertEquals(0, average.getCurrent(), epsilon);
        assertEquals(0, average.get(), epsilon);

        average.update(10);

        assertEquals(10, average.getCurrent(), epsilon);
        assertEquals(10, average.get(), epsilon);

        average.update(11);
        assertEquals(10.5, average.getCurrent(), epsilon);
        assertEquals(10, average.get(), epsilon);

        average.update(12);
        assertEquals(11.25, average.getCurrent(), epsilon);
        assertEquals(10, average.get(), epsilon);

        timeSource.sleep(5, TimeUnit.SECONDS);
        average.update(12);

        assertEquals(0, average.getCurrent(), epsilon);
        assertEquals(10.1299, average.get(), epsilon);
    }
}