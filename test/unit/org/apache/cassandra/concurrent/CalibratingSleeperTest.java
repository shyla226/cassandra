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

package org.apache.cassandra.concurrent;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CalibratingSleeperTest
{
    long time;
    long parkTime;

    @Test
    public void callibrationShouldConverge()
    {
        ParkedThreadsMonitor.CalibratingSleeper sleeper = new ParkedThreadsMonitor.CalibratingSleeper(1000)
        {
            @Override
            long nanoTime()
            {
                return time;
            }

            void park()
            {
                time += parkTime;
            }
        };
        parkTime = 1000;
        for (int i = 0; i < 1000; i++)
        {
            // sleeps for exactly the expected time
            sleeper.sleep();
            // expect no change to calibratedSleepNs
            assertEquals(sleeper.calibratedSleepNs, 1000);
        }

        for (int i = 0; i < 1000; i++)
        {
            parkTime = 900l + ThreadLocalRandom.current().nextLong(200);
            // sleep is within 10% of target
            sleeper.sleep();
            // expect no change to calibratedSleepNs
            assertEquals(sleeper.calibratedSleepNs, 1000);
        }

        // should converge from above
        for (int i = 0; i < 10000; i++)
        {
            parkTime = sleeper.calibratedSleepNs * 10;
            // sleep is 10x what we ask
            sleeper.sleep();
        }
        assertTrue(900 < parkTime && parkTime < 1100);

        // should converge from below
        for (int i = 0; i < 10000; i++)
        {
            parkTime = sleeper.calibratedSleepNs / 10;
            // sleep is 10x what we ask
            sleeper.sleep();
        }
        assertTrue(900 < parkTime && parkTime < 1100);
    }
}