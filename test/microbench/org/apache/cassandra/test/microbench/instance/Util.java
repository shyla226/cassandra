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

package org.apache.cassandra.test.microbench.instance;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCMetrics;
import org.apache.cassandra.concurrent.TPCTaskType;

public class Util
{
    public static void printTPCStats()
    {
        for (TPCTaskType stage : TPCTaskType.values())
        {
            String v = "";
            for (int i = 0; i < TPC.perCoreMetrics.length; ++i)
            {
                TPCMetrics metrics = TPC.perCoreMetrics[i];
                if (metrics.completedTaskCount(stage) > 0)
                    v += String.format(" %d: %,d(%,d)", i, metrics.completedTaskCount(stage), metrics.blockedTaskCount(stage));
            }
            if (!v.isEmpty())
                System.out.println(stage + ":" + v);
        }
    }
}