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

package org.apache.cassandra.db.compaction.unified;

import java.util.Arrays;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.SystemTimeSource;

/**
 * The static compaction controller periodically checks the IO costs
 * that result from the current configuration of the {@link UnifiedCompactionStrategy}.
 */
public class StaticController extends Controller
{
    /**
     * The scaling factors W, one per bucket index and separated by a comma.
     * Higher indexes will use the value of the last index with a W specified.
     */
    static String SCALING_FACTORS_OPTION = "static_scaling_factors";
    private static String DEFAULT_SCALING_FACTORS = System.getProperty(PREFIX + SCALING_FACTORS_OPTION, "2, -8");

    private final int[] Ws;

    @VisibleForTesting // comp. simulation
    public StaticController(Environment env,
                            int[] Ws,
                            double survivalFactor,
                            long dataSetSizeMB,
                            int numShards,
                            long minSSTableSizeMB,
                            double maxSpaceOverhead)
    {
        super(new SystemTimeSource(), env, survivalFactor, dataSetSizeMB, numShards, minSSTableSizeMB, maxSpaceOverhead);
        this.Ws = Ws;
    }

    static Controller fromOptions(Environment env, double o, long dataSetSizeMB, int numShards, long minSSTableSizeMB, double maxSpaceOverhead, Map<String, String> options)
    {
        int[] Ws = parseScalingFactors(options.containsKey(SCALING_FACTORS_OPTION) ? options.get(SCALING_FACTORS_OPTION) : DEFAULT_SCALING_FACTORS);
        return new StaticController(env, Ws, o, dataSetSizeMB, numShards, minSSTableSizeMB, maxSpaceOverhead);
    }

    @VisibleForTesting
    static int[] parseScalingFactors(String str)
    {
        String[] vals = str.split(",");
        int[] ret = new int[vals.length];
        for (int i = 0; i < vals.length; i++)
            ret[i] = Integer.parseInt(vals[i].trim());

        return ret;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        options.remove(SCALING_FACTORS_OPTION);
        return options;
    }

    @Override
    public int getW(int index)
    {
        if (index < 0)
            throw new IllegalArgumentException("Index should be >= 0: " + index);

        return index < Ws.length ? Ws[index] : Ws[Ws.length - 1];
    }

    @Override
    public String toString()
    {
        return String.format("Static controller, m: %d, o: %f, Ws: %s, cost: %s", minSstableSizeMB, survivalFactor, Arrays.toString(Ws), calculator);
    }
}