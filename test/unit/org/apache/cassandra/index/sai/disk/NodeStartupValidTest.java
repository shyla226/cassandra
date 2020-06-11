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

package org.apache.cassandra.index.sai.disk;

import java.util.LinkedList;
import java.util.List;

import org.junit.runners.Parameterized;

public class NodeStartupValidTest extends AbstractNodeStartupTest
{
    @SuppressWarnings("unused")
    @Parameterized.Parameters(name = "{0} {1} {2}")
    public static List<Object[]> startupScenarios()
    {
        List<Object[]> scenarios = new LinkedList<>();

        scenarios.add( new Object[] { AbstractNodeStartupTest.Populator.INDEXABLE_ROWS, AbstractNodeStartupTest.IndexStateOnRestart.VALID, AbstractNodeStartupTest.StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 0, 0, DOCS });
        scenarios.add( new Object[] { AbstractNodeStartupTest.Populator.INDEXABLE_ROWS, AbstractNodeStartupTest.IndexStateOnRestart.VALID, AbstractNodeStartupTest.StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 0, 0, DOCS });
        scenarios.add( new Object[] { AbstractNodeStartupTest.Populator.NON_INDEXABLE_ROWS, AbstractNodeStartupTest.IndexStateOnRestart.VALID, AbstractNodeStartupTest.StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 0, 0, 0 });
        scenarios.add( new Object[] { AbstractNodeStartupTest.Populator.NON_INDEXABLE_ROWS, AbstractNodeStartupTest.IndexStateOnRestart.VALID, AbstractNodeStartupTest.StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 0, 0, 0 });
        scenarios.add( new Object[] { AbstractNodeStartupTest.Populator.TOMBSTONES, AbstractNodeStartupTest.IndexStateOnRestart.VALID, AbstractNodeStartupTest.StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 0, 0, 0 });
        scenarios.add( new Object[] { AbstractNodeStartupTest.Populator.TOMBSTONES, AbstractNodeStartupTest.IndexStateOnRestart.VALID, AbstractNodeStartupTest.StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 0, 0, 0 });

        return scenarios;
    }

}
