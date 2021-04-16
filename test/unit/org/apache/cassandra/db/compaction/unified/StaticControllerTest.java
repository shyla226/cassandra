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
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StaticControllerTest extends ControllerTest
{
    static final int[] Ws = new int[] { 30, 2, -6};

    @Test
    public void testFromOptions()
    {
        Map<String, String> options = new HashMap<>();
        String wStr = String.join(",", Arrays.stream(Ws).mapToObj(i -> Integer.toString(i)).collect(Collectors.toList()));
        options.put(StaticController.SCALING_FACTORS_OPTION, wStr);

        Controller controller = testFromOptions(false, options);
        assertTrue(controller instanceof StaticController);

        for (int i = 0; i < Ws.length; i++)
            assertEquals(Ws[i], controller.getW(i));

        assertEquals(Ws[Ws.length-1], controller.getW(Ws.length));
    }

    @Test
    public void testValidateOptions()
    {
        Map<String, String> options = new HashMap<>();
        String wStr = String.join(",", Arrays.stream(Ws).mapToObj(i -> Integer.toString(i)).collect(Collectors.toList()));
        options.put(StaticController.SCALING_FACTORS_OPTION, wStr);

        super.testValidateOptions(options, false);
    }

    @Test
    public void testStartShutdown()
    {
        StaticController controller = new StaticController(env, Ws, Controller.DEFAULT_SURVIVAL_FACTOR, minSSTableSizeMB);
        super.testStartShutdown(controller);
    }

    @Test
    public void testShutdownNotStarted()
    {
        StaticController controller = new StaticController(env, Ws, Controller.DEFAULT_SURVIVAL_FACTOR, minSSTableSizeMB);
        super.testShutdownNotStarted(controller);
    }

    @Test(expected = IllegalStateException.class)
    public void testStartAlreadyStarted()
    {
        StaticController controller = new StaticController(env, Ws, Controller.DEFAULT_SURVIVAL_FACTOR, minSSTableSizeMB);
        super.testStartAlreadyStarted(controller);
    }
}