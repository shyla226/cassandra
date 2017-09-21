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
package org.apache.cassandra.tools.nodetool.nodesync;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "status", description = "Return the current status of the NodeSync service (whether it is running on the connected node or not)")
public class Status extends NodeTool.NodeToolCmd
{
    @Option(name = { "-b", "--boolean-output"}, description = "Simply output 'true' if the service is running and 'false' otherwise; "
                                                              + "makes the output less human readable but more easily consumed by scripts")
    private boolean booleanOutput = false;

    @Override
    public void execute(NodeProbe probe)
    {
        try
        {
            boolean isRunning = probe.nodeSyncStatus();
            if (booleanOutput)
                System.out.println(isRunning);
            else
                System.out.println(isRunning
                                   ? "The NodeSync service is running"
                                   : "The NodeSync service is not running");
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unexpected error while checking NodeSync status", e);
        }
    }
}
