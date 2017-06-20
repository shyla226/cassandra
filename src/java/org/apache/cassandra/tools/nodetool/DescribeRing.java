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
package org.apache.cassandra.tools.nodetool;

import io.airlift.command.Arguments;
import io.airlift.command.Command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "describering", description = "Shows the token ranges info of a given keyspace")
public class DescribeRing extends NodeToolCmd
{
    @Arguments(description = "The keyspace name(s)")
    List<String> keyspace = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        System.out.println("Schema Version:" + probe.getSchemaVersion());
        try
        {
            if (keyspace.isEmpty())
            {
                keyspace.addAll(probe.getNonLocalStrategyKeyspaces());
            }

            for (String ks : keyspace)
            {
                forKeyspace(probe, ks);
            }
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void forKeyspace(NodeProbe probe, String ksName) throws IOException
    {
        System.out.println("Keyspace: " + ksName);
        System.out.println("TokenRange: ");
        for (String tokenRangeString : probe.describeRing(ksName))
        {
            System.out.println("\t" + tokenRangeString);
        }
    }
}
