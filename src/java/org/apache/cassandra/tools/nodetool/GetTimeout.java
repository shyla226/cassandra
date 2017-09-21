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

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "gettimeout", description = "Print the timeout of the given type in ms")
public class GetTimeout extends NodeToolCmd
{
    public static final String TIMEOUT_TYPES = "read, range, write, counterwrite, cascontention, truncate, misc (general rpc_timeout_in_ms)";

    @Arguments(usage = "<timeout_type>", description = "The timeout type, one or more of (" + TIMEOUT_TYPES + ")")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        if (args.isEmpty())
            args.addAll(Arrays.asList("read", "range", "write", "counterwrite", "cascontention", "truncate", "misc"));

        try
        {
            for (String type : args)
            {
                System.out.println("Current timeout for type " + type + ": " + probe.getTimeout(type) + " ms");
            }
        } catch (Exception e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }

    }
}
