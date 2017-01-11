/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.tools.nodetool.nodesync;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "getrate", description = "Get synchronization rate")
public class GetRate extends NodeTool.NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        System.out.println(String.format("Configured rate=%d rows/second", probe.getNodeSyncRate()));
    }
}

