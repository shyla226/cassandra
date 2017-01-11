/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.tools.nodetool.nodesync;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "enable", description = "Enable the NodeSync service")
public class Enable extends NodeTool.NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        try
        {
            if (!probe.enableNodeSync())
                System.out.println("NodeSync is already running");
        }
        catch (Exception e)
        {
            throw new RuntimeException("Got error enabling NodeSync", e);
        }
    }
}
