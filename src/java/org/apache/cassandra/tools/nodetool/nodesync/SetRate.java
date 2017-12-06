/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.tools.nodetool.nodesync;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "setrate", description = "Set NodeSync validation rate limit")
public class SetRate extends NodeTool.NodeToolCmd
{
    @Arguments(title = "sync_rate", usage = "<value_in_kb_per_sec>", description = "Value in KB per second", required = true)
    private Integer syncRate = null;

    @Override
    public void execute(NodeProbe probe)
    {
        probe.setNodeSyncRate(syncRate);
    }
}

