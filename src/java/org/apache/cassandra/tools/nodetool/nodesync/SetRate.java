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

@Command(name = "setrate", description = "Set NodeSync validation rate")
public class SetRate extends NodeTool.NodeToolCmd
{
    @Arguments(title = "sync_rate", usage = "<rows_per_second>", description = "Value in rows/second,", required = true)
    private Integer syncRate = null;

    @Override
    public void execute(NodeProbe probe)
    {
        probe.setNodeSyncRate(syncRate);
    }
}

