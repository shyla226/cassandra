/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.tools.nodesync;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import io.airlift.airline.Option;

public abstract class NodeSyncTracingCommand extends NodeSyncCommand
{
    @Option(name = { "-n", "--nodes" }, description = "Comma separated of nodes address on which to act; if omitted, " +
                                                      "all (live) nodes will be included")
    private String nodeList;

    protected Set<InetAddress> nodes(Metadata metadata)
    {
        if (nodeList == null)
        {
            Set<InetAddress> alive = new HashSet<>();
            Set<InetAddress> dead = new HashSet<>();
            for (Host h : metadata.getAllHosts())
            {
                if (h.isUp())
                    alive.add(h.getBroadcastAddress());
                else
                    dead.add(h.getBroadcastAddress());
            }
            if (!dead.isEmpty())
                printWarning("Cannot enable tracing on the following dead nodes: %s", dead);

            return alive;
        }

        Set<InetAddress> provided = parseInetAddressList(nodeList);
        // If you explicitly asked for a node but it's dead, consider that an error. Easy enough to retry without the
        // node listed if that is what you want.
        for (InetAddress node : provided)
        {
            Host h = findHost(node, metadata);
            if (h == null)
                throw new NodeSyncException(node + " does not seem to be a known node in the cluster");
            if (!h.isUp())
                throw new NodeSyncException("Cannot enable tracing on " + node + " as it is currently down");
        }
        return provided;
    }

    protected boolean isOnAllNodes()
    {
        return nodeList == null;
    }

    // Kind of a shame the driver don't provide this directly but well...
    private static Host findHost(InetAddress address, Metadata metadata)
    {
        for (Host h : metadata.getAllHosts())
        {
            if (h.getBroadcastAddress().equals(address))
                return h;
        }
        return null;
    }

}
