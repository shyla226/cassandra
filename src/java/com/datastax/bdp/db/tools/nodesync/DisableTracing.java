/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.tools.nodesync;

import java.net.InetAddress;
import java.util.Set;
import java.util.function.Consumer;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;

/**
 * {@link NodeSyncCommand} to stop tracing (on one or more nodes).
 */
@Command(name = "disable", description = "Disable NodeSync tracing")
public class DisableTracing extends NodeSyncTracingCommand
{
    @Override
    protected void execute(Metadata metadata, Session session, NodeProbes probes)
    {
        disableTracing(nodes(metadata), probes, this::printVerbose, this::printWarning);
    }

    @SuppressWarnings("resource")
    static void disableTracing(Set<InetAddress> nodes,
                               NodeProbes probes,
                               Consumer<String> verboseLogger,
                               Consumer<String> warningLogger)
    {
        probes.run(nodes, () -> false, (address, probeSupplier) ->
        {
            NodeProbe probe = null;
            try
            {
                probe = probeSupplier.get();
                probe.disableNodeSyncTracing();
                verboseLogger.accept(String.format("Disabled tracing on %s", address));
            }
            catch (Exception e)
            {
                warningLogger.accept(String.format("failed to disable tracing on %s: %s", address, e.getMessage()));
            }
            finally
            {
                NodeProbes.close(probe, address);
            }
        });
    }
}
