/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.tools.nodesync;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;

/**
 * {@link NodeSyncCommand} to stop tracing (on one or more nodes).
 */
@Command(name = "status", description = "Check status of NodeSync tracing")
public class TracingStatus extends NodeSyncTracingCommand
{
    @Override
    @SuppressWarnings("resource")
    protected void execute(Metadata metadata, Session session, NodeProbes probes)
    {
        Set<InetAddress> nodes = nodes(metadata);
        Status status = checkStatus(nodes, probes, this::printVerbose, this::printWarning);

        if (!isOnAllNodes() && nodes.size() == 1)
        {
            System.out.println("Tracing is " + (status.enabled.isEmpty() ? "disabled" : "enabled"));
        }
        else
        {
            if (status.disabled.isEmpty())
            {
                System.out.println(String.format("Tracing is enabled on all%s nodes", isOnAllNodes() ? "" : " requested"));
            }
            else if (status.enabled.isEmpty())
            {
                System.out.println(String.format("Tracing is disabled on all%s nodes", isOnAllNodes() ? "" : " requested"));
            }
            else
            {
                System.out.println("Tracing is only enabled on " + status.enabled.keySet());
            }
        }
    }

    static Status checkStatus(Set<InetAddress> nodes,
                              NodeProbes probes,
                              Consumer<String> verboseLogger,
                              Consumer<String> warningLogger)
    {
        ImmutableMap.Builder<InetAddress, UUID> enabled = ImmutableMap.builder();
        ImmutableSet.Builder<InetAddress> disabled = ImmutableSet.builder();
        probes.run(nodes, () -> false, (address, probeSupplier) ->
        {
            NodeProbe probe = null;
            try
            {
                probe = probeSupplier.get();
                UUID id = probe.currentNodeSyncTracingSession();
                if (id == null)
                {
                    verboseLogger.accept(String.format("Tracing disabled on %s", address));
                    disabled.add(address);
                }
                else
                {
                    verboseLogger.accept(String.format("Tracing enabled on %s with id %s", address, id));
                    enabled.put(address, id);
                }
            }
            catch (Exception e)
            {
                warningLogger.accept(String.format("failed to retrieve tracing status on %s: %s", address, e.getMessage()));
                disabled.add(address);
            }
            finally
            {
                NodeProbes.close(probe, address);
            }
        });
        return new Status(enabled.build(), disabled.build());
    }

    static class Status
    {
        final ImmutableMap<InetAddress, UUID> enabled;
        final ImmutableSet<InetAddress> disabled;

        private Status(ImmutableMap<InetAddress, UUID> enabled, ImmutableSet<InetAddress> disabled)
        {
            this.enabled = enabled;
            this.disabled = disabled;
        }

        /**
         * If all the nodes with tracing enabled have the same trace ID, returns it, otherwise return {@code null}.
         */
        UUID traceIdIfCommon()
        {
            if (enabled.isEmpty())
                return null;

            Iterator<UUID> iter = enabled.values().iterator();
            UUID common = iter.next();
            while (iter.hasNext())
            {
                if (!common.equals(iter.next()))
                    return null;
            }
            return common;
        }
    }
}
