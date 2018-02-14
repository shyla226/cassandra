/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.tools.nodesync;

import java.net.InetAddress;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import org.apache.cassandra.tools.NodeProbe;

/**
 * Class for performing JMX client operations in multiple nodes of a Cassandra cluster.
 */
class NodeProbes
{
    private final Metadata metadata;
    private final Function<Host, NodeProbe> nodeSupplier;

    NodeProbes(Metadata metadata, Function<Host, NodeProbe> nodeSupplier)
    {
        this.metadata = metadata;
        this.nodeSupplier = nodeSupplier;
    }

    /**
     * Runs the specified JMX node connection consumer for each node identified by the specifed broadcast addresses.
     * <p>
     * The JMX connection will be open at the invocation of the consumed {@link NodeProbe} supplier
     * {@link Supplier#get()} method.
     * <p>
     * Consumers should either use the {@link #close(NodeProbe, InetAddress)} method to close the supplied
     * {@link NodeProbe}s, or close it by themselves.
     *
     * @param broadcastAddresses a set of broadcast addresses identifying the target nodes
     * @param stopCondition a nodes iteration stop condition, evaluated at the begining of each iteration
     * @param nodeProbeConsumer a cosumer of 2-tuples composed by node contact address and a JMX connection supplier
     */
    @SuppressWarnings("resource")
    void run(Set<InetAddress> broadcastAddresses,
             Supplier<Boolean> stopCondition,
             BiConsumer<InetAddress, Supplier<NodeProbe>> nodeProbeConsumer)
    {
        for (InetAddress broadcastAddress : broadcastAddresses)
        {
            if (stopCondition.get())
                return;

            Host host = metadata.getAllHosts()
                                .stream()
                                .filter(h -> h.getBroadcastAddress().equals(broadcastAddress))
                                .findFirst()
                                .orElseThrow(() -> new NodeSyncException("Unable to find node " + broadcastAddress));

            nodeProbeConsumer.accept(broadcastAddress, () -> nodeSupplier.apply(host));
        }
    }

    /**
     * Closes the specified JMX node connection catching any error.
     *
     * @param probe a JMX connection of a node
     * @param broadcastAddresses the broadcast address of the node
     */
    static void close(NodeProbe probe, InetAddress broadcastAddresses)
    {
        if (probe != null)
        {
            try
            {
                probe.close();
            }
            catch (Exception e)
            {
                System.err.printf("Unable to close JMX connection to %s: %s%n", broadcastAddresses, e.getMessage());
            }
        }
    }
}
