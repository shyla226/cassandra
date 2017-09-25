/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.tools.nodesync;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.datastax.driver.core.Session;
import org.apache.cassandra.tools.NodeProbe;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

/**
 * Class for performing JMX client operations in multiple nodes of a Cassandra cluster.
 */
class NodeProbes
{
    private static final int DEFAULT_JMX_PORT = 7199;

    private final Session session;
    private final Function<InetSocketAddress, NodeProbe> nodeSupplier;

    NodeProbes(Session session, Function<InetSocketAddress, NodeProbe> nodeSupplier)
    {
        this.session = session;
        this.nodeSupplier = nodeSupplier;
    }

    /**
     * Runs the specified JMX node connection consumer for each node identified by the specifed RPC addresses.
     * <p>
     * The JMX connection will be open at the invocation of the consumed {@link NodeProbe} supplier
     * {@link Supplier#get()} method.
     * <p>
     * Consumers should either use the {@link #close(NodeProbe, InetAddress)} method to close the supplied
     * {@link NodeProbe}s, or close it by themselves.
     *
     * @param rpcAddresses a set of RPC addresses
     * @param stopCondition a nodes iteration stop condition, evaluated at the begining of each iteration
     * @param nodeProbeConsumer a cosumer of 2-tuples composed by one of the RPC address of {@code rpcAddresses} and a
     * supplier of a JMX connection for the node at that address
     */
    @SuppressWarnings("resource")
    void run(Set<InetAddress> rpcAddresses,
             Supplier<Boolean> stopCondition,
             BiConsumer<InetAddress, Supplier<NodeProbe>> nodeProbeConsumer)
    {
        Map<InetAddress, InetSocketAddress> jmxPerRPC = jmxPerRPC();

        for (InetAddress address : rpcAddresses)
        {
            if (stopCondition.get())
                return;

            InetSocketAddress jmxAddress = jmxPerRPC.computeIfAbsent(address, k ->
            {
                InetSocketAddress defaultAddress = new InetSocketAddress(address, DEFAULT_JMX_PORT);
                System.out.println("WARN: JMX address and port not found in system tables for node %s, " +
                                   "failing back to " + defaultAddress);
                return defaultAddress;
            });

            nodeProbeConsumer.accept(address, () -> nodeSupplier.apply(jmxAddress));
        }
    }

    /**
     * Closes the specified JMX node connection catching any error.
     *
     * @param probe a JMX connection of a node
     * @param rpcAddress the RPC address of the node
     */
    static void close(NodeProbe probe, InetAddress rpcAddress)
    {
        if (probe != null)
        {
            try
            {
                probe.close();
            }
            catch (Exception e)
            {
                System.err.printf("Unable to close JMX connection to %s: %s%n", rpcAddress, e.getMessage());
            }
        }
    }

    /**
     * Returns a map associating the RPC addresses of each node of the cluster to its JMX socket address.
     */
    private Map<InetAddress, InetSocketAddress> jmxPerRPC()
    {
        // JAVA-1620 will allows to get this info directly from the cluster meatadata
        Map<InetAddress, InetSocketAddress> result = new HashMap<>();

        session.execute(select("rpc_address", "jmx_port", "listen_address").from("system", "local"))
               .forEach(r -> result.put(r.getInet("rpc_address"),
                                        new InetSocketAddress(r.getInet("listen_address"), r.getInt("jmx_port"))));

        session.execute(select("rpc_address", "jmx_port", "peer").from("system", "peers"))
               .forEach(r -> result.put(r.getInet("rpc_address"),
                                        new InetSocketAddress(r.getInet("peer"), r.getInt("jmx_port"))));

        return result;
    }
}
