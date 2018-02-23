/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.tools.nodesync;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.datastax.bdp.db.nodesync.NodeSyncService;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.Streams;

import static com.datastax.bdp.db.nodesync.UserValidationProposer.Status;
import static java.util.stream.Collectors.toSet;
import static org.apache.cassandra.repair.SystemDistributedKeyspace.NODESYNC_USER_VALIDATIONS;
import static org.apache.cassandra.schema.SchemaConstants.DISTRIBUTED_KEYSPACE_NAME;

/**
 * {@link NodeSyncCommand} to cancel a user-triggered validation.
 */
@Command(name = "cancel", description = "Cancel a user-triggered validation")
public class CancelValidation extends NodeSyncCommand
{
    @Arguments(usage = "<id>", description = "The validation ID")
    private String id = null;

    @Override
    @SuppressWarnings("resource")
    public final void execute(Metadata metadata, Session session, NodeProbes probes)
    {
        if (StringUtils.isBlank(id))
            throw new NodeSyncException("Validation ID is required");

        // Get the RPC addresss of all the live nodes
        Set<InetAddress> allLiveNodes = metadata.getAllHosts()
                                                .stream()
                                                .filter(Host::isUp)
                                                .map(Host::getBroadcastAddress)
                                                .collect(toSet());

        // Get the nodes where the validation is running
        Statement select = QueryBuilder.select("node", "status")
                                       .from(DISTRIBUTED_KEYSPACE_NAME, NODESYNC_USER_VALIDATIONS)
                                       .where(QueryBuilder.eq("id", id));
        Set<InetAddress> nodes = Streams.of(session.execute(select))
                                        .filter(r -> Objects.equals(r.getString("status"), Status.RUNNING.toString()))
                                        .map(r -> r.getInet("node"))
                                        .filter(allLiveNodes::contains)
                                        .collect(Collectors.toSet());

        Set<InetAddress> successfulNodes = new HashSet<>(nodes.size());
        Set<InetAddress> failedNodes = new HashSet<>();
        probes.run(nodes, () -> false, (address, probeSupplier) ->
        {
            NodeProbe probe = null;
            try
            {
                probe = probeSupplier.get();
                probe.cancelUserValidation(id);
                successfulNodes.add(address);
                printVerbose("%s: Cancelled", address);
            }
            catch (NodeSyncService.NotFoundValidationException e)
            {
                successfulNodes.add(address);
                printVerbose("%s: Not found", address);
            }
            catch (NodeSyncService.CancelledValidationException e)
            {
                successfulNodes.add(address);
                printVerbose("%s: Already cancelled", address);
            }
            catch (Exception e)
            {
                failedNodes.add(address);
                System.err.printf("%s: Error while cancelling: %s%n", address, e.getMessage());
            }
            finally
            {
                NodeProbes.close(probe, address);
            }
        });

        if (!failedNodes.isEmpty())
            throw new NodeSyncException("The cancellation has failed in nodes: " + failedNodes);

        if (successfulNodes.isEmpty())
            throw new NodeSyncException("The validation to be cancelled hasn't been found in any node");

        printVerbose("The validation has been cancelled in nodes %s", successfulNodes);
    }
}