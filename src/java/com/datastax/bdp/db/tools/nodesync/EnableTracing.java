/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.tools.nodesync;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import com.datastax.bdp.db.nodesync.TracingLevel;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.UUIDGen;

/**
 * {@link NodeSyncCommand} to start tracing (on one or more nodes).
 */
@Command(name = "enable", description = "Enable NodeSync tracing")
public class EnableTracing extends NodeSyncTracingCommand
{

    @Option(name = { "-l", "--level" }, description = "The tracing level: either 'low' or 'high'. If omitted, " +
                                                      "the 'low' level is used. Note that the 'high' level is somewhat " +
                                                      "verbose and should be used with care.")
    private String levelStr = "low";

    @Option(name= { "--tables" }, description = "A comma separated list of fully-qualified table names to trace. If omitted, " +
                                                "all tables are trace.")
    private String tableStr;

    @Option(name = { "-f", "--follow" }, description = "After having enabled tracing, continuously show the trace events," +
                                                       "showing new events as they come. Note that this won't exit unless " +
                                                       "you either manually exit (with Ctrl-c) or use a timeout " +
                                                       "(--timeout option)")
    private boolean follow;

    @Option(name = { "-t", "--timeout" }, description = "Timeout on the tracing; after that amount of time, tracing will " +
                                                        "be automatically disabled (and if --follow is used, the command " +
                                                        "will return). This default in seconds, but a 's', 'm' " +
                                                        "or 'h' suffix can be used for seconds, minutes or hours respectively.")
    private String timeoutStr;

    @Option(name = { "-c", "--color" }, description = "If --follow is used, color each trace event according from which host it originates from")
    private boolean useColors;

    private Map<String, String> makeTracingOptions(UUID id, long timeoutSec)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.put("id", id.toString());
        builder.put("level", TracingLevel.parse(levelStr).toString());
        if (timeoutStr != null)
            builder.put("timeout_sec", Long.toString(timeoutSec));
        if (tableStr != null)
            builder.put("tables", tableStr);
        return builder.build();
    }

    @Override
    @SuppressWarnings("resource")
    protected void execute(Metadata metadata, Session session, NodeProbes probes)
    {
        UUID id = UUIDGen.getTimeUUID();
        long timeoutSec = ShowTracing.parseTimeoutSec(timeoutStr);
        Map<String, String> tracingOptions = makeTracingOptions(id, timeoutSec);
        Set<InetAddress> nodes = nodes(metadata);
        Set<InetAddress> enabled = new HashSet<>();
        try
        {
            probes.run(nodes, () -> false, (address, probeSupplier) ->
            {
                NodeProbe probe = null;
                try
                {
                    probe = probeSupplier.get();
                    probe.enableNodeSyncTracing(tracingOptions);
                    printVerbose("Enabled tracing on %s with id %s", address, id);
                    enabled.add(address);
                }
                catch (IllegalArgumentException | IllegalStateException e)
                {
                    // Those may happen if either tracing is already enabled on a node, or some of the option don't
                    // validate properly. They are no bug but "normal" errors, so use NodeSyncException (which will
                    // not log a scary stack trace).
                    throw new NodeSyncException(String.format("Unable to enable tracing on %s: %s", address, e.getMessage()));
                }
                finally
                {
                    NodeProbes.close(probe, address);
                }
            });
        }
        catch (RuntimeException e)
        {
            // On exception, disable on every node we had already enabled before throwing back the exception. Of course,
            // that can error out in turn so we make sure to log but otherwise swallow exception.
            if (!enabled.isEmpty())
            {
                printVerbose("Failed enabling tracing on all nodes, disabling it on just enabled ones");
                DisableTracing.disableTracing(enabled, probes, this::printVerbose, this::printWarning);
            }
            throw e;
        }

        if (follow)
        {
            printVerbose("Displaying events for session id %s...", id);
            new ShowTracing.TraceDisplayer(session,
                                           id,
                                           true,
                                           timeoutSec,
                                           useColors && nodes.size() > 1,
                                           500,
                                           nodes.size() > 1,
                                           () -> {
                                               if (timeoutSec <= 0)
                                                   printWarning("Do not forget to stop tracing with 'nodesync tracing disable'.");
                                           }).run();

        }
        else
        {
            if (timeoutSec <= 0)
                printWarning("Do not forget to stop tracing with 'nodesync tracing disable'.");
            System.out.println("Enabled tracing. Session id is " + id);
        }
    }
}
