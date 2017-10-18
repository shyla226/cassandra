/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.tools.nodetool.nodesync;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "enable", description = "Enable the NodeSync service")
public class Enable extends NodeTool.NodeToolCmd
{
    @Option(name = { "-t", "--timeout"}, description = "Timeout (in seconds) for waiting on the service to finish startup (default: 2 minutes)")
    private long timeoutSec = TimeUnit.MINUTES.toSeconds(2);

    @Override
    public void execute(NodeProbe probe)
    {
        try
        {
            if (!probe.enableNodeSync(timeoutSec, TimeUnit.SECONDS))
                System.out.println("The NodeSync service is already running");
        }
        catch (TimeoutException e)
        {
            System.err.println("Error: timed-out waiting for the NodeSync service to start (timeout was " + timeoutSec + " seconds).");
            System.exit(1);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unexpected error enabling the NodeSync service", e);
        }
    }
}
