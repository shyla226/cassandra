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

@Command(name = "disable", description = "Disable the NodeSync service")
public class Disable extends NodeTool.NodeToolCmd
{
    @Option(name = {"-f", "--force"}, description = "Triggers a forced shutdown that don't let already started segment validations finished")
    private boolean force = false;

    @Option(name = {"-t", "--timeout"}, description = "Timeout (in seconds) for waiting on the service to finish shutdown (default: 2 minutes)")
    private long timeoutSec = TimeUnit.MINUTES.toSeconds(2);

    @Override
    public void execute(NodeProbe probe)
    {
        try
        {
            if (!probe.disableNodeSync(force, timeoutSec, TimeUnit.SECONDS))
                System.out.println("NodeSync is not running");
        }
        catch (TimeoutException e)
        {
            System.err.println("Error: timed-out waiting for nodeSync to stop (timeout was " + timeoutSec + " seconds).");
            System.exit(1);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Got error disabling NodeSync", e);
        }
    }
}
