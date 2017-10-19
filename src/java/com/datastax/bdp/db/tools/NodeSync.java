/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.tools;

import com.google.common.base.Throwables;
import com.datastax.bdp.db.nodesync.NodeSyncService;
import com.datastax.bdp.db.tools.nodesync.CancelValidation;
import com.datastax.bdp.db.tools.nodesync.ListValidations;
import com.datastax.bdp.db.tools.nodesync.NodeSyncException;
import com.datastax.bdp.db.tools.nodesync.SubmitValidation;
import com.datastax.bdp.db.tools.nodesync.Toggle;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.airline.ParseException;

/**
 * Command line tool to manage NodeSync at the cluster level.
 */
public class NodeSync
{
    private static final String TOOL_NAME = "nodesync";

    public static void main(String... args)
    {
        Runnable runnable = parse(args);
        int status = 0;
        try
        {
            runnable.run();
        }
        catch (NodeSyncException |
               NoHostAvailableException |
               OperationTimedOutException |
               ReadTimeoutException |
               AuthenticationException |
               NodeSyncService.NodeSyncNotRunningException e)
        {
            printExpectedError(e);
            status = 1;
        }
        catch (Throwable t)
        {
            printUnexpectedError(t);
            status = 2;
        }

        System.exit(status);
    }

    private static Runnable parse(String... args)
    {
        Cli<Runnable> cli = createCli();
        Runnable runnable = null;

        try
        {
            runnable = cli.parse(args);
        }
        catch (ParseException e)
        {
            printBadUse(e);
            System.exit(1);
        }
        catch (Throwable t)
        {
            printUnexpectedError(Throwables.getRootCause(t));
            System.exit(2);
        }

        return runnable;
    }

    private static Cli<Runnable> createCli()
    {
        Cli.CliBuilder<Runnable> builder = Cli.builder(TOOL_NAME);

        builder.withDescription("Manage NodeSync service at cluster level")
               .withDefaultCommand(Help.class)
               .withCommand(Help.class)
               .withCommand(Toggle.Enable.class)
               .withCommand(Toggle.Disable.class);

        builder.withGroup("validation")
               .withDescription("Monitor/manage user-triggered validations")
               .withDefaultCommand(Help.class)
               .withCommand(SubmitValidation.class)
               .withCommand(CancelValidation.class)
               .withCommand(ListValidations.class);

        return builder.build();
    }

    private static void printBadUse(ParseException e)
    {
        System.err.printf("%s: %s%n", TOOL_NAME, e.getMessage());
        System.err.printf("See '%s help' or '%s help <command>'.%n", TOOL_NAME, TOOL_NAME);
    }

    private static void printExpectedError(Throwable e)
    {
        System.err.println("error: " + e.getMessage());
    }

    private static void printUnexpectedError(Throwable e)
    {
        System.err.printf("Unexpected error: %s (this indicates a bug, please report to DataStax support " +
                          "along with the following stack trace)%n", e.getMessage());
        System.err.println("-- StackTrace --");
        e.printStackTrace();
    }
}
