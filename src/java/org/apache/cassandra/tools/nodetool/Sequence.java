/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.tools.nodetool;

import java.io.*;
import java.net.*;
import java.text.DateFormat;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;

import io.airlift.airline.*;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.utils.Pair;

@Command(name = "sequence",
         description = "Run multiple nodetool commands from a file, resource or stdin in sequence. Common options (host, port, username, password) are passed to child commands.")
public class Sequence extends NodeTool.NodeToolCmd
{
    @Option(name = "--stoponerror",
            description = "By default, if one child command fails, the sequence command continues with remaining commands. Set this option to true to stop on error.")
    private boolean stopOnError = false;

    @Option(name = "--failonerror",
            description = "By default, the sequence command will not fail (return an error exit code) if one or more child commands fail. Set this option to true to return an error exit code if a child command fails.")
    private boolean failOnError = false;

    @Option(name = { "-i", "--input" },
            description = "The file or classpath resource to read nodetool commands from, one command per line. Use /dev/stdin to read from standard input. Multiple input can be provided.")
    private List<String> input;

    @Arguments(
    description = "Commands to execute. Separate individual commands using a colon surrounded by whitespaces (' : ')." +
                  "Example: 'nodetool sequence 'info : gettimeout read : gettimeout write : status'.")
    private List<String> commands;

    public Sequence()
    {
    }

    protected void execute(NodeProbe probe)
    {
        List<Pair<Runnable, String>> runnables = new ArrayList<>();

        if (input != null)
            for (String in : input)
            {
                readInput(runnables, in);
            }

        if (commands != null)
        {
            collectCommandLine(runnables);
        }

        long tTotal0 = System.currentTimeMillis();

        System.out.println("################################################################################");
        System.out.printf("# Executing %d commands:%n", runnables.size());
        for (Pair<Runnable, String> cmd : runnables)
            System.out.printf("# %s%n", cmd.right);
        System.out.println("################################################################################");

        try
        {
            for (Enumeration<NetworkInterface> niEnum = NetworkInterface.getNetworkInterfaces(); niEnum.hasMoreElements(); )
            {
                NetworkInterface ni = niEnum.nextElement();
                System.out.printf("# Network interface %s (%s): %s%n",
                                  ni.getName(),
                                  ni.getDisplayName(),
                                  Joiner.on(", ").join(ni.getInterfaceAddresses()));
            }
            System.out.println("################################################################################");
        }
        catch (SocketException e)
        {
            //
        }

        boolean failed = false;
        int success = 0;
        List<Pair<String, String>> failures = new ArrayList<>();
        for (Pair<Runnable, String> cmd : runnables)
        {
            if (executeRunnable(probe, cmd, failures))
                success++;
            else
                failed = true;

            System.out.println("################################################################################");

            if (failed && stopOnError)
                break;
        }

        System.out.printf("# Total duration: %dms%n", System.currentTimeMillis() - tTotal0);
        System.out.printf("# Out of %d commands, %d completed successfully, %d failed.%n", runnables.size(), success, failures.size());
        for (Pair<String, String> failure : failures)
        {
            System.out.printf("# Failed: '%s': %s%n", failure.left, failure.right);
        }
        System.out.println("################################################################################");

        if (failed && failOnError)
            probe.failed();
    }

    private void collectCommandLine(List<Pair<Runnable, String>> runnables)
    {
        StringBuilder sb = new StringBuilder();
        List<String> args = new ArrayList<>();
        for (String arg : commands)
        {
            arg = arg.trim();

            if (arg.equals(":"))
            {
                collectCommand(runnables, sb.toString(), args.toArray(new String[0]));
                sb.setLength(0);
                args.clear();

                continue;
            }

            if (sb.length() > 0)
                sb.append(' ');
            sb.append(arg);
            args.add(arg);
        }

        if (!args.isEmpty())
        {
            collectCommand(runnables, sb.toString(), args.toArray(new String[0]));
            sb.setLength(0);
            args.clear();
        }
    }

    private boolean executeRunnable(NodeProbe probe, Pair<Runnable, String> cmd, List<Pair<String, String>> failures)
    {
        DateFormat dfZulu = DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG);
        dfZulu.setTimeZone(TimeZone.getTimeZone(ZoneId.of("Z")));
        DateFormat dfLocal = DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG);

        long t = System.currentTimeMillis();
        Date d = new Date(t);
        System.out.println("# Command: " + cmd.right);
        System.out.println("# Timestamp: " + dfZulu.format(d));
        System.out.println("# Timestamp (local): " + dfLocal.format(d));
        System.out.println("# Timestamp (millis since epoch): " + t);
        System.out.println("################################################################################");

        if (cmd.left instanceof NodeTool.NodeToolCmd)
        {
            NodeTool.NodeToolCmd nodeToolCmd = (NodeTool.NodeToolCmd) cmd.left;
            nodeToolCmd.applyGeneralArugments(this);

            long t0 = System.nanoTime();
            try
            {
                nodeToolCmd.sequenceRun(probe);
                if (!probe.isFailed())
                {
                    System.out.printf("# Command '%s' completed successfully in %d ms%n", cmd.right, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0));
                    return true;
                }
                else
                {
                    probe.clearFailed();
                    System.out.printf("# Command '%s' failed%n", cmd.right);
                    failures.add(Pair.create(cmd.right, "(see above)"));
                    return false;
                }
            }
            catch (RuntimeException e)
            {
                probe.clearFailed();
                System.out.printf("# Command '%s' failed with exception %s%n", cmd.right, e);
                failures.add(Pair.create(cmd.right, e.toString()));
                return false;
            }
        }
        else
        {
            long t0 = System.nanoTime();
            try
            {
                cmd.left.run();
                System.out.printf("# Command '%s' completed in %d ms%n", cmd.right, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0));
                return true;
            }
            catch (RuntimeException e)
            {
                StringWriter sw = new StringWriter();
                try (PrintWriter pw = new PrintWriter(sw))
                {
                    e.getCause().printStackTrace(pw);
                }
                String err = sw.toString();
                System.out.printf("# Command '%s' failed with exception %s%n", cmd.right, e.getCause());
                failures.add(Pair.create(cmd.right, err));
                return false;
            }
        }
    }

    @SuppressWarnings("resource")
    private void readInput(List<Pair<Runnable, String>> runnables, String in)
    {
        URL inputUrl = NodeTool.class.getClassLoader().getResource(in);
        if (inputUrl == null)
        {
            File file = new File(in);
            if (file.isFile())
                try
                {
                    inputUrl = file.toURI().toURL();
                }
                catch (MalformedURLException e)
                {
                    // ignore
                }
        }

        InputStream inputStream = null;

        if (inputUrl != null)
        {
            try
            {
                inputStream = inputUrl.openConnection().getInputStream();
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
        if (inputStream == null && (in.equals("/dev/stdin") || in.equals("-")))
        {
            inputStream = System.in;
        }

        if (inputStream == null)
        {
            throw new IOError(new FileNotFoundException("File/resource " + in + " not found"));
        }

        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream)))
        {
            String line;
            while ((line = br.readLine()) != null)
            {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#"))
                    // comment or empty line
                    continue;

                collectCommand(runnables, line, line.split("[ \t]"));
            }
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    private void collectCommand(List<Pair<Runnable, String>> runnables, String line, String[] args)
    {
        Cli<Runnable> cli = NodeTool.createCli(false);
        Runnable runnable;
        try
        {
            runnable = cli.parse(args);
        }
        catch (Throwable t)
        {
            runnable = () ->
            {
                throw new RuntimeException(t);
            };
        }
        runnables.add(Pair.create(runnable, line));
    }
}
