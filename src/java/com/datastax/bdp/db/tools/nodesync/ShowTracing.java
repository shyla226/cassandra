/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.tools.nodesync;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.utils.units.TimeValue;

/**
 * {@link NodeSyncCommand} to start tracing (on one or more nodes).
 */
@Command(name = "show", description = "Display the events of a NodeSync tracing session")
public class ShowTracing extends NodeSyncTracingCommand
{
    @Option(name = { "-i", "--id"}, description = "The trace ID to show. If omitted, this will check if some nodes have tracing " +
                                                  "enable, and if all node that have it use the same trace ID, it will default " +
                                                  "to showing that. Otherwise, the command error out.")
    public String traceIdStr;

    @Option(name = { "-f", "--follow" }, description = "Continuously show the trace events, showing new events as they come. " +
                                                       "Note that this won't exit unless you either manually exit (with Ctrl-c) " +
                                                       "or use a timeout (--timeout option)")
    private boolean follow;

    @Option(name = { "-t", "--timeout" }, description = "When --follow is used, automatically exit after the provided amount" +
                                                        "of time elapses. This default to seconds, but a 's', 'm' or 'h' " +
                                                        "suffix can be used for seconds, minutes or hours respectively.")
    private String timeoutStr;

    @Option(name = { "-c", "--color" }, description = "Colorize each trace event according from which host it originates from. " +
                                                      "Has no effect if the trace only come from a single host")
    private boolean useColors;

    static long parseTimeoutSec(String timeoutStr)
    {
        if (timeoutStr == null)
            return -1;

        String timeout = timeoutStr.trim();
        TimeUnit unit = TimeUnit.SECONDS;
        int last = timeout.length() - 1;
        switch (timeout.toLowerCase().charAt(last))
        {
            case 's':
                timeout = timeout.substring(0, last);
                break;
            case 'm':
                unit = TimeUnit.MINUTES;
                timeout = timeout.substring(0, last);
                break;
            case 'h':
                unit = TimeUnit.HOURS;
                timeout = timeout.substring(0, last);
                break;
        }
        try
        {
            return unit.toSeconds(Long.parseLong(timeout.trim()));
        }
        catch (NumberFormatException e)
        {
            throw new NodeSyncException("Invalid timeout value: " + timeout);
        }
    }

    private UUID getTraceId(Set<InetAddress> nodes, NodeProbes probes)
    {
        if (traceIdStr != null)
        {
            traceIdStr = traceIdStr.trim();
            try
            {
                return UUID.fromString(traceIdStr);
            }
            catch (Exception e)
            {
                throw new NodeSyncException(String.format("Invalid id provided for the trace session (got '%s' but must be a UUID)",
                                                          traceIdStr));
            }
        }

        TracingStatus.Status status = TracingStatus.checkStatus(nodes, probes, this::printVerbose, this::printWarning);
        UUID id = status.traceIdIfCommon();
        if (id == null)
        {
            String reason = status.enabled.isEmpty()
                            ? "no node have tracing enabled."
                            : "not all node with tracing enabled use the same tracing ID; use --id to disambiguate.";
            throw new NodeSyncException("Cannot auto-detect the trace ID: " + reason);
        }
        return id;
    }

    protected void execute(Metadata metadata, Session session, NodeProbes probes)
    {
        Set<InetAddress> nodes = nodes(metadata);
        UUID traceId = getTraceId(nodes, probes);
        new TraceDisplayer(session,
                           traceId,
                           follow,
                           parseTimeoutSec(timeoutStr),
                           useColors && nodes.size() > 1,
                           500,
                           nodes.size() > 1,
                           () -> {}).run();
    }

    static class TraceDisplayer implements Runnable
    {
        private final Session session;
        private final UUID tracingId;
        private final boolean continuous;
        private final long timeoutSec;
        private final boolean useColors;
        private final long delayBetweenQueryMs;
        private final boolean showHost;
        private final Runnable runOnExit;

        private final BlockingQueue<TraceEvent> events = new LinkedBlockingQueue<>();

        TraceDisplayer(Session session,
                       UUID tracingId,
                       boolean continuous,
                       long timeoutSec,
                       boolean useColors,
                       long delayBetweenQueryMs,
                       boolean showHost,
                       Runnable runOnExit)
        {
            this.session = session;
            this.tracingId = tracingId;
            this.continuous = continuous;
            this.timeoutSec = timeoutSec;
            this.useColors = useColors;
            this.showHost = showHost;
            this.delayBetweenQueryMs = delayBetweenQueryMs;
            this.runOnExit = runOnExit;
        }

        public void run()
        {
            TraceFetcher fetcher = new TraceFetcher();
            fetcher.start();

            ScheduledExecutorService service = null;
            if (timeoutSec > 0)
            {
                service = Executors.newSingleThreadScheduledExecutor();
                service.schedule(fetcher::stopBlocking, timeoutSec, TimeUnit.SECONDS);
            }

            // Catch Ctrl-c
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                fetcher.stopBlocking();
                runOnExit.run();
            }));

            displayEvents(events);

            if (service != null)
                service.shutdown();

            runOnExit.run();
        }

        private void displayEvents(BlockingQueue<TraceEvent> events)
        {
            Colorizer colorizer = useColors ? new Colorizer() : Colorizer.NOOP;
            TraceEvent event;
            while ((event = Uninterruptibles.takeUninterruptibly(events)) != TraceEvent.SENTINEL)
            {
                String msg = showHost
                             ? String.format("%s: %s (elapsed: %s)", event.source, event.description, event.sourceElapsed)
                             : String.format("%s (elapsed: %s)", event.description, event.sourceElapsed);
                System.out.println(colorizer.color(event.source, msg));
            }
        }

        private enum Color
        {
            GREEN("\u001B[32m"),
            BLUE("\u001B[34m"),
            YELLOW("\u001B[33m"),
            PURPLE("\u001B[35m"),
            CYAN("\u001B[36m"),
            RED("\u001B[31m");

            private static final String RESET = "\u001B[0m";
            private final String code;

            Color(String code)
            {
                this.code = code;
            }

            String set(String str)
            {
                return code + str + RESET;
            }
        }

        private static class Colorizer
        {
            private static final Colorizer NOOP = new Colorizer()
            {
                @Override
                String color(InetAddress host, String str)
                {
                    return str;
                }
            };

            private final Map<InetAddress, Color> colors = new HashMap<>();
            private int i;

            String color(InetAddress host, String str)
            {
                Color c = colors.get(host);
                if (c == null)
                {
                    c = Color.values()[(i++) % Color.values().length];
                    colors.put(host, c);
                }
                return c.set(str);
            }
        }

        // The driver has that also, but it's not usable. Could make sense to have the driver provide access somehow
        // (even providing the whole TraceFetch functionally).
        private static class TraceEvent
        {
            private static final TraceEvent SENTINEL = new TraceEvent(null, null, null);

            private final String description;
            private final InetAddress source;
            private final TimeValue sourceElapsed;

            private TraceEvent(String description, InetAddress source, TimeValue sourceElapsed)
            {
                this.description = description;
                this.source = source;
                this.sourceElapsed = sourceElapsed;
            }
        }

        private class TraceFetcher extends Thread
        {
            private final PreparedStatement query;

            private volatile boolean stopped;

            private long lastQueryTimestamp = -1L;
            private UUID lastEventId = UUIDs.startOf(0);

            private TraceFetcher()
            {
                this.query = session.prepare("SELECT * FROM system_traces.events WHERE session_id=" + tracingId + " AND event_id > ?");
            }

            private void stopBlocking()
            {
                stopped = true;
                Uninterruptibles.joinUninterruptibly(this);
            }

            public void run()
            {
                while (!stopped)
                {
                    if (lastQueryTimestamp > 0)
                    {
                        if (!continuous)
                        {
                            stopped = true;
                            break;
                        }

                        long timeSinceLast = System.currentTimeMillis() - lastQueryTimestamp;
                        long toSleep = delayBetweenQueryMs - timeSinceLast;
                        Uninterruptibles.sleepUninterruptibly(toSleep, TimeUnit.MILLISECONDS);
                    }

                    lastQueryTimestamp = System.currentTimeMillis();
                    try
                    {
                        ResultSet result = session.execute(query.bind(lastEventId));
                        for (Row row : result)
                        {
                            lastEventId = row.getUUID("event_id");
                            String description = row.getString("activity");
                            InetAddress source = row.getInet("source");
                            TimeValue sourceElapsed = TimeValue.of(row.getInt("source_elapsed"), TimeUnit.MICROSECONDS);
                            events.offer(new TraceEvent(description, source, sourceElapsed));
                        }
                    }
                    catch (Exception e)
                    {
                        System.err.println(String.format("Error reading trace events (%s); will retry", e.getMessage()));
                    }
                }
                events.offer(TraceEvent.SENTINEL);
            }
        }
    }
}
