/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.Units;

/**
 * If tracing is currently enabled on NodeSync, track information pertaining to that session.
 */
class NodeSyncTracing
{
    private static final Logger logger = LoggerFactory.getLogger(NodeSyncTracing.class);

    private final AtomicLong idGenerator = new AtomicLong();

    private volatile TraceState state;
    private volatile TracingLevel level;
    private volatile Future<?> timeouterFuture;
    private volatile Set<TableId> includeTables;

    synchronized UUID enable(TracingOptions options)
    {
        if (state != null)
            throw new IllegalStateException(String.format("Tracing for NodeSync is already enabled (session id: %s)", state.sessionId));

        assert timeouterFuture == null;

        idGenerator.set(0);
        this.level = options.level;
        this.includeTables = options.tables;
        try
        {
            Tracing.instance.newSession(options.id, Tracing.TraceType.NODESYNC);
            Tracing.instance.begin("NodeSync Tracing", Collections.emptyMap());
            this.state = Tracing.instance.get();


            if (options.timeoutSec > 0)
                timeouterFuture = ScheduledExecutors.nonPeriodicTasks.schedule(() -> disable(true), options.timeoutSec, TimeUnit.SECONDS);

            String durationStr = options.timeoutSec <= 0 ? "" : " for " + Units.toString(options.timeoutSec, TimeUnit.SECONDS);
            state.trace("Starting NodeSync tracing on {}{}", FBUtilities.getBroadcastAddress(), durationStr);
            logger.info("Starting NodeSync tracing{}. Tracing session ID is {}", durationStr, options.id);
            return options.id;
        }
        finally
        {
            // This method will be called on a random thread, typically a JMX one, and we shouldn't leave the state in the
            // thread local state (we've saved the state in the 'state' field).
            Tracing.instance.set(null);
        }
    }

    synchronized UUID currentTracingSession()
    {
        return state == null ? null : state.sessionId;
    }

    synchronized boolean isEnabled()
    {
        return state != null;
    }

    void disable()
    {
        disable(false);
    }

    private synchronized void disable(boolean wasAutomatic)
    {
        if (state == null)
            return;

        if (timeouterFuture != null)
            timeouterFuture.cancel(true);
        timeouterFuture = null;

        // Nullify state now to make it less likely that our stop message won't be the last one written.
        TraceState current = state;
        state = null;

        current.trace("Stopped NodeSync tracing on {}", FBUtilities.getBroadcastAddress());

        Tracing.instance.set(current);
        Tracing.instance.stopSession();
        Tracing.instance.set(null); // shouldn't be necessary, but just to be sure.

        logger.info("Stopped NodeSync tracing{}", wasAutomatic ? " (timeout on tracing expired)" : "");
    }

    private TraceState stateFor(Segment segment)
    {
        TraceState current = state;
        if (current == null)
            return null;

        if (includeTables != null && !includeTables.contains(segment.table.id))
            return null;

        return current;
    }

    SegmentTracing startContinuous(Segment segment, String details)
    {
        TraceState current = stateFor(segment);
        if (current == null)
            return SegmentTracing.NO_TRACING;

        long id = idGenerator.getAndIncrement();
        current.trace("[#{}] Starting validation on {} of {} ({})", id, segment.range, segment.table, details);
        return new SegmentTracing(current, id, level);
    }

    void skipContinuous(Segment segment, String reason)
    {
        TraceState current = stateFor(segment);
        if (current == null)
            return;

        current.trace("[#-] Skipping {} of {}, {}", segment.range, segment.table, reason);
    }

    SegmentTracing forUserValidation(Segment segment)
    {
        TraceState current = stateFor(segment);
        if (current == null)
            return SegmentTracing.NO_TRACING;

        long id = idGenerator.getAndIncrement();
        current.trace("Starting user validation on segment {} (id: #{})", segment, id);
        return new SegmentTracing(current, id, level);
    }

    static class SegmentTracing
    {
        @VisibleForTesting
        static final SegmentTracing NO_TRACING = new SegmentTracing();

        private final TraceState state;
        private final String id;
        private final TracingLevel level;
        private final long startTime = NodeSyncHelpers.time().currentTimeMillis();

        private SegmentTracing()
        {
            this.state = null;
            this.id = null;
            this.level = TracingLevel.LOW;
        }

        private SegmentTracing(TraceState state, long id, TracingLevel level)
        {
            assert state != null;
            this.state = state;
            this.id = String.format("[#%d] ", id);
            this.level = level;
        }

        boolean isEnabled()
        {
            return state != null;
        }

        void trace(String message)
        {
            if (level != TracingLevel.LOW && state != null)
                state.trace(id + message);
        }

        void trace(String message, Object arg)
        {
            if (level != TracingLevel.LOW && state != null)
                state.trace(id + message, arg);
        }

        void trace(String message, Object... args)
        {
            if (level != TracingLevel.LOW && state != null)
                state.trace(id + message, args);
        }

        void onSegmentCompletion(ValidationOutcome outcome, ValidationMetrics metrics)
        {
            if (state == null)
                return;

            long durationMs = NodeSyncHelpers.time().currentTimeMillis() - startTime;
            state.trace(id + "Completed validation ({}) in {}: validated {} and repaired {}",
                        outcome,
                        Units.toString(durationMs, TimeUnit.MILLISECONDS),
                        Units.toString(metrics.dataValidated(), SizeUnit.BYTES),
                        Units.toString(metrics.dataRepaired(), SizeUnit.BYTES));
        }
    }
}
