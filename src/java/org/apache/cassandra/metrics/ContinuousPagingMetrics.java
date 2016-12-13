/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.cql3.continuous.paging.ContinuousPagingService;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * The metrics for continous paging.
 */
public class ContinuousPagingMetrics
{
    private final MetricNameFactory factory;

    /** Measures the total latency for requests executing with
     * the locally optimized path (local data at CL.ONE) */
    private final LatencyMetrics optimizedPathLatency;

    /** Measures the total latency for requests executing with
     * the ordinary distributed path (CL > ONE or non local data).
     */
    private final LatencyMetrics slowPathLatency;

    /** Measure the number of sessions currently executing. */
    public final Gauge liveSessions;

    /** Measure the number of pages that are ready to be sent. */
    public final Gauge pendingPages;

    /** Measures the number of continuous paging requests. */
    public final Meter requests;

    /** Measures the number of continuous paging sessions that could not be created,
     * includes tooManySessions below.
     */
    public final Meter creationFailures;

    /** Measures the number of sessions that could not be created because there were
     * too many sessions already running.
     */
    public final Meter tooManySessions;

    /** Measure the number of ClientWriteExceptions, which includes client timeouts. */
    public final Meter clientWriteExceptions;

    /** Measures the number of failures that are not session creation failures or ClientWriteExceptions. */
    public final Meter failures;

    /** Measures how long continuous paging sessions are waiting to execute, that is how long before
     * they start execution since they were scheduled to execute.
     */
    public final LatencyMetrics waitingTime;

    /** Measures how often the server is blocked because the client is not reading from the socket. */
    public final Counter serverBlocked;

    /** Measures how long the server is blocked because the client is not reading from the socket. */
    public final LatencyMetrics serverBlockedLatency;

    public ContinuousPagingMetrics()
    {
        this.factory = new DefaultNameFactory("ContinuousPaging", "");

        this.optimizedPathLatency = new LatencyMetrics("ContinuousPaging", "OptimizedPathLatency");
        this.slowPathLatency = new LatencyMetrics("ContinuousPaging", "SlowPathLatency");
        this.liveSessions = Metrics.register(factory.createMetricName("LiveSessions"), (Gauge<Long>) ContinuousPagingService::liveSessions);
        this.pendingPages = Metrics.register(factory.createMetricName("PendingPages"), (Gauge<Long>) ContinuousPagingService::pendingPages);
        this.requests = Metrics.meter(factory.createMetricName("Requests"));
        this.creationFailures = Metrics.meter(factory.createMetricName("CreationFailures"));
        this.tooManySessions = Metrics.meter(factory.createMetricName("TooManySessions"));
        this.clientWriteExceptions = Metrics.meter(factory.createMetricName("ClientWriteExceptions"));
        this.failures = Metrics.meter(factory.createMetricName("Failures"));
        this.waitingTime = new LatencyMetrics("ContinuousPaging", "WaitingTime");
        this.serverBlocked = Metrics.counter(factory.createMetricName("ServerBlocked"));
        this.serverBlockedLatency = new LatencyMetrics("ContinuousPaging", "ServerBlockedLatency");
    }

    public void addTotalDuration(boolean isLocal, long nanos)
    {
        if (isLocal)
            optimizedPathLatency.addNano(nanos);
        else
            slowPathLatency.addNano(nanos);
    }

    public void release()
    {
        optimizedPathLatency.release();
        slowPathLatency.release();
        Metrics.remove(factory.createMetricName("LiveSessions"));
        Metrics.remove(factory.createMetricName("PendingPages"));
        Metrics.remove(factory.createMetricName("Requests"));
        Metrics.remove(factory.createMetricName("CreationFailures"));
        Metrics.remove(factory.createMetricName("TooManySessions"));
        Metrics.remove(factory.createMetricName("ClientWriteExceptions"));
        Metrics.remove(factory.createMetricName("Failures"));
        waitingTime.release();
        Metrics.remove(factory.createMetricName("ServerBlocked"));
        serverBlockedLatency.release();
    }
}
