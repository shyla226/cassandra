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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.net.DroppedMessages;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for dropped messages, stored in {@link DroppedMessages} by verb "dropped group",
 * that is by {@link DroppedMessages.Group}.
 */
public class DroppedMessageMetrics
{
    /** Number of dropped messages */
    public final Meter dropped;

    /** The dropped latency within node */
    public final Timer internalDroppedLatency;

    /** The cross node dropped latency */
    public final Timer crossNodeDroppedLatency;

    // The two following counters are not exposed through JMX per-set, but are used to log dropped messages at regular
    // interval, and are reset each time they are logged (which is why we can't rely on the metrics above). See
    // MessagingService#getDroppedMessagesLogs for details.
    /** Dropped messages that were locally delivered (from == to) */
    private final AtomicInteger internalDropped;
    /** Dropped messages that crossed node boundary (from != to)  */
    private final AtomicInteger crossNodeDropped;

    public DroppedMessageMetrics(DroppedMessages.Group group)
    {
        MetricNameFactory factory = new DefaultNameFactory("DroppedMessage", group.toString());
        dropped = Metrics.meter(factory.createMetricName("Dropped"));
        internalDroppedLatency = Metrics.timer(factory.createMetricName("InternalDroppedLatency"));
        crossNodeDroppedLatency = Metrics.timer(factory.createMetricName("CrossNodeDroppedLatency"));
        internalDropped = new AtomicInteger();
        crossNodeDropped = new AtomicInteger();
    }

    public void onMessageDropped(long timeTakenMillis, boolean hasCrossedNode)
    {
        dropped.mark();
        if (hasCrossedNode)
        {
            crossNodeDropped.incrementAndGet();
            crossNodeDroppedLatency.update(timeTakenMillis, TimeUnit.MILLISECONDS);
        }
        else
        {
            internalDropped.incrementAndGet();
            internalDroppedLatency.update(timeTakenMillis, TimeUnit.MILLISECONDS);
        }
    }

    public int getAndResetInternalDropped()
    {
        return internalDropped.getAndSet(0);
    }

    public int getAndResetCrossNodeDropped()
    {
        return crossNodeDropped.getAndSet(0);
    }
}
