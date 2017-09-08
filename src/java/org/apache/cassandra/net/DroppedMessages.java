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
package org.apache.cassandra.net;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.metrics.DroppedMessageMetrics;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.StatusLogger;

/**
 * Records messages dropped by the messaging service.
 * <p>
 * To make this more user friendly/useful, we count dropped messages by "dropped group" ({@link Group} below). Those
 * group differ from {@link VerbGroup} because verb groups is a grouping that make sense internally, but the grouping
 * here is mean to be meaningful for end user. Further, we sometime want to count separately messages of the same
 * {@link Verb} base on more specific criteria; typically, for reads, we distinguish single partition and range reads
 * because they have different timeouts and tend to be used for different type of tasks, so counting the numbers dropped
 * separately make sense. Also, we're trying to preserve "some" backward compatibility from pre-APOLLO-497, which
 * explains some of the naming of those groups.
 */
public class DroppedMessages
{
    private static final Logger logger = LoggerFactory.getLogger(DroppedMessages.class);
    private static final int LOG_DROPPED_INTERVAL_IN_MS = 5000;

    public enum Group
    {
        MUTATION,
        COUNTER_MUTATION,
        VIEW_MUTATION,
        READ,
        RANGE_SLICE,
        READ_REPAIR,
        LWT,
        HINTS,
        TRUNCATE,
        SNAPSHOT,
        SCHEMA,
        REPAIR,
        OTHER
    }

    private final EnumMap<Group, DroppedMessageMetrics> metrics = new EnumMap<>(Group.class);

    DroppedMessages()
    {
        for (Group group : Group.values())
            metrics.put(group, new DroppedMessageMetrics(group));
    }

    void scheduleLogging()
    {
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::log, LOG_DROPPED_INTERVAL_IN_MS, LOG_DROPPED_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);
    }

    void onDroppedMessage(Message<?> message)
    {
        DroppedMessageMetrics messageMetrics = metrics.get(message.verb().droppedGroup());
        if (messageMetrics == null)
        {
            // This really shouldn't happen or is a bug, but throwing don't feel necessary either
            NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 5, TimeUnit.MINUTES, "Cannot increment dropped message for message {}", message);
        }
        else
        {
            messageMetrics.onMessageDropped(message.lifetimeMillis(), !message.isLocal());
        }
    }

    private void log()
    {
        List<String> logs = getDroppedMessagesLogs();
        for (String log : logs)
            logger.info(log);

        if (logs.size() > 0)
            StatusLogger.log();
    }

    @VisibleForTesting
    List<String> getDroppedMessagesLogs()
    {
        List<String> ret = new ArrayList<>();
        for (Group group : Group.values())
        {
            DroppedMessageMetrics groupMetrics = metrics.get(group);

            int internalDropped = groupMetrics.getAndResetInternalDropped();
            int crossNodeDropped = groupMetrics.getAndResetCrossNodeDropped();
            if (internalDropped > 0 || crossNodeDropped > 0)
            {
                ret.add(String.format("%s messages were dropped in last %d ms: %d internal and %d cross node."
                                      + " Mean internal dropped latency: %d ms and Mean cross-node dropped latency: %d ms",
                                      group,
                                      LOG_DROPPED_INTERVAL_IN_MS,
                                      internalDropped,
                                      crossNodeDropped,
                                      TimeUnit.NANOSECONDS.toMillis((long)groupMetrics.internalDroppedLatency.getSnapshot().getMean()),
                                      TimeUnit.NANOSECONDS.toMillis((long)groupMetrics.crossNodeDroppedLatency.getSnapshot().getMean())));
            }
        }
        return ret;
    }

    Map<String, Integer> getSnapshot()
    {
        Map<String, Integer> map = new HashMap<>(Group.values().length);
        for (Group group : Group.values())
            map.put(group.toString(), (int) metrics.get(group).dropped.getCount());
        return map;
    }
}
