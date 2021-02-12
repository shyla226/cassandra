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
package org.apache.cassandra.cdc;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for commit log replication
 */
public class CdcReplicationMetrics
{
    public static final MetricNameFactory factory = new DefaultNameFactory("CdcReplication");

    public final Meter replicated;
    public final Meter errors;
    public final Meter flushes;

    public final Gauge<Integer> pendingCommitLogFiles;
    public final Gauge<Integer> pendingSentMutations;
    public final Gauge<Long> replicationLag;

    public CdcReplicationMetrics(CommitLogReaderProcessor commitLogReaderProcessor, OffsetFileWriter offsetFileWriter)
    {
        replicated = Metrics.meter(factory.createMetricName("replicated"));
        errors = Metrics.meter(factory.createMetricName("errors"));
        flushes = Metrics.meter(factory.createMetricName("flushes"));

        pendingCommitLogFiles = commitLogReaderProcessor.commitLogQueue::size;
        pendingSentMutations = offsetFileWriter.sentMutations::size;
        replicationLag = offsetFileWriter::replicationLag;
    }
}
