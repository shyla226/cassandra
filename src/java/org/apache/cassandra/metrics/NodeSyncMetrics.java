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

import java.util.HashSet;
import java.util.Set;

import com.datastax.apollo.nodesync.ValidationOutcome;
import com.datastax.apollo.nodesync.NodeSyncService;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics related to NodeSync.
 * <p>
 * Those are set directly by the {@link NodeSyncService} in its {@link NodeSyncService#updateMetrics} method.
 */
public class NodeSyncMetrics
{
    /** Meter on the size (in bytes) of data validated by NodeSync. */
    public final Meter dataValidated;
    /** Meter on the size (in bytes) of data repaired (or rather, the subset of {@link #dataValidated} that is detected
     * as inconsistent) by NodeSync. */
    public final Meter dataRepaired;

    /** Meter on the number of "objects" (rows + range tombstones) validated by NodeSync. This is the number of
     * rows and range tombstones equivalent to {@link #dataValidated}. */
    private final Meter objectsValidated;
    /** Meter on the number of "objects" (rows + range tombstones) repaired by NodeSync (the subset of
     * {@link #objectsValidated} that wasn't in sync). This is the number of rows and range tombstones equivalent to
     * {@link #dataRepaired}. */
    private final Meter objectsRepaired;

    /**
     * Meter on the size (in bytes) of the data sent for repairing inconsistencies found by NodeSync.
     * This differs from {@link #dataRepaired} in that if a row of size X is inconsistent, X is counted in {@link
     * #dataRepaired}, but more or less than X can be counted in {@code repairDataSent} since on the one side the
     * row can be inconsistent on more than one node, but on the other side only a subpart of the row may be inconsistent
     * (and thus be sent).
     */
    private final Meter repairDataSent;
    /** Meter on the number of objects (row, range tombstone markers or partition deletion) sent for repairing
        inconsistencies found by NodeSync. */
    private final Meter repairObjectsSent;

    /** The total number of pages processed by NodeSync since the last node restart. */
    public final Counter processedPages;
    /** The subset of {@link #processedPages} that were full (<b>all</b> replicas replied) and with no inconsistencies */
    public final Counter fullInSyncPages;
    /** The subset of {@link #processedPages} that were full (<b>all</b> replicas replied) and had inconsistencies that
     * were properly repaired. */
    public final Counter fullRepairedPages;
    /** The subset of {@link #processedPages} that were partial (only a subset of the replicas was up/responded), but the
     * subset of replicas that participated had no inconsistencies */
    public final Counter partialInSyncPages;
    /** The subset of {@link #processedPages} that were partial (only a subset of the replicas was up/responded) and
     * where the subset of replicas that participated had inconsistencies but that were properly repaired. */
    public final Counter partialRepairedPages;
    /** The subset of {@link #processedPages} that weren't completed because less than 2 replicas were either alive or
     * responded. */
    public final Counter uncompletedPages;
    /** The subset of {@link #processedPages} that failed for some reason (maybe only one node was up and the page was
        skipped, or some replica(s) failed mid-page, or some unexpected error occurred). */
    public final Counter failedPages;

    private final MetricNameFactory factory;
    private final String namePrefix;

    private final Set<CassandraMetricsRegistry.MetricName> metricsNames = new HashSet<>();

    public NodeSyncMetrics(MetricNameFactory factory, String namePrefix)
    {
        this.factory = factory;
        this.namePrefix = namePrefix;

        this.dataValidated = Metrics.meter(name("DataValidated"));
        this.dataRepaired = Metrics.meter(name("DataRepaired"));
        this.repairDataSent = Metrics.meter(name("RepairDataSent"));

        // Note that we could but don't distinguish between rows and range tombstones in the metrics below. Doing so
        // duplicate each metrics, adding cruft, without really adding much in practice.
        this.objectsValidated = Metrics.meter(name("ObjectsValidated"));
        this.objectsRepaired = Metrics.meter(name("ObjectsRepaired"));
        this.repairObjectsSent = Metrics.meter(name("RepairObjectsSent"));

        this.processedPages = Metrics.counter(name("ProcessedPages"));
        this.fullInSyncPages = Metrics.counter(name("FullInSyncPages"));
        this.fullRepairedPages = Metrics.counter(name("FullRepairedPages"));
        this.partialInSyncPages = Metrics.counter(name("PartialInSyncPages"));
        this.partialRepairedPages = Metrics.counter(name("PartialRepairedPages"));
        this.uncompletedPages = Metrics.counter(name("UncompletedPages"));
        this.failedPages = Metrics.counter(name("FailedPages"));
    }

    public void incrementRows(long validated, long repaired)
    {
        objectsValidated.mark(validated);
        objectsRepaired.mark(repaired);
    }

    public void incrementRangeTombstoneMarkers(long validated, long repaired)
    {
        objectsValidated.mark(validated);
        objectsRepaired.mark(repaired);
    }

    public void incrementDataSizes(long validated, long repaired)
    {
        dataValidated.mark(validated);
        dataRepaired.mark(repaired);
    }

    public void incrementRepairSent(long dataSent, long objectsSent)
    {
        repairDataSent.mark(dataSent);
        repairObjectsSent.mark(objectsSent);
    }

    public void addPageOutcomes(ValidationOutcome outcome, long pageCount)
    {
        processedPages.inc(pageCount);
        switch (outcome)
        {
            case FULL_IN_SYNC:
                fullInSyncPages.inc(pageCount);
                break;
            case FULL_REPAIRED:
                fullRepairedPages.inc(pageCount);
                break;
            case PARTIAL_IN_SYNC:
                partialInSyncPages.inc(pageCount);
                break;
            case PARTIAL_REPAIRED:
                partialRepairedPages.inc(pageCount);
                break;
            case UNCOMPLETED:
                uncompletedPages.inc(pageCount);
                break;
            case FAILED:
                failedPages.inc(pageCount);
                break;
        }
    }

    private CassandraMetricsRegistry.MetricName name(String name)
    {
        CassandraMetricsRegistry.MetricName metricName = factory.createMetricName(namePrefix + name);
        metricsNames.add(metricName);
        return metricName;
    }

    public void release()
    {
        for (CassandraMetricsRegistry.MetricName name : metricsNames)
            Metrics.remove(name);
    }
}
