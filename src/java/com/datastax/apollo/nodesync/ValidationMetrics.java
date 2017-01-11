/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.nodesync;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.metrics.NodeSyncMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.Units;

/**
 * Collects metrics during a NodeSync validation (performed by {@link Validator}).
 * <p>
 * Note that this object is only used for collection of the metrics during a particular segment validation (some
 * of the values in this class are updated on every validated row so directly updating {@link NodeSyncMetrics} every time
 * would be too costly), but as far as exposing metrics to JMX, those stats are consolidated in {@link NodeSyncMetrics}
 * at the end of each validation (see {@link NodeSyncService#updateMetrics(TableMetadata, ValidationMetrics)}).
 * <p>
 * This is NOT thread-safe.
 */
class ValidationMetrics implements Serializable
{
    private static final long serialVersionUID = 42L;

    private final long startTime = System.currentTimeMillis();

    // The number of pages having a particular outcome. Indexed by ValidationOutcome.ordinal().
    private final long[] pagesByOutcome = new long[ValidationOutcome.values().length];

    // Number of rows we read/validated, and the number for which at least one replica was out of sync.
    private long rowsValidated;
    private long rowsRepaired;

    // Number of range tombstones markers (and partition deletion) we read/validated, and the number for which at least
    // one replica was out of sync.
    private long rangeTombstoneMarkersValidated;
    private long rangeTombstoneMarkersRepaired;

    // How much data (in bytes) was validated (resp. repaired), so  basically the size in bytes of what is counted by
    // rowsValidated + rangeTombstoneMarkersValidated (resp. rowsRepaired + rangeTombstoneMarkersRepaired).
    private long dataValidated;
    private long dataRepaired;
    // How much data (in bytes, computed using PartitionUpdate.dataSize() so more in term of "user data size") was sent
    // for repair purposes. This differs from dataRepaired in that this is what goes on the wire for repair.
    private long repairDataSent;
    // How much objects (rows and range tombstones) was sent for repair purposes
    private long repairObjectsSent;

    ValidationMetrics()
    {
    }

    void addPageOutcome(ValidationOutcome outcome)
    {
        ++pagesByOutcome[outcome.ordinal()];
    }

    void addRepair(PartitionUpdate update)
    {
        repairDataSent += update.dataSize();
        repairObjectsSent += update.operationCount();
    }

    void addDataValidated(int size, boolean isConsistent)
    {
        dataValidated += size;
        if (!isConsistent)
            dataRepaired += size;
    }

    void incrementRowsRead(boolean isConsistent)
    {
        ++rowsValidated;
        if (!isConsistent)
            ++rowsRepaired;
    }

    void incrementRangeTombstoneMarkersRead(boolean isConsistent)
    {
        ++rangeTombstoneMarkersValidated;
        if (!isConsistent)
            ++rangeTombstoneMarkersRepaired;
    }

    void addTo(NodeSyncMetrics metrics)
    {
        for (ValidationOutcome outcome : ValidationOutcome.values())
            metrics.addPageOutcomes(outcome, pagesByOutcome[outcome.ordinal()]);

        metrics.incrementRows(rowsValidated, rowsRepaired);
        metrics.incrementRangeTombstoneMarkers(rangeTombstoneMarkersValidated, rangeTombstoneMarkersRepaired);

        metrics.incrementDataSizes(dataValidated, dataRepaired);
        metrics.incrementRepairSent(repairDataSent, repairObjectsSent);
    }

    static ValidationMetrics merge(ValidationMetrics m1, ValidationMetrics m2)
    {
        ValidationMetrics result = new ValidationMetrics();
        int s = result.pagesByOutcome.length;
        System.arraycopy(m1.pagesByOutcome, 0, result.pagesByOutcome, 0, s);
        for (int i = 0; i < s; i++)
            result.pagesByOutcome[i] += m2.pagesByOutcome[i];

        result.rowsValidated = m1.rowsValidated + m2.rowsValidated;
        result.rowsRepaired = m1.rowsRepaired + m2.rowsRepaired;

        result.rangeTombstoneMarkersValidated = m1.rangeTombstoneMarkersValidated + m2.rangeTombstoneMarkersValidated;
        result.rangeTombstoneMarkersRepaired = m1.rangeTombstoneMarkersRepaired + m2.rangeTombstoneMarkersRepaired;

        result.dataValidated = m1.dataValidated + m2.dataValidated;
        result.dataRepaired = m1.dataRepaired + m2.dataRepaired;

        result.repairDataSent = m1.repairDataSent + m2.repairDataSent;
        result.repairObjectsSent = m1.repairObjectsSent + m2.repairObjectsSent;

        return result;
    }

    /**
     * A less digestible but more detailed version of {@link #toString()} suitable when one wants as much details
     * as possible.
     *
     * @return a string containing all the information stored by those metrics.
     */
    String toDebugString()
    {
        StringBuilder sb = new StringBuilder();

        long duration = System.currentTimeMillis() - startTime;
        sb.append("duration=").append(duration).append("ms");
        if (duration > 1000)
            sb.append(duration > 1000 * 1000 ? " (~" : " (").append(Units.toString(duration, TimeUnit.MILLISECONDS)).append(')');

        sb.append(", pages={ ");
        int i = 0;
        for (ValidationOutcome outcome : ValidationOutcome.values())
        {
            if (pagesByOutcome[outcome.ordinal()] == 0)
                continue;

            sb.append(i++ == 0 ? "" : ", ");
            sb.append(outcome).append(": ").append(pagesByOutcome[outcome.ordinal()]);
        }
        sb.append(" }, ");

        sb.append("rows={ validated: ").append(rowsValidated)
          .append(", repaired: ").append(rowsRepaired).append("}, ");

        sb.append("range tombstones={ validated: ").append(rangeTombstoneMarkersValidated)
          .append(", repaired: ").append(rangeTombstoneMarkersRepaired).append("}, ");

        sb.append("validated=").append(Units.toLogString(dataValidated, SizeUnit.BYTES)).append(", ");
        sb.append("repaired data sent=").append(Units.toLogString(repairDataSent, SizeUnit.BYTES));

        return sb.toString();
    }

    @Override
    public String toString()
    {
        long totalPages = LongStream.of(pagesByOutcome).sum();
        long partialPages = pagesByOutcome[ValidationOutcome.PARTIAL_IN_SYNC.ordinal()]
                            + pagesByOutcome[ValidationOutcome.PARTIAL_REPAIRED.ordinal()];
        long uncompletedPages = pagesByOutcome[ValidationOutcome.UNCOMPLETED.ordinal()];
        long failedPages = pagesByOutcome[ValidationOutcome.FAILED.ordinal()];

        String partialString = partialPages == 0
                               ? ""
                               : String.format("; %d%% only partially validated/repaired", percent(partialPages, totalPages));

        String uncompletedString = uncompletedPages == 0
                              ? ""
                              : String.format("; %d%% uncompleted", percent(uncompletedPages, totalPages));

        String failedString = failedPages == 0
                              ? ""
                              : String.format("; %d%% failed", percent(failedPages, totalPages));

        String repairString = repairDataSent == 0
                              ? "everything was in sync"
                              : String.format("%d repaired (%d%%); %s of repair data sent",
                                              rowsRepaired,
                                              percent(rowsRepaired, rowsValidated),
                                              Units.toString(repairDataSent, SizeUnit.BYTES));

        return String.format("validated %s - %s%s%s%s",
                             Units.toString(dataValidated, SizeUnit.BYTES),
                             repairString,
                             partialString,
                             uncompletedString,
                             failedString);
    }

    private int percent(long value, long total)
    {
        return total == 0 ? 0 : (int)((value * 100) / total);
    }
}
