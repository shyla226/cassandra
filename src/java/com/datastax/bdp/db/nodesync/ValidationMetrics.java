/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.metrics.NodeSyncMetrics;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
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
public class ValidationMetrics implements Serializable
{
    private static final long serialVersionUID = 42L;

    private final long startTime = NodeSyncHelpers.time().currentTimeMillis();

    // The number of pages having a particular outcome. Indexed by ValidationOutcome.ordinal().
    private final long[] pagesByOutcome = new long[ValidationOutcome.values().length];

    // Number of "objects" (rows+range tombstones) we read/validated, and the number for which at least one replica was out of sync.
    private long objectsValidated;
    private long objectsRepaired;

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
        ++objectsValidated;
        if (!isConsistent)
            ++objectsRepaired;
    }

    void incrementRangeTombstoneMarkersRead(boolean isConsistent)
    {
        ++objectsValidated;
        if (!isConsistent)
            ++objectsRepaired;
    }

    long dataValidated()
    {
        return dataValidated;
    }

    long dataRepaired()
    {
        return dataRepaired;
    }

    void addTo(NodeSyncMetrics metrics)
    {
        for (ValidationOutcome outcome : ValidationOutcome.values())
            metrics.addPageOutcomes(outcome, pagesByOutcome[outcome.ordinal()]);

        metrics.incrementObjects(objectsValidated, objectsRepaired);

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

        result.objectsValidated = m1.objectsValidated + m2.objectsValidated;
        result.objectsRepaired = m1.objectsRepaired + m2.objectsRepaired;

        result.dataValidated = m1.dataValidated + m2.dataValidated;
        result.dataRepaired = m1.dataRepaired + m2.dataRepaired;

        result.repairDataSent = m1.repairDataSent + m2.repairDataSent;
        result.repairObjectsSent = m1.repairObjectsSent + m2.repairObjectsSent;

        return result;
    }

    /**
     * Deserialize a binary {@link SystemDistributedKeyspace#NODESYNC_METRICS} value into a corresponding
     * {@link ValidationMetrics} object.
     *
     * @param bytes the value to deserialize.
     * @return the deserialized {@link ValidationMetrics}.
     *
     * @throws IllegalArgumentException if an error forbid a proper deserialization of the provided {@code bytes}.
     */
    public static ValidationMetrics fromBytes(ByteBuffer bytes)
    {
        UserType type = SystemDistributedKeyspace.NodeSyncMetrics;
        ByteBuffer[] values = type.split(bytes);
        if (values.length < type.size())
            throw new IllegalArgumentException(String.format("Invalid number of components for nodesync_metrics, expected %d but got %d", type.size(), values.length));

        try
        {
            ValidationMetrics m = new ValidationMetrics();
            m.dataValidated = type.composeField(0, values[0]);
            m.dataRepaired = type.composeField(1, values[1]);
            m.objectsValidated = type.composeField(2, values[2]);
            m.objectsRepaired = type.composeField(3, values[3]);
            m.repairDataSent = type.composeField(4, values[4]);
            m.repairObjectsSent = type.composeField(5, values[5]);
            Map<String, Long> outcomesMap = type.composeField(6, values[6]);
            for (ValidationOutcome outcome : ValidationOutcome.values())
                m.pagesByOutcome[outcome.ordinal()] = outcomesMap.getOrDefault(outcome.toString(), 0L);
            return m;
        }
        catch (MarshalException e)
        {
            throw new IllegalArgumentException("Error deserializing nodesync_metrics from " + ByteBufferUtil.toDebugHexString(bytes), e);
        }
    }

    /**
     * Serializes the validation info into a {@link SystemDistributedKeyspace#NODESYNC_VALIDATION} value.
     *
     * @return the serialized value.
     */
    public ByteBuffer toBytes()
    {
        UserType type = SystemDistributedKeyspace.NodeSyncMetrics;
        ByteBuffer[] values = new ByteBuffer[type.size()];
        values[0] = type.decomposeField(0, dataValidated);
        values[1] = type.decomposeField(1, dataRepaired);
        values[2] = type.decomposeField(2, objectsValidated);
        values[3] = type.decomposeField(3, objectsRepaired);
        values[4] = type.decomposeField(4, repairDataSent);
        values[5] = type.decomposeField(5, repairObjectsSent);
        values[6] = type.decomposeField(6, ValidationOutcome.toMap(pagesByOutcome));
        return UserType.buildValue(values);
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

        long duration = NodeSyncHelpers.time().currentTimeMillis() - startTime;
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

        sb.append("objects={ validated: ").append(objectsValidated)
          .append(", repaired: ").append(objectsRepaired).append("}, ");

        sb.append("data={ validated: ").append(Units.toLogString(dataValidated, SizeUnit.BYTES))
          .append(", repaired: ").append(dataRepaired).append("}, ");

        sb.append("repair sent={ data: ").append(Units.toLogString(repairDataSent, SizeUnit.BYTES))
          .append(", objects: ").append(repairObjectsSent).append('}');


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
                                              objectsRepaired,
                                              percent(objectsRepaired, objectsValidated),
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
