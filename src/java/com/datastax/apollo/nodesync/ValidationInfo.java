/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.nodesync;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.collect.Sets;

import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.units.TimeValue;

/**
 * Information regarding a particular NodeSync segment validation.
 * <p>
 * The information of this class are stored in the {@link SystemDistributedKeyspace#NODESYNC_STATUS} using
 * the {@link SystemDistributedKeyspace#NODESYNC_VALIDATION} UDT and can be converted to/from that UDT value
 * using the {@link #fromBytes(ByteBuffer)} and {@link #toBytes()} methods.
 */
public class ValidationInfo
{
    /** The starting time (epoch timestamp in milliseconds) of the validation. */
    final long startedAt;
    /** The outcome of the validation. */
    final ValidationOutcome outcome;
    /** If the outcome of the validation was partial ({@code outcome.wasPartial()}), the nodes that were missing, {@code null}
     * otherwise. Note that if the validation was partial, this won't be {@code null} but this can be empty, in which case
     * this mean it was partial because the table replication factor was greater than the total number of nodes (pretty
     * edge-case on a prod cluster, but will happen for distributed system tables on small test cluster since they default
     * to RF > 1). */
    @Nullable
    final Set<InetAddress> missingNodes;

    ValidationInfo(long startedAt, ValidationOutcome outcome, Set<InetAddress> missingNodes)
    {
        this.startedAt = startedAt;
        this.outcome = outcome;
        this.missingNodes = outcome.wasPartial()
                            ? (missingNodes == null ? Collections.emptySet() : missingNodes)
                            : null;
    }

    /**
     * Whether the validation was successful, which in this context means that all replica for the segment were
     * involved and responded and so the segment was fully validated and repaired if necessary.
     */
    public boolean wasSuccessful()
    {
        return outcome.wasSuccessful();
    }

    /**
     * Deserialize a binary {@link SystemDistributedKeyspace#NODESYNC_VALIDATION} value into a corresponding
     * {@link ValidationInfo} object.
     *
     * @param bytes the value to deserialize.
     * @return the deserialized {@link ValidationInfo}, or {@code null} if an error forbade proper deserialization.
     *
     * @throw IllegalArgumentException is an error forbid a proper deserialization of the provided {@code bytes}.
     */
    public static ValidationInfo fromBytes(ByteBuffer bytes)
    {
        UserType type = SystemDistributedKeyspace.NodeSyncValidation;
        ByteBuffer[] values = type.split(bytes);
        if (values.length != 3)
            throw new IllegalArgumentException(String.format("Invalid number of components for nodesync_validation, expected %d but got %d", 3, values.length));

        try
        {
            long startedAt = ((Date)type.composeField(0, values[0])).getTime();
            ValidationOutcome outcome = ValidationOutcome.fromCode(type.composeField(1, values[1]));
            Set<InetAddress> missingNodes = values[2] == null ? null : type.composeField(2, values[2]);
            return new ValidationInfo(startedAt, outcome, missingNodes);
        }
        catch (MarshalException e)
        {
            throw new IllegalArgumentException("Error deserializing nodesync_validation from " + ByteBufferUtil.toDebugHexString(bytes), e);
        }
    }

    /**
     * Serializes the validation info into a {@link SystemDistributedKeyspace#NODESYNC_VALIDATION} value.
     *
     * @return the serialized value.
     */
    public ByteBuffer toBytes()
    {
        UserType type = SystemDistributedKeyspace.NodeSyncValidation;
        ByteBuffer[] values = new ByteBuffer[type.size()];
        values[0] = type.decomposeField(0, new Date(startedAt));
        values[1] = type.decomposeField(1, outcome.code());
        values[2] = missingNodes == null ? null : type.decomposeField(2, missingNodes);
        return UserType.buildValue(values);
    }

    /**
     * Whether the validation represented by this info is more recent than the one represented by {@code other}.
     */
    public boolean isMoreRecentThan(ValidationInfo other)
    {
        return startedAt >= other.startedAt;
    }

    /**
     * Composes two validation info.
     * <p>
     * This is the equivalent of {@link ValidationOutcome#composeWith} but for {@link ValidationInfo}. Given this info
     * and the provided one, and assuming those info correspond to consecutive (<b>but not overlapping</b>) sub-ranges,
     * it returns the best info we can infer for the whole range covered by those info. In practice, this return a
     * {@link ValidationInfo} that uses the "worst" outcome of both info and the oldest validation time, as that is
     * the best we can say.
     */
    ValidationInfo composeWith(ValidationInfo other)
    {
        long composedStartTime = Math.min(startedAt, other.startedAt);
        ValidationOutcome composedOutcome = outcome.composeWith(other.outcome);
        Set<InetAddress> composedMissingNodes = null;
        if (composedOutcome.wasPartial())
        {
            // We know it's only partial if one of the outcome was partial. Both can be partial though, in which case
            // we should merge the missing nodes.
            composedMissingNodes = outcome.wasPartial()
                                   ? (other.outcome.wasPartial() ? Sets.union(missingNodes, other.missingNodes) : missingNodes)
                                   : other.missingNodes;
        }
        return new ValidationInfo(composedStartTime, composedOutcome, composedMissingNodes);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(startedAt, outcome, missingNodes);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ValidationInfo))
            return false;

        ValidationInfo that = (ValidationInfo)o;
        return this.startedAt == that.startedAt
               && this.outcome == that.outcome
               && Objects.equals(this.missingNodes, that.missingNodes);
    }

    @Override
    public String toString()
    {
        long now = System.currentTimeMillis();
        TimeValue age = TimeValue.of(now - startedAt, TimeUnit.MILLISECONDS);
        return String.format("%s=%s ago%s",
                             outcome,
                             age,
                             missingNodes == null ? "" : String.format(" (missing: %s)", missingNodes));
    }
}
