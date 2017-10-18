/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.NodeSyncParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.nodetool.nodesync.RateSimulatorCmd;
import org.apache.cassandra.utils.units.RateUnit;
import org.apache.cassandra.utils.units.RateValue;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.SizeValue;
import org.apache.cassandra.utils.units.TimeValue;
import org.apache.cassandra.utils.units.Units;

/**
 * Computes, _for a particular replica_, the minimum rate necessary for NodeSync to validate all tables within their
 * configured deadline targets, using a number of configurable {@link Parameters} to account for events like node
 * failures, temporal slowdowns or simple data growth.
 * <p>
 * The rate simulator is used internally in order to warn users when their configured rate is seemingly (or provably)
 * too low (see {@link NodeSyncService}), but is also expose to users through nodetool ({@link RateSimulatorCmd}) in
 * order to help with rate configuration.
 * <p>
 * To perform its computation, the simulator relies on the {@link Info} class that groups the information on tables
 * necessary to the simulations. Note that those information intrinsically represents a single replica, and as such
 * the simulator simulate rates for that replica. However, as in most clusters, data is evenly distributed among
 * replica, the rate can generally be the same on all replica.
 */
public class RateSimulator
{
    private final Info info;
    private final Parameters parameters;

    private Consumer<String> eventLogger;
    private boolean ignoreReplicationFactor;

    /**
     * Creates a new simulator using the provided table info and parameters.
     *
     * @param info information on tables to use for the simulation.
     * @param parameters the parameters of the simulation.
     */
    public RateSimulator(Info info, Parameters parameters)
    {
        this.info = info;
        this.parameters = parameters;
        this.eventLogger = s -> {};
    }

    /**
     * Register an "event logger" for the simulation. If such logger is registered and {@link #computeRate()} is called
     * afterwards, then the logger will be called with string messages corresponding to the steps of the simulation.
     * This is meant as a way to get insights into the computation.
     * <p>
     * Note that only a single logger can be register (at a time) and this method, as this whole class, is not
     * thread-safe in any way.
     *
     * @param eventLogger a string consumer that will passed message describing the steps of future simulations.
     * @return this simulator.
     */
    public RateSimulator withLogger(Consumer<String> eventLogger)
    {
        this.eventLogger = eventLogger;
        return this;
    }

    /**
     * Instructs the simulator to ignore the replication factor in future simulation (calls to {@link #computeRate()}).
     * <p>
     * By default, simulation are performed for a particular replica, but with the assumption that NodeSync is ran
     * on all the nodes of the cluster this replica is part of (which is strongly recommended). As such, and because
     * NodeSync coordinates work amongst replica, the simulator assumes that for any given table, each replica only has
     * to validate 1/RF of the data it owns for that table. This option allows the simulator to ignore that assumption,
     * simulating a rate that assumes the node has to validate all the data it owns.
     *
     * @return this simulator.
     */
    public RateSimulator ignoreReplicationFactor()
    {
        this.ignoreReplicationFactor = true;
        return this;
    }

    /**
     * Performs the rate simulation using the information and parameters passed to the constructor of this validator.
     * <p>
     * Note that while this method simply return the final simulated rate, one can easily get insights into how that
     * rate is computed by registering a logger through {@link #withLogger(Consumer)}.
     *
     * @return the simulated rate.
     */
    public RateValue computeRate()
    {
        logParameters();

        long minRate = 0; // Min computed rate so far, in bytes/seconds.
        long cumulativeSize = 0; // Cumulative sizes of tables in bytes.
        for (TableInfo table : info.tables())
        {
            log("%s:", table.tableName());

            // Tiny detail, but make things a bit more pleasant on an empty cluster.
            if (table.dataSize.equals(SizeValue.ZERO))
            {
                log("  - No data so nothing to validate.");
                continue;
            }

            long target = table.deadlineTarget.in(TimeUnit.SECONDS);
            long adjustedTarget = parameters.adjustedDeadline(target);
            String adjustedTargetFrom = target == adjustedTarget
                                        ? ""
                                        : String.format(", adjusted from %s for safety", table.deadlineTarget);
            log("  - Deadline target=%s%s.", Units.toString(adjustedTarget, TimeUnit.SECONDS), adjustedTargetFrom);

            long tableSize = table.dataSize.in(SizeUnit.BYTES);
            long size = ignoreReplicationFactor ? tableSize : tableSize / table.replicationFactor;
            long adjustedSize = parameters.adjustedSize(size);
            String adjustedSizeFrom = size == adjustedSize
                                      ? ""
                                      : String.format(", adjusted from %s for future growth", table.dataSize);
            if (ignoreReplicationFactor)
                log("  - Size=%s%s.", Units.toString(adjustedSize, SizeUnit.BYTES), adjustedSizeFrom);
            else
                log("  - Size=%s total, so %s to validate (RF=%d)%s.",
                    table.dataSize, Units.toString(size, SizeUnit.BYTES), table.replicationFactor, adjustedSizeFrom);

            cumulativeSize += adjustedSize;
            long rate = rate(cumulativeSize, adjustedTarget);
            log("  - Added to previous tables, %s to validate in %s => %s",
                Units.toString(cumulativeSize, SizeUnit.BYTES),
                Units.toString(adjustedTarget, TimeUnit.SECONDS),
                Units.toString(rate, RateUnit.B_S));

            if (rate > minRate)
            {
                minRate = rate;
                log("  => New minimum rate: %s", Units.toString(minRate, RateUnit.B_S));
            }
            else
            {
                log("  => Unchanged minimum rate: %s", Units.toString(minRate, RateUnit.B_S));
            }

        }

        logSeparation();
        long adjustedRate = parameters.adjustedRate(minRate);
        RateValue rate = RateValue.of(adjustedRate, RateUnit.B_S);
        log("Computed rate: %s%s.",
            rate,
            minRate == adjustedRate ? ""
                                    : String.format(", adjusted from %s for safety", Units.toString(minRate, RateUnit.B_S)));
        return rate;
    }

    private void logParameters()
    {
        log("Using parameters:");
        log(" - Size growing factor:    %.2f", parameters.sizeGrowingFactor);
        log(" - Deadline safety factor: %.2f", parameters.deadlineSafetyFactor);
        log(" - Rate safety factor:     %.2f", parameters.rateSafetyFactor);
        logSeparation();
    }

    private void logSeparation()
    {
        eventLogger.accept("");
    }

    private void log(String msg, Object... values)
    {
        eventLogger.accept(String.format(msg, values));
    }

    private static long rate(long sizeInBytes, long timeInSeconds)
    {
        return (long)Math.ceil(((double)sizeInBytes)/timeInSeconds);
    }

    /**
     * Parameters of the simulator.
     * <p>
     * Computing the theoretical minimum rate necessary to repair all tables within their deadline target is
     * informative (and provided by {@link Parameters#THEORETICAL_MINIMUM}) but such rate is definitively too low
     * in practice because it assumes perfect conditions. In practice, node will never sustain the configured rate
     * exactly and at all time even in the best circumstances, and may even get seriously off temporarily if the
     * node is overloaded. As importantly, node will fail from time to time, and when that happen, not only will they
     * not participate in their part of validation, but every other replica validates will have to be re-validated
     * when they get up in practice. Lastly, a rate appropriate for a given amount of data hold by a node, will be
     * too low once more data is added, and so user should plan for data growth if they don't want to have to update
     * the rate all the time.
     * <p>
     * That is why those simulation parameters exists: they provide an hopefully convenient way for users to simulate
     * rate that takes those imperfect conditions into account with more or less weight.
     */
    public static class Parameters
    {
        /** The parameters that allow to compute the theoretically minimal rate to validated all tables within their
         * deadline. As said on the class javadoc, this is purely indicative as the rate computed with those parameters
         * is too low for real world use. */
        public static final Parameters THEORETICAL_MINIMUM = new Parameters(0f, 0f, 0f);
        /** Parameters that simulates the minimum rate recommended, that is the minimum rate that we believe is usable
         * in the real world. Note that account for little data growth and generally account for a fairly healthy and
         * well managed cluster. If anyone doesn't know where to start, they should rather start with the
         * {@link #RECOMMENDED} parameters instead. */
        public static final Parameters MINIMUM_RECOMMENDED = new Parameters(.2f, .25f, 0f);
        /** Parameters that simulates the recommended rate, by which we mean a good starting point for new user to
         * experiment from.. Those parameters sill only account for doubling data, so especially on tiny clusters (where
         * doubling data  may happen quite quickly), this may still be way lower than appropriate. */
        public static final Parameters RECOMMENDED = new Parameters(1f, .25f, .1f);

        /**
         * A factor to account for data size growth. Basically allows to say how much data growth you want the computed
         * rate to accommodate (meaning that once data growth to that point, you probably will have to update the rate).
         * For instances, a factor of 0.2 will account for data growing 20%, one of 1 will account for data doubling, etc.
         */
        public final float sizeGrowingFactor;
        /**
         * A factor to take a safety margin over each table deadline target. To account for imperfect conditions, one
         * shouldn't compute a rate that allow to validate a table just within it's deadline, it should take a security
         * margin, and this is what this does. For instance, a factor of 0.2 means that the rate simulate will attempt
         * to validate each table within 20% of the end of the their deadline (in other words, it targets 80% of the
         * deadline).
         */
        public final float deadlineSafetyFactor;
        /**
         * A factor that somewhat blindly increase the simulated rate to account for anything that the 2 other factor
         * may not account well enough.
         */
        public final float rateSafetyFactor;

        private Parameters(float sizeGrowingFactor,
                           float deadlineSafetyFactor,
                           float rateSafetyFactor)
        {
            assert sizeGrowingFactor >= 0;
            assert deadlineSafetyFactor >= 0 && deadlineSafetyFactor < 1;
            assert rateSafetyFactor >= 0;
            this.sizeGrowingFactor = sizeGrowingFactor;
            this.deadlineSafetyFactor = deadlineSafetyFactor;
            this.rateSafetyFactor = rateSafetyFactor;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        long adjustedSize(long size)
        {
            return size + (long)Math.ceil(size * sizeGrowingFactor);
        }

        long adjustedDeadline(long deadline)
        {
            return deadline - (long)Math.ceil(deadline * deadlineSafetyFactor);
        }

        long adjustedRate(long rate)
        {
            return rate + (long)Math.ceil(rate * rateSafetyFactor);
        }

        /**
         * A simple builder for simulation parameters.
         * <p>
         * Note that by default the builder sets all factors to 0, so builds {@link #THEORETICAL_MINIMUM} by default.
         */
        public static class Builder
        {
            private float sizeGrowingFactor;
            private float deadlineSafetyFactor;
            private float rateSafetyFactor;

            private Builder()
            {}

            /**
             * Sets the size growing factory
             *
             * @param factor the factor to set, which must be positive.
             * @return this builder.
             *
             * @throws IllegalArgumentException if {@code factor} is negative.
             */
            public Builder sizeGrowingFactor(float factor)
            {
                if (factor < 0)
                    throw new IllegalArgumentException("The size growing factor must be positive (>= 0)");
                this.sizeGrowingFactor = factor;
                return this;
            }

            /**
             * Sets the deadline safety factory
             *
             * @param factor the factor to set, which must be within [0, 1).
             * @return this builder.
             *
             * @throws IllegalArgumentException if {@code factor} is negative or if {@code factor >= 1}.
             */
            public Builder deadlineSafetyFactor(float factor)
            {
                this.deadlineSafetyFactor = factor;
                return this;
            }

            /**
             * Sets the rate growing factory
             *
             * @param factor the factor to set, which must be positive.
             * @return this builder.
             *
             * @throws IllegalArgumentException if {@code factor} is negative.
             */
            public Builder rateSafetyFactor(float factor)
            {
                this.rateSafetyFactor = factor;
                return this;
            }

            public Parameters build()
            {
                return new Parameters(sizeGrowingFactor, deadlineSafetyFactor, rateSafetyFactor);
            }
        }
    }

    /**
     * Groups all info needed on tables for a given node to make a rate simulation.
     * <p>
     * For each table, this includes the replication factor, data size and deadline target for the table.
     */
    public static class Info
    {
        // Compare by deadline first as that makes all computation work. Then break any tie in whatever way make sure
        // that comparator is still "compatible" with equals in practice.
        private static final Comparator<TableInfo> COMPARATOR = Comparator.<TableInfo, TimeValue>comparing(t -> t.deadlineTarget)
                                                                .thenComparing(t -> t.keyspace)
                                                                .thenComparing(t -> t.table);
        private final SortedSet<TableInfo> tables;

        private Info(SortedSet<TableInfo> tables)
        {
            this.tables = tables;
        }

        private static SortedSet<TableInfo> newBackingSet()
        {
            return new TreeSet<>(COMPARATOR);
        }

        @VisibleForTesting
        static Info from(TableInfo... infos)
        {
            SortedSet<TableInfo> tables = newBackingSet();
            tables.addAll(Arrays.asList(infos));
            return new Info(tables);
        }

        /**
         * Builds the Info for all tables for the node this is called on (this should thus only be called server-side,
         * not in a tool).
         *
         * @param includeAllTables if {@code false}, only the NodeSync-enable tables will be included in the returned
         *                         infos. Otherwise, all tables will be included.
         */
        public static Info compute(boolean includeAllTables)
        {
            return new Info((includeAllTables ? allStores() : NodeSyncHelpers.nodeSyncEnabledStores())
                            .map(TableInfo::fromStore)
                            .collect(Collectors.toCollection(Info::newBackingSet)));
        }

        private static Stream<ColumnFamilyStore> allStores()
        {
            return StorageService.instance.getNonSystemKeyspaces()
                                          .stream()
                                          .map(Keyspace::open)
                                          .flatMap(k -> k.getColumnFamilyStores().stream());
        }

        /**
         * Returns a new Info object where all table info are transformed by the provided operator.
         */
        public Info transform(UnaryOperator<TableInfo> transformation)
        {
            return new Info(tables.stream()
                                  .map(transformation)
                                  .collect(Collectors.toCollection(Info::newBackingSet)));
        }

        /**
         * Serialize those information into a JMX-compatible format.
         */
        public List<Map<String, String>> toJMX()
        {
            return tables.stream().map(TableInfo::toStringMap).collect(Collectors.toList());
        }

        /**
         * Deserialize a {@link Info} object from the JMX-compatible format built with {@link #toJMX()}.
         */
        public static Info fromJMX(List<Map<String, String>> l)
        {
            return new Info(l.stream().map(TableInfo::fromStringMap)
                             .collect(Collectors.toCollection(Info::newBackingSet)));
        }

        /**
         * Whether this contains information on no tables.
         */
        public boolean isEmpty()
        {
            return tables.isEmpty();
        }

        /**
         * The info on all tables <b>with NodeSync enabled</b> and <b>in growing {@link NodeSyncParams#deadlineTarget}
         * order</b>.
         */
        public Iterable<TableInfo> tables()
        {
            return Iterables.filter(tables, t -> t.isNodeSyncEnabled);
        }

        @Override
        public String toString()
        {
            return tables.toString();
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof Info))
                return false;

            Info that = (Info) other;
            return this.tables.equals(that.tables);
        }

        @Override
        public int hashCode()
        {
            return tables.hashCode();
        }
    }

    /**
     * For a given table, groups info necessary to the rate simulator.
     * <p>
     * Implementation Note: we might be repeating the keyspace name and replication factor for many tables, so we could
     * imagine to have a 2-level hierarchy of "info" with keyspace->table, but doing so gets in the way of how the
     * simulator wants to use this, and make serializing everything on JMX a bit more annoying. Said duplication will
     * also be irrelevant in practice.
     */
    public static class TableInfo
    {
        @VisibleForTesting
        enum Property
        {
            KEYSPACE, TABLE, REPLICATION_FACTOR, NODESYNC_ENABLED, DATA_SIZE, DEADLINE_TARGET;

            public static Property fromString(String str)
            {
                return Property.valueOf(str.toUpperCase());
            }

            @Override
            public String toString()
            {
                return super.toString().toLowerCase();
            }
        }

        public final String keyspace;
        public final String table;
        public final int replicationFactor;
        public final boolean isNodeSyncEnabled;
        public final SizeValue dataSize;
        public final TimeValue deadlineTarget;

        @VisibleForTesting
        TableInfo(String keyspace,
                  String table,
                  int replicationFactor,
                  boolean isNodeSyncEnabled,
                  SizeValue dataSize,
                  TimeValue deadlineTarget)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.replicationFactor = replicationFactor;
            this.isNodeSyncEnabled = isNodeSyncEnabled;
            this.dataSize = dataSize;
            this.deadlineTarget = deadlineTarget;
        }

        public String tableName()
        {
            return String.format("%s.%s", ColumnIdentifier.maybeQuote(keyspace), ColumnIdentifier.maybeQuote(table));
        }

        static TableInfo fromStore(ColumnFamilyStore store)
        {
            TableMetadata table = store.metadata();
            return new TableInfo(table.keyspace,
                                 table.name,
                                 store.keyspace.getReplicationStrategy().getReplicationFactor(),
                                 NodeSyncHelpers.isNodeSyncEnabled(store),
                                 SizeValue.of(NodeSyncHelpers.estimatedSizeOf(store), SizeUnit.BYTES),
                                 TimeValue.of(table.params.nodeSync.deadlineTarget(table, TimeUnit.SECONDS), TimeUnit.SECONDS));
        }

        Map<String, String> toStringMap()
        {
            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            return builder.put(Property.KEYSPACE.toString(), keyspace)
                          .put(Property.TABLE.toString(), table)
                          .put(Property.REPLICATION_FACTOR.toString(), Integer.toString(replicationFactor))
                          .put(Property.NODESYNC_ENABLED.toString(), Boolean.toString(isNodeSyncEnabled))
                          .put(Property.DATA_SIZE.toString(), Long.toString(dataSize.in(SizeUnit.BYTES)))
                          .put(Property.DEADLINE_TARGET.toString(), Long.toString(deadlineTarget.in(TimeUnit.SECONDS)))
                          .build();
        }

        static TableInfo fromStringMap(Map<String, String> m)
        {
            return new TableInfo(get(Property.KEYSPACE, m),
                                 get(Property.TABLE, m),
                                 Integer.parseInt(get(Property.REPLICATION_FACTOR, m)),
                                 Boolean.parseBoolean(get(Property.NODESYNC_ENABLED, m)),
                                 SizeValue.of(Long.parseLong(get(Property.DATA_SIZE, m)), SizeUnit.BYTES),
                                 TimeValue.of(Long.parseLong(get(Property.DEADLINE_TARGET, m)), TimeUnit.SECONDS));
        }

        public TableInfo withNewDeadline(TimeValue newDeadlineTarget)
        {
            return new TableInfo(keyspace, table, replicationFactor, isNodeSyncEnabled, dataSize, newDeadlineTarget);
        }

        public TableInfo withNodeSyncEnabled()
        {
            return new TableInfo(keyspace, table, replicationFactor, true, dataSize, deadlineTarget);
        }

        public TableInfo withoutNodeSyncEnabled()
        {
            return new TableInfo(keyspace, table, replicationFactor, false, dataSize, deadlineTarget);
        }

        private static String get(Property p, Map<String, String> m)
        {
            String v = m.get(p.toString());
            if (v == null)
                throw new IllegalArgumentException(String.format("Missing mandatory property '%s' in TableInfo map: %s", p, m));
            return v;
        }

        @Override
        public String toString()
        {
            return String.format("%s[rf=%d, nodesync=%b, size=%s, deadline=%s",
                                 tableName(), replicationFactor, isNodeSyncEnabled, dataSize, deadlineTarget);
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof TableInfo))
                return false;

            TableInfo that = (TableInfo)other;
            return this.keyspace.equals(that.keyspace)
                   && this.table.equals(that.table)
                   && this.replicationFactor == that.replicationFactor
                   && this.isNodeSyncEnabled == that.isNodeSyncEnabled
                   && this.dataSize.equals(that.dataSize)
                   && this.deadlineTarget.equals(that.deadlineTarget);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(keyspace, table, replicationFactor, isNodeSyncEnabled, dataSize, deadlineTarget);
        }
    }
}
