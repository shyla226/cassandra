/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.SystemTimeSource;
import org.apache.cassandra.utils.TimeSource;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.Units;

/**
 * Static helper methods for NodeSync.
 */
abstract class NodeSyncHelpers
{
    static final long NO_VALIDATION_TIME = Long.MIN_VALUE;

    private NodeSyncHelpers() {}

    // For unit testing, we sometime want to fake the size on disk of tables, as well as the local ranges of a keyspace
    // (some NodeSync unit tests only depend on those parameters and it's easier to provide it that way than to go
    // through the painful setup that would yield the same results). This is what the following static variables allow
    // us to achieve (note that we don't bother with volatile because those shouldn't be updated except for tests, and
    // for tests they are updated before tests are even started).

    private static ToLongFunction<ColumnFamilyStore> tableSizeProvider = NodeSyncHelpers::defaultTableSizeProvider;
    private static Function<String, Collection<Range<Token>>> localRangesProvider = NodeSyncHelpers::defaultLocalRangeProvider;

    /**
     * The target size for table segments. NodeSync will compute segments so that, in the hypothesis of perfect
     * distribution, segments are lower than this this. Of course, we won't have perfect distribution in practice,
     * so this is more a target size, but distribution should still be good enough in practice (or you will have
     * bigger problem than large NodeSync segments).
     */
    // TODO(Sylvain): Not sure how good of a default it is, could be worth some experimentation (but doesn't seem too bad either)
    private static final long DEFAULT_SEGMENT_SIZE_TARGET = Long.getLong("dse.nodesync.segment_size_target_bytes", SizeUnit.MEGABYTES.toBytes(200));
    private static long segmentSizeTarget = DEFAULT_SEGMENT_SIZE_TARGET;

    private static TimeSource timeSource = new SystemTimeSource();


    /**
     * Used by some unit tests to fake the size of table on disk and the local ranges.
     * Should <b>not</b> be used outside of tests.
     * Also, tests that do use that should make sure to use {@link #resetTestParameters} after the test.
     */
    @VisibleForTesting
    static void setTestParameters(ToLongFunction<ColumnFamilyStore> sizeProvider,
                                  Function<String, Collection<Range<Token>>> rangeProvider,
                                  long segmentSize,
                                  TimeSource time)
    {
        if (sizeProvider != null)
            tableSizeProvider = sizeProvider;
        if (rangeProvider != null)
            localRangesProvider = rangeProvider;
        if (segmentSize >= 0)
            segmentSizeTarget = segmentSize;
        if (time != null)
            timeSource = time;
    }

    /**
     * Reset the providers to their default after it has been changed through {@link #setTestParameters}.
     * Should <b>not</b> be used outside of tests.
     */
    @VisibleForTesting
    static void resetTestParameters()
    {
        tableSizeProvider = NodeSyncHelpers::defaultTableSizeProvider;
        localRangesProvider = NodeSyncHelpers::defaultLocalRangeProvider;
        segmentSizeTarget = DEFAULT_SEGMENT_SIZE_TARGET;
        timeSource = new SystemTimeSource();
    }

    // Default method used for get the local range. Note: the only reason we don't directly refer to
    // StorageService.instance::getLocalRanges directly in the static field initialization of this class is that this
    // make class initialization trigger StorageService initialization and that's annoying for some tests which do
    // initialize this class but don't use this in particular.
    private static Collection<Range<Token>> defaultLocalRangeProvider(String str)
    {
        return StorageService.instance.getLocalRanges(str);
    }

    private static long defaultTableSizeProvider(ColumnFamilyStore t)
    {
        return t.getMemtablesLiveSize() + t.metric.liveDiskSpaceUsed.getCount();
    }

    static long segmentSizeTarget()
    {
        return segmentSizeTarget;
    }

    static TimeSource time()
    {
        return timeSource;
    }

    /**
     * Return the size of the data the node is currently holding for the table.
     * <p>
     * Note that this is based on disk space usage, so doesn't directly account for 'user data' size (both because of
     * serialization overhead, but also because it doesn't "merge" data). This also account for memtable, though it does
     * count 'user data' in that case. As such, this should really be considered as an estimate (but this is the best
     * we can currently easily/cheaply do). Note that NodeSync uses this to estimate how much it will have validate for
     * the table and a decent estimation is enough for those use case (this also actually always  over-estimate the data
     * to validate, so it tends to make our estimations conservative, which is not a bad thing).
     *
     * @param table the table for which to return the size.
     * @return an estimate of the size of the data currently stored by this node for {@code table}.
     */
    static long estimatedSizeOf(ColumnFamilyStore table)
    {
        return tableSizeProvider.applyAsLong(table);
    }

    /**
     * The local ranges for the provided keyspace on this node.
     * <p>
     * This is delegating to {@link StorageService#getLocalRanges} in practice, but this should be used within NodeSync
     * instead of the latter method for the sake of making some unit testing easier.
     *
     * @param keyspace the keyspace for which to get the local ranges.
     * @return the local ranges for {@code keyspace}.
     */
    static Collection<Range<Token>> localRanges(String keyspace)
    {
        return localRangesProvider.apply(keyspace);
    }

    /**
     * Lists the tables on which NodeSync is enabled and runnable (meaning that they are not in a keyspace with RF <= 1).
     *
     * @return a stream of the {@link ColumnFamilyStore} of every tables that NodeSync should be validating.
     */
    static Stream<TableMetadata> nodeSyncEnabledTables()
    {
        return nodeSyncEnabledStores().map(ColumnFamilyStore::metadata);
    }

    /**
     * Same as {@link #nodeSyncEnabledTables()} but return the {@link ColumnFamilyStore} instead of the metadata.
     */
    static Stream<ColumnFamilyStore> nodeSyncEnabledStores()
    {
        return StorageService.instance.getNonSystemKeyspaces()
                                      .stream()
                                      .map(Keyspace::open)
                                      .flatMap(NodeSyncHelpers::nodeSyncEnabledStores);
    }

    /**
     * Lists the tables of the provided keyspace on which NodeSync is enabled and runnable (that is, if the keyspace has
     * RF <= 1, this will return an empty stream).
     *
     * @param keyspace the keyspace for which to list NodeSync enabled tables.
     * @return a stream of the {@link ColumnFamilyStore} of every tables in {@code keyspace} that NodeSync should be validating.
     */
    static Stream<TableMetadata> nodeSyncEnabledTables(Keyspace keyspace)
    {
        return nodeSyncEnabledStores(keyspace).map(ColumnFamilyStore::metadata);
    }

    /**
     * Same as {@link #nodeSyncEnabledTables(Keyspace)} but return the {@link ColumnFamilyStore} instead of the metadata.
     */
    static Stream<ColumnFamilyStore> nodeSyncEnabledStores(Keyspace keyspace)
    {
        if (!isReplicated(keyspace))
            return Stream.empty();

        return keyspace.getColumnFamilyStores()
                       .stream()
                       .filter(s -> s.metadata().params.nodeSync.isEnabled(s.metadata()));
    }

    private static boolean isReplicated(Keyspace keyspace)
    {
        return keyspace.getReplicationStrategy().getReplicationFactor() > 1 && !localRanges(keyspace.getName()).isEmpty();
    }

    /**
     * Returns whether NodeSync is enabled on the provided table.
     * <p>
     * Note that check both if NodeSync is enabled as a table option, but also that the table is replicated and so that
     * NodeSync will concretely run on it.
     *
     * @param store the table to check.
     * @return {@code true} is the table is/will be validated by NodeSync, {@code false} otherwise.
     */
    static boolean isNodeSyncEnabled(ColumnFamilyStore store)
    {
        return StorageService.instance.getTokenMetadata().getAllEndpoints().size() > 1
               && isReplicated(store.keyspace)
               && store.metadata().params.nodeSync.isEnabled(store.metadata());
    }

    /**
     * Creates a string used for pretty printing when a validation happened base on the provided time of said validation.
     */
    static String sinceStr(long validationTimeMs)
    {
        if (validationTimeMs < 0)
            return "<no validation recorded>";

        long now = NodeSyncHelpers.time().currentTimeMillis();
        return String.format("%s ago", Units.toString(now - validationTimeMs, TimeUnit.MILLISECONDS));
    }
}
