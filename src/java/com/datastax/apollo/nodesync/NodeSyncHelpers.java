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
package com.datastax.apollo.nodesync;

import java.util.Collection;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

/**
 * Static helper methods for NodeSync.
 */
abstract class NodeSyncHelpers
{
    private NodeSyncHelpers() {}

    // For unit testing, we sometime want to fake the size on disk of tables, as well as the local ranges of a keyspace
    // (some NodeSync unit tests only depend on those parameters and it's easier to provide it that way than to go
    // through the painful setup that would yield the same results). This is what the following static variables allow
    // us to achieve.

    private static final ToLongFunction<ColumnFamilyStore> DEFAULT_TABLE_SIZE_PROVIDER = t -> t.getMemtablesLiveSize() + t.metric.liveDiskSpaceUsed.getCount();
    private static final Function<String, Collection<Range<Token>>> DEFAULT_LOCAL_RANGES_PROVIDER = StorageService.instance::getLocalRanges;
    private static ToLongFunction<ColumnFamilyStore> tableSizeProvider = DEFAULT_TABLE_SIZE_PROVIDER;
    private static Function<String, Collection<Range<Token>>> localRangesProvider = DEFAULT_LOCAL_RANGES_PROVIDER;

    /**
     * Used by some unit tests to fake the size of table on disk and the local ranges.
     * Should <b>not</b> be used outside of tests.
     * Also, tests that do use that should make sure to use {@link #resetTableSizeAndLocalRangeProviders} after the test.
     */
    @VisibleForTesting
    static void setTableSizeAndLocalRangeProviders(ToLongFunction<ColumnFamilyStore> sizeProvider,
                                                   Function<String, Collection<Range<Token>>> rangeProvider)
    {
        tableSizeProvider = sizeProvider;
        localRangesProvider = rangeProvider;
    }

    /**
     * Reset the providers to their default after it has been changed through {@link #setTableSizeAndLocalRangeProviders}.
     * Should <b>not</b> be used outside of tests.
     */
    @VisibleForTesting
    static void resetTableSizeAndLocalRangeProviders()
    {
        tableSizeProvider = DEFAULT_TABLE_SIZE_PROVIDER;
        localRangesProvider = DEFAULT_LOCAL_RANGES_PROVIDER;
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
    static Stream<ColumnFamilyStore> nodeSyncEnabledTables()
    {
        return StorageService.instance.getNonSystemKeyspaces()
                                      .stream()
                                      .map(Keyspace::open)
                                      .flatMap(NodeSyncHelpers::nodeSyncEnabledTables);
    }

    /**
     * Lists the tables of the provided keyspace on which NodeSync is enabled and runnable (that is, if the keyspace has
     * RF <= 1, this will return an empty stream).
     *
     * @param keyspace the keyspace for which to list NodeSync enabled tables.
     * @return a stream of the {@link ColumnFamilyStore} of every tables in {@code keyspace} that NodeSync should be validating.
     */
    static Stream<ColumnFamilyStore> nodeSyncEnabledTables(Keyspace keyspace)
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
        return isReplicated(store.keyspace) && store.metadata().params.nodeSync.isEnabled(store.metadata());
    }
}
