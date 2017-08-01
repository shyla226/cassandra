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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A simple helper that caches the average row size of tables validated by NodeSync for the sake of converting page
 * in bytes to page size in rows until we implement APOLLO-414.
 * <p>
 * We use a cache because getting said average row size iterates over all sstables and we really don't need to
 * recompute for every segment validation.
 * <p>
 * This only exists as a hack until we implement APOLLO-414, so should be removed once that lends.
 */
class TableRowSizeEstimator
{
    final static TableRowSizeEstimator instance = new TableRowSizeEstimator();

    // Expires values to handle table drop (or if nodesync is disabled on the table and we don't need this anymore (we
    // could make that a SchemaChangeListener instead but given the lack of information we currently have in that
    // listener methods, this would be a lot more complex for little benefits).
    // Also refresh from time to time because things can change and we need to account for it.
    private final LoadingCache<TableId, Integer> cachedSize = CacheBuilder.newBuilder()
                                                                          .expireAfterAccess(10, TimeUnit.MINUTES)
                                                                          .refreshAfterWrite(30, TimeUnit.MINUTES)
                                                                          .build(new Loader());

    public int getAverageRowSize(TableMetadata table)
    {
        try
        {
            return cachedSize.get(table.id);
        }
        catch (ExecutionException e)
        {
            // We don't throw in the loader, so this isn't supposed to happen.
            throw new AssertionError(e);
        }
    }

    private static class Loader extends CacheLoader<TableId, Integer>
    {
        public Integer load(TableId tableId) throws Exception
        {
            ColumnFamilyStore store = ColumnFamilyStore.getIfExists(tableId);
            // Can be null if the table has been dropped concurrently so simply return a random page size (not using 0
            // though cause it's not a valid page size and no point in risking to trigger an assertion).
            if (store == null)
                return 1;

            int averageRowSize = store.getAverageRowSize();
            // We'l get 0 if there is no data in the table yet (though this doesn't fully guarantee we'll have nothing to
            // validate since memtables are not taking into account). Anyway, we need to come up with a value here so
            // using 200 bytes semi-randomly (there can certainly be smaller and bigger rows, but a row with 6-7
            // columns, including a few big-ish values (maybe small collections) and adding some encoding overhead, feels
            // reasonable)). Of course we really want to implement APOLLO-414 here and get rid of those guess-estimates.
            return averageRowSize == 0 ? 200 : averageRowSize;
        }
    }
}



