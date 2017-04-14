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

package org.apache.cassandra.db.rows.publisher;

import java.util.concurrent.Callable;


import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;
import io.reactivex.functions.Function3;
import org.apache.cassandra.db.rows.PartitionTrait;
import org.apache.cassandra.db.rows.Unfiltered;

/**
 * A set of callbacks to be implemented for reducing partitions into a single final result, see
 * {@link PartitionsPublisher#reduce(ReduceCallbacks)}.
 * <p>
 * The initial callback, the result supplier, is responsible for creating the final result. The second
 * callback, the partition supplier, receives a partition and creates an intermediate result specific
 * to this partition. The third callback, the item reducer, reduces an unfiltered using the two result.
 * The last callback, the partition reducer, reduces the partition using the two results.
 * <p>
 * For an example of how to use these callbacks see
 * {@link org.apache.cassandra.db.partitions.ImmutableBTreePartition#create(PartitionsPublisher)}.
 * <p>
 * R1 - the final result, e.g. a list of {@link org.apache.cassandra.db.partitions.ImmutableBTreePartition}
 * R2 - the result for each partition, e.g. a {@link org.apache.cassandra.utils.btree.BTree.Builder}
 *
 */
public class ReduceCallbacks<R1, R2>
{
    /** Supplies the final result. */
    final Callable<R1> resultSupplier;

    /** Given the final result and a partition, supplies an intermediate result
     * specific to this partition, or null to ignore the partition */
    final BiFunction<R1, PartitionTrait, R2> partitionSupplier;

    /** Given the final result, the intermediate partition result and an item, reduces the item and returns a new
     * intermediate result for the partition or null to ignore the rest of the partition*/
    final Function3<R1, R2, ? super Unfiltered, R2> itemReducer;

    /** Given the final result, the intermediate partition result and the partition iteself, reduces the partition
     * and returns a new final result */
    final Function3<R1, R2, PartitionTrait, R1> partitionReducer;

    public ReduceCallbacks(Callable<R1> resultSupplier,
                           BiFunction<R1, PartitionTrait, R2> partitionSupplier,
                           Function3<R1, R2, ? super Unfiltered, R2> itemReducer,
                           Function3<R1, R2, PartitionTrait, R1> partitionReducer)
    {
        this.resultSupplier = resultSupplier;
        this.partitionSupplier = partitionSupplier;
        this.itemReducer = itemReducer;
        this.partitionReducer = partitionReducer;
    }

    /**
     * Return a set of callbacks for the trivial case where a single result builder is all that's required
     * for the client to consume partitions and unfiltered items.
     *
     * @param result - the result needed by the callbacks to reduce partitions and items
     * @param partitionConsumer - the partition reducer
     * @param unfilteredConsumer - the item reducer
     * @param <R> - the type of result
     * @return - a set of callbacks to be used when calling {@link PartitionsPublisher#reduce(ReduceCallbacks)}
     */
    public static <R> ReduceCallbacks<R, R> trivial(R result,
                                                    BiFunction<R, PartitionTrait, R> partitionConsumer,
                                                    BiFunction<R, Unfiltered, R> unfilteredConsumer)
    {
        return new ReduceCallbacks<>(() -> result,
                                     (r1, partition) -> partitionConsumer.apply(result, partition),
                                     (r1, r2, unfiltered) -> unfilteredConsumer.apply(result, unfiltered),
                                     (r1, r2, partition) -> result);
    }
}
