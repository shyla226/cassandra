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
package org.apache.cassandra.db.partitions;

import java.security.MessageDigest;
import java.util.*;

import io.reactivex.Completable;
import io.reactivex.Single;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.FilteredPartitions;
import org.apache.cassandra.db.transform.MorePartitions;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.flow.CsFlow;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Reducer;

/**
 * Static methods to work with partition iterators.
 */
public abstract class UnfilteredPartitionIterators
{
    private static final Comparator<UnfilteredRowIterator> partitionComparator = Comparator.comparing(BaseRowIterator::partitionKey);

    private UnfilteredPartitionIterators() {}

    public interface MergeListener
    {
        public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions);

        /**
         * Forces merge listener to be called even when there is only
         * a single iterator.
         * <p>
         * This can be useful for listeners that require seeing all row updates.
         */
        public default boolean callOnTrivialMerge()
        {
            return true;
        }

        public void close();

        public static final MergeListener NONE = new MergeListener()
        {
            public org.apache.cassandra.db.rows.UnfilteredRowIterators.MergeListener getRowMergeListener(
                    DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
            {
                return null;
            }

            public boolean callOnTrivialMerge()
            {
                return false;
            }

            public void close()
            {
            }
        };
    }

    @SuppressWarnings("resource") // The created resources are returned right away
    public static UnfilteredRowIterator getOnlyElement(final UnfilteredPartitionIterator iter, SinglePartitionReadCommand command)
    {
        // If the query has no results, we'll get an empty iterator, but we still
        // want a RowIterator out of this method, so we return an empty one.
        UnfilteredRowIterator toReturn = iter.hasNext()
                              ? iter.next()
                              : EmptyIterators.unfilteredRow(command.metadata(),
                                                             command.partitionKey(),
                                                             command.clusteringIndexFilter().isReversed());

        // Note that in general, we should wrap the result so that it's close method actually
        // close the whole UnfilteredPartitionIterator.
        class Close extends Transformation
        {
            public void onPartitionClose()
            {
                // asserting this only now because it bothers Serializer if hasNext() is called before
                // the previously returned iterator hasn't been fully consumed.
                boolean hadNext = iter.hasNext();
                iter.close();
                assert !hadNext;
            }
        }
        return Transformation.apply(toReturn, new Close());
    }

    public static UnfilteredPartitionIterator concat(final List<UnfilteredPartitionIterator> iterators, TableMetadata metadata)
    {
        if (iterators.isEmpty())
            return EmptyIterators.unfilteredPartition(metadata);

        if (iterators.size() == 1)
            return iterators.get(0);

        class Extend implements MorePartitions<UnfilteredPartitionIterator>
        {
            int i = 1;
            public UnfilteredPartitionIterator moreContents()
            {
                if (i >= iterators.size())
                    return null;
                return iterators.get(i++);
            }
        }

        return MorePartitions.extend(iterators.get(0), new Extend());
    }

    public static Single<UnfilteredPartitionIterator> concat(final Iterable<Single<UnfilteredPartitionIterator>> iterators)
    {
        Iterator<Single<UnfilteredPartitionIterator>> it = iterators.iterator();
        assert it.hasNext();
        Single<UnfilteredPartitionIterator> it0 = it.next();
        if (!it.hasNext())
            return it0;

        class Extend implements MorePartitions<UnfilteredPartitionIterator>
        {
            public UnfilteredPartitionIterator moreContents()
            {
                if (!it.hasNext())
                    return null;
                return it.next().blockingGet();
            }
        }
        return it0.map(i -> MorePartitions.extend(i, new Extend()));
    }

    public static PartitionIterator mergeAndFilter(List<UnfilteredPartitionIterator> iterators, int nowInSec, MergeListener listener)
    {
        // TODO: we could have a somewhat faster version if we were to merge the UnfilteredRowIterators directly as RowIterators
        return filter(merge(iterators, nowInSec, listener), nowInSec);
    }

    public static PartitionIterator filter(final UnfilteredPartitionIterator iterator, final int nowInSec)
    {
        return FilteredPartitions.filter(iterator, nowInSec);
    }

    public static UnfilteredPartitionIterator merge(final List<? extends UnfilteredPartitionIterator> iterators, final int nowInSec, final MergeListener listener)
    {
        assert listener != null;
        assert !iterators.isEmpty();

        final TableMetadata metadata = iterators.get(0).metadata();

        // TODO review the merge of this
        UnfilteredMergeReducer reducer = new UnfilteredMergeReducer(metadata, iterators.size(), nowInSec, listener);
        final MergeIterator<UnfilteredRowIterator, UnfilteredRowIterator> merged = MergeIterator.get(iterators, partitionComparator, reducer);

        return new AbstractUnfilteredPartitionIterator()
        {
            public TableMetadata metadata()
            {
                return metadata;
            }

            public boolean hasNext()
            {
                return merged.hasNext();
            }

            public UnfilteredRowIterator next()
            {
                return merged.next();
            }

            @Override
            public void close()
            {
                merged.close();
                listener.close();
            }
        };
    }

    static class UnfilteredMergeReducer extends Reducer<UnfilteredRowIterator, UnfilteredRowIterator>
    {
        private final int numIterators;
        private final int nowInSec;
        private final MergeListener listener;
        private final List<UnfilteredRowIterator> toMerge;
        private final UnfilteredRowIterator emptyIterator;

        private DecoratedKey partitionKey;
        private boolean isReverseOrder;

        UnfilteredMergeReducer(TableMetadata metadata, int numIterators, int nowInSec, final MergeListener listener)
        {
            this.numIterators = numIterators;
            this.nowInSec = nowInSec;
            this.listener = listener;

            emptyIterator = EmptyIterators.unfilteredRow(metadata, null, false);
            toMerge = new ArrayList<>(numIterators);
        }

        public void reduce(int idx, UnfilteredRowIterator current)
        {
            partitionKey = current.partitionKey();
            isReverseOrder = current.isReverseOrder();

            // Note that because the MergeListener cares about it, we want to preserve the index of the iterator.
            // Non-present iterator will thus be set to empty in getReduced.
            toMerge.set(idx, current);
        }

        public UnfilteredRowIterator getReduced()
        {
            UnfilteredRowIterators.MergeListener rowListener = listener.getRowMergeListener(partitionKey, toMerge);

            ((EmptyIterators.EmptyUnfilteredRowIterator)emptyIterator).reuse(partitionKey, isReverseOrder, DeletionTime.LIVE);

            // Replace nulls by empty iterators
            UnfilteredRowIterator nonEmptyRowIterator = null;
            int numNonEmptyRowIterators = 0;

            for (int i = 0, length = toMerge.size(); i < length; i++)
            {
                UnfilteredRowIterator element = toMerge.get(i);
                if (element == null)
                {
                    toMerge.set(i, emptyIterator);
                }
                else
                {
                    numNonEmptyRowIterators++;
                    nonEmptyRowIterator = element;
                }
            }

            return numNonEmptyRowIterators == 1 && !listener.callOnTrivialMerge() ? nonEmptyRowIterator : UnfilteredRowIterators.merge(toMerge, nowInSec, rowListener);
        }

        public void onKeyChange()
        {
            toMerge.clear();
            for (int i = 0, length = numIterators; i < length; i++)
                toMerge.add(null);
        }
    }

    /**
     * Digests the the provided iterator.
     *
     * @param iterator the iterator to digest.
     * @param digest the {@code MessageDigest} to use for the digest.
     * @param version the version to use when producing the digest.
     */
    public static Completable digest(UnfilteredPartitionIterator iterator, MessageDigest digest, DigestVersion version)
    {
        return Completable.fromAction(() ->
          {
              try (UnfilteredPartitionIterator iter = iterator)
              {
                  while (iter.hasNext())
                  {
                      try (UnfilteredRowIterator partition = iter.next())
                      {
                          UnfilteredRowIterators.digest(partition, digest, version);
                      }
                  }
              }
          });
    }

    /**
     * Digests the the provided partition flow.
     *
     * @param partitions the partitions to digest.
     * @param digest the {@code MessageDigest} to use for the digest.
     * @param version the version to use when producing the digest.
     */
    public static CsFlow<Void> digest(CsFlow<FlowableUnfilteredPartition> partitions, MessageDigest digest, DigestVersion version)
    {
        return partitions.flatProcess(partition -> UnfilteredRowIterators.digest(partition, digest, version));
    }

    /**
     * Wraps the provided iterator so it logs the returned rows/RT for debugging purposes.
     * <p>
     * Note that this is only meant for debugging as this can log a very large amount of
     * logging at INFO.
     */
    public static UnfilteredPartitionIterator loggingIterator(UnfilteredPartitionIterator iterator, final String id, final boolean fullDetails)
    {
        class Logging extends Transformation<UnfilteredRowIterator>
        {
            public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
            {
                return UnfilteredRowIterators.loggingIterator(partition, id, fullDetails);
            }
        }
        return Transformation.apply(iterator, new Logging());
    }


}
