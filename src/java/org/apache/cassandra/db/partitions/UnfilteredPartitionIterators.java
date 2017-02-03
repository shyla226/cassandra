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

import java.io.IOError;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.*;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.Single;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators.MergeListener;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.FilteredPartitions;
import org.apache.cassandra.db.transform.MorePartitions;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Reducer;

/**
 * Static methods to work with partition iterators.
 */
public abstract class UnfilteredPartitionIterators
{
    private static final Serializer serializer = new Serializer();

    private static final Comparator<Single<UnfilteredRowIterator>> partitionComparator = (x, y) -> x.blockingGet().partitionKey().compareTo(y.blockingGet().partitionKey());
//            Comparator.comparing(BaseRowIterator::partitionKey);

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
    public static Single<UnfilteredRowIterator> getOnlyElement(final UnfilteredPartitionIterator iter, SinglePartitionReadCommand command)
    {
        // If the query has no results, we'll get an empty iterator, but we still
        // want a RowIterator out of this method, so we return an empty one.
        Single<UnfilteredRowIterator> toReturn = iter.hasNext()
                              ? iter.next()
                              : Single.just(EmptyIterators.unfilteredRow(command.metadata(),
                                                             command.partitionKey(),
                                                             command.clusteringIndexFilter().isReversed()));

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
        return toReturn.map(t -> Transformation.apply(t, new Close()));
    }

    public static UnfilteredPartitionIterator concat(final List<UnfilteredPartitionIterator> iterators)
    {
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

        final CFMetaData metadata = iterators.get(0).metadata();
        final UnfilteredRowIterator emptyIterator = EmptyIterators.unfilteredRow(metadata, null, false);

        final MergeIterator<Single<UnfilteredRowIterator>, Single<UnfilteredRowIterator>> merged = MergeIterator.get(iterators, partitionComparator, new Reducer<Single<UnfilteredRowIterator>, Single<UnfilteredRowIterator>>()
        {
            private final List<UnfilteredRowIterator> toMerge = new ArrayList<>(iterators.size());

            private DecoratedKey partitionKey;
            private boolean isReverseOrder;

            public void reduce(int idx, Single<UnfilteredRowIterator> current)
            {
                UnfilteredRowIterator it = current.blockingGet();

                partitionKey = it.partitionKey();
                isReverseOrder = it.isReverseOrder();

                // Note that because the MergeListener cares about it, we want to preserve the index of the iterator.
                // Non-present iterator will thus be set to empty in getReduced.
                toMerge.set(idx, it);
            }

            protected Single<UnfilteredRowIterator> getReduced()
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

                return Single.just(numNonEmptyRowIterators == 1 && !listener.callOnTrivialMerge() ? nonEmptyRowIterator : UnfilteredRowIterators.merge(toMerge, nowInSec, rowListener));
            }

            protected void onKeyChange()
            {
                toMerge.clear();
                for (int i = 0, length = iterators.size(); i < length; i++)
                    toMerge.add(null);
            }
        });

        return new AbstractUnfilteredPartitionIterator()
        {
            public CFMetaData metadata()
            {
                return metadata;
            }

            public boolean hasNext()
            {
                return merged.hasNext();
            }

            public Single<UnfilteredRowIterator> next()
            {
                return merged.next();
            }

            @Override
            public void close()
            {
                merged.close();
            }
        };
    }

    public static UnfilteredPartitionIterator mergeLazily(final List<? extends UnfilteredPartitionIterator> iterators, final int nowInSec)
    {
        // Merge is already lazy (in the sense we only do it if the Single is requested).
        return merge(iterators, nowInSec, MergeListener.NONE);
    }

    /**
     * Digests the the provided iterator.
     *
     * @param iterator the iterator to digest.
     * @param digest the {@code MessageDigest} to use for the digest.
     * @param version the messaging protocol to use when producing the digest.
     */
    public static Completable digest(UnfilteredPartitionIterator iterator, MessageDigest digest, int version)
    {
        return iterator.asObservable().flatMapCompletable(i -> {
            UnfilteredRowIterators.digest(i, digest, version);
            return CompletableObserver::onComplete;
        });
    }

    public static Serializer serializerForIntraNode()
    {
        return serializer;
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

    /**
     * Serialize each UnfilteredSerializer one after the other, with an initial byte that indicates whether
     * we're done or not.
     */
    public static class Serializer
    {
        public Completable serialize(UnfilteredPartitionIterator iter, ColumnFilter selection, DataOutputPlus out, int version) throws IOException
        {
            // Previously, a boolean indicating if this was for a thrift query.
            // Unused since 4.0 but kept on wire for compatibility.

            return Completable.fromAction(() -> out.writeBoolean(false))
                              .concatWith(iter.asObservable()
                                              .flatMapCompletable(partition ->
                                                                  {
                                                                      out.writeBoolean(true);
                                                                      UnfilteredRowIteratorSerializer.serializer.serialize(partition, selection, out, version);
                                                                      return CompletableObserver::onComplete;
                                                                  }))
                              .concatWith(Completable.fromAction(() -> out.writeBoolean(false)));
        }

        public UnfilteredPartitionIterator deserialize(final DataInputPlus in, final int version, final CFMetaData metadata, final ColumnFilter selection, final SerializationHelper.Flag flag) throws IOException
        {
            // Skip now unused isForThrift boolean
            in.readBoolean();

            return new AbstractUnfilteredPartitionIterator()
            {
                private UnfilteredRowIterator next;
                private boolean hasNext;
                private boolean nextReturned = true;

                public CFMetaData metadata()
                {
                    return metadata;
                }

                public boolean hasNext()
                {
                    if (!nextReturned)
                        return hasNext;

                    // We can't answer this until the previously returned iterator has been fully consumed,
                    // so complain if that's not the case.
                    if (next != null && next.hasNext())
                        throw new IllegalStateException("Cannot call hasNext() until the previous iterator has been fully consumed");

                    try
                    {
                        hasNext = in.readBoolean();
                        nextReturned = false;
                        return hasNext;
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                }

                public Single<UnfilteredRowIterator> next()
                {
                    if (nextReturned && !hasNext())
                        throw new NoSuchElementException();

                    try
                    {
                        nextReturned = true;
                        next = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, metadata, selection, flag);
                        return Single.just(next);
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                }

                @Override
                public void close()
                {
                    if (next != null)
                        next.close();
                }
            };
        }
    }
}
