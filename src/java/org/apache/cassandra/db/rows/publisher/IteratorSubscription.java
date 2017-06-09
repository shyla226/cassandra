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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Single;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.subjects.BehaviorSubject;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractBTreePartition;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.PartitionTrait;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.btree.BTree;

/**
 * Subscribe to {@link PartitionsPublisher} to convert asynchronous events back to an iterator.
 * This is required only for backwards compatibility, to avoid making all code asynchronous in one go.
 * It should be limited to tests.
 */
class IteratorSubscription implements UnfilteredPartitionIterator, PartitionsSubscriber<Unfiltered>
{
    private static final Logger logger = LoggerFactory.getLogger(IteratorSubscription.class);

    private static final int INITIAL_ROW_CAPACITY = ImmutableBTreePartition.INITIAL_ROW_CAPACITY;

    private final static UnfilteredRowIterator POISON_PILL = new AbstractUnfilteredRowIterator(PartitionData.EMPTY)
    {
        protected Unfiltered computeNext()
        {
            return endOfData();
        }
    };

    private final BlockingQueue<UnfilteredRowIterator> queue;
    private final TableMetadata metadata;
    private final BehaviorSubject<UnfilteredPartitionIterator> subject;

    private PartitionsSubscription subscription;
    private Throwable error = null;
    private UnfilteredRowIterator next;

    private PartitionTrait partition;
    private Pair<BTree.Builder, MutableDeletionInfo.Builder> partitionBuilders;

    IteratorSubscription(TableMetadata metadata)
    {
        this.queue = new LinkedBlockingDeque<>();
        this.metadata = metadata;
        this.subject = BehaviorSubject.create();
    }

    public Single<UnfilteredPartitionIterator> toSingle()
    {
        return subject.firstOrError();
    }

    public TableMetadata metadata()
    {
        return metadata;
    }

    public void close()
    {
        subscription.close();
    }

    public void onSubscribe(PartitionsSubscription subscription)
    {
        this.subscription = subscription;
    }

    public void onNextPartition(PartitionTrait partition)
    {
        publishPartition();

        this.partition = partition;
        this.partitionBuilders = AbstractBTreePartition.getBuilders(partition, INITIAL_ROW_CAPACITY, true);
    }

    public void onNext(Unfiltered item)
    {
        assert partitionBuilders != null;
        AbstractBTreePartition.addUnfiltered(partitionBuilders, item);
    }

    private void publishPartition()
    {
        if (partition == null)
            return;

        assert partitionBuilders != null;
        Uninterruptibles.putUninterruptibly(queue,ImmutableBTreePartition.create(partition, partitionBuilders)
                                                                         .unfilteredIterator(ColumnFilter.selection(partition.columns()),
                                                                                             Slices.ALL,
                                                                                             partition.isReverseOrder()));

        partitionBuilders = null;
        partition = null;
    }

    public void onComplete()
    {
        publishPartition();
        Uninterruptibles.putUninterruptibly(queue, POISON_PILL);

        // signal any delayed observers, see PartitionsPublisher.toDelayedIterator
        subject.onNext(this);
    }

    public void onError(Throwable error)
    {
        publishPartition();
        this.error = error;
        Uninterruptibles.putUninterruptibly(queue, POISON_PILL);

        // signal any delayed observers, see PartitionsPublisher.toDelayedIterator
        // note that we want to publish the iterator and not the error, which will
        // be thrown during iteration
        subject.onNext(this);
    }

    protected UnfilteredRowIterator computeNext()
    {
        if (next != null)
        {
            maybePropagateError();
            return next;
        }

        next = Uninterruptibles.takeUninterruptibly(queue);
        maybePropagateError();
        return next;
    }

    private void maybePropagateError()
    {
        if (next == POISON_PILL && error != null)
        {
            Throwable t = error;
            error = null;
            throw Exceptions.propagate(t);
        }
    }

    @Override
    public boolean hasNext()
    {
        return computeNext() != POISON_PILL || error != null;
    }

    @Override
    public UnfilteredRowIterator next()
    {
        boolean hasNext = hasNext();
        assert hasNext;

        UnfilteredRowIterator ret = next;
        next = null;
        return ret;
    }
}
