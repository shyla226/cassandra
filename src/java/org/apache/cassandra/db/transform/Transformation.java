/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.rows.publisher.PartitionsPublisher;
import org.apache.cassandra.utils.flow.CsFlow;
import org.apache.cassandra.utils.flow.CsSubscriber;
import org.apache.cassandra.utils.flow.CsSubscription;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * We have a single common superclass for all Transformations to make implementation efficient.
 * we have a shared stack for all transformations, and can share the same transformation across partition and row
 * iterators, reducing garbage. Internal code is also simplified by always having a basic no-op implementation to invoke.
 *
 * Only the necessary methods need be overridden. Early termination is provided by invoking the method's stop or stopInPartition
 * methods, rather than having their own abstract method to invoke, as this is both more efficient and simpler to reason about.
 */
public abstract class Transformation<I extends BaseRowIterator<?>>
{
    // internal methods for StoppableTransformation only
    public void attachTo(PartitionsPublisher publisher) { }
    void attachTo(BasePartitions partitions) { }
    void attachTo(BaseRows rows) { }

    /**
     * Run on the close of any (logical) partitions iterator this function was applied to
     *
     * We stipulate logical, because if applied to a transformed iterator the lifetime of the iterator
     * object may be longer than the lifetime of the "logical" iterator it was applied to; if the iterator
     * is refilled with MoreContents, for instance, the iterator may outlive this function
     */
    public void onClose() { }

    /**
     * Run on the close of any (logical) rows iterator this function was applied to
     *
     * We stipulate logical, because if applied to a transformed iterator the lifetime of the iterator
     * object may be longer than the lifetime of the "logical" iterator it was applied to; if the iterator
     * is refilled with MoreContents, for instance, the iterator may outlive this function
     */
    public void onPartitionClose() { }

    /**
     * Applied to any rows iterator (partition) we encounter in a partitions iterator
     */
    protected I applyToPartition(I partition)
    {
        return partition;
    }

    /**
     * Applied to any partition we encounter in a partitions iterator.
     * Normally includes a transformation of the partition through Transformation.apply(partition, this).
     */
    public FlowableUnfilteredPartition applyToPartition(FlowableUnfilteredPartition partition)
    {
        return Transformation.apply(partition, this);
    }

    /**
     * Applied to any row we encounter in a rows iterator
     */
    protected Row applyToRow(Row row)
    {
        return row;
    }

    /**
     * Applied to any RTM we encounter in a rows/unfiltered iterator
     */
    protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
    {
        return marker;
    }

    /**
     * Applied to the partition key of any rows/unfiltered iterator we are applied to
     */
    protected DecoratedKey applyToPartitionKey(DecoratedKey key) { return key; }

    /**
     * Applied to the static row of any rows iterator.
     *
     * NOTE that this is only applied to the first iterator in any sequence of iterators filled by a MoreContents;
     * the static data for such iterators is all expected to be equal
     */
    public Row applyToStatic(Row row)
    {
        return row;
    }

    /**
     * Applied to the partition-level deletion of any rows iterator.
     *
     * NOTE that this is only applied to the first iterator in any sequence of iterators filled by a MoreContents;
     * the static data for such iterators is all expected to be equal
     */
    protected DeletionTime applyToDeletion(DeletionTime deletionTime)
    {
        return deletionTime;
    }

    /**
     * Applied to the {@code PartitionColumns} of any rows iterator.
     *
     * NOTE: same remark than for applyToDeletion: it is only applied to the first iterator in a sequence of iterators
     * filled by MoreContents.
     */
    protected RegularAndStaticColumns applyToPartitionColumns(RegularAndStaticColumns columns)
    {
        return columns;
    }

    public Unfiltered applyToUnfiltered(Unfiltered item)
    {
        if (item.isRow())
            return applyToRow((Row) item);
        else
            return applyToMarker((RangeTombstoneMarker) item);
    }

    // FlowableOp interpretation of transformation
    public void onNextUnfiltered(CsSubscriber<Unfiltered> subscriber, CsSubscription source, Unfiltered item)
    {
        Unfiltered next;
        try
        {
            next = applyToUnfiltered(item);
        }
        catch (Throwable t)
        {
            subscriber.onError(t);
            return;
        }

        if (next != null)
            subscriber.onNext(next);
        else
            source.request();
    }

    // FlowableOp interpretation of transformation
    public void onNextPartition(CsSubscriber<FlowableUnfilteredPartition> subscriber, CsSubscription source, FlowableUnfilteredPartition item)
    {
        FlowableUnfilteredPartition next;
        try
        {
            next = applyToPartition(item);
        }
        catch (Throwable t)
        {
            subscriber.onError(t);
            return;
        }

        if (next != null)
            subscriber.onNext(next);
        else
            source.request();
    }


    //******************************************************
    //          Static Application Methods
    //******************************************************


    public static UnfilteredPartitionIterator apply(UnfilteredPartitionIterator iterator, Transformation<? super UnfilteredRowIterator> transformation)
    {
        return add(mutable(iterator), transformation);
    }
    public static PartitionIterator apply(PartitionIterator iterator, Transformation<? super RowIterator> transformation)
    {
        return add(mutable(iterator), transformation);
    }
    public static UnfilteredRowIterator apply(UnfilteredRowIterator iterator, Transformation<?> transformation)
    {
        return add(mutable(iterator), transformation);
    }
    public static RowIterator apply(RowIterator iterator, Transformation<?> transformation)
    {
        return add(mutable(iterator), transformation);
    }

    static UnfilteredPartitions mutable(UnfilteredPartitionIterator iterator)
    {
        return iterator instanceof UnfilteredPartitions
               ? (UnfilteredPartitions) iterator
               : new UnfilteredPartitions(iterator);
    }
    static FilteredPartitions mutable(PartitionIterator iterator)
    {
        return iterator instanceof FilteredPartitions
               ? (FilteredPartitions) iterator
               : new FilteredPartitions(iterator);
    }
    static UnfilteredRows mutable(UnfilteredRowIterator iterator)
    {
        return iterator instanceof UnfilteredRows
               ? (UnfilteredRows) iterator
               : new UnfilteredRows(iterator);
    }
    static FilteredRows mutable(RowIterator iterator)
    {
        return iterator instanceof FilteredRows
               ? (FilteredRows) iterator
               : new FilteredRows(iterator);
    }

    static <E extends BaseIterator> E add(E to, Transformation add)
    {
        to.add(add);
        return to;
    }
    static <E extends BaseIterator> E add(E to, MoreContents add)
    {
        to.add(add);
        return to;
    }

    static class FlowableRowOp implements CsFlow.FlowableOp<Unfiltered, Unfiltered>
    {
        final Transformation transformation;

        FlowableRowOp(Transformation transformation)
        {
            this.transformation = transformation;
        }

        public void onNext(CsSubscriber<Unfiltered> subscriber, CsSubscription source, Unfiltered next)
        {
            transformation.onNextUnfiltered(subscriber, source, next);
        }

        public void close()
        {
            transformation.onPartitionClose();
        }
    }

    public static FlowableUnfilteredPartition apply(FlowableUnfilteredPartition src, Transformation transformation)
    {
        CsFlow<Unfiltered> content = src.content; // src.content.apply(new FlowableRowOp(transformation));
        FlowableUnfilteredPartition ret = new FlowableUnfilteredPartition(apply(src.header, transformation),
                                                                          transformation.applyToStatic(src.staticRow),
                                                                          content,
                                                                          src.hasData);

        if (transformation instanceof StoppingTransformation)
        {
            StoppingTransformation s = (StoppingTransformation) transformation;
            s.stopInPartition = ret.stop;
        }

        return ret;
    }

    static class FlowablePartitionOp implements CsFlow.FlowableOp<FlowableUnfilteredPartition,FlowableUnfilteredPartition>
    {
        final Transformation transformation;

        FlowablePartitionOp(Transformation transformation)
        {
            this.transformation = transformation;
        }

        public void onNext(CsSubscriber<FlowableUnfilteredPartition> subscriber, CsSubscription source, FlowableUnfilteredPartition next)
        {
            transformation.onNextPartition(subscriber, source, next);
        }

        public void close()
        {
            transformation.onClose();
        }
    }

    public static CsFlow<FlowableUnfilteredPartition> apply(CsFlow<FlowableUnfilteredPartition> src, Transformation transformation)
    {
        CsFlow<FlowableUnfilteredPartition> content = src.apply(new FlowablePartitionOp(transformation));

        if (transformation instanceof StoppingTransformation)
        {
            StoppingTransformation s = (StoppingTransformation) transformation;
            s.stop = new BaseIterator.Stop();
        }

        return content;
    }

    private static PartitionHeader apply(PartitionHeader header, Transformation transformation)
    {
        DecoratedKey key = transformation.applyToPartitionKey(header.partitionKey);
        RegularAndStaticColumns columns = transformation.applyToPartitionColumns(header.columns);
        DeletionTime dt = transformation.applyToDeletion(header.partitionLevelDeletion);
        if (dt != header.partitionLevelDeletion || columns != header.columns || key != header.partitionKey)
            return new PartitionHeader(header.metadata, key, dt, columns, header.isReverseOrder, header.stats);
        return header;
    }
}
