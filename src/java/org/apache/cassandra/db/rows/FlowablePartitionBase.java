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
package org.apache.cassandra.db.rows;

import com.google.common.base.Throwables;

import io.reactivex.functions.Function;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.FlowSubscriber;

/**
 * Base class for the Flow versions of partitions.
 */
public abstract class FlowablePartitionBase<T> implements PartitionTrait
{
    /**
     * The header contains information about the partition: key, metadata etc.
     * Normally reused through transformations, merging, filtering etc.
     */
    public final PartitionHeader header;

    /**
     * The static part corresponding to this partition.
     */
    public final Row staticRow;

    /**
     * The partition's contents as a Flow. This must be subscribed to exactly once, and will close all
     * associated resources when the subscription completes (complete/error/cancel).
     */
    public final Flow<T> content;

    public FlowablePartitionBase(PartitionHeader header,
                                 Row staticRow,
                                 Flow<T> content)
    {
        this.header = header;
        this.staticRow = staticRow;
        this.content = content;
    }

    /**
     * Return a new partition with the specified header and static row, but the same content.
     *
     * @param header - the new header
     * @param staticRow - the new static row
     *
     * @return a new partition with the specified header and static row, but the same content.
     */
    public abstract FlowablePartitionBase<T> withHeader(PartitionHeader header, Row staticRow);

    /**
     * Return a new partition with the same header but different content.
     *
     * @param content the new content
     *
     * @return the partition with the new content
     */
    public abstract FlowablePartitionBase<T> withContent(Flow<T> content);

    /**
     * Apply the mapper to the partition content
     *
     * @param mappingOp the content mapper
     *
     * @return a new identical partition with the content mapped
     */
    public abstract FlowablePartitionBase<T> mapContent(Function<T, T> mappingOp);

    /**
     * Only to be called on requested but unused partitions (e.g. when aborting).
     * Since we usually verify one use only, this will throw if the partition was already used.
     */
    public void unused()
    {
        try
        {
            content.subscribe(new FlowSubscriber<T>()
            {
                public void onNext(T item)
                {
                    throw new AssertionError(); // We haven't requested, this should not be called.
                }

                public void onComplete()
                {
                    throw new AssertionError(); // We haven't requested, this should not be called.
                }

                public void onError(Throwable t)
                {
                    throw new AssertionError(); // We haven't requested, this should not be called.
                }
            }).close();
        }
        catch (Exception e)
        {
            throw Throwables.propagate(e);
        }
    }

    public TableMetadata metadata()
    {
        return header.metadata;
    }

    public boolean isReverseOrder()
    {
        return header.isReverseOrder;
    }

    public RegularAndStaticColumns columns()
    {
        return header.columns;
    }

    public DecoratedKey partitionKey()
    {
        return header.partitionKey;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return header.partitionLevelDeletion;
    }

    public EncodingStats stats()
    {
        return header.stats;
    }
}
