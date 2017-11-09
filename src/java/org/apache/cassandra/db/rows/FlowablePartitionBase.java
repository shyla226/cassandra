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

import io.reactivex.functions.Function;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.flow.Flow;

/**
 * Base interface for partitions whose content is processed as a Flow.
 *
 * Flowable partitions must always be used (i.e. subscribed to and closed) because they usually hold resources
 * (e.g. file readers). If a partition instance happens to be not required before processing the contents, the method
 * unused() must be called (this will subscribe to and immediately close the flow).
 */
public interface FlowablePartitionBase<T> extends PartitionTrait
{
    public PartitionHeader header();

    public Flow<T> content();

    public Row staticRow();

    /**
     * Return a new partition with the specified header and static row, but the same content.
     *
     * @param header - the new header
     * @param staticRow - the new static row
     *
     * @return a new partition with the specified header and static row, but the same content.
     */
    public FlowablePartitionBase<T> withHeader(PartitionHeader header, Row staticRow);

    /**
     * Return a new partition with the same header but different content.
     *
     * @param content the new content
     *
     * @return the partition with the new content
     */
    public FlowablePartitionBase<T> withContent(Flow<T> content);

    /**
     * Apply the mapper to the partition content
     *
     * @param mappingOp the content mapper
     *
     * @return a new identical partition with the content mapped
     */
    public FlowablePartitionBase<T> mapContent(Function<T, T> mappingOp);

    /**
     * Apply the mapper to the partition content, skipping null items
     *
     * @param mappingOp the content mapper
     * @param staticRow new value for the static row
     *
     * @return a new identical partition with the content mapped
     */
    public FlowablePartitionBase<T> skippingMapContent(Function<T, T> mappingOp, Row staticRow);

    default TableMetadata metadata()
    {
        return header().metadata;
    }

    default boolean isReverseOrder()
    {
        return header().isReverseOrder;
    }

    default RegularAndStaticColumns columns()
    {
        return header().columns;
    }

    default DecoratedKey partitionKey()
    {
        return header().partitionKey;
    }

    default DeletionTime partitionLevelDeletion()
    {
        return header().partitionLevelDeletion;
    }

    default EncodingStats stats()
    {
        return header().stats;
    }
}
