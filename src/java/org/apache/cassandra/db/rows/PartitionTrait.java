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

import java.security.MessageDigest;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.DigestVersion;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;

/**
 * An interface for grouping all the properties of a partition, except for the content itself.
 * We need it for as long as we need to live with the dual iterator / async publishers paradigm,
 * it avoids duplicating code for serializing iterators or async publishers, see
 * {@link UnfilteredRowIterators#digestPartition(PartitionTrait, MessageDigest, DigestVersion)} or
 * {@link UnfilteredRowIteratorSerializer#serializeBeginningOfPartition(PartitionTrait, SerializationHeader, ColumnFilter, DataOutputPlus, int)}.
 */
public interface PartitionTrait
{
    /**
     * The metadata for the table this iterator on.
     */
    public TableMetadata metadata();

    /**
     * Whether or not the rows returned by this iterator are in reversed
     * clustering order.
     */
    public boolean isReverseOrder();

    /**
     * A subset of the columns for the (static and regular) rows returned by this iterator.
     * Every row returned by this iterator must guarantee that it has only those columns.
     */
    public RegularAndStaticColumns columns();

    /**
     * The partition key of the partition this in an iterator over.
     */
    public DecoratedKey partitionKey();

    /**
     * The static part corresponding to this partition (this can be an empty
     * row but cannot be {@code null}).
     */
    public Row staticRow();

    /**
     * The partition level deletion for the partition this iterate over.
     */
    public DeletionTime partitionLevelDeletion();

    /**
     * Return "statistics" about what is returned by this iterator. Those are used for
     * performance reasons (for delta-encoding for instance) and code should not
     * expect those to be exact.
     */
    public EncodingStats stats();
}
