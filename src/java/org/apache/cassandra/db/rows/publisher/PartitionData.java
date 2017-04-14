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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.PartitionHeader;
import org.apache.cassandra.db.rows.PartitionTrait;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A class grouping partition information for partition subscribers.
 */
class PartitionData implements PartitionTrait
{
    public static final PartitionData EMPTY = new PartitionData(new PartitionHeader(null,
                                                                                    null,
                                                                                    DeletionTime.LIVE,
                                                                                    RegularAndStaticColumns.NONE,
                                                                                    false,
                                                                                    EncodingStats.NO_STATS),
                                                                Rows.EMPTY_STATIC_ROW,
                                                                false);
    public final PartitionHeader header;

    public final Row staticRow;

    public boolean hasData;


    public PartitionData(PartitionHeader header, Row staticRow, boolean hasData)
    {
        this.header = header;
        this.staticRow = staticRow;
        this.hasData = hasData;
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

    public boolean isEmpty()
    {
        return header.partitionLevelDeletion.isLive()
               && staticRow.isEmpty()
               && !hasData;
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
