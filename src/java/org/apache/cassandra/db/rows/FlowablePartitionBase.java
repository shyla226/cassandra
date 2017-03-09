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

import io.reactivex.Flowable;
import io.reactivex.Single;

/**
 * Base class for the flowable versions of partitions.
 */
public abstract class FlowablePartitionBase<T, Header>
{
    /**
     * The header contains information about the partition: key, metadata etc.
     * Normally reused through transformations, merging, filtering etc.
     */
    public final Header header;

    /**
     * The static part corresponding to this partition.
     */
    public final Single<Row> staticRow;

    /**
     * The partition's contents as a Flowable. This must be subscribed to exactly once, and will close all
     * associated resources when the subscription completes (complete/error/cancel).
     */
    public final Flowable<T> content;

    public FlowablePartitionBase(Header header,
                                 Single<Row> staticRow,
                                 Flowable<T> content)
    {
        this.header = header;
        this.staticRow = staticRow;
        this.content = content;
    }
}
