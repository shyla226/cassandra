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
import io.reactivex.Maybe;
import io.reactivex.Single;

/**
 * A partition container providing access to the rows of the partition together with deletion informations.
 * <p>
 * The supplied {@code PartitionHeader} contains a few partition top-level informations.
 * The content is a flowable stream of {@code Unfiltered}, that is of either {@code Row} or {@code RangeTombstoneMarker}.
 * An unfiltered partition <b>must</b> provide the following guarantees:
 *   1. the returned {@code Unfiltered} must be in clustering order, or in reverse clustering
 *      order iff {@code header.isReverseOrder} is true.
 *   2. the iterator should not shadow its own data. That is, no deletion
 *      (partition level deletion, row deletion, range tombstone, complex
 *      deletion) should delete anything else returned by the iterator (cell, row, ...).
 *   3. every "start" range tombstone marker should have a corresponding "end" marker, and no other
 *      marker should be in-between this start-end pair of marker. Note that due to the
 *      previous rule this means that between a "start" and a corresponding "end" marker there
 *      can only be rows that are not deleted by the markers. Also note that when iterating
 *      in reverse order, "end" markers are returned before their "start" counterpart (i.e.
 *      "start" and "end" are always in the sense of the clustering order).
 *
 * Note: providers of data can no longer reuse mutable returned objects as we can't guarantee only one reference
 * exists at a time (e.g. Flowable.concatMap always requests one in reserve).
 */
public class FlowableUnfilteredPartition extends FlowablePartitionBase<Unfiltered, PartitionHeader>
{
    public FlowableUnfilteredPartition(PartitionHeader header, Maybe<Row> staticRow, Flowable<Unfiltered> content)
    {
        super(header, staticRow, content);
    }
}
