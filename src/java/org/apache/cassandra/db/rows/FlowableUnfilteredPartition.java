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
import io.reactivex.functions.Predicate;
import org.apache.cassandra.utils.flow.Flow;

/**
 * A partition container providing access to the rows of the partition together with deletion information.
 * <p>
 * The supplied {@code PartitionHeader} contains partition top-level information.
 * The content is a flow of {@code Unfiltered}, that is of either {@code Row} or {@code RangeTombstoneMarker}.
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
 */
public interface FlowableUnfilteredPartition extends FlowablePartitionBase<Unfiltered>
{
    public static FlowableUnfilteredPartition create(PartitionHeader header, Row staticRow, Flow<Unfiltered> content)
    {
        return new Instance(header, staticRow, content);
    }

    public class Instance implements FlowableUnfilteredPartition
    {
        private final PartitionHeader header;
        private final Row staticRow;
        private final Flow<Unfiltered> content;

        public Instance(PartitionHeader header, Row staticRow, Flow<Unfiltered> content)
        {
            this.header = header;
            this.staticRow = staticRow;
            this.content = content;
        }

        public PartitionHeader header()
        {
            return header;
        }

        public Row staticRow()
        {
            return staticRow;
        }

        public Flow<Unfiltered> content()
        {
            return content;
        }
    }

    default FlowableUnfilteredPartition withHeader(PartitionHeader header, Row staticRow)
    {
        return create(header, staticRow, content());
    }

    default FlowableUnfilteredPartition withContent(Flow<Unfiltered> content)
    {
        return create(header(),
                      staticRow(),
                      content);
    }

    abstract class FlowTransformNext extends org.apache.cassandra.utils.flow.FlowTransformNext<Unfiltered, Unfiltered>
    implements FlowableUnfilteredPartition
    {
        private final PartitionHeader header;
        private final Row staticRow;

        protected FlowTransformNext(Flow<Unfiltered> sourceContent, Row staticRow, PartitionHeader header)
        {
            super(sourceContent);
            this.header = header;
            this.staticRow = staticRow;
        }

        public PartitionHeader header()
        {
            return header;
        }

        public Flow<Unfiltered> content()
        {
            return this;
        }

        public Row staticRow()
        {
            return staticRow;
        }

        // onNext, onFinal to be implemented by op
    }

    abstract class FlowTransform extends org.apache.cassandra.utils.flow.FlowTransform<Unfiltered, Unfiltered>
    implements FlowableUnfilteredPartition
    {
        private final PartitionHeader header;
        private final Row staticRow;

        protected FlowTransform(Flow<Unfiltered> sourceContent, Row staticRow, PartitionHeader header)
        {
            super(sourceContent);
            this.header = header;
            this.staticRow = staticRow;
        }

        public PartitionHeader header()
        {
            return header;
        }

        public Flow<Unfiltered> content()
        {
            return this;
        }

        public Row staticRow()
        {
            return staticRow;
        }

        // onNext, onFinal and requestNext to be implemented by op
    }

    abstract class FlowSource extends org.apache.cassandra.utils.flow.FlowSource<Unfiltered>
    implements FlowableUnfilteredPartition
    {
        private final PartitionHeader header;
        private final Row staticRow;

        protected FlowSource(PartitionHeader header, Row staticRow)
        {
            this.header = header;
            this.staticRow = staticRow;
        }

        public PartitionHeader header()
        {
            return header;
        }

        public Flow<Unfiltered> content()
        {
            return this;
        }

        public Row staticRow()
        {
            return staticRow;
        }

        // requestNext() to be implemented by op
    }

    class Map extends org.apache.cassandra.utils.flow.Flow.Map<Unfiltered, Unfiltered>
    implements FlowableUnfilteredPartition
    {
        private final PartitionHeader header;
        private final Row staticRow;

        protected Map(Flow<Unfiltered> sourceContent, Row staticRow, PartitionHeader header, Function<Unfiltered, Unfiltered> mapper)
        {
            super(sourceContent, mapper);
            this.header = header;
            this.staticRow = staticRow;
        }

        public PartitionHeader header()
        {
            return header;
        }

        public Flow<Unfiltered> content()
        {
            return this;
        }

        public Row staticRow()
        {
            return staticRow;
        }
    }

    default FlowableUnfilteredPartition mapContent(Function<Unfiltered, Unfiltered> mapper)
    {
        return new Map(content(), staticRow(), header(), mapper);
    }

    class SkippingMap extends org.apache.cassandra.utils.flow.Flow.SkippingMap<Unfiltered, Unfiltered>
    implements FlowableUnfilteredPartition
    {
        private final PartitionHeader header;
        private final Row staticRow;

        protected SkippingMap(Flow<Unfiltered> sourceContent, Row staticRow, PartitionHeader header, Function<Unfiltered, Unfiltered> mapper)
        {
            super(sourceContent, mapper);
            this.header = header;
            this.staticRow = staticRow;
        }

        public PartitionHeader header()
        {
            return header;
        }

        public Flow<Unfiltered> content()
        {
            return this;
        }

        public Row staticRow()
        {
            return staticRow;
        }
    }

    default FlowableUnfilteredPartition skippingMapContent(Function<Unfiltered, Unfiltered> mapper, Row staticRow)
    {
        return new SkippingMap(content(), staticRow, header(), mapper);
    }

    class Filter extends org.apache.cassandra.utils.flow.Flow.Filter<Unfiltered>
    implements FlowableUnfilteredPartition
    {
        private final PartitionHeader header;
        private final Row staticRow;

        protected Filter(Flow<Unfiltered> sourceContent, Row staticRow, PartitionHeader header, Predicate<Unfiltered> tester)
        {
            super(sourceContent, tester);
            this.header = header;
            this.staticRow = staticRow;
        }

        public PartitionHeader header()
        {
            return header;
        }

        public Flow<Unfiltered> content()
        {
            return this;
        }

        public Row staticRow()
        {
            return staticRow;
        }
    }

    default FlowableUnfilteredPartition filterContent(Predicate<Unfiltered> tester)
    {
        return new Filter(content(), staticRow(), header(), tester);
    }
}
