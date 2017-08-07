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
import org.apache.cassandra.utils.flow.Flow;

/**
 * A partition container providing access to the rows of the partition together with deletion informations.
 *
 * A FlowablePartition is a FlowableUnfilteredPartition in which any deletion information has been
 * filtered out. As such, all cell of all rows returned by this iterator are,
 * by definition, live, and hence code using a RowIterator don't have to worry
 * about tombstones and other deletion information.
 *
 * Note that as for FlowableUnfilteredPartition, the rows returned must be in clustering order (or
 * reverse clustering order if isReverseOrder is true).
 */
public interface FlowablePartition extends FlowablePartitionBase<Row>
{
    public static FlowablePartition create(PartitionHeader header, Row staticRow, Flow<Row> content)
    {
        return new Instance(header, staticRow, content);
    }

    class Instance implements FlowablePartition
    {
        private final PartitionHeader header;
        private final Row staticRow;
        private final Flow<Row> content;

        private Instance(PartitionHeader header, Row staticRow, Flow<Row> content)
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

        public Flow<Row> content()
        {
            return content;
        }
    }

    default FlowablePartition withHeader(PartitionHeader header, Row staticRow)
    {
        return create(header, staticRow, content());
    }

    default FlowablePartition withContent(Flow<Row> content)
    {
        return create(header(),
                      staticRow(),
                      content);
    }

    abstract class FlowTransformNext extends org.apache.cassandra.utils.flow.FlowTransformNext<Row, Row>
    implements FlowablePartition
    {
        private final PartitionHeader header;
        private final Row staticRow;

        protected FlowTransformNext(Flow<Row> sourceContent, Row staticRow, PartitionHeader header)
        {
            super(sourceContent);
            this.header = header;
            this.staticRow = staticRow;
        }

        public PartitionHeader header()
        {
            return header;
        }

        public Flow<Row> content()
        {
            return this;
        }

        public Row staticRow()
        {
            return staticRow;
        }

        // onNext and onFinal to be implemented by op
    }

    abstract class FlowTransform extends org.apache.cassandra.utils.flow.FlowTransform<Row, Row>
    implements FlowablePartition
    {
        private final PartitionHeader header;
        private final Row staticRow;

        protected FlowTransform(Flow<Row> sourceContent, Row staticRow, PartitionHeader header)
        {
            super(sourceContent);
            this.header = header;
            this.staticRow = staticRow;
        }

        public PartitionHeader header()
        {
            return header;
        }

        public Flow<Row> content()
        {
            return this;
        }

        public Row staticRow()
        {
            return staticRow;
        }

        // onNext, onFinal and requestNext() to be implemented by op
    }

    abstract class FlowSource extends org.apache.cassandra.utils.flow.FlowSource<Row>
    implements FlowablePartition
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

        public Flow<Row> content()
        {
            return this;
        }

        public Row staticRow()
        {
            return staticRow;
        }

        // requestNext() to be implemented by op
    }

    class Map extends org.apache.cassandra.utils.flow.Flow.Map<Row, Row>
    implements FlowablePartition
    {
        private final PartitionHeader header;
        private final Row staticRow;

        protected Map(Flow<Row> sourceContent, Row staticRow, PartitionHeader header, Function<Row, Row> mapper)
        {
            super(sourceContent, mapper);
            this.header = header;
            this.staticRow = staticRow;
        }

        public PartitionHeader header()
        {
            return header;
        }

        public Flow<Row> content()
        {
            return this;
        }

        public Row staticRow()
        {
            return staticRow;
        }
    }

    default FlowablePartition mapContent(Function<Row, Row> mapper)
    {
        return new Map(content(), staticRow(), header(), mapper);
    }

    class SkippingMap extends org.apache.cassandra.utils.flow.Flow.SkippingMap<Row, Row>
    implements FlowablePartition
    {
        private final PartitionHeader header;
        private final Row staticRow;

        protected SkippingMap(Flow<Row> sourceContent, Row staticRow, PartitionHeader header, Function<Row, Row> mapper)
        {
            super(sourceContent, mapper);
            this.header = header;
            this.staticRow = staticRow;
        }

        public PartitionHeader header()
        {
            return header;
        }

        public Flow<Row> content()
        {
            return this;
        }

        public Row staticRow()
        {
            return staticRow;
        }
    }

    default FlowablePartition skippingMapContent(Function<Row, Row> mapper, Row staticRow)
    {
        return new SkippingMap(content(), staticRow, header(), mapper);
    }
}
