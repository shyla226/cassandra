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

package org.apache.cassandra.io.sstable.format;

import com.codahale.metrics.Histogram;
import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * The base RowIndexEntry is not stored on disk, only specifies a position in the data file
 */
public class RowIndexEntry<T> implements IMeasurableMemory
{
    protected static final Histogram indexEntrySizeHistogram;
    protected static final Histogram indexInfoCountHistogram;
    protected static final Histogram indexInfoGetsHistogram;
    protected static final Histogram indexInfoReadsHistogram;

    static
    {
        MetricNameFactory factory = new DefaultNameFactory("Index", "RowIndexEntry");
        indexEntrySizeHistogram = Metrics.histogram(factory.createMetricName("IndexedEntrySize"), false);
        indexInfoCountHistogram = Metrics.histogram(factory.createMetricName("IndexInfoCount"), false);
        indexInfoGetsHistogram = Metrics.histogram(factory.createMetricName("IndexInfoGets"), false);
        indexInfoReadsHistogram = Metrics.histogram(factory.createMetricName("IndexInfoReads"), false);
    }

    private static final long EMPTY_SIZE = ObjectSizes.measure(new RowIndexEntry<>(0));
    public final long position;

    public RowIndexEntry(long position)
    {
        this.position = position;
    }

    /**
     * @return true if this index entry contains the row-level tombstone and column summary.  Otherwise,
     * caller should fetch these from the row header.
     */
    public boolean isIndexed()
    {
        return columnsIndexCount() > 1;
    }

    public DeletionTime deletionTime()
    {
        throw new UnsupportedOperationException();
    }

    public int columnsIndexCount()
    {
        return 0;
    }

    @Override
    public long unsharedHeapSize()
    {
        return EMPTY_SIZE;
    }
}
