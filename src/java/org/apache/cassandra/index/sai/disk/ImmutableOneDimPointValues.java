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
package org.apache.cassandra.index.sai.disk;

import java.io.IOException;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.bkd.BKDWriter;

/**
 * {@link MutablePointValues} that prevents buffered points from reordering, and always skips sorting phase in Lucene
 * It's the responsibility of the underlying implementation to ensure that all points are correctly sorted.
 * <p>
 * It allows to take advantage of an optimised 1-dim writer {@link BKDWriter}
 * (that is enabled only for {@link MutablePointValues}), and reduce number of times we sort point values.
 */
public class ImmutableOneDimPointValues extends MutablePointValues
{
    private static final byte[] EMPTY = new byte[0];

    private final TermsIterator termEnum;
    private final AbstractType termComparator;
    private final byte[] scratch;

    private ImmutableOneDimPointValues(TermsIterator termEnum, AbstractType termComparator)
    {
        this.termEnum = termEnum;
        this.termComparator = termComparator;
        this.scratch = new byte[TypeUtil.fixedSizeOf(termComparator)];
    }

    public static ImmutableOneDimPointValues fromTermEnum(TermsIterator termEnum, AbstractType termComparator)
    {
        return new ImmutableOneDimPointValues(termEnum, termComparator);
    }

    @Override
    public long estimatePointCount(IntersectVisitor visitor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDocCount()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void intersect(IntersectVisitor visitor) throws IOException
    {
        while (termEnum.hasNext())
        {
            TypeUtil.toComparableBytes(termEnum.next(), termComparator, scratch);
            try (final PostingList postings = termEnum.postings())
            {
                int segmentRowId;
                while ((segmentRowId = postings.nextPosting()) != PostingList.END_OF_STREAM)
                {
                    visitor.visit(segmentRowId, scratch);
                }
            }
        }
    }

    @Override
    public long size()
    {
        // hack to skip sorting in Lucene
        return 1;
    }

    @Override
    public void getValue(int i, BytesRef packedValue)
    {
        // no-op
    }

    @Override
    public byte getByteAt(int i, int k)
    {
        return 0;
    }

    @Override
    public int getDocID(int i)
    {
        return 0;
    }

    @Override
    public void swap(int i, int j)
    {
        throw new IllegalStateException("unexpected sorting");
    }

    @Override
    public byte[] getMinPackedValue()
    {
        return EMPTY;
    }

    @Override
    public byte[] getMaxPackedValue()
    {
        return EMPTY;
    }

    @Override
    public int getNumDimensions()
    {
        return 1;
    }

    @Override
    public int getBytesPerDimension()
    {
        return scratch.length;
    }
}
