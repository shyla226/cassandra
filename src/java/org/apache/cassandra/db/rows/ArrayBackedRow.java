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


import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.Consumer;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIndexedListIterator;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;

/**
 * Lightweight native array based row impl
 */
public class ArrayBackedRow extends AbstractRow
{
    public static final ColumnData[] EMPTY_ARRAY = new ColumnData[]{};
    private static final long EMPTY_SIZE = ObjectSizes.measure(emptyRow(Clustering.EMPTY));

    private final ColumnData[] data;
    private final int limit;

    private ArrayBackedRow(Clustering clustering,
                           LivenessInfo primaryKeyLivenessInfo,
                           Deletion deletion,
                           ColumnData[] data,
                           int limit,
                           int minLocalDeletionTime)
    {
        super(clustering, primaryKeyLivenessInfo, deletion, minLocalDeletionTime);
        this.data = data;
        this.limit = limit;
    }

    private ArrayBackedRow(Clustering clustering, ColumnData[] data, int minLocalDeletionTime)
    {
        this(clustering, LivenessInfo.EMPTY, Deletion.LIVE, data, data.length, minLocalDeletionTime);
    }

    // Note that it's often easier/safer to use the sortedBuilder/unsortedBuilder or one of the static creation method below. Only directly useful in a small amount of cases.
    public static ArrayBackedRow create(Clustering clustering,
                                        LivenessInfo primaryKeyLivenessInfo,
                                        Deletion deletion,
                                        ColumnData[] data, int limit)
    {
        int minDeletionTime = Math.min(minDeletionTime(primaryKeyLivenessInfo), minDeletionTime(deletion.time()));
        if (minDeletionTime != Integer.MIN_VALUE)
        {
            for (int i = 0; i < limit; i++)
            {
                ColumnData cd = data[i];
                minDeletionTime = Math.min(minDeletionTime, minDeletionTime(cd));
            }
        }

        return new ArrayBackedRow(clustering, primaryKeyLivenessInfo, deletion, data, limit, minDeletionTime);
    }

    public static ArrayBackedRow emptyRow(Clustering clustering)
    {
        return new ArrayBackedRow(clustering, EMPTY_ARRAY, Integer.MAX_VALUE);
    }

    public static ArrayBackedRow singleCellRow(Clustering clustering, Cell cell)
    {
        if (cell.column().isSimple())
            return new ArrayBackedRow(clustering, new ColumnData[]{ cell}, minDeletionTime(cell));

        ComplexColumnData complexData = new ComplexColumnData(cell.column(), new Cell[]{ cell }, DeletionTime.LIVE);
        return new ArrayBackedRow(clustering, new ColumnData[]{ complexData}, minDeletionTime(cell));
    }

    public static ArrayBackedRow emptyDeletedRow(Clustering clustering, Deletion deletion)
    {
        assert !deletion.isLive();
        return new ArrayBackedRow(clustering, LivenessInfo.EMPTY, deletion, EMPTY_ARRAY, 0, Integer.MIN_VALUE);
    }

    public static ArrayBackedRow noCellLiveRow(Clustering clustering, LivenessInfo primaryKeyLivenessInfo)
    {
        assert !primaryKeyLivenessInfo.isEmpty();
        return new ArrayBackedRow(clustering,
                                  primaryKeyLivenessInfo,
                                  Deletion.LIVE,
                                  EMPTY_ARRAY,
                                  0,
                                  minDeletionTime(primaryKeyLivenessInfo));
    }


    public Iterator<ColumnData> iterator()
    {
        return new AbstractIndexedListIterator<ColumnData>(limit, 0)
        {
            protected ColumnData get(int index)
            {
                return data[index];
            }
        };
    }

    public int size()
    {
        return limit;
    }

    public void setValue(ColumnMetadata column, CellPath path, ByteBuffer value)
    {
        int idx = Arrays.binarySearch(data, 0, limit, column, ColumnMetadata.asymmetricColumnDataComparator);
        if (idx < 0)
            return;

        ColumnData current = data[idx];

        if (column.isSimple())
            data[idx] = ((Cell)current).withUpdatedValue(value);
        else
            ((ComplexColumnData) current).setValue(path, value);
    }

    public boolean isEmpty()
    {
        return primaryKeyLivenessInfo().isEmpty()
               && deletion().isLive()
               && limit == 0;
    }

    public Cell getCell(ColumnMetadata cm)
    {
        assert cm.isSimple();
        return (Cell) getColumnData(cm);
    }

    public Cell getCell(ColumnMetadata cm, CellPath path)
    {
        assert cm.isComplex();

        ComplexColumnData cd = getComplexColumnData(cm);
        if (cd == null)
            return null;
        return cd.getCell(path);
    }

    public ColumnData getColumnData(ColumnMetadata cm)
    {
        int idx = Arrays.binarySearch(data, 0, limit, cm, ColumnMetadata.asymmetricColumnDataComparator);
        return idx < 0 ? null : data[idx];
    }

    public ComplexColumnData getComplexColumnData(ColumnMetadata cm)
    {
        assert cm.isComplex();
        return (ComplexColumnData) getColumnData(cm);
    }

    public Iterable<Cell> cells()
    {
        return CellIterator::new;
    }

    public Iterable<Cell> cellsInLegacyOrder(TableMetadata metadata, boolean reversed)
    {
        return () -> new CellInLegacyOrderIterator(metadata, reversed);
    }

    public boolean hasComplexDeletion()
    {
        // We start by the end cause we know complex columns sort before simple ones
        for (int i = limit - 1; i >= 0; i--)
        {
            ColumnData cd = data[i];
            if (cd.column.isSimple())
                break;

            ComplexColumnData ccd = (ComplexColumnData) cd;
            if (!ccd.complexDeletion().isLive())
                return true;
        }

        return false;
    }

    public boolean hasComplex()
    {
        // We start by the end cause we know complex columns sort before simple ones
        for (int i = limit - 1; i >= 0; i--)
        {
            ColumnData cd = data[i];
            if (cd.column.isSimple())
                break;

            return true;
        }

        return false;
    }

    static int minDeletionTime(Object[] data, int limit, LivenessInfo info, DeletionTime rowDeletion)
    {
        int min = Math.min(minDeletionTime(info), minDeletionTime(rowDeletion));

        for (int i = 0; i < limit; i++)
        {
            min = Math.min(min, minDeletionTime((ColumnData) data[i]));

            if (min == Integer.MIN_VALUE)
                break;
        }

        return min;
    }

    Row transformAndFilter(LivenessInfo info, Deletion deletion, Function<ColumnData, ColumnData> function)
    {
        ColumnData[] transformed = data;
        int tlimit = 0;

        for (int i = 0; i < limit; i++)
        {
            ColumnData cd1 = data[i];
            ColumnData cd2 = function.apply(cd1);

            // Only copy if the function is making a change
            if (transformed == data && cd1 != cd2)
            {
                transformed = new ColumnData[limit];
                System.arraycopy(data, 0, transformed, 0, i);
            }

            if (cd2 != null)
                transformed[tlimit++] = cd2;
        }

        if (data == transformed && info == this.primaryKeyLivenessInfo && deletion == this.deletion)
            return this;

        if (info.isEmpty() && deletion.isLive() && tlimit == 0)
            return null;

        int minDeletionTime = minDeletionTime(transformed, tlimit, info, deletion.time());
        return new ArrayBackedRow(clustering, info, deletion, transformed, tlimit, minDeletionTime);
    }

    public int dataSize()
    {
        int dataSize = clustering.dataSize()
                       + primaryKeyLivenessInfo.dataSize()
                       + deletion.dataSize();

        for (int i = 0; i < limit; i++)
        {
            ColumnData c = data[i];
            dataSize += c.dataSize();
        }
        return dataSize;
    }

    public long unsharedHeapSizeExcludingData()
    {
        long heapSize = EMPTY_SIZE
                        + clustering.unsharedHeapSizeExcludingData()
                        + ObjectSizes.sizeOfArray(data);

        for (int i = 0; i < limit; i++)
        {
            ColumnData c = data[i];
            heapSize += c.unsharedHeapSizeExcludingData();
        }

        return heapSize;
    }

    public Row withRowDeletion(DeletionTime newDeletion)
    {
        // Note that:
        //  - it is a contract with the caller that the new deletion shouldn't shadow anything in
        //    the row, and so in particular it can't shadow the row deletion. So if there is a
        //    already a row deletion we have nothing to do.
        //  - we set the minLocalDeletionTime to MIN_VALUE because we know the deletion is live
        return newDeletion.isLive() || !deletion.isLive()
               ? this
               : new ArrayBackedRow(clustering, primaryKeyLivenessInfo, Deletion.regular(newDeletion), data, limit, Integer.MIN_VALUE);

    }

    public void apply(Consumer<ColumnData> function, boolean reverse)
    {
        apply(function, null, reverse);
    }

    public void apply(Consumer<ColumnData> function, Predicate<ColumnData> stopCondition, boolean reverse)
    {
        if (!reverse)
            applyForwards(function, stopCondition);
        else
            applyReversed(function, stopCondition);
    }

    private boolean applyForwards(Consumer<ColumnData> function, Predicate<ColumnData> stopCondition)
    {
        for (int i = 0; i < limit; i++)
        {
            ColumnData cd = data[i];
            function.accept(cd);

            if (stopCondition != null && stopCondition.apply(cd))
                return true;
        }

        return false;
    }

    private boolean applyReversed(Consumer<ColumnData> function, Predicate<ColumnData> stopCondition)
    {
        for (int i = limit - 1; i >= 0; i--)
        {
            ColumnData cd = data[i];
            function.accept(cd);

            if (stopCondition != null && stopCondition.apply(cd))
                return true;
        }

        return false;
    }

    public <R> R reduce(R seed, BTree.ReduceFunction<R, ColumnData> reducer)
    {
        for (int i = 0; i < limit; i++)
        {
            seed = reducer.apply(seed, data[i]);

            if (reducer.stop(seed))
                break;
        }

        return seed;
    }

    public <R> R reduceCells(R seed, BTree.ReduceFunction<R, Cell> reducer)
    {
        return reduce(seed, new BTree.ReduceFunction<R, ColumnData>() {
            public R apply(R ret, ColumnData cd)
            {
                if (cd.column().isComplex())
                    return ((ComplexColumnData)cd).reduce(ret, reducer);
                else
                    return reducer.apply(ret, (Cell)cd);
            }

            @Override
            public boolean stop(R ret)
            {
                return reducer.stop(ret);
            }
        });
    }

    public static Builder sortedBuilder()
    {
        return new Builder(true, Integer.MIN_VALUE);
    }

    public static Builder unsortedBuilder(int nowInSeconds)
    {
        return new Builder(false, nowInSeconds);
    }

    private class CellIterator extends AbstractIterator<Cell>
    {
        private Iterator<ColumnData> columnData = iterator();
        private Iterator<Cell> complexCells;

        protected Cell computeNext()
        {
            while (true)
            {
                if (complexCells != null)
                {
                    if (complexCells.hasNext())
                        return complexCells.next();

                    complexCells = null;
                }

                if (!columnData.hasNext())
                    return endOfData();

                ColumnData cd = columnData.next();
                if (cd.column().isComplex())
                    complexCells = ((ComplexColumnData)cd).iterator();
                else
                    return (Cell)cd;
            }
        }
    }

    private class CellInLegacyOrderIterator extends AbstractIterator<Cell>
    {
        private final Comparator<ByteBuffer> comparator;
        private final boolean reversed;
        private final int firstComplexIdx;
        private int simpleIdx;
        private int complexIdx;
        private Iterator<Cell> complexCells;

        private CellInLegacyOrderIterator(TableMetadata metadata, boolean reversed)
        {
            AbstractType<?> nameComparator = metadata.columnDefinitionNameComparator(isStatic() ? ColumnMetadata.Kind.STATIC : ColumnMetadata.Kind.REGULAR);
            this.comparator = reversed ? Collections.reverseOrder(nameComparator) : nameComparator;
            this.reversed = reversed;

            // copy btree into array for simple separate iteration of simple and complex columns
            int idx = Iterators.indexOf(Iterators.forArray(data), cd -> cd instanceof ComplexColumnData);
            this.firstComplexIdx = idx < 0 ? limit : idx;
            this.complexIdx = firstComplexIdx;
        }

        private int getSimpleIdx()
        {
            return reversed ? firstComplexIdx - simpleIdx - 1 : simpleIdx;
        }

        private int getSimpleIdxAndIncrement()
        {
            int idx = getSimpleIdx();
            ++simpleIdx;
            return idx;
        }

        private int getComplexIdx()
        {
            return reversed ? limit + firstComplexIdx - complexIdx - 1 : complexIdx;
        }

        private int getComplexIdxAndIncrement()
        {
            int idx = getComplexIdx();
            ++complexIdx;
            return idx;
        }

        private Iterator<Cell> makeComplexIterator(Object complexData)
        {
            ComplexColumnData ccd = (ComplexColumnData)complexData;
            return reversed ? ccd.reverseIterator() : ccd.iterator();
        }

        protected Cell computeNext()
        {
            while (true)
            {
                if (complexCells != null)
                {
                    if (complexCells.hasNext())
                        return complexCells.next();

                    complexCells = null;
                }

                if (simpleIdx >= firstComplexIdx)
                {
                    if (complexIdx >= limit)
                        return endOfData();

                    complexCells = makeComplexIterator(data[getComplexIdxAndIncrement()]);
                }
                else
                {
                    if (complexIdx >= limit)
                        return (Cell)data[getSimpleIdxAndIncrement()];

                    if (comparator.compare(data[getSimpleIdx()].column().name.bytes, data[getComplexIdx()].column().name.bytes) < 0)
                        return (Cell)data[getSimpleIdxAndIncrement()];
                    else
                        complexCells = makeComplexIterator(data[getComplexIdxAndIncrement()]);
                }
            }
        }
    }

    public static class Builder implements Row.Builder
    {
        protected Clustering clustering;
        protected LivenessInfo primaryKeyLivenessInfo = LivenessInfo.EMPTY;
        protected Deletion deletion = Deletion.LIVE;

        private ColumnData[] cells = EMPTY_ARRAY;
        private int length = 0;
        private int lastLength = 0;
        private boolean hasComplex = false;
        private final boolean isSorted;
        private final int nowInSeconds;
        private BTree.Builder.Resolver resolver = null;

        protected Builder(boolean isSorted, int nowInSeconds, int size)
        {
            this.isSorted = isSorted;
            this.nowInSeconds = nowInSeconds;
            this.cells = new ColumnData[size];
        }

        protected Builder(boolean isSorted, int nowInSeconds)
        {
            this.isSorted = isSorted;
            this.nowInSeconds = nowInSeconds;
        }

        public Row.Builder copy()
        {
            Builder copy = new Builder(isSorted, nowInSeconds);
            copy.deletion = deletion;
            copy.length = length;
            copy.clustering = clustering;
            copy.primaryKeyLivenessInfo = primaryKeyLivenessInfo;
            copy.cells = Arrays.copyOf(cells, cells.length);
            copy.lastLength = lastLength;
            copy.hasComplex = hasComplex;

            return copy;
        }

        public boolean isSorted()
        {
            return isSorted;
        }

        public void newRow(Clustering clustering)
        {
            assert this.clustering == null; // Ensures we've properly called build() if we've use this builder before
            this.clustering = clustering;
        }

        public Clustering clustering()
        {
            return clustering;
        }

        public void addPrimaryKeyLivenessInfo(LivenessInfo info)
        {
            // The check is only required for unsorted builders, but it's worth the extra safety to have it unconditional
            if (!deletion.deletes(info))
                this.primaryKeyLivenessInfo = info;
        }

        public void addRowDeletion(Deletion deletion)
        {
            this.deletion = deletion;
            // The check is only required for unsorted builders, but it's worth the extra safety to have it unconditional
            if (deletion.deletes(primaryKeyLivenessInfo))
                this.primaryKeyLivenessInfo = LivenessInfo.EMPTY;
        }

        @Inline
        void maybeGrow()
        {
            if (length == cells.length)
            {
                ColumnData[] newCells;
                if (lastLength > 0)
                {
                    newCells = new ColumnData[lastLength];
                    lastLength = 0;
                }
                else
                {
                    newCells = new ColumnData[Math.max(8, length * 2)];
                }

                System.arraycopy(cells, 0, newCells, 0, length);
                cells = newCells;
            }
        }

        public void addCell(Cell cell)
        {
            assert cell.column().isStatic() == (clustering == Clustering.STATIC_CLUSTERING) : "Column is " + cell.column() + ", clustering = " + clustering;

            // In practice, only unsorted builder have to deal with shadowed cells, but it doesn't cost us much to deal with it unconditionally in this case
            if (deletion.deletes(cell))
                return;

            maybeGrow();

            cells[length++] = cell;
            hasComplex |= cell.column.isComplex();
        }

        public void addComplexDeletion(ColumnMetadata column, DeletionTime complexDeletion)
        {
            maybeGrow();
            cells[length++] = new ComplexColumnDeletion(column, complexDeletion);
            hasComplex = true;
        }

        public void reset()
        {
            clustering = null;
            primaryKeyLivenessInfo = LivenessInfo.EMPTY;
            deletion = Deletion.LIVE;
            cells = EMPTY_ARRAY;
            lastLength = length;
            length = 0;
            hasComplex = false;
        }

        public void resolve()
        {
            if (length > 0)
            {
                if (resolver == null)
                    resolver = new CellResolver(nowInSeconds);

                int c = 0;
                int prev = 0;
                for (int i = 1; i < length; i++)
                {
                    if (ColumnData.comparator.compare(cells[i], cells[prev]) != 0)
                    {
                        cells[c++] = (ColumnData) resolver.resolve(cells, prev, i);
                        prev = i;
                    }
                }
                cells[c++] = (ColumnData) resolver.resolve(cells, prev, length);
                length = c;
            }
        }

        public Row build()
        {
            if (!isSorted)
                Arrays.sort(cells, 0, length, ColumnData.comparator);

            if (!isSorted | hasComplex)
                resolve();

            if (deletion.isShadowedBy(primaryKeyLivenessInfo))
                deletion = Deletion.LIVE;

            Row result = ArrayBackedRow.create(clustering, primaryKeyLivenessInfo, deletion, cells, length);
            reset();

            return result;
        }
    }
}
