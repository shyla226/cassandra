/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file * to you under the Apache License, Version 2.0 (the
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
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.Iterables;
import com.google.common.hash.Hasher;

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.HashingUtils;
import org.apache.cassandra.utils.btree.UpdateFunction;

/**
 * Base abstract class for {@code Row} implementations.
 *
 * Unless you have a very good reason not to, every row implementation
 * should probably extend this class.
 */
public abstract class AbstractRow extends AbstractCollection<ColumnData> implements Row
{
    protected final Clustering clustering;
    protected final LivenessInfo primaryKeyLivenessInfo;
    protected final Deletion deletion;

    protected Collection<ColumnMetadata> columns;

    // We need to filter the tombstones of a row on every read (twice in fact: first to remove purgeable tombstone, and then after reconciliation to remove
    // all tombstone since we don't return them to the client) as well as on compaction. But it's likely that many rows won't have any tombstone at all, so
    // we want to speed up that case by not having to iterate/copy the row in this case. We could keep a single boolean telling us if we have tombstones,
    // but that doesn't work for expiring columns. So instead we keep the deletion time for the first thing in the row to be deleted. This allow at any given
    // time to know if we have any deleted information or not. If we any "true" tombstone (i.e. not an expiring cell), this value will be forced to
    // Integer.MIN_VALUE, but if we don't and have expiring cells, this will the time at which the first expiring cell expires. If we have no tombstones and
    // no expiring cells, this will be Integer.MAX_VALUE;
    protected final int minLocalDeletionTime;

    protected AbstractRow(Clustering clustering,
                      LivenessInfo primaryKeyLivenessInfo,
                      Deletion deletion,
                      int minLocalDeletionTime)
    {
        assert !deletion.isShadowedBy(primaryKeyLivenessInfo);
        this.clustering = clustering;
        this.primaryKeyLivenessInfo = primaryKeyLivenessInfo;
        this.deletion = deletion;
        this.minLocalDeletionTime = minLocalDeletionTime;
    }

    // This is a bit of a giant hack as this is the only place where we mutate a Row object. This makes it more efficient
    // for counters however and this won't be needed post-#6506 so that's probably fine.
    public abstract void setValue(ColumnMetadata column, CellPath path, ByteBuffer value);

    protected static int minDeletionTime(Cell cell)
    {
        return cell.isTombstone() ? Integer.MIN_VALUE : cell.localDeletionTime();
    }

    protected static int minDeletionTime(LivenessInfo info)
    {
        return info.isExpiring() ? info.localExpirationTime() : Integer.MAX_VALUE;
    }

    protected static int minDeletionTime(DeletionTime dt)
    {
        return dt.isLive() ? Integer.MAX_VALUE : Integer.MIN_VALUE;
    }

    protected static int minDeletionTime(ComplexColumnData cd)
    {
        int min = minDeletionTime(cd.complexDeletion());
        for (Cell cell : cd)
        {
            min = Math.min(min, minDeletionTime(cell));
            if (min == Integer.MIN_VALUE)
                break;
        }
        return min;
    }

    protected static int minDeletionTime(ColumnData cd)
    {
        return cd.column().isSimple() ? minDeletionTime((Cell) cd) : minDeletionTime((ComplexColumnData)cd);
    }

    public Clustering clustering()
    {
        return clustering;
    }

    public Collection<ColumnMetadata> columns()
    {
        if (columns == null)
        {
            columns = reduce(new ArrayList<>(size()), (list, c) ->  {
                list.add(c.column);
                return list;
            });
        }

        return columns;
    }

    public Deletion deletion()
    {
        return deletion;
    }

    public LivenessInfo primaryKeyLivenessInfo()
    {
        return primaryKeyLivenessInfo;
    }

    public Unfiltered.Kind kind()
    {
        return Unfiltered.Kind.ROW;
    }

    public boolean hasDeletion(int nowInSec)
    {
        return nowInSec >= minLocalDeletionTime;
    }

    public Row filter(ColumnFilter filter, TableMetadata metadata)
    {
        return filter(filter, DeletionTime.LIVE, false, metadata);
    }

    public Row filter(ColumnFilter filter, DeletionTime activeDeletion, boolean setActiveDeletionToRow, TableMetadata metadata)
    {
        Map<ByteBuffer, DroppedColumn> droppedColumns = metadata.droppedColumns;

        boolean mayFilterColumns = !filter.fetchesAllColumns(isStatic());
        boolean mayHaveShadowed = activeDeletion.supersedes(deletion.time());

        if (!mayFilterColumns && !mayHaveShadowed && droppedColumns.isEmpty())
            return this;

        LivenessInfo newInfo = primaryKeyLivenessInfo;
        Deletion newDeletion = deletion;
        if (mayHaveShadowed)
        {
            if (activeDeletion.deletes(newInfo.timestamp()))
                newInfo = LivenessInfo.EMPTY;
            // note that mayHaveShadowed means the activeDeletion shadows the row deletion. So if don't have setActiveDeletionToRow,
            // the row deletion is shadowed and we shouldn't return it.
            newDeletion = setActiveDeletionToRow ? Deletion.regular(activeDeletion) : Deletion.LIVE;
        }

        Columns columns = filter.fetchedColumns().columns(isStatic());
        java.util.function.Predicate<ColumnMetadata> inclusionTester = columns.inOrderInclusionTester();
        java.util.function.Predicate<ColumnMetadata> queriedByUserTester = filter.queriedColumns().columns(isStatic()).inOrderInclusionTester();
        final LivenessInfo rowLiveness = newInfo;
        return transformAndFilter(newInfo, newDeletion, (cd) -> {

            ColumnMetadata column = cd.column();
            if (!inclusionTester.test(column))
                return null;

            DroppedColumn dropped = droppedColumns.get(column.name.bytes);
            if (column.isComplex())
                return ((ComplexColumnData) cd).filter(filter, mayHaveShadowed ? activeDeletion : DeletionTime.LIVE, dropped, rowLiveness);

            Cell cell = (Cell) cd;
            // We include the cell unless it is 1) shadowed, 2) for a dropped column or 3) skippable.
            // And a cell is skippable if it is for a column that is not queried by the user and its timestamp
            // is lower than the row timestamp (see #10657 or SerializationHelper.includes() for details).
            boolean isForDropped = dropped != null && cell.timestamp() <= dropped.droppedTime;
            boolean isShadowed = mayHaveShadowed && activeDeletion.deletes(cell);
            boolean isSkippable = !queriedByUserTester.test(column) && cell.timestamp() < rowLiveness.timestamp();
            return isForDropped || isShadowed || isSkippable ? null : cell;
        });
    }

    abstract Row transformAndFilter(LivenessInfo info, Deletion deletion, com.google.common.base.Function<ColumnData, ColumnData> function);

    public Row purge(DeletionPurger purger, int nowInSec, boolean enforceStrictLiveness)
    {
        if (!hasDeletion(nowInSec))
            return this;

        LivenessInfo newInfo = purger.shouldPurge(primaryKeyLivenessInfo, nowInSec) ? LivenessInfo.EMPTY : primaryKeyLivenessInfo;
        Deletion newDeletion = purger.shouldPurge(deletion.time()) ? Deletion.LIVE : deletion;

        // when enforceStrictLiveness is set, a row is considered dead when it's PK liveness info is not present
        if (enforceStrictLiveness && newDeletion.isLive() && newInfo.isEmpty())
            return null;

        return transformAndFilter(newInfo, newDeletion, (cd) -> cd.purge(purger, nowInSec));
    }


    public Row withOnlyQueriedData(ColumnFilter filter)
    {
        if (filter.allFetchedColumnsAreQueried())
            return this;

        return transformAndFilter(primaryKeyLivenessInfo, deletion, (cd) -> {

            ColumnMetadata column = cd.column();
            if (column.isComplex())
                return ((ComplexColumnData)cd).withOnlyQueriedData(filter);

            return filter.fetchedColumnIsQueried(column) ? cd : null;
        });

    }

    public Row markCounterLocalToBeCleared()
    {
        return transformAndFilter(primaryKeyLivenessInfo, deletion, (cd) -> cd.column().isCounterColumn()
                                                                            ? cd.markCounterLocalToBeCleared()
                                                                            : cd);
    }

    public Row updateAllTimestamp(long newTimestamp)
    {
        LivenessInfo newInfo = primaryKeyLivenessInfo.isEmpty() ? primaryKeyLivenessInfo : primaryKeyLivenessInfo.withUpdatedTimestamp(newTimestamp);
        // If the deletion is shadowable and the row has a timestamp, we'll forced the deletion timestamp to be less than the row one, so we
        // should get rid of said deletion.
        Deletion newDeletion = deletion.isLive() || (deletion.isShadowable() && !primaryKeyLivenessInfo.isEmpty())
                               ? Deletion.LIVE
                               : new Deletion(new DeletionTime(newTimestamp - 1, deletion.time().localDeletionTime()), deletion.isShadowable());

        return transformAndFilter(newInfo, newDeletion, (cd) -> cd.updateAllTimestamp(newTimestamp));
    }

    @Override
    public boolean hasLiveData(int nowInSec, boolean enforceStrictLiveness)
    {
        /**
         * This logic is duplicated in {@link ReadCommand#withMetricsRecording(Flow, TableMetrics, long)},
         * see the countRow method. If you make a change here then remember to update that code too.
         */
        if (primaryKeyLivenessInfo().isLive(nowInSec))
            return true;
        else if (enforceStrictLiveness)
            return false;

        return reduceCells(false, new BTree.ReduceFunction<Boolean, Cell>() {
            public Boolean apply(Boolean ret, Cell cell)
            {
                return ret || cell.isLive(nowInSec);
            }

            @Override
            public boolean stop(Boolean ret)
            {
                return ret; // if we find a cell that is live, then we can stop
            }
        });
    }

    public boolean isStatic()
    {
        return clustering() == Clustering.STATIC_CLUSTERING;
    }

    public void digest(Hasher hasher)
    {
        HashingUtils.updateWithByte(hasher, kind().ordinal());
        clustering().digest(hasher);

        deletion().digest(hasher);
        primaryKeyLivenessInfo().digest(hasher);

        for (ColumnData cd : this)
            cd.digest(hasher);
    }

    public void validateData(TableMetadata metadata)
    {
        Clustering clustering = clustering();
        for (int i = 0; i < clustering.size(); i++)
        {
            ByteBuffer value = clustering.get(i);
            if (value != null)
                metadata.comparator.subtype(i).validate(value);
        }

        primaryKeyLivenessInfo().validate();
        if (deletion().time().localDeletionTime() < 0)
            throw new MarshalException("A local deletion time should not be negative");

        for (ColumnData cd : this)
            cd.validate();
    }

    public String toString(TableMetadata metadata)
    {
        return toString(metadata, false);
    }

    public String toString(TableMetadata metadata, boolean fullDetails)
    {
        return toString(metadata, true, fullDetails);
    }

    public String toString(TableMetadata metadata, boolean includeClusterKeys, boolean fullDetails)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Row");
        if (fullDetails)
        {
            sb.append("[info=").append(primaryKeyLivenessInfo());
            if (!deletion().isLive())
                sb.append(" del=").append(deletion());
            sb.append(" ]");
        }
        sb.append(": ");
        if(includeClusterKeys)
            sb.append(clustering().toString(metadata));
        else
            sb.append(clustering().toCQLString(metadata));
        sb.append(" | ");
        boolean isFirst = true;
        for (ColumnData cd : this)
        {
            if (isFirst) isFirst = false; else sb.append(", ");
            if (fullDetails)
            {
                if (cd.column().isSimple())
                {
                    sb.append(cd);
                }
                else
                {
                    ComplexColumnData complexData = (ComplexColumnData)cd;
                    if (!complexData.complexDeletion().isLive())
                        sb.append("del(").append(cd.column().name).append(")=").append(complexData.complexDeletion());
                    for (Cell cell : complexData)
                        sb.append(", ").append(cell);
                }
            }
            else
            {
                if (cd.column().isSimple())
                {
                    Cell cell = (Cell)cd;
                    sb.append(cell.column().name).append('=');
                    if (cell.isTombstone())
                        sb.append("<tombstone>");
                    else
                        sb.append(cell.column().type.getString(cell.value()));
                }
                else
                {
                    sb.append(cd.column().name).append('=');
                    ComplexColumnData complexData = (ComplexColumnData) cd;
                    Function<Cell, String> transform;
                    if (cd.column().type.isCollection())
                    {
                        CollectionType ct = (CollectionType) cd.column().type;
                        transform = cell -> String.format("%s -> %s",
                                                  ct.nameComparator().getString(cell.path().get(0)),
                                                  ct.valueComparator().getString(cell.value()));

                    }
                    else if (cd.column().type.isUDT())
                    {
                        UserType ut = (UserType)cd.column().type;
                        transform = cell -> {
                            Short fId = ut.nameComparator().getSerializer().deserialize(cell.path().get(0));
                            return String.format("%s -> %s",
                                                 ut.fieldNameAsString(fId),
                                                 ut.fieldType(fId).getString(cell.value()));
                        };
                    }
                    else
                    {
                        transform = cell -> "";
                    }
                    sb.append(StreamSupport.stream(complexData.spliterator(), false)
                                           .map(transform)
                                           .collect(Collectors.joining(", ", "{", "}")));
                }
            }
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof Row))
            return false;

        Row that = (Row)other;
        if (!this.clustering().equals(that.clustering())
             || !this.primaryKeyLivenessInfo().equals(that.primaryKeyLivenessInfo())
             || !this.deletion().equals(that.deletion()))
            return false;

        return Iterables.elementsEqual(this, that);
    }

    @Override
    public int hashCode()
    {
        int hash = Objects.hash(clustering(), primaryKeyLivenessInfo(), deletion());
        for (ColumnData cd : this)
            hash += 31 * cd.hashCode();
        return hash;
    }

    // a simple marker class that will sort to the beginning of a run of complex cells to store the deletion time
    protected static class ComplexColumnDeletion extends BufferCell
    {
        public ComplexColumnDeletion(ColumnMetadata column, DeletionTime deletionTime)
        {
            super(column, deletionTime.markedForDeleteAt(), 0, deletionTime.localDeletionTime(), ByteBufferUtil.EMPTY_BYTE_BUFFER, CellPath.BOTTOM);
        }
    }

    // converts a run of Cell with equal column into a ColumnData
    static class CellResolver implements BTree.Builder.Resolver
    {
        final int nowInSec;
        CellResolver(int nowInSec)
        {
            this.nowInSec = nowInSec;
        }

        public ColumnData resolve(Object[] cells, int lb, int ub)
        {
            Cell cell = (Cell) cells[lb];
            ColumnMetadata column = cell.column;
            if (cell.column.isSimple())
            {
                assert lb + 1 == ub || nowInSec != Integer.MIN_VALUE;
                while (++lb < ub)
                    cell = Cells.reconcile(cell, (Cell) cells[lb], nowInSec);
                return cell;
            }

            // TODO: relax this in the case our outer provider is sorted (want to delay until remaining changes are
            // bedded in, as less important; galloping makes it pretty cheap anyway)
            Arrays.sort(cells, lb, ub, (Comparator<Object>) column.cellComparator());
            DeletionTime deletion = DeletionTime.LIVE;
            // Deal with complex deletion (for which we've use "fake" ComplexColumnDeletion cells that we need to remove).
            // Note that in almost all cases we'll at most one of those fake cell, but the contract of {{Row.Builder.addComplexDeletion}}
            // does not forbid it being called twice (especially in the unsorted case) and this can actually happen when reading
            // legacy sstables (see #10743).
            while (lb < ub)
            {
                cell = (Cell) cells[lb];
                if (!(cell instanceof ComplexColumnDeletion))
                    break;

                if (cell.timestamp() > deletion.markedForDeleteAt())
                    deletion = new DeletionTime(cell.timestamp(), cell.localDeletionTime());
                lb++;
            }

            List<Object> buildFrom = new ArrayList<>(ub - lb);
            Cell previous = null;
            for (int i = lb; i < ub; i++)
            {
                Cell c = (Cell) cells[i];

                if (deletion == DeletionTime.LIVE || c.timestamp() >= deletion.markedForDeleteAt())
                {
                    if (previous != null && column.cellComparator().compare(previous, c) == 0)
                    {
                        c = Cells.reconcile(previous, c, nowInSec);
                        buildFrom.set(buildFrom.size() - 1, c);
                    }
                    else
                    {
                        buildFrom.add(c);
                    }
                    previous = c;
                }
            }

            Object[] btree = BTree.build(buildFrom, UpdateFunction.noOp());
            return new ComplexColumnData(column, btree, deletion);
        }
    }
}
