package org.apache.cassandra.db.rows;

import java.util.Comparator;

import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.rows.UnfilteredRowIterators.MergeListener;
import org.apache.cassandra.utils.Reducer;

public class MergeReducer extends Reducer<Unfiltered, Unfiltered> implements Comparator<Unfiltered>
{
    private final MergeListener listener;

    private Unfiltered.Kind nextKind;

    private final int size;
    private final int nowInSec;

    private Row.Merger rowMerger;
    private RangeTombstoneMarker.Merger markerMerger;
    private boolean regularRowMergerConstructed;

    private PartitionHeader.Merger headerMerger;
    private PartitionHeader header;
    private Comparator<Clusterable> comparator;

    public MergeReducer(int size, int nowInSec, MergeListener listener)
    {
        this.size = size;
        this.markerMerger = null;
        this.listener = listener;
        this.headerMerger = null;
        this.nowInSec = nowInSec;
        this.rowMerger = null;
        this.regularRowMergerConstructed = false;
    }

    private void maybeInitMarkerMerger()
    {
        if (markerMerger == null)
            markerMerger = new RangeTombstoneMarker.Merger(size, header.partitionLevelDeletion, header.isReverseOrder);
    }

    @Override
    public boolean trivialReduceIsTrivial()
    {
        // If we have a listener, we must signal it even when we have a single version
        return listener == null;
    }

    public void reduce(int idx, Unfiltered current)
    {
        nextKind = current.kind();
        switch (nextKind)
        {
        case ROW:
            assert header != null;
            if (rowMerger == null)
            {
                Columns columns;
                if (!((Row) current).isStatic())
                {
                    columns = header.columns.regulars;
                    regularRowMergerConstructed = true;
                }
                else
                    columns = header.columns.statics;

                rowMerger = new Row.Merger(size, nowInSec, columns.size(), columns.hasComplex());
            }
            rowMerger.add(idx, (Row) current);
            break;
        case RANGE_TOMBSTONE_MARKER:
            assert headerMerger == null;
            maybeInitMarkerMerger();
            markerMerger.add(idx, (RangeTombstoneMarker) current);
            break;
        case HEADER:
            if (headerMerger == null)
                headerMerger = new PartitionHeader.Merger(size, idx, (PartitionHeader) current, listener);
            else
                headerMerger.add(idx, (PartitionHeader) current);
            break;
        }
    }

    protected Unfiltered getReduced()
    {
        switch (nextKind)
        {
            case ROW:
            {
                Row merged = rowMerger.merge(markerMerger == null ? header.partitionLevelDeletion : markerMerger.activeDeletion());
                if (merged == null)
                    // we are not allowed to return null
                    return BTreeRow.emptyRow(rowMerger.mergedClustering());

                if (listener != null)
                    listener.onMergedRows(merged, rowMerger.mergedRows());
                return merged;
            }
            case RANGE_TOMBSTONE_MARKER:
            {
                maybeInitMarkerMerger();
                RangeTombstoneMarker merged = markerMerger.merge();
                if (listener != null)
                    listener.onMergedRangeTombstoneMarkers(merged, markerMerger.mergedMarkers());
                return merged;
            }
            case HEADER:
            {
                header = headerMerger.merge();
                headerMerger = null;
                comparator = header.metadata.comparator;
                if (header.isReverseOrder)
                    comparator = comparator.reversed();
                return header;
            }
        }
        throw new AssertionError();
    }

    protected void onKeyChange()
    {
        if (nextKind == null)
            return;
        switch (nextKind)
        {
        case ROW:
            rowMerger.clear();
            if (!regularRowMergerConstructed)
                rowMerger = null;
            break;
        case RANGE_TOMBSTONE_MARKER:
            if (markerMerger != null)
                markerMerger.clear();
            break;
        case HEADER:
            break;
        }
    }

    @Override
    public int compare(Unfiltered o1, Unfiltered o2)
    {
        switch (o1.clustering().kind())
        {
        case HEADER_CLUSTERING:
        case STATIC_CLUSTERING:
            // Note this doesn't use comparator (which is not set yet for header, and wrong for static in reverse)
            assert o2.clustering().kind() == o2.clustering().kind();
            return 0;
        default:
            return comparator.compare(o1, o2);
        }
    }
}