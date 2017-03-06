package org.apache.cassandra.db.rows;

import java.util.Comparator;

import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.rows.UnfilteredRowIterators.MergeListener;
import org.apache.cassandra.utils.Reducer;

public class MergeReducer extends Reducer<Unfiltered, Unfiltered>
{
    private final MergeListener listener;

    private Unfiltered.Kind nextKind;

    private final int size;
    private final int nowInSec;

    private Row.Merger rowMerger;
    private RangeTombstoneMarker.Merger markerMerger;

    private PartitionHeader header;

    public MergeReducer(int size, int nowInSec, PartitionHeader header, MergeListener listener)
    {
        this.size = size;
        this.markerMerger = null;
        this.listener = listener;
        this.nowInSec = nowInSec;
        assert header != null;
        Columns columns = header.columns.regulars;
        this.rowMerger = new Row.Merger(size, nowInSec, columns.size(), columns.hasComplex());
        this.header = header;
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
                rowMerger.add(idx, (Row) current);
                break;
            case RANGE_TOMBSTONE_MARKER:
                maybeInitMarkerMerger();
                markerMerger.add(idx, (RangeTombstoneMarker) current);
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
                    return null;

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
            break;
        case RANGE_TOMBSTONE_MARKER:
            markerMerger.clear();
            break;
        }
    }
}