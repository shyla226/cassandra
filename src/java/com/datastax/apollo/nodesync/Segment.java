/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.nodesync;

import java.util.Objects;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A segment as manipulated by NodeSync, that is a (non-wrapping) token range for a particular table.
 * <p>
 * Segments are NodeSync natural save points and granularity: NodeSync will validate table segments one at a time and
 * save progress after each segment. So they correspond to ranges of data that are neither too big (we don't have to
 * redo too much work on failure and tables are validated "fairly") nor too small (to keep the cost of saving progress
 * low).
 * <p>
 * It is an invariant of table segments that the range they represent cannot be wrapping (to keep things simple for
 * segment consumers).
 * <p>
 * See {@link Segments} for details on how segments are generated and the practical implications of it.
 */
public class Segment implements Comparable<Segment>
{
    public final TableMetadata table;
    public final Range<Token> range;

    public Segment(TableMetadata table, Range<Token> range)
    {
        assert table != null && range != null;
        assert !range.isTrulyWrapAround();
        this.table = table;
        this.range = range;
    }

    public int compareTo(Segment that)
    {
        // The actual order in which we compare segment is not that relevant since we only use it to pick between 2
        // segments when we don't have better information to decide which should have higher priority, so typically
        // when both are in a equal situation of validation, and the actual choice isn't too meaningful. We do compare
        // ranges first so that when prioritizing lots of segments from different tables and we have no better info
        // (say, NodeSync wasn't enabled and we just started it so we have no NodeSync info whatsoever), we interleave
        // segments from different tables better (instead of "everything for table 1, then table2, ...").
        // As an aside, for some (unknown to me) reason the default Range.compareTo() compares by right token (first and
        // only) which feels a bit unintuitive here so we don't use that.
        int cmp = this.range.left.compareTo(that.range.left);
        if (cmp != 0)
            return cmp;
        cmp = this.range.right.compareTo(that.range.right);
        if (cmp != 0)
            return cmp;
        return this.table.id.compareTo(that.table.id);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof Segment))
            return false;

        Segment that = (Segment)o;
        return this.table.id.equals(that.table.id) && this.range.equals(that.range);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, range);
    }

    @Override
    public String toString()
    {
        return String.format("%s-%s", table, range);
    }
}
