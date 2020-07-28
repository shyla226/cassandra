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
package org.apache.cassandra.index.sai.plan;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ListMultimap;

import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.plan.Expression.Op;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.ColumnMetadata.Kind;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.index.sai.plan.Operation.OperationType;

/**
 * Tree-like structure to filter base table data using indexed expressions and non-user-defined filters.
 *
 * This is needed because:
 * 1. SAI doesn't index tombstones, base data may have been shadowed.
 * 2. SAI indexes partition offset, not all rows in partition match index condition.
 * 3. Replica filter protecting may fetch data that doesn't match index expressions.
 */
public class FilterTree
{
    protected final OperationType op;
    protected final ListMultimap<ColumnMetadata, Expression> expressions;

    protected final FilterTree left;
    protected final FilterTree right;

    FilterTree(OperationType operation,
               ListMultimap<ColumnMetadata, Expression> expressions,
               FilterTree left, FilterTree right)
    {
        this.op = operation;
        this.expressions = expressions;

        this.left = left;
        this.right = right;
    }

    /**
     * Recursive "satisfies" checks based on operation
     * and data from the lower level members using depth-first search
     * and bubbling the results back to the top level caller.
     *
     * Most of the work here is done by localSatisfiedBy(Unfiltered, Row, boolean)
     * see it's comment for details, if there are no local expressions
     * assigned to Operation it will call satisfiedBy(Row) on it's children.
     *
     * Query: first_name = X AND (last_name = Y OR address = XYZ AND street = IL AND city = C) OR (state = 'CA' AND country = 'US')
     * Row: key1: (first_name: X, last_name: Z, address: XYZ, street: IL, city: C, state: NY, country:US)
     *
     * #1                       OR
     *                        /    \
     * #2       (first_name) AND   AND (state, country)
     *                          \
     * #3            (last_name) OR
     *                             \
     * #4                          AND (address, street, city)
     *
     *
     * Evaluation of the key1 is top-down depth-first search:
     *
     * --- going down ---
     * Level #1 is evaluated, OR expression has to pull results from it's children which are at level #2 and OR them together,
     * Level #2 AND (state, country) could be be evaluated right away, AND (first_name) refers to it's "right" child from level #3
     * Level #3 OR (last_name) requests results from level #4
     * Level #4 AND (address, street, city) does logical AND between it's 3 fields, returns result back to level #3.
     * --- bubbling up ---
     * Level #3 computes OR between AND (address, street, city) result and it's "last_name" expression
     * Level #2 computes AND between "first_name" and result of level #3, AND (state, country) which is already computed
     * Level #1 does OR between results of AND (first_name) and AND (state, country) and returns final result.
     *
     * @param key The partition key for the row.
     * @param currentCluster The row cluster to check.
     * @param staticRow The static row associated with current cluster.
     * @param allowMissingColumns allow columns value to be null.
     * @return true if give Row satisfied all of the expressions in the tree,
     *         false otherwise.
     */
    public boolean satisfiedBy(DecoratedKey key, Unfiltered currentCluster, Row staticRow, boolean allowMissingColumns)
    {
        boolean sideL, sideR;

        if (expressions == null || expressions.isEmpty())
        {
            sideL =  left != null &&  left.satisfiedBy(key, currentCluster, staticRow, allowMissingColumns);
            sideR = right != null && right.satisfiedBy(key, currentCluster, staticRow, allowMissingColumns);

            // one of the expressions was skipped
            // because it had no indexes attached
            if (left == null)
                return sideR;
        }
        else
        {
            sideL = localSatisfiedBy(key, currentCluster, staticRow, allowMissingColumns);

            // if there is no right it means that this expression
            // is last in the sequence, we can just return result from local expressions
            if (right == null)
                return sideL;

            sideR = right.satisfiedBy(key, currentCluster, staticRow, allowMissingColumns);
        }


        return op.apply(sideL, sideR);
    }

    /**
     * Check every expression in the analyzed list to figure out if the
     * columns in the give row match all of the based on the operation
     * set to the current operation node.
     *
     * The algorithm is as follows: for every given expression from analyzed
     * list get corresponding column from the Row:
     *   - apply {@link Expression#isSatisfiedBy(ByteBuffer)}
     *     method to figure out if it's satisfied;
     *   - apply logical operation between boolean accumulator and current boolean result;
     *   - if result == false and node's operation is AND return right away;
     *
     * After all of the expressions have been evaluated return resulting accumulator variable.
     *
     * Example:
     *
     * Operation = (op: AND, columns: [first_name = p, 5 < age < 7, last_name: y])
     * Row = (first_name: pavel, last_name: y, age: 6, timestamp: 15)
     *
     * #1 get "first_name" = p (expressions)
     *      - row-get "first_name"                      => "pavel"
     *      - compare "pavel" against "p"               => true (current)
     *      - set accumulator current                   => true (because this is expression #1)
     *
     * #2 get "last_name" = y (expressions)
     *      - row-get "last_name"                       => "y"
     *      - compare "y" against "y"                   => true (current)
     *      - set accumulator to accumulator & current  => true
     *
     * #3 get 5 < "age" < 7 (expressions)
     *      - row-get "age"                             => "6"
     *      - compare 5 < 6 < 7                         => true (current)
     *      - set accumulator to accumulator & current  => true
     *
     * #4 return accumulator => true (row satisfied all of the conditions)
     *
     * @param key The partition key for the row.
     * @param currentCluster The row cluster to check.
     * @param staticRow The static row associated with current cluster.
     * @param allowMissingColumns allow columns value to be null.
     * @return true if give Row satisfied all of the analyzed expressions,
     *         false otherwise.
     */
    private boolean localSatisfiedBy(DecoratedKey key, Unfiltered currentCluster, Row staticRow, boolean allowMissingColumns)
    {
        if (currentCluster == null || !currentCluster.isRow())
            return false;

        final int now = FBUtilities.nowInSeconds();
        boolean result = false;
        int idx = 0;

        for (Map.Entry<ColumnMetadata, Expression> entry : expressions.entries())
        {
            ColumnMetadata column = entry.getKey();
            ColumnContext context = entry.getValue().context;
            Row row = column.kind == Kind.STATIC ? staticRow : (Row)currentCluster;

            // If there is a column with multiple expressions that effectively means an OR
            // e.g. comment = 'x y z' could be split into 'comment' EQ 'x', 'comment' EQ 'y', 'comment' EQ 'z'
            // by analyzer, in situation like that we only need to check if at least one of expressions matches,
            // and there is no hit on the NOT_EQ (if any) which are always at the end of the filter list.
            // Loop always starts from the end of the list, which makes it possible to break after the last
            // NOT_EQ condition on first EQ/RANGE condition satisfied, instead of checking every
            // single expression in the column filter list.
            List<Expression> filters = expressions.get(column);

            boolean match;

            if (context.isCollection())
            {
                Iterator<ByteBuffer> valueIterator = context.getValuesOf(row, now);

                if (!allowMissingColumns && valueIterator == null)
                    throw new IllegalStateException("All indexed columns should be included into the column slice, missing: " + column);

                match = valueIterator != null && collectionMatch(valueIterator, filters);
            }
            else
            {
                ByteBuffer value = context.getValueOf(key, row, now);

                if (!allowMissingColumns && value == null)
                    throw new IllegalStateException("All indexed columns should be included into the column slice, missing: " + column);

                match = singletonMatch(value, filters);
            }

            if (idx++ == 0)
            {
                result = match;
                continue;
            }

            result = op.apply(result, match);

            // exit early because we already got a single false
            if (op == OperationType.AND && !result)
                return false;
        }

        return idx == 0 || result;
    }

    private boolean singletonMatch(ByteBuffer value, List<Expression> filters)
    {
        boolean isMissingColumn = value == null;
        boolean match = false;

        for (int i = filters.size() - 1; i >= 0; i--)
        {
            Expression expression = filters.get(i);
            match = !isMissingColumn && expression.isSatisfiedBy(value);
            if (expression.getOp() == Op.NOT_EQ)
            {
                // since this is NOT_EQ operation we have to
                // inverse match flag (to check against other expressions),
                // and break in case of negative inverse because that means
                // that it's a positive hit on the not-eq clause.
                match = !match;
                if (!match)
                    break;
            } // if it was a match on EQ/RANGE or column is missing
            else if (match || isMissingColumn)
                break;
        }
        return match;
    }

    private boolean collectionMatch(Iterator<ByteBuffer> valueIterator, List<Expression> filters)
    {
        // We currently always treat multiple expressions as an and because
        // we can expect collections to contain more than one value
        Set<Integer> matchedExpressions = new HashSet<>(filters.size());
        while (valueIterator.hasNext() && (matchedExpressions.size() != filters.size()))
        {
            ByteBuffer value = valueIterator.next();
            if (value == null)
                continue;
            for (int index = 0; index < filters.size(); index++)
            {
                Expression expression = filters.get(index);
                if (expression.isSatisfiedBy(value))
                    matchedExpressions.add(index);
            }
        }
        return matchedExpressions.size() == filters.size();
    }
}
