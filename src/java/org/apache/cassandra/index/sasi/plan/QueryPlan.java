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
package org.apache.cassandra.index.sasi.plan;

import java.util.*;

import io.reactivex.Flowable;
import io.reactivex.functions.Function;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sasi.plan.Operation.OperationType;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FlowableUtils;

public class QueryPlan
{
    private final QueryController controller;

    public QueryPlan(ColumnFamilyStore cfs, ReadCommand command, long executionQuotaMs)
    {
        this.controller = new QueryController(cfs, (PartitionRangeReadCommand) command, executionQuotaMs);
    }

    /**
     * Converts expressions into operation tree (which is currently just a single AND).
     *
     * Operation tree allows us to do a couple of important optimizations
     * namely, group flattening for AND operations (query rewrite), expression bounds checks,
     * "satisfies by" checks for resulting rows with an early exit.
     *
     * @return root of the operations tree.
     */
    private Operation analyze()
    {
        try
        {
            Operation.Builder and = new Operation.Builder(OperationType.AND, controller);
            controller.getExpressions().forEach(and::add);
            return and.complete();
        }
        catch (Exception | Error e)
        {
            controller.finish();
            throw e;
        }
    }

    public Flowable<FlowableUnfilteredPartition> execute(ReadExecutionController executionController) throws RequestTimeoutException
    {
        return new ResultRetriever(analyze(), controller, executionController).getPartitions();
    }

    private static class ResultRetriever implements Function<DecoratedKey, Flowable<FlowableUnfilteredPartition>>
    {
        private final AbstractBounds<PartitionPosition> keyRange;
        private final Operation operationTree;
        private final QueryController controller;
        private final ReadExecutionController executionController;

        private Iterator<DecoratedKey> currentKeys = null;

        public ResultRetriever(Operation operationTree, QueryController controller, ReadExecutionController executionController)
        {
            this.keyRange = controller.dataRange().keyRange();
            this.operationTree = operationTree;
            this.controller = controller;
            this.executionController = executionController;
        }

        public Flowable<FlowableUnfilteredPartition> getPartitions()
        {
            if (operationTree == null)
                return Flowable.empty();

            operationTree.skipTo((Long) keyRange.left.getToken().getTokenValue());

            Flowable<DecoratedKey> keys = Flowable.fromIterable(() -> operationTree)
                                                  .lift(FlowableUtils.concatMapLazy(Flowable::fromIterable));

            if (!keyRange.right.isMinimum())
                keys = keys.takeWhile(key -> keyRange.right.compareTo(key) >= 0);

            return keys.lift(FlowableUtils.concatMapLazy(this))
                       .doFinally(this::close);
        }

        public Flowable<FlowableUnfilteredPartition> apply(DecoratedKey key)
        {
            Flowable<FlowableUnfilteredPartition> fp = controller.getPartition(key, executionController);
            return fp.map(partition ->
            {
                Row staticRow = partition.staticRow;

                Flowable<Unfiltered> filteredContent = partition.content
                    .filter(row -> operationTree.satisfiedBy(row, staticRow, true));

                return new FlowableUnfilteredPartition(partition.header, partition.staticRow, filteredContent);
            });
        }

        public void close()
        {
            FileUtils.closeQuietly(operationTree);
            controller.finish();
        }
    }
}
