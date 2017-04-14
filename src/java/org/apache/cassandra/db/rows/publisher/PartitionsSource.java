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

package org.apache.cassandra.db.rows.publisher;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.utils.flow.CsFlow;

/**
 * A source of partitions for {@link PartitionsPublisher}.
 * This is required because in some cases the source needs to be protected by an execution controller, in some
 * other cases it doesn't need to. In some cases, the flowable is instead created at some later point in time, TBD.
 */
interface PartitionsSource extends Supplier<CsFlow<FlowableUnfilteredPartition>>, AutoCloseable
{
    @Override
    public default void close() { }

    /**
     * Create a partition source coming from data on disk or memtable, therefore protected by a read execution controller.
     *
     * @param command - the command for which we need to create the read execution controller
     * @param source - a function that given a controller, creates a flowable of unfiltered partitions
     * @return the partition source
     */
    static PartitionsSource withController(ReadCommand command, Function<ReadExecutionController, CsFlow<FlowableUnfilteredPartition>> source)
    {
        return new PartitionsSource()
        {
            ReadExecutionController controller;
            public void close()
            {
                if (controller != null)
                {
                    controller.close();
                    controller = null;
                }
            }

            public CsFlow<FlowableUnfilteredPartition> get()
            {
                controller = ReadExecutionController.forCommand(command);
                return source.apply(controller);
            }
        };
    }

    /**
     * Concat a list of sources.
     *
     * @param sources - all the sources to concatenate
     *
     * @return - a concatenated source
     */
    static PartitionsSource concat(final List<PartitionsSource> sources)
    {
        return new PartitionsSource()
        {
            public void close()
            {
                for (PartitionsSource source : sources)
                    source.close();
            }

            public CsFlow<FlowableUnfilteredPartition> get()
            {
                return CsFlow.fromIterable(sources).flatMap(PartitionsSource::get);
            }
        };
    }
}
