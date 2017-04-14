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

import com.google.common.base.Throwables;

import org.apache.cassandra.db.transform.BaseIterator;
import org.apache.cassandra.utils.flow.CsFlow;
import org.apache.cassandra.utils.flow.CsSubscriber;

/**
 * Base class for the CsFlow versions of partitions.
 */
public abstract class FlowablePartitionBase<T>
{
    /**
     * The header contains information about the partition: key, metadata etc.
     * Normally reused through transformations, merging, filtering etc.
     */
    public final PartitionHeader header;

    /**
     * The static part corresponding to this partition.
     */
    public final Row staticRow;

    /**
     * The partition's contents as a CsFlow. This must be subscribed to exactly once, and will close all
     * associated resources when the subscription completes (complete/error/cancel).
     */
    public final CsFlow<T> content;

    /** Signalled by a stopping transformation when it wants to stop */
    public BaseIterator.Stop stop = new BaseIterator.Stop();

    public FlowablePartitionBase(PartitionHeader header,
                                 Row staticRow,
                                 CsFlow<T> content)
    {
        this.header = header;
        this.staticRow = staticRow;
        this.content = content;
    }

    /**
     * Only to be called on requested but unused partitions (e.g. when aborting).
     * Since we usually verify one use only, this will throw if the partition was already used.
     */
    public void unused()
    {
        try
        {
            content.subscribe(new CsSubscriber<T>()
            {
                public void onNext(T item)
                {
                }

                public void onComplete()
                {
                }

                public void onError(Throwable t)
                {
                }
            }).close();
        }
        catch (Exception e)
        {
            throw Throwables.propagate(e);
        }
    }
}
