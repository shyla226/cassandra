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
package org.apache.cassandra.db.transform;

import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Unfiltered;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

// A Transformation that can stop an iterator earlier than its natural exhaustion
public abstract class StoppingTransformation<I extends BaseRowIterator<?>> extends Transformation<I>
{
    BaseIterator.Stop stop;
    BaseIterator.Stop stopInPartition;

    /**
     * If invoked by a subclass, any partitions iterator this transformation has been applied to will terminate
     * after any currently-processing item is returned, as will any row/unfiltered iterator
     */
    @DontInline
    public void stop()
    {
        if (stop != null)
            stop.isSignalled = true;
        stopInPartition();
    }

    /**
     * If invoked by a subclass, any rows/unfiltered iterator this transformation has been applied to will terminate
     * after any currently-processing item is returned
     */
    @DontInline
    protected void stopInPartition()
    {
        if (stopInPartition != null)
            stopInPartition.isSignalled = true;
    }

    @Override
    protected void attachTo(BasePartitions partitions)
    {
        assert this.stop == null;
        this.stop = partitions.stop;
    }

    @Override
    protected void attachTo(BaseRows rows)
    {
        assert this.stopInPartition == null;
        this.stopInPartition = rows.stop;
    }

    @Override
    public void onClose()
    {
        stop = null;
    }

    @Override
    protected void onPartitionClose()
    {
        stopInPartition = null;
    }

    // FlowableOp interpretation of transformation
    @Override
    public void onNextUnfiltered(Subscriber<? super Unfiltered> subscriber, Subscription source, Unfiltered item)
    {
        if (stopInPartition != null && stopInPartition.isSignalled)
        {
            source.cancel();
            onPartitionClose();
            subscriber.onComplete();
            return;
        }

        super.onNextUnfiltered(subscriber, source, item);
    }

    // FlowableOp interpretation of transformation
    @Override
    public void onNextPartition(Subscriber<? super FlowableUnfilteredPartition> subscriber, Subscription source, FlowableUnfilteredPartition item)
    {
        if (stop != null && stop.isSignalled)
        {
            item.unused();
            source.cancel();
            onClose();
            subscriber.onComplete();
            return;
        }

        super.onNextPartition(subscriber, source, item);
    }

}
