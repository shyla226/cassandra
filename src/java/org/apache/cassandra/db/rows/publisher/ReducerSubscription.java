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


import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.internal.disposables.EmptyDisposable;
import io.reactivex.internal.functions.ObjectHelper;
import org.apache.cassandra.db.rows.PartitionTrait;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * A subscription for reducing partitions into an item of type R.
 *
 * R1 - the type of the final result
 * R2 - the type of the intermediate partition result
 */
class ReducerSubscription<R1, R2> extends Single<R1> implements PartitionsSubscriber<Unfiltered>
{
    private final PartitionsPublisher publisher;
    private final ReduceCallbacks<R1, R2> callbacks;

    private PartitionsSubscription subscription;
    private SingleObserver<? super R1> downstream;

    private R1 result;
    private PartitionTrait partition;
    private R2 partitionResult;

    ReducerSubscription(PartitionsPublisher publisher,
                        ReduceCallbacks<R1, R2> callbacks)
    {
        this.publisher = publisher;
        this.callbacks = callbacks;
    }

    @Override
    protected void subscribeActual(SingleObserver<? super R1> observer) {
        assert subscription == null: "Can only subscribe once";

        try {
            result = callbacks.resultSupplier.call();
        } catch (Throwable ex) {
            Exceptions.throwIfFatal(ex);
            EmptyDisposable.error(ex, observer);
            return;
        }

        downstream = observer;
        publisher.subscribe(this);
    }

    public void onSubscribe(PartitionsSubscription subscription)
    {
        this.subscription = subscription;
    }

    public void onNextPartition(PartitionTrait partition)
    {
        try
        {
            if (partitionResult != null)
                completePartition(partitionResult); // close the previous partition

            this.partition = partition;
            partitionResult = callbacks.partitionSupplier.apply(result, partition);

            if (partitionResult == null)
            {
                subscription.closePartition(partition.partitionKey());
                completePartition(partitionResult);
            }
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            subscription.close();
            onError(t);
        }
    }

    public void onNext(Unfiltered item)
    {
        try
        {
            if (partitionResult != null)
            {
                R2 current = partitionResult;
                partitionResult = callbacks.itemReducer.apply(result, current, item);
                if (partitionResult == null)
                {
                    subscription.closePartition(partition.partitionKey());
                    completePartition(current);
                }
            }
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            subscription.close();
            onError(t);
        }
    }

    @Override
    public void onError(Throwable e)
    {
        downstream.onError(e);
    }

    @Override
    public void onComplete()
    {
        if (partitionResult != null)
            completePartition(partitionResult);
        downstream.onSuccess(result);
    }

    private void completePartition(R2 partitionResult)
    {
        try
        {
            result = callbacks.partitionReducer.apply(result, partitionResult, partition);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            subscription.close();
            onError(t);
        }
        finally
        {
            this.partitionResult = null;
            this.partition = null;
        }
    }
}
