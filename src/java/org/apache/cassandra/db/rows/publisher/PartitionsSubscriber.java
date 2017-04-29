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

import org.apache.cassandra.db.rows.PartitionTrait;
import org.apache.cassandra.db.rows.Unfiltered;

/**
 * A class to subscribe to a flow of partitions, either filtered or unfiltered.
 * The onXXXX() methods below are guaranteed to be thread safe, either because they
 * execute on the same thread or because they execute sequentially and without overlap.
 * Therefore, no further synchronization is required.
 *
 * Throwing an exception in onNextPartition or onNext will result in onError being
 * called with said exception.
 *
 * @param <T> - Unfiltered for unfiltered partitions, Row for filtered partitions.
 */
public interface PartitionsSubscriber<T extends Unfiltered>
{
    /**
     * Called when the subscriber has been subscribed to the publisher.
     *
     * @param subscription - the subscription, to be used for cancelling the flow.
     */
    public void onSubscribe(PartitionsSubscription subscription);

    /**
     * A new partition is starting.
     *
     * @param partition - the partition information
     */
    public void onNextPartition(PartitionTrait partition);

    /**
     * A new item is available, an item is either an Unfiltered or a Row depending
     * on whether the partition has been filtered.
     *
     * @param item - the next item to process
     */
    public void onNext(T item);

    /**
     * An error has occurred.
     *
     * @param error - an error that has occurred upstream or in a previous onNextXXXX invocation.
     */
    public void onError(Throwable error);

    /**
     * The stream is finished. This is sent when the data is completed.
     */
    public void onComplete();
}
