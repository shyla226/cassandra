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

import org.apache.cassandra.db.DecoratedKey;

/**
 * A subscription is to {@link PartitionsPublisher}.
 *
 * Subscribers can use this class to stop the current partition or the entire flow.
 */
public interface PartitionsSubscription extends AutoCloseable
{
    /**
     * Stop receiving items for the current partition and release
     * any resources specific to the current partition.
     *
     * @param partitionKey - the partition key for the partition to close
     */
    public abstract void closePartition(DecoratedKey partitionKey);

    /**
     * Stop the entire flow and release any resources.
     */
    @Override
    public abstract void close();
}
