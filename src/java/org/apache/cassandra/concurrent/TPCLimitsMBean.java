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

package org.apache.cassandra.concurrent;

/**
 * Created by blambov on 11.08.17.
 */
public interface TPCLimitsMBean
{
    /**
     * Maximum size of the pending queue.
     *
     * "Pendable" task types (ones that start an operation, e.g. READ and WRITE) are moved to this queue if the
     * queue size above is reached. When this is full we will start rejecting pendable tasks.
     */
    int maxPendingQueueSize();

    /**
     * Returns the number of permitted concurrent requests.
     */
    int getMaxConcurrentRequests();

    /**
     * Adjusts the number of permitted concurrent requests.
     */
    void setMaxConcurrentRequests(int maxConcurrentRequests);
}
