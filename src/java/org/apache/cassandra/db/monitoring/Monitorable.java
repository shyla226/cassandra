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

package org.apache.cassandra.db.monitoring;

public interface Monitorable
{
    String name();

    long constructionTime();
    long timeout();
    long slowTimeout();

    boolean isInProgress();
    boolean isAborted();
    boolean isCompleted();
    boolean isSlow();
    boolean isCrossNode();

    boolean abort();
    boolean complete();

    /**
     * Normal queries get monitored by {@link org.apache.cassandra.db.ReadCommandVerbHandler} and by
     * {@link org.apache.cassandra.service.StorageProxy.LocalReadRunnable} in order to abort queries that
     * take longer than the RPC timeout and report any unusually slow queries.
     *
     * @param name - the name of the query
     * @param constructionTime - the time when the query was received, if cross node, or started, if local
     * @param timeout - the timeout after which the query should be aborted
     * @param slowTimeout - the timeout after which the query should be reported slow
     * @param isCrossNode - true if the query originated on a cross node
     *
     * @return a monitorable for the query
     */
    public static Monitorable forNormalQuery(String name, long constructionTime, long timeout, long slowTimeout, boolean isCrossNode)
    {
        return new MonitorableImpl(name, constructionTime, timeout, slowTimeout, isCrossNode, false);
    }

    /**
     * Local queries get monitored by {@link org.apache.cassandra.service.StorageProxy} in order to
     * avoid keeping resources for too long. They are resumed later on, so they should not be reported as
     * slow or failed.
     *
     * @param name - the name of the query
     * @param constructionTime - the time when the query started
     * @param timeout - the timeout after which the query should be aborted
     *
     * @return a monitorable for the query
     */
    public static Monitorable forLocalQuery(String name, long constructionTime, long timeout)
    {
        return new MonitorableImpl(name, constructionTime, timeout, 0, false, true);
    }
}
