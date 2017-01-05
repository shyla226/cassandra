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

class MonitorableImpl implements Monitorable
{
    private MonitoringState state;
    private boolean isSlow;
    private final String name;
    private final long constructionTime;
    private final long timeout;
    private final long slowTimeout;
    private final boolean isCrossNode;
    private final boolean skipReporting;

    /**
     * Create a monitorable for the query invoking this constructor.
     *
     * @param name - the name of the query, this will be reported in log files by {@link MonitoringTask}
     * @param constructionTime - the time the query was constructed
     * @param timeout - the timeout after which the query should be aborted and optionally reported as failed
     * @param slowTimeout - the timeout after which the query should be reported as slow, if > zero
     * @param isCrossNode - true if the query originated cross node
     * @param skipReporting - true if the query should not be reported as failed or slow, but simply aborted
     */
    MonitorableImpl(String name, long constructionTime, long timeout, long slowTimeout, boolean isCrossNode, boolean skipReporting)
    {
        this.name = name;
        this.constructionTime = constructionTime;
        this.timeout = timeout;
        this.slowTimeout = slowTimeout;
        this.state = MonitoringState.IN_PROGRESS;
        this.isSlow = false;
        this.isCrossNode = isCrossNode;
        this.skipReporting = skipReporting;
    }

    public String name()
    {
        return name;
    }

    public long constructionTime()
    {
        return constructionTime;
    }

    public long timeout()
    {
        return timeout;
    }

    public boolean isCrossNode()
    {
        return isCrossNode;
    }

    public long slowTimeout()
    {
        return slowTimeout;
    }

    public boolean isInProgress()
    {
        check();
        return state == MonitoringState.IN_PROGRESS;
    }

    public boolean isAborted()
    {
        check();
        return state == MonitoringState.ABORTED;
    }

    public boolean isCompleted()
    {
        check();
        return state == MonitoringState.COMPLETED;
    }

    public boolean isSlow()
    {
        check();
        return isSlow;
    }

    public boolean abort()
    {
        if (state == MonitoringState.IN_PROGRESS)
        {
            if (!skipReporting && constructionTime >= 0)
                MonitoringTask.addFailedOperation(this, ApproximateTime.currentTimeMillis());

            state = MonitoringState.ABORTED;
            return true;
        }

        return state == MonitoringState.ABORTED;
    }

    public boolean complete()
    {
        if (state == MonitoringState.IN_PROGRESS)
        {
            if (!skipReporting && isSlow && slowTimeout > 0 && constructionTime >= 0)
                MonitoringTask.addSlowOperation(this, ApproximateTime.currentTimeMillis());

            state = MonitoringState.COMPLETED;
            return true;
        }

        return state == MonitoringState.COMPLETED;
    }

    private void check()
    {
        if (constructionTime < 0 || state != MonitoringState.IN_PROGRESS)
            return;

        long elapsed = ApproximateTime.currentTimeMillis() - constructionTime;

        if (elapsed >= slowTimeout && !isSlow)
            isSlow = true;

        if (elapsed >= timeout)
            abort();
    }
}
