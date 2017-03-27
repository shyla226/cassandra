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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.utils.FBUtilities;

public class Monitor
{
    private static final int TEST_ITERATION_DELAY_MILLIS = Integer.parseInt(System.getProperty("cassandra.test.read_iteration_delay_ms", "0"));

    final Monitorable operation;
    private final boolean withReporting;

    // We distinguish 2 start time: 'operationCreationTimeMillis' and 'monitoringStartTimeMillis'.
    // The former is when the operation monitored was created. This is generally before monitoring starts as it
    // includes the messaging of the operation to its destination. For instance, for a read request,
    // 'operationCreationTimeMillis' is when the read is initiated on the coordinator, while 'monitoringStartTimeMillis'
    // is when the read is locally started on the replica using this monitor.
    // We use 'operationCreationTimeMillis' as the start for timeouting queries, since that's the starting point the
    // coordinator will use to give up on the query, but we use 'monitoringStartMillis' for slow query logging as there
    // no real point in logging a query just because messaging is backing away (that doesn't mean the query itself is
    // slow, just that the cluster is overwhelm)
    final long operationCreationTimeMillis;
    private final long monitoringStartTimeMillis;

    final long timeoutMillis;
    final long slowQueryTimeoutMillis;

    // Whether the operation was locally delivered or if it has crossed node boundaries
    final boolean isLocalOperation;

    private MonitoringState state = MonitoringState.IN_PROGRESS;
    private boolean isSlow;

    private long lastChecked;

    private Monitor(Monitorable operation,
                    boolean withReporting,
                    long operationCreationTimeMillis,
                    long monitoringStartTimeMillis,
                    long timeoutMillis,
                    long slowQueryTimeoutMillis,
                    boolean isLocalOperation)
    {
        this.operation = operation;
        this.withReporting = withReporting;
        this.operationCreationTimeMillis = operationCreationTimeMillis;
        this.monitoringStartTimeMillis = monitoringStartTimeMillis;
        this.timeoutMillis = timeoutMillis;
        this.slowQueryTimeoutMillis = slowQueryTimeoutMillis;
        this.isLocalOperation = isLocalOperation;
    }

    public static boolean isTesting()
    {
        return TEST_ITERATION_DELAY_MILLIS != 0;
    }

    /**
     * Create and start an monitor one a particular operation.
     * <p>
     * The created monitor will log the operation if it run longer that the configured slow query timeout, and abort it
     * if it goes longer than the provided timeout.
     *
     * @param operation the monitored operation.
     * @param operationCreationTimeMillis the time at which the operation was created. This is the time use to timeout
     *                                    the operation.
     * @param timeoutMillis the timeout for the operation.
     * @param isLocalOperation whether the operation is local to the node or has crossed node boundaries.
     */
    public static Monitor createAndStart(Monitorable operation, long operationCreationTimeMillis, long timeoutMillis, boolean isLocalOperation)
    {
        return createAndStart(operation, operationCreationTimeMillis, timeoutMillis, isLocalOperation, DatabaseDescriptor.getSlowQueryTimeout());
    }

    /**
     * Create and start an monitor one a particular operation.
     * <p>
     * The created monitor will log the operation if it run longer that the configured slow query timeout, and abort it
     * if it goes longer than the provided timeout.
     *
     * @param operation the monitored operation.
     * @param operationCreationTimeMillis the time at which the operation was created. This is the time use to timeout
     *                                    the operation.
     * @param timeoutMillis the timeout for the operation.
     */
    public static Monitor createAndStart(Monitorable operation, long operationCreationTimeMillis, long timeoutMillis, boolean isLocalOperation, long slowQueryTimeout)
    {
        return new Monitor(operation,
                           true,
                           operationCreationTimeMillis,
                           System.currentTimeMillis(),
                           timeoutMillis,
                           slowQueryTimeout,
                           isLocalOperation);
    }

    /**
     * Create and start an monitor that doesn't report failed or slow operations but simply abort the operation after
     * the timeout.
     *
     * @param operation the monitored operation.
     * @param operationCreationTimeMillis the time at which the operation was created. This is the time use to timeout
     *                                    the operation.
     * @param timeoutMillis the timeout for the operation.
     */
    public static Monitor createAndStartNoReporting(Monitorable operation, long operationCreationTimeMillis, long timeoutMillis)
    {
        return new Monitor(operation,
                           false,
                           operationCreationTimeMillis,
                           System.currentTimeMillis(),
                           timeoutMillis,
                           0,
                           false); // We're not reporting so whether it's local or not doesn't matter
    }

    long timeoutMillis()
    {
        return timeoutMillis;
    }

    long slowQueryTimeoutMillis()
    {
        return slowQueryTimeoutMillis;
    }

    boolean isInProgress()
    {
        checkSilently();
        return state == MonitoringState.IN_PROGRESS;
    }

    boolean isCompleted()
    {
        checkSilently();
        return state == MonitoringState.COMPLETED;
    }

    boolean isAborted()
    {
        checkSilently();
        return state == MonitoringState.ABORTED;
    }

    boolean isSlow()
    {
        checkSilently();
        return isSlow;
    }

    private void abort()
    {
        switch (state)
        {
            case IN_PROGRESS:
                if (withReporting)
                    MonitoringTask.addFailedOperation(this, ApproximateTime.currentTimeMillis());
                state = MonitoringState.ABORTED;
                // Fallback on purpose
            case ABORTED:
                // We should really get here but well...
                throw new AbortedOperationException();
        }
    }

    public boolean complete()
    {
        if (state == MonitoringState.IN_PROGRESS)
        {
            if (withReporting && isSlow && slowQueryTimeoutMillis > 0)
                MonitoringTask.addSlowOperation(this, ApproximateTime.currentTimeMillis());

            state = MonitoringState.COMPLETED;
            return true;
        }

        return state == MonitoringState.COMPLETED;
    }

    private void checkSilently()
    {
        try
        {
            check();
        }
        catch (AbortedOperationException e)
        {
            // Ignore on purpose
        }
    }

    private void check()
    {
        if (state != MonitoringState.IN_PROGRESS)
            return;

        // The value returned by ApproximateTime.currentTimeMillis() is updated only every
        // ApproximateTime.CHECK_INTERVAL_MS, by default 10 millis. Since we rely on ApproximateTime, we don't need
        // to check unless the approximate time has elapsed.
        long currentTime = ApproximateTime.currentTimeMillis();

        if (lastChecked == currentTime)
            return;

        lastChecked = currentTime;

        if (currentTime - monitoringStartTimeMillis >= slowQueryTimeoutMillis)
            isSlow = true;

        if (currentTime - operationCreationTimeMillis >= timeoutMillis)
            abort();
    }

    public UnfilteredPartitionIterator withMonitoring(UnfilteredPartitionIterator iter)
    {
        return Transformation.apply(iter, new CheckForAbort());
    }

    private class CheckForAbort extends Transformation<UnfilteredRowIterator>
    {
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            check();
            return Transformation.apply(partition, this);
        }

        protected Row applyToRow(Row row)
        {
            if (isTesting()) // delay for testing
                FBUtilities.sleepQuietly(TEST_ITERATION_DELAY_MILLIS);

            check();
            return row;
        }
    }
}
