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
package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.math.IntMath;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.RangeHash;

/**
 * RepairJob runs repair on given ColumnFamily.
 */
public class RepairJob extends AbstractFuture<RepairResult> implements Runnable
{
    private static Logger logger = LoggerFactory.getLogger(RepairJob.class);

    private final RepairSession session;
    private final RepairJobDesc desc;
    private final RepairParallelism parallelismDegree;
    private final ListeningExecutorService taskExecutor;
    private final boolean isIncremental;
    private final PreviewKind previewKind;

    private int MAX_WAIT_FOR_REMAINING_TASKS_IN_HOURS = 3;

    /**
     * Create repair job to run on specific columnfamily
     *
     * @param session RepairSession that this RepairJob belongs
     * @param columnFamily name of the ColumnFamily to repair
     */
    public RepairJob(RepairSession session, String columnFamily, boolean isIncremental, PreviewKind previewKind)
    {
        this.session = session;
        this.desc = new RepairJobDesc(session.parentRepairSession, session.getId(), session.keyspace, columnFamily, session.getRanges());
        this.taskExecutor = session.taskExecutor;
        this.parallelismDegree = session.parallelismDegree;
        this.isIncremental = isIncremental;
        this.previewKind = previewKind;
    }

    /**
     * Runs repair job.
     *
     * This sets up necessary task and runs them on given {@code taskExecutor}.
     * After submitting all tasks, waits until validation with replica completes.
     */
    public void run()
    {
        Keyspace ks = Keyspace.open(desc.keyspace);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(desc.columnFamily);
        cfs.metric.repairsStarted.inc();
        List<InetAddress> allEndpoints = new ArrayList<>(session.endpoints);
        allEndpoints.add(FBUtilities.getBroadcastAddress());

        List<ListenableFuture<TreeResponse>> validations;
        // Create a snapshot at all nodes unless we're using pure parallel repairs
        if (parallelismDegree != RepairParallelism.PARALLEL)
        {
            ListenableFuture<List<InetAddress>> snapshotResult;
            List<ListenableFuture<InetAddress>> snapshotTasks = new ArrayList<>(allEndpoints.size());
            if (isIncremental)
            {
                // consistent repair does it's own "snapshotting"
                snapshotResult = Futures.immediateFuture(allEndpoints);
            }
            else
            {
                // Request snapshot to all replica
                for (InetAddress endpoint : allEndpoints)
                {
                    SnapshotTask snapshotTask = new SnapshotTask(desc, endpoint);
                    snapshotTasks.add(snapshotTask);
                    taskExecutor.execute(snapshotTask);
                }
                snapshotResult = Futures.allAsList(snapshotTasks);
            }

            try
            {
                //this will throw exception if any snapshot task fails
                List<InetAddress> endpoints = snapshotResult.get();
                if (parallelismDegree == RepairParallelism.SEQUENTIAL)
                    validations = sendSequentialValidationRequest(endpoints);
                else
                    validations = sendDCAwareValidationRequest(endpoints);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                waitForRemainingTasksAndFail("snapshot", snapshotTasks, t);
                return;
            }
        }
        else
        {
            // If not sequential, just send validation request to all replica
            validations = sendValidationRequest(allEndpoints);
        }

        // This caches token ranges digests received by a given node in this session
        // to avoid sending the same range to a destination multiple times (APOLLO-390)
        Map<InetAddress, Set<RangeHash>> receivedRangeCache = new HashMap<>();
        List<SyncTask> syncTasks = new ArrayList<>();
        try
        {
            //this will throw exception if any validation task fails
            List<TreeResponse> trees = Futures.allAsList(validations).get();

            List<List<TreeResponse>> treesByDc = new ArrayList<>(trees
                                                                 .stream()
                                                                 .collect(Collectors
                                                                          .groupingBy(t -> DatabaseDescriptor.getEndpointSnitch()
                                                                                                             .getDatacenter(t.endpoint)))
                                                                 .values());

            SyncTask previous = null;

            // Sync tasks are submitted sequentially
            // At the tail, inter-dc syncs task so ranges already transferred from local DC are skipped (APOLLO-390)
            for (int i = 0; i < treesByDc.size(); i++)
            {
                for (int j = i + 1; j < treesByDc.size(); j++)
                {
                    for (TreeResponse r1 : treesByDc.get(i))
                    {
                        for (TreeResponse r2 : treesByDc.get(j))
                        {
                            previous = createSyncTask(receivedRangeCache, syncTasks, previous, r1, r2);
                        }
                    }
                }
            }

            // At the head, intra-dc sync tasks so local range transfers are prioritized (APOLLO-390)
            for (List<TreeResponse> localDc : treesByDc)
            {
                for (int i = 0; i < localDc.size(); i++)
                {
                    TreeResponse r1 = localDc.get(i);
                    for (int j = i + 1; j < localDc.size(); j++)
                    {
                        TreeResponse r2 = localDc.get(j);
                        previous = createSyncTask(receivedRangeCache, syncTasks, previous, r1, r2);
                    }
                }
            }

            assert syncTasks.size() == IntMath.binomial(trees.size(), 2) : "Not enough sync tasks.";

            //run sync tasks sequentially to avoid overloading coordinator (APOLLO-216)
            //after the stream session is started, it will run in paralell
            if (previous != null)
                taskExecutor.submit(previous);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            if (parallelismDegree == RepairParallelism.SEQUENTIAL)
            {
                failJob(t); //fail straight away as tasks are running sequentially
            }
            else
            {
                waitForRemainingTasksAndFail("validation", validations, t);
            }
            return;
        }

        try
        {
            //this will throw exception if any sync task fails
            List<SyncStat> stats = Futures.allAsList(syncTasks).get();
            logger.info("[repair #{}] {} is fully synced", session.getId(), desc.columnFamily);
            SystemDistributedKeyspace.successfulRepairJob(session.getId(), desc.keyspace, desc.columnFamily);
            cfs.metric.repairsCompleted.inc();
            set(new RepairResult(desc, stats));
        }
        catch (Throwable t)
        {
            cfs.metric.repairsCompleted.inc();
            JVMStabilityInspector.inspectThrowable(t);
            waitForRemainingTasksAndFail("sync", syncTasks, t);
            return;
        }
    }

    private SyncTask createSyncTask(Map<InetAddress, Set<RangeHash>> receivedRangeCache, List<SyncTask> syncTasks,
                                    SyncTask previous, TreeResponse r1, TreeResponse r2)
    {
        InetAddress local = FBUtilities.getLocalAddress();
        SyncTask task;
        if (r1.endpoint.equals(local) || r2.endpoint.equals(local))
        {
            task = new LocalSyncTask(desc, r1, r2, isIncremental ? desc.parentSessionId : null, session.pullRepair, taskExecutor, previous, receivedRangeCache, previewKind);
        }
        else
        {
            task = new RemoteSyncTask(desc, r1, r2, session, taskExecutor, previous, receivedRangeCache, previewKind);
        }
        syncTasks.add(task);
        return task;
    }

    private <T> void waitForRemainingTasksAndFail(String phase, Iterable<? extends ListenableFuture<? extends T>> tasks,
                                                  Throwable t)
    {
        logger.warn("[{}] [repair #{}] {} {} failed. Will wait a maximum of {} hours for remaining tasks to finish.",
                    session.parentRepairSession, session.getId(), desc.columnFamily, phase, MAX_WAIT_FOR_REMAINING_TASKS_IN_HOURS, t);
        //wait for remaining tasks to complete
        try
        {
            Futures.successfulAsList(tasks).get(MAX_WAIT_FOR_REMAINING_TASKS_IN_HOURS, TimeUnit.HOURS);
            logger.debug("[{}][{}] All remaining repair tasks finished.", session.parentRepairSession, session.getId());
        }
        catch (Throwable t2)
        {
            JVMStabilityInspector.inspectThrowable(t2);
            logger.warn("[{}] Exception while waiting for remaining repair tasks to complete.", session.parentRepairSession, t2);
        }
        failJob(t);
    }

    private void failJob(Throwable t)
    {
        SystemDistributedKeyspace.failedRepairJob(session.getId(), desc.keyspace, desc.columnFamily, t);
        setException(t);
    }

    /**
     * Creates {@link ValidationTask} and submit them to task executor in parallel.
     *
     * @param endpoints Endpoint addresses to send validation request
     * @return Future that can get all {@link TreeResponse} from replica, if all validation succeed.
     */
    private List<ListenableFuture<TreeResponse>> sendValidationRequest(Collection<InetAddress> endpoints)
    {
        String message = String.format("Requesting merkle trees for %s (to %s)", desc.columnFamily, endpoints);
        logger.info("[repair #{}] {}", desc.sessionId, message);
        Tracing.traceRepair(message);
        int nowInSec = FBUtilities.nowInSeconds();
        List<ListenableFuture<TreeResponse>> tasks = new ArrayList<>(endpoints.size());
        for (InetAddress endpoint : endpoints)
        {
            ValidationTask task = new ValidationTask(desc, endpoint, nowInSec, previewKind);
            tasks.add(task);
            session.waitForValidation(Pair.create(desc, endpoint), task);
            taskExecutor.execute(task);
        }
        return tasks;
    }

    /**
     * Creates {@link ValidationTask} and submit them to task executor so that tasks run sequentially.
     */
    private List<ListenableFuture<TreeResponse>> sendSequentialValidationRequest(Collection<InetAddress> endpoints)
    {
        String message = String.format("Requesting merkle trees for %s (to %s)", desc.columnFamily, endpoints);
        logger.info("[repair #{}] {}", desc.sessionId, message);
        Tracing.traceRepair(message);
        int nowInSec = FBUtilities.nowInSeconds();
        List<ListenableFuture<TreeResponse>> tasks = new ArrayList<>(endpoints.size());

        Queue<InetAddress> requests = new LinkedList<>(endpoints);
        InetAddress address = requests.poll();
        ValidationTask firstTask = new ValidationTask(desc, address, nowInSec, previewKind);
        logger.info("Validating {}", address);
        session.waitForValidation(Pair.create(desc, address), firstTask);
        tasks.add(firstTask);
        ValidationTask currentTask = firstTask;
        while (requests.size() > 0)
        {
            final InetAddress nextAddress = requests.poll();
            final ValidationTask nextTask = new ValidationTask(desc, nextAddress, nowInSec, previewKind);
            tasks.add(nextTask);
            Futures.addCallback(currentTask, new FutureCallback<TreeResponse>()
            {
                public void onSuccess(TreeResponse result)
                {
                    logger.info("Validating {}", nextAddress);
                    session.waitForValidation(Pair.create(desc, nextAddress), nextTask);
                    taskExecutor.execute(nextTask);
                }

                // failure is handled at root of job chain
                public void onFailure(Throwable t) {}
            });
            currentTask = nextTask;
        }
        // start running tasks
        taskExecutor.execute(firstTask);
        return tasks;
    }

    /**
     * Creates {@link ValidationTask} and submit them to task executor so that tasks run sequentially within each dc.
     */
    private List<ListenableFuture<TreeResponse>> sendDCAwareValidationRequest(Collection<InetAddress> endpoints)
    {
        String message = String.format("Requesting merkle trees for %s (to %s)", desc.columnFamily, endpoints);
        logger.info("[repair #{}] {}", desc.sessionId, message);
        Tracing.traceRepair(message);
        int nowInSec = FBUtilities.nowInSeconds();
        List<ListenableFuture<TreeResponse>> tasks = new ArrayList<>(endpoints.size());

        Map<String, Queue<InetAddress>> requestsByDatacenter = new HashMap<>();
        for (InetAddress endpoint : endpoints)
        {
            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);
            Queue<InetAddress> queue = requestsByDatacenter.get(dc);
            if (queue == null)
            {
                queue = new LinkedList<>();
                requestsByDatacenter.put(dc, queue);
            }
            queue.add(endpoint);
        }

        for (Map.Entry<String, Queue<InetAddress>> entry : requestsByDatacenter.entrySet())
        {
            Queue<InetAddress> requests = entry.getValue();
            InetAddress address = requests.poll();
            ValidationTask firstTask = new ValidationTask(desc, address, nowInSec, previewKind);
            logger.info("Validating {}", address);
            session.waitForValidation(Pair.create(desc, address), firstTask);
            tasks.add(firstTask);
            ValidationTask currentTask = firstTask;
            while (requests.size() > 0)
            {
                final InetAddress nextAddress = requests.poll();
                final ValidationTask nextTask = new ValidationTask(desc, nextAddress, nowInSec, previewKind);
                tasks.add(nextTask);
                Futures.addCallback(currentTask, new FutureCallback<TreeResponse>()
                {
                    public void onSuccess(TreeResponse result)
                    {
                        logger.info("Validating {}", nextAddress);
                        session.waitForValidation(Pair.create(desc, nextAddress), nextTask);
                        taskExecutor.execute(nextTask);
                    }

                    // failure is handled at root of job chain
                    public void onFailure(Throwable t) {}
                });
                currentTask = nextTask;
            }
            // start running tasks
            taskExecutor.execute(firstTask);
        }
        return tasks;
    }
}
