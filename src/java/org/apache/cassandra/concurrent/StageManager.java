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

import java.util.EnumMap;
import java.util.concurrent.*;

import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.DatabaseDescriptor.*;


/**
 * This class manages executor services for Messages received: each Message requests
 * running on a specific "stage" for concurrency control; hence the Map approach,
 * even though stages (executors) are not created dynamically.
 */
public class StageManager
{
    private static final EnumMap<Stage, TracingAwareExecutorService> stages = new EnumMap<>(Stage.class);

    // The RX schedulers derived from the stages executors
    private static final EnumMap<Stage, Scheduler> schedulers = new EnumMap<>(Stage.class);

    public static final long KEEPALIVE = 60; // seconds to keep "extra" threads alive for when idle

    static
    {
        stages.put(Stage.MUTATION, multiThreadedStage(Stage.MUTATION, getConcurrentWriters()));
        stages.put(Stage.COUNTER_MUTATION, multiThreadedStage(Stage.COUNTER_MUTATION, getConcurrentCounterWriters()));
        stages.put(Stage.VIEW_MUTATION, multiThreadedStage(Stage.VIEW_MUTATION, getConcurrentViewWriters()));
        stages.put(Stage.READ, multiThreadedStage(Stage.READ, getConcurrentReaders()));
        stages.put(Stage.REQUEST_RESPONSE, multiThreadedStage(Stage.REQUEST_RESPONSE, FBUtilities.getAvailableProcessors()));
        stages.put(Stage.INTERNAL_RESPONSE, multiThreadedStage(Stage.INTERNAL_RESPONSE, FBUtilities.getAvailableProcessors()));
        // the rest are all single-threaded
        stages.put(Stage.GOSSIP, new JMXEnabledThreadPoolExecutor(Stage.GOSSIP));
        stages.put(Stage.ANTI_ENTROPY, new JMXEnabledThreadPoolExecutor(Stage.ANTI_ENTROPY));
        stages.put(Stage.MIGRATION, new JMXEnabledThreadPoolExecutor(Stage.MIGRATION));
        stages.put(Stage.MISC, new JMXEnabledThreadPoolExecutor(Stage.MISC));
        stages.put(Stage.READ_REPAIR, multiThreadedStage(Stage.READ_REPAIR, FBUtilities.getAvailableProcessors()));

        // add the corresponding scheduler to each stage
        stages.forEach((stage, executor) -> schedulers.put(stage, Schedulers.from(executor)));
    }

    public static final ThreadPoolExecutor tracingExecutor = tracingExecutor();

    private static ThreadPoolExecutor tracingExecutor()
    {
        RejectedExecutionHandler reh = (r, executor) -> Tracing.onDroppedTask(r);
        return new ThreadPoolExecutor(1,
                                      1,
                                      KEEPALIVE,
                                      TimeUnit.SECONDS,
                                      new ArrayBlockingQueue<>(1000),
                                      new NamedThreadFactory("TracingStage"),
                                      reh);
    }

    private static JMXEnabledThreadPoolExecutor multiThreadedStage(Stage stage, int numThreads)
    {
        return new JMXEnabledThreadPoolExecutor(numThreads,
                                                KEEPALIVE,
                                                TimeUnit.SECONDS,
                                                new LinkedBlockingQueue<>(),
                                                new NamedThreadFactory(stage.getJmxName()),
                                                stage.getJmxType());
    }

    /**
     * Retrieve a stage from the StageManager
     * @param stage name of the stage to be retrieved.
     */
    public static TracingAwareExecutorService getStage(Stage stage)
    {
        return stages.get(stage);
    }

    /**
     * Retrieve the RX scheduler corresponding to a stage.
     *
     * @param stage name of the stage to be retrieved.
     * @return RX scheduler executing on the stage
     */
    public static Scheduler getScheduler(Stage stage)
    {
        return schedulers.get(stage);
    }

    /**
     * This method shuts down all registered stages.
     */
    public static void shutdownNow()
    {
        for (Stage stage : Stage.values())
        {
            StageManager.stages.get(stage).shutdownNow();
        }
    }
}
