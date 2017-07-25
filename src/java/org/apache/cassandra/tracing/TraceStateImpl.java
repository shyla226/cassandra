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
package org.apache.cassandra.tracing;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Completable;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * ThreadLocal state for a tracing session. The presence of an instance of this class as a ThreadLocal denotes that an
 * operation is being traced.
 */
public class TraceStateImpl extends TraceState
{
    private static final Logger logger = LoggerFactory.getLogger(TraceStateImpl.class);

    @VisibleForTesting
    public static int WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS =
      Integer.parseInt(System.getProperty("cassandra.wait_for_tracing_events_timeout_secs", "0"));

    private final Set<Future<Void>> pendingFutures = ConcurrentHashMap.newKeySet();

    public TraceStateImpl(InetAddress coordinator, UUID sessionId, Tracing.TraceType traceType)
    {
        super(coordinator, sessionId, traceType);
    }

    protected void traceImpl(String message)
    {
        final String threadName = Thread.currentThread().getName();
        final int elapsed = elapsed();

        executeMutation(TraceKeyspace.makeEventMutation(sessionIdBytes, message, elapsed, threadName, ttl));
        if (logger.isTraceEnabled())
            logger.trace("Adding <{}> to trace events", message);
    }

    /**
     * Wait on submitted futures
     */
    protected Completable waitForPendingEvents()
    {
        if (WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS <= 0)
            return Completable.complete();

        CompletableFuture<Void> fut = CompletableFuture.allOf(pendingFutures.toArray(new CompletableFuture<?>[pendingFutures.size()]));
        return Completable.create(subscriber -> fut.whenComplete((result, error) -> {
                              if (error != null)
                                  subscriber.onError(error);
                              else
                                  subscriber.onComplete();
                           }))
                          .timeout(WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS, TimeUnit.SECONDS)
                          .onErrorResumeNext(ex -> {
                              if (ex instanceof TimeoutException)
                              {
                                  if (logger.isTraceEnabled())
                                      logger.trace("Failed to wait for tracing events to complete in {} seconds",
                                                   WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS);
                              }
                              else
                              {
                                  JVMStabilityInspector.inspectThrowable(ex);
                                  logger.error("Got exception whilst waiting for tracing events to complete", ex);
                              }

                              // don't pass on exceptions, just log them
                              return Completable.complete();
                          });
    }


    void executeMutation(final Mutation mutation)
    {
        CompletableFuture<Void> fut = CompletableFuture.runAsync(new Tracing.TracingRunnable()
        {
            protected void runMayThrow()
            {
                mutateWithCatch(mutation);
            }
        }, StageManager.tracingExecutor);

        boolean ret = pendingFutures.add(fut);
        if (!ret)
            logger.warn("Failed to insert pending future, tracing synchronization may not work");
    }

    static void mutateWithCatch(Mutation mutation)
    {
        try
        {
            StorageProxy.mutate(Collections.singletonList(mutation), ConsistencyLevel.ANY, System.nanoTime()).blockingGet();
        }
        catch (OverloadedException e)
        {
            Tracing.logger.warn("Too many nodes are overloaded to save trace events");
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Could not apply tracing mutation {}", mutation, t);
        }
    }

}
