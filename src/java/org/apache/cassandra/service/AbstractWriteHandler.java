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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import io.reactivex.Completable;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.FailureResponse;

abstract class AbstractWriteHandler extends WriteHandler
{
    protected final WriteEndpoints endpoints;
    protected final ConsistencyLevel consistency;
    protected final WriteType writeType;
    private final long queryStartNanos;

    protected final int blockFor;

    private static final AtomicInteger failures = new AtomicInteger(0);
    private final Map<InetAddress, RequestFailureReason> failureReasonByEndpoint = new ConcurrentHashMap<>();

    AbstractWriteHandler(WriteEndpoints endpoints,
                         ConsistencyLevel consistency,
                         WriteType writeType,
                         long queryStartNanos)
    {
        this.endpoints = endpoints;
        this.consistency = consistency;
        this.writeType = writeType;
        this.queryStartNanos = queryStartNanos;

        this.blockFor = consistency.blockFor(endpoints.keyspace())
                        + pendingToBlockFor();
    }

    public WriteEndpoints endpoints()
    {
        return endpoints;
    }

    public ConsistencyLevel consistencyLevel()
    {
        return consistency;
    }

    public WriteType writeType()
    {
        return writeType;
    }

    protected long queryStartNanos()
    {
        return queryStartNanos;
    }

    @Override
    public Void get() throws WriteTimeoutException, WriteFailureException
    {
        long timeout = currentTimeout();

        try
        {
            return super.get(timeout, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        catch (TimeoutException e)
        {
            int acks = ackCount();
            // It's pretty unlikely, but we can race between exiting get() above and here, so
            // that we could now have enough acks. In that case, we "lie" on the acks count to
            // avoid sending confusing info to the user (see CASSANDRA-6491).
            if (acks >= blockFor)
                acks = blockFor - 1;
            throw new WriteTimeoutException(writeType, consistency, acks, blockFor);
        }
        catch (ExecutionException e)
        {
            // This is the only exception we ever complete the future with
            assert e.getCause() instanceof WriteFailureException;
            throw (WriteFailureException)e.getCause();
        }
    }

    public Completable toObservable()
    {
        return Completable.create(subscriber -> whenComplete((result, error) -> {
            if (logger.isTraceEnabled())
                logger.trace("{} - Completed with {}/{}", AbstractWriteHandler.this.hashCode(), result, error == null ? null : error.getClass().getName());
            if (error != null)
                subscriber.onError(error);
            else
                subscriber.onComplete();
            })).timeout(currentTimeout(), TimeUnit.NANOSECONDS)
               .onErrorResumeNext(exc -> {
               if (exc instanceof TimeoutException)
               {
                   int acks = ackCount();
                   // It's pretty unlikely, but we can race between exiting get() above and here, so
                   // that we could now have enough acks. In that case, we "lie" on the acks count to
                   // avoid sending confusing info to the user (see CASSANDRA-6491).
                   if (acks >= blockFor)
                       acks = blockFor - 1;
                   return Completable.error(new WriteTimeoutException(writeType, consistency, acks, blockFor));
               }
               if (logger.isTraceEnabled())
                   logger.trace("{} - Returning error {}", AbstractWriteHandler.this.hashCode(), exc.getClass().getName());
               return Completable.error(exc);
          });
    }

    /**
     * @return the minimum number of endpoints that must reply.
     */
    protected int pendingToBlockFor()
    {
        // During bootstrap, we have to include the pending endpoints or we may fail the consistency level
        // guarantees (see #833)
        return endpoints.pendingCount();
    }

    protected abstract int ackCount();

    /**
     * @return true if the message should be counted towards blockFor.
     */
    protected boolean waitingFor(InetAddress from)
    {
        return true;
    }

    public void onFailure(FailureResponse<EmptyPayload> response)
    {
        InetAddress from = response.from();
        if (logger.isTraceEnabled())
            logger.trace("{} - Got failure from {}: {}", hashCode(), from, response);

        int n = waitingFor(from) ? failures.incrementAndGet() : failures.get();

        failureReasonByEndpoint.put(from, response.reason());

        // We only send messages to live nodes
        if (blockFor + n > endpoints.liveCount())
            completeExceptionally(new WriteFailureException(consistency,
                                                            ackCount(),
                                                            blockFor,
                                                            writeType,
                                                            failureReasonByEndpoint));
    }

    public void onTimeout(InetAddress host)
    {
        // We do nothing by default, but a WriteHandler can be wrapped to run
        // task on timeout (typically, submitting hints) using
        // WriteHandler.Builder.onTimeout())
    }
}
