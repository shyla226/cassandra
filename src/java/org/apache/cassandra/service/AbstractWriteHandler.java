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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.internal.disposables.EmptyDisposable;
import org.apache.cassandra.concurrent.TPCTimeoutTask;
import org.apache.cassandra.concurrent.TPCTimer;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.FailureResponse;

import static org.apache.cassandra.service.WriteHandler.logger;

abstract class AbstractWriteHandler extends WriteHandler
{
    private static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException();

    protected final WriteEndpoints endpoints;
    protected final ConsistencyLevel consistency;
    protected final WriteType writeType;
    private final long queryStartNanos;
    private final TPCTimer requestExpirer;

    protected final int blockFor;

    private static final AtomicIntegerFieldUpdater<AbstractWriteHandler> FAILURES_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AbstractWriteHandler.class, "failures");
    private volatile int failures = 0;

    private final Map<InetAddress, RequestFailureReason> failureReasonByEndpoint = new ConcurrentHashMap<>();

    AbstractWriteHandler(WriteEndpoints endpoints,
                         ConsistencyLevel consistency,
                         int blockFor,
                         WriteType writeType,
                         long queryStartNanos,
                         TPCTimer requestExpirer)
    {
        this.endpoints = endpoints;
        this.consistency = consistency;
        this.writeType = writeType;
        this.queryStartNanos = queryStartNanos;
        this.requestExpirer = requestExpirer;

        this.blockFor = blockFor < 0
                        ? consistency.blockFor(endpoints.keyspace()) + pendingToBlockFor()
                        : blockFor;
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
        return new Completable()
        {
            protected void subscribeActual(CompletableObserver subscriber)
            {
                subscriber.onSubscribe(EmptyDisposable.INSTANCE);
                TPCTimeoutTask<WriteHandler> timeoutTask = new TPCTimeoutTask<>(requestExpirer, AbstractWriteHandler.this);
                timeoutTask.submit(new TimeoutAction(), currentTimeout(), TimeUnit.NANOSECONDS);

                whenComplete((result, error) -> {
                    if (logger.isTraceEnabled())
                        logger.trace("{} - Completed with {}/{}", AbstractWriteHandler.this.hashCode(), result, error == null ? null : error.getClass().getName());

                    timeoutTask.dispose();
                    if (error != null)
                    {
                        if (logger.isTraceEnabled())
                            logger.trace("{} - Returning error {}", AbstractWriteHandler.this.hashCode(), error.getClass().getName());
                        if (error instanceof TimeoutException)
                        {
                            int acks = ackCount();
                            // It's pretty unlikely, but we can race between exiting get() above and here, so
                            // that we could now have enough acks. In that case, we "lie" on the acks count to
                            // avoid sending confusing info to the user (see CASSANDRA-6491).
                            if (acks >= blockFor)
                                acks = blockFor - 1;
                            subscriber.onError(new WriteTimeoutException(writeType, consistency, acks, blockFor));
                        }
                        else
                            subscriber.onError(error);
                    }
                    else
                        subscriber.onComplete();
                });
            }
        };
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

        int n = waitingFor(from) ? FAILURES_UPDATER.incrementAndGet(this) : failures;

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

    private static class TimeoutAction implements Consumer<WriteHandler>
    {
        @Override
        public void accept(WriteHandler handler)
        {
            handler.completeExceptionally(TIMEOUT_EXCEPTION);
        }
    }
}
