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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Completable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

public abstract class WriteHandler extends CompletableFuture<Void> implements MessageCallback<EmptyPayload>
{
    protected static final Logger logger = LoggerFactory.getLogger(WriteHandler.class);

    // This is theoretically dodgy: we use ReadWriteMessages.WRITE even though WriteHandler is used for other message
    // types, and we use a bogus creation time. We know this is ok though as only the payload and source of message is used.
    private static final Response<EmptyPayload> LOCAL_RESPONSE = Response.local(Verbs.WRITES.WRITE, EmptyPayload.instance, -1);

    public abstract WriteEndpoints endpoints();

    public abstract ConsistencyLevel consistencyLevel();

    public abstract WriteType writeType();

    protected abstract long queryStartNanos();

    @Override
    public abstract Void get() throws WriteTimeoutException, WriteFailureException;

    public abstract Completable toObservable();

    public long currentTimeout()
    {
        long requestTimeout = writeType() == WriteType.COUNTER
                              ? DatabaseDescriptor.getCounterWriteRpcTimeout()
                              : DatabaseDescriptor.getWriteRpcTimeout();
        return TimeUnit.MILLISECONDS.toNanos(requestTimeout) - (System.nanoTime() - queryStartNanos());
    }

    public void onLocalResponse()
    {
        onResponse(LOCAL_RESPONSE);
    }

    /**
     * A shorthand for {@code builder(...).build()} for when no specific options
     * outside of the arguments of this method have to be passed.
     */
    public static WriteHandler create(WriteEndpoints endpoints,
                                      ConsistencyLevel consistencyLevel,
                                      WriteType writeType,
                                      long queryStartNanos)
    {
        return builder(endpoints, consistencyLevel, writeType, queryStartNanos).build();
    }

    public static Builder builder(WriteEndpoints endpoints,
                                  ConsistencyLevel consistencyLevel,
                                  WriteType writeType,
                                  long queryStartNanos)
    {
        return new Builder(endpoints, consistencyLevel, writeType, queryStartNanos);
    }

    public static class Builder
    {
        private final WriteEndpoints endpoints;
        private final ConsistencyLevel consistencyLevel;
        private final WriteType writeType;
        private final long queryStartNanos;

        private List<Consumer<Response<EmptyPayload>>> onResponseTasks;
        private List<Consumer<InetAddress>> onTimeoutTasks;
        private List<Consumer<FailureResponse<EmptyPayload>>> onFailureTasks;

        private Builder(WriteEndpoints endpoints,
                        ConsistencyLevel consistencyLevel,
                        WriteType writeType,
                        long queryStartNanos)
        {
            this.endpoints = endpoints;
            this.consistencyLevel = consistencyLevel;
            this.writeType = writeType;
            this.queryStartNanos = queryStartNanos;
        }

        /**
         * Register the provided task to be run for each node that responds
         * successfully.
         *
         * @param task the task to run for each node that responds successfully.
         * The task gets the response as argument. Note that tasks are run
         * after the sucess has been accounted by the handler (so the handler
         * can technically return before a task has run) and in the order in
         * which they are registered (if more than one was registered).
         * @return this builder.
         */
        public Builder onResponse(Consumer<Response<EmptyPayload>> task)
        {
            if (onResponseTasks == null)
                onResponseTasks = new ArrayList<>();
            onResponseTasks.add(task);
            return this;
        }

        /**
         * Register the provided task to be run for each node that responds
         * with a failure.
         *
         * @param task the task to run for each node that responds with a
         * failure. The task gets the failure response as argument. Note that
         * tasks are run after the failure has been accounted by the handler and
         * in the order in which they are registered (if more than one was registered)
         * @return this builder.
         */
        public Builder onFailure(Consumer<FailureResponse<EmptyPayload>> task)
        {
            if (onFailureTasks == null)
                onFailureTasks = new ArrayList<>();
            onFailureTasks.add(task);
            return this;
        }

        /**
         * Register the provided task to be run for each node that doesn't
         * reply to the write before the handler timeout.
         *
         * @param task the task to run for each node that doesn't respond before
         * the timeout. The task gets the host as argument. Note that tasks are
         * run after the timeout has been accounted by the handler and in the
         * order in which they are registered (if more than one was registered)
         * @return this builder.
         */
        public Builder onTimeout(Consumer<InetAddress> task)
        {
            if (onTimeoutTasks == null)
                onTimeoutTasks = new ArrayList<>();
            onTimeoutTasks.add(task);
            return this;
        }

        /**
         * Register a task to submit hints for the provided mutation for each
         * node that doesn't respond before the handler timeout.
         * <p>
         * This is a shorthand for {@code onTimeout} that calls {@code
         * StorageProxy.submitHint()} unless it's the localhost, in which case
         * the mutation is written locally since it is as costly as storing a hint.
         *
         * @param mutation - the mutation to hint on timeout
         * @return this builder
         */
        public Builder hintOnTimeout(Mutation mutation)
        {
            // Don't bother hinting for ANY as we'll hint everything on catching a timeout in StorageProxy.mutate().
            // This is done this way because we wouldn't be sure in StorageProxy.mutate() if hints have been properly
            // written or not otherwise, and we need hints to be successful before responding to the client.
            if (consistencyLevel == ConsistencyLevel.ANY)
                return this;

            InetAddress local = FBUtilities.getBroadcastAddress();
            return onTimeout(host ->
            {
                if (!StorageProxy.shouldHint(host))
                    return;

                if (host.equals(local))
                {
                    StorageProxy.submitHint(new StorageProxy.HintRunnable(Collections.singleton(local))
                    {
                        protected void runMayThrow() throws Exception
                        {
                            // Locally, there is no point to write a hint (it's not really less costly than writing the
                            // mutation itself) so we just write the mutation, but still use HintRunnable which provides
                            // proper back-pressure for hints.
                            mutation.apply();
                        }
                    });
                }
                else
                {
                    StorageProxy.submitHint(mutation, host, null);
                }
            });
        }

        private WriteHandler makeHandler()
        {
            if (consistencyLevel.isDatacenterLocal())
                return new WriteHandlers.DatacenterLocalHandler(endpoints, consistencyLevel, writeType, queryStartNanos);
            else if (consistencyLevel == ConsistencyLevel.EACH_QUORUM && (endpoints.keyspace().getReplicationStrategy() instanceof NetworkTopologyStrategy))
                return new WriteHandlers.DatacenterSyncHandler(endpoints, consistencyLevel, writeType, queryStartNanos);
            else
                return new WriteHandlers.SimpleHandler(endpoints, consistencyLevel, writeType, queryStartNanos);
        }

        private static <T> List<T> freeze(List<T> l)
        {
            return l == null
                 ? Collections.emptyList()
                 : ImmutableList.copyOf(l);
        }

        private WriteHandler withTasks(WriteHandler handler)
        {
            final List<Consumer<Response<EmptyPayload>>> onResponseTasks = freeze(this.onResponseTasks);
            final List<Consumer<InetAddress>> onTimeoutTasks = freeze(this.onTimeoutTasks);
            final List<Consumer<FailureResponse<EmptyPayload>>> onFailureTasks = freeze(this.onFailureTasks);
            return new WrappingWriteHandler(handler)
            {
                @Override
                public void onResponse(Response<EmptyPayload> response)
                {
                    super.onResponse(response);
                    for (Consumer<Response<EmptyPayload>> task : onResponseTasks)
                        accept(task, response, "onResponse");
                }

                @Override
                public void onFailure(FailureResponse<EmptyPayload> response)
                {
                    super.onFailure(response);
                    for (Consumer<FailureResponse<EmptyPayload>> task : onFailureTasks)
                        accept(task, response, "onFailure");
                }

                @Override
                public void onTimeout(InetAddress host)
                {
                    super.onTimeout(host);
                    for (Consumer<InetAddress> task : onTimeoutTasks)
                        accept(task, host, "onTimeout");
                }
            };
        }

        private static <T> void accept(Consumer<T> task, T value, String taskType)
        {
            try
            {
                task.accept(value);
            }
            catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                logger.error("Unexpected error while executing post-write {} task with value {}", taskType, value, e);
            }
        }

        public WriteHandler build()
        {
            WriteHandler handler = makeHandler();
            return onResponseTasks == null && onFailureTasks == null && onTimeoutTasks == null
                 ? handler
                 : withTasks(handler);
        }
    }
}
