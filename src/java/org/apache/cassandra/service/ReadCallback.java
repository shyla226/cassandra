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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.flow.DeferredFlow;
import org.apache.cassandra.utils.flow.Flow;


public class ReadCallback<T> implements MessageCallback<ReadResponse>
{
    protected static final Logger logger = LoggerFactory.getLogger(ReadCallback.class);

    final ResponseResolver<T> resolver;
    final List<InetAddress> endpoints;

    private final int blockfor;

    private final AtomicInteger received = new AtomicInteger(0);
    private final AtomicInteger failures = new AtomicInteger(0);
    private final Map<InetAddress, RequestFailureReason> failureReasonByEndpoint;

    private final DeferredFlow<T> result;

    private ReadCallback(ResponseResolver<T> resolver, List<InetAddress> endpoints)
    {
        this.resolver = resolver;
        this.endpoints = endpoints;
        this.blockfor = resolver.ctx.blockFor(endpoints);
        this.failureReasonByEndpoint = new ConcurrentHashMap<>();

        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(command().getTimeout());
        this.result = DeferredFlow.create(queryStartNanos() + timeoutNanos, this::generateFlowOnTimeout);

        if (logger.isTraceEnabled())
            logger.trace("Blockfor is {}; setting up requests to {}", blockfor, StringUtils.join(this.endpoints, ","));
    }

    static <T> ReadCallback<T> forResolver(ResponseResolver<T> resolver, List<InetAddress> targets)
    {
        return new ReadCallback<>(resolver, targets);
    }

    // Publicly visible for testing purposes
    public static ReadCallback<FlowablePartition> forInitialRead(ReadCommand command, List<InetAddress> targets, ReadContext ctx)
    {
        return forResolver(ctx.withDigests ? new DigestResolver(command, ctx, targets.size())
                                           : new DataResolver(command, ctx, targets.size()),
                           targets);
    }

    /**
     * Assuming this callback was for a digest read and the corresponding read resulted in a mismatch, returns a new
     * callback suitable for a followup full data read, as well as the endpoints that should be queried for said data
     * read.
     *
     * @param targets the target for the data read. This would always be the same as this callback endpoints if not for
     *                speculative read retries. Namely, the initial digest callback was created including potential
     *                speculative targets since we _may_ decide to speculate. If we didn't however (meaning that all
     *                non-speculative endpoints responded quickly, just with non-matching responses), we'll only query
     *                the non-speculative endpoints for the data read (we're kind of assuming those endpoints are fast
     *                enough).
     * @return a pair of the created {@code ReadCallback} and the endpoints to query. The reason we also return the
     * endpoints to query (instead of directly using the returned callback {@code endpoints} field) is that we reuse the
     * data response we got on the digest read (APOLLO-368) and so should not query the corresponding node. In other
     * words, the returned endpoints will be the endpoints of the callback minus one.
     */
    Pair<ReadCallback<FlowablePartition>, Collection<InetAddress>> forDigestMismatchRepair(List<InetAddress> targets)
    {
        assert resolver instanceof DigestResolver;
        DigestResolver digestResolver = (DigestResolver)resolver;
        assert digestResolver.isDataPresent(); // We should only call this method when we got a mismatch, which imply we go
                                               // the data read (we don't bother comparing any digest until we have it)

        Response<ReadResponse> dataResponse = digestResolver.dataResponse;

        // Create the data read callback and directly feed the already known response
        ReadCallback<FlowablePartition> callback = forResolver(new DataResolver(command(), readContext(), targets.size()), targets);
        callback.onResponse(dataResponse);
        return Pair.create(callback, subtractTarget(targets, dataResponse.from()));
    }

    private List<InetAddress> subtractTarget(List<InetAddress> targets, InetAddress toSubstract)
    {
        assert !targets.isEmpty() : "We shouldn't have got a mismatch with no targets";
        List<InetAddress> toQuery = new ArrayList<>(targets.size() - 1);
        for (InetAddress target : targets)
        {
            if (!target.equals(toSubstract))
                toQuery.add(target);
        }
        return toQuery;
    }

    ReadCommand command()
    {
        return resolver.command;
    }

    ReadContext readContext()
    {
        return resolver.ctx;
    }

    private ConsistencyLevel consistency()
    {
        return resolver.consistency();
    }

    private long queryStartNanos()
    {
        return readContext().queryStartNanos;
    }

    public Flow<T> result()
    {
        return result;
    }

    boolean hasValue()
    {
        return result.hasSource();
    }

    public int blockFor()
    {
        return blockfor;
    }

    private Flow<T> generateFlowOnSuccess(int receivedResponses)
    {
        if (readContext().readObserver != null)
        {
            readContext().readObserver.responsesReceived(receivedResponses == endpoints.size()
                                                         ? endpoints
                                                         : ImmutableSet.copyOf(Iterables.transform(resolver.getMessages(), Message::from)));
        }

        try
        {
            return (blockfor == 1 ? resolver.getData() : resolver.resolve()).doOnError(this::onError);
        }
        catch (Throwable e)
        { // typically DigestMismatchException, but safer to report all errors to the subscriber
            if (logger.isTraceEnabled())
                logger.trace("Got error: {}/{}", e.getClass().getName(), e.getMessage());
            return Flow.error(e);
        }
    }

    private Flow<T> generateFlowOnTimeout()
    {
        // It's possible to have ReadContext#blockFor() > ReadContext#requiredResponses() (use by NodeSync at least),
        // in which case what we want is that on timeout (not enough blockFor), if we have enough required responses,
        // we still consider it a success and provide the result.

        int responses = received.get();
        int requiredResponses = readContext().requiredResponses();
        if (responses >= requiredResponses && resolver.isDataPresent())
            return generateFlowOnSuccess(responses);

        return Flow.error(new ReadTimeoutException(consistency(), responses, blockfor, resolver.isDataPresent()));
    }

    public void onResponse(Response<ReadResponse> message)
    {
        if (logger.isTraceEnabled())
            logger.trace("Received response: {}", message);

        resolver.preprocess(message);
        int n = waitingFor(message.from()) ? received.incrementAndGet() : received.get();

        if (n >= blockfor && resolver.isDataPresent())
        {
            if (result.onSource(generateFlowOnSuccess(n)))
            {
                if (logger.isTraceEnabled())
                    logger.trace("Read: {} ms.", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - queryStartNanos()));
            }


            // kick off a background digest comparison if this is a result that (may have) arrived after
            // the original resolve that get() kicks off as soon as the condition is signaled
            if (blockfor < endpoints.size() && n == endpoints.size())
            {
                TraceState traceState = Tracing.instance.get();
                if (traceState != null)
                    traceState.trace("Initiating read-repair");
                if (logger.isTraceEnabled())
                    logger.trace("Initiating read-repair");
                StageManager.getStage(Stage.READ_REPAIR).execute(new AsyncRepairRunner(traceState));
            }
        }
    }

    @Override
    public void onTimeout(InetAddress host)
    {
        result.onSource(generateFlowOnTimeout());
    }

    /**
     * @return true if the message counts towards the blockfor threshold
     */
    private boolean waitingFor(InetAddress from)
    {
        return !consistency().isDatacenterLocal() || DatabaseDescriptor.getLocalDataCenter().equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(from));
    }

    void assureSufficientLiveNodes() throws UnavailableException
    {
        consistency().assureSufficientLiveNodes(readContext().keyspace, endpoints);
    }

    private class AsyncRepairRunner implements Runnable
    {
        private final TraceState traceState;

        private AsyncRepairRunner(TraceState traceState)
        {
            this.traceState = traceState;
        }

        public void run()
        {
            // If the resolver is a DigestResolver, we need to do a full data read if there is a mismatch.
            // Otherwise, resolve will send the repairs directly if needs be (and in that case we should never
            // get a digest mismatch).
            try
            {
                resolver.compareResponses().blockingAwait();
            }
            catch (Throwable e)
            {
                if (e instanceof RuntimeException && e.getCause() != null)
                    e = e.getCause();

                if (e instanceof DigestMismatchException)
                    retryOnDigestMismatch((DigestMismatchException) e);
                else
                    throw Throwables.propagate(e);
            }
        }

        private void retryOnDigestMismatch(DigestMismatchException e)
        {
            assert resolver instanceof DigestResolver;

            if (traceState != null)
                traceState.trace("Digest mismatch: {}", e.toString());
            if (logger.isDebugEnabled())
                logger.debug("Digest mismatch:", e);

            ReadRepairMetrics.repairedBackground.mark();

            // We only run the AsyncRepairRunner when we got a data response, so reuse that response to save work.
            Response<ReadResponse> dataResponse = ((DigestResolver)resolver).dataResponse;
            assert dataResponse != null;
            final DataResolver repairResolver = new DataResolver(command(), readContext(), endpoints.size());
            AsyncRepairCallback repairHandler = new AsyncRepairCallback(repairResolver, endpoints.size());
            repairHandler.onResponse(dataResponse);
            MessagingService.instance().send(command().dispatcherTo(subtractTarget(endpoints, dataResponse.from())), repairHandler);
        }
    }

    @Override
    public void onFailure(FailureResponse<ReadResponse> failureResponse)
    {
        if (logger.isTraceEnabled())
            logger.trace("Received failure response: {}", failureResponse);

        int n = waitingFor(failureResponse.from()) ? failures.incrementAndGet() : failures.get();

        failureReasonByEndpoint.put(failureResponse.from(), failureResponse.reason());

        if (blockfor + n > endpoints.size() && !result.hasSource())
            result.onSource(Flow.error(new ReadFailureException(consistency(),
                                                                      received.get(),
                                                                      blockfor,
                                                                      resolver.isDataPresent(),
                                                                      failureReasonByEndpoint)));
    }

    private void onError(Throwable error)
    {
        // There is 3 "normal" exceptions we can get here:
        //   - ReadTimeoutException if we timeout.
        //   - ReadFailureException if we receive failure responses.
        //   - UnavailableException if too many node become unavailable in the middle of a range query (when fetching
        //     more sub-ranges).
        //   - DigestMismatchException on a digest mismatch for a digest read.
        // Anything else is a programming error, so log a proper message if that happens (we still propagate the
        // exception in all cases, so it's possible we get double-logging upper in the stack, but better that than
        // no logging at all if we have a bug).

        int received = ReadCallback.this.received.get();

        boolean isTimeout = error instanceof ReadTimeoutException;
        boolean isFailure = error instanceof ReadFailureException;

        if (isTimeout || isFailure)
        {
            if (Tracing.isTracing())
            {
                String gotData = received > 0 ? (resolver.isDataPresent() ? " (including data)" : " (only digests)") : "";
                Tracing.trace("{}; received {} of {} responses{}", isFailure ? "Failed" : "Timed out", received, blockfor, gotData);
            }
            else if (logger.isDebugEnabled())
            {
                String gotData = received > 0 ? (resolver.isDataPresent() ? " (including data)" : " (only digests)") : "";
                logger.debug("{}; received {} of {} responses{}", isFailure ? "Failed" : "Timed out", received, blockfor, gotData);
            }
        }
        else if (!(error instanceof DigestMismatchException || error instanceof UnavailableException))
        {
            logger.error("Unexpected error handling read responses for {}. Have received {} of {} responses.",
                         resolver.command, received, blockfor, error);
        }
    }
}
