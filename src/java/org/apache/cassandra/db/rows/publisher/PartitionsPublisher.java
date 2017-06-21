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

package org.apache.cassandra.db.rows.publisher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Single;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.transform.BaseIterator;
import org.apache.cassandra.db.transform.Stack;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.flow.CsFlow;
import org.apache.cassandra.utils.flow.CsSubscriber;
import org.apache.cassandra.utils.flow.CsSubscription;

/**
 * A publisher will notify the subscriber when partitions or rows
 * are available, until the subscription is closed.
 * <p>
 * It receives data from an upstream flowable of partitions, normally retrieved from disk
 * or the network and per-merged, it then applies any transformation and publishes
 * to the subscriber.
 */
@SuppressWarnings("unchecked")
public class PartitionsPublisher
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionsPublisher.class);

    /** The upstream source */
    private final PartitionsSource source;

    /** A stack of transformations to apply to the source before sending downstream */
    private final Stack transformations;

    /** Signalled by the transformation when it wants to stop */
    public volatile BaseIterator.Stop stop = new BaseIterator.Stop();

    private PartitionsPublisher(final PartitionsSource source)
    {
        this.source = source;
        this.transformations = new Stack();
    }

    public static PartitionsPublisher create(ReadCommand command, Function<ReadExecutionController, CsFlow<FlowableUnfilteredPartition>> sourceProvider)
    {
        return new PartitionsPublisher(PartitionsSource.withController(command, sourceProvider));
    }

    public static PartitionsPublisher empty()
    {
        return new PartitionsPublisher(CsFlow::empty);
    }

    /**
     * Extend the current publisher by concatenating the sources of extra publishers.
     * This method assumes that all publishers are not yet subscribed to and that they
     * have the same exact transformations (in principle) therefore discarding the
     * transformations of the publishers whose sources are appended.
     *
     * This is not strictly correct to the existing behavior but we know it to be
     * correct for now, also the existing behavior is less efficient in that, for example,
     * it applies the global limits to each individual publisher, whereas we apply them
     * globally, which is more efficient.
     *
     * TODO: we should really ensure equivalence of the transformations that have
     * been applied to the publishers that are being appended.
     */
    public PartitionsPublisher extend(Collection<PartitionsPublisher> publishers)
    {
        //logger.debug("{} - extending publisher", hashCode());

        List<PartitionsSource> sources = new ArrayList<>(publishers.size() + 1);
        sources.add(source);

        for (PartitionsPublisher publisher : publishers)
        {
            assert publisher.transformations.size() == transformations.size() : "Publishers must have identical transformation when extending";
            sources.add(publisher.source);
        }

        PartitionsPublisher ret = new PartitionsPublisher(PartitionsSource.concat(sources));
        for (Transformation transformation : transformations)
            ret.transform(transformation);

        return ret;
    }

    /**
     * Convert the asynchronous partitions to an iterator.
     *
     * @param metadata - the metadata of the read command for this publisher
     *
     * @return an iterator that might block
     */
    public UnfilteredPartitionIterator toIterator(TableMetadata metadata)
    {
        IteratorSubscription ret = new IteratorSubscription(metadata);
        subscribe(ret);
        return ret;
    }

    /**
     * Convert the asynchronous partitions to an iterator and then return a Single
     * that will complete only when the iterator has been fully materialized in memory.
     *
     * @param metadata - the metadata of the read command for this publisher
     *
     * @return an iterator that will not block
     */
    public Single<UnfilteredPartitionIterator> toDelayedIterator(TableMetadata metadata)
    {
        IteratorSubscription ret = new IteratorSubscription(metadata);
        subscribe(ret);
        return ret.toSingle();
    }

    public final <R1, R2> Single<R1> reduce(ReduceCallbacks<R1, R2> callbacks) {
        return new ReducerSubscription<>(this, callbacks);
    }

    public PartitionsPublisher transform(Transformation transformation)
    {
        transformation.attachTo(this);
        transformations.add(transformation);
        return this;
    }

    /**
     * Subscribing will start the flow of items. Onlyone subscription is allowed.
     *
     * @param subscriber - the subscriber that will receive the items.
     */
    @SuppressWarnings("resource") // subscription closed when source has terminated or by subscriber
    public void subscribe(PartitionsSubscriber<Unfiltered> subscriber)
    {
        try
        {
            OuterSubscription subscription = new OuterSubscription(this, subscriber);
            subscriber.onSubscribe(subscription);
            subscription.request();
        }
        catch(Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Failed to subscribe to partitions flow", t);
            subscriber.onError(t);
        }
    }

    /**
     * The subscription to the outer flowable of partitions.
     */
    private static final class OuterSubscription extends RequestLoop<FlowableUnfilteredPartition>
    implements PartitionsSubscription
    {
        /**
         * A stack of transformations to apply to the source before sending downstream
         */
        private final Stack transformations;

        /**
         * Signalled by the transformation when it wants to stop
         */
        public volatile BaseIterator.Stop stop;

        /**
         * The upstream source
         */
        private final PartitionsSource source;

        /**
         * The downstream subscriber
         */
        private final PartitionsSubscriber<Unfiltered> subscriber;

        /**
         * A subscription to the current partition source, null when no subscription exists
         */
        private volatile InnerSubscription innerSubscription = null;

        /**
         * Set to true when the downstream has closed the subscription
         */
        private volatile boolean closed;

        /**
         * Set to true when either onComplete or onError have been called.
         */
        private volatile boolean completed;

        /** Any error that might have been received */
        private volatile  Throwable error;

        private OuterSubscription(PartitionsPublisher publisher, PartitionsSubscriber<Unfiltered> subscriber) throws Exception
        {
            super(publisher.source.get());
            this.transformations = publisher.transformations;
            this.stop = publisher.stop;
            this.source = publisher.source;
            this.subscriber = subscriber;
        }

        public void closePartition(DecoratedKey partitionKey)
        {
            InnerSubscription subscription = this.innerSubscription;
            if (subscription != null && subscription.partitionKey().equals(partitionKey))
                subscription.closed = true;
        }

        public void close()
        {
            closed = true;

            InnerSubscription subscription = this.innerSubscription;
            if (subscription != null)
                subscription.closed = true;
        }

        private void onInnerSubscriptionClosed()
        {
            innerSubscription = null;
            request();
        }

        @Override
        public void onNext(FlowableUnfilteredPartition partition)
        {
            //logger.debug("{} - onNext", hashCode());
            if (closed || stop.isSignalled)
            {
                onComplete();
                return;
            }

            try
            {
                assert innerSubscription == null : "Receive partition before the previous one was completed";
                innerSubscription = new InnerSubscription(this, subscriber, partition);
                innerSubscription.request();
            }
            catch (Throwable t)
            {
                onError(t);
            }
        }

        @Override
        public void onComplete()
        {
            if (!completed)
            {
                //logger.debug("{} - onComplete", hashCode());
                completed = true;

                try
                {
                    for (Transformation transformation : transformations)
                        transformation.onClose();

                    // we must close before onComplete, otherwise actions performed onClose
                    // will not be synchronous, e.g. updating the metrics (reproduce with KeySpaceTest.testLimitSSTables
                    // by adding a sleep at the beginning of SinglePartitionsReadCommand.updateMetrics()
                    release();

                    if (error == null)
                        subscriber.onComplete();
                }
                catch(Throwable t)
                {
                    release(); // idem-potent but it must be called before the final onError, see comment above
                    onError(t);
                }
            }
        }

        @Override
        public void onError(Throwable error)
        {
            error = addSubscriberChainFromSource(error);

            JVMStabilityInspector.inspectThrowable(error);

            logger.debug("Got exception: {}/{}", error.getClass().getName(), error.getMessage());

            release();

            if (this.error == null)
            {
                this.error = error;
                subscriber.onError(error);
            }
        }

        private void release()
        {
            if (!closed())
            {
                try
                {
                    super.close();
                }
                catch (Exception ex)
                {
                    logger.warn("Exception when closing: {}", ex.getMessage());
                }

                if (innerSubscription != null)
                    innerSubscription.close();

                source.close();
            }
        }

        @Override
        public String toString()
        {
            return CsFlow.formatTrace("outerSubscription");
        }
    }

    /**
     * The subscription to the inner flowable of partition items, e.g. unfiltered items.
     * TODO - support rows as well.
    */
    private final static class InnerSubscription extends RequestLoop<Unfiltered>
    {
        private final OuterSubscription outerSubscription;
        private final PartitionsSubscriber subscriber;
        private final DecoratedKey partitionKey;
        @Nullable // will be null if transformations suppress it
        private FlowableUnfilteredPartition partition;
        private boolean partitionPublished;
        private volatile boolean closed;

        InnerSubscription(OuterSubscription outerSubscription, PartitionsSubscriber subscriber, FlowableUnfilteredPartition partition) throws Exception
        {
            super(partition.content);

            this.outerSubscription = outerSubscription;
            this.subscriber = subscriber;
            this.partitionKey = partition.header.partitionKey;
            this.partition = partition;
            this.partitionPublished = false;

            // we set this back to true when we get the first item
            this.partition.hasData = false;
        }

        DecoratedKey partitionKey()
        {
            return partitionKey;
        }

        @Override
        public void onNext(Unfiltered item)
        {
            try
            {
                //logger.debug("{} - onNext item", outerSubscription.hashCode());

                if (partition != null && !partition.hasData)
                    partition.hasData = true;

                if (!maybePublishPartition())
                { // partition was suppressed, close silently without calling onComplete
                    //logger.debug("{} - partition suppressed", outerSubscription.hashCode());
                    close();
                    return;
                }

                if (closed || (partition != null && partition.stop.isSignalled))
                {
                    //logger.debug("{} - closed or stop signalled {}, {}", outerSubscription.hashCode(), closed, partition.stop.isSignalled);
                    onComplete();
                    return;
                }

                for (Transformation transformation : outerSubscription.transformations)
                {
                    item = transformation.applyToUnfiltered(item);
                    if (item == null)
                        break;
                }

                if (item != null)
                {
                    //logger.debug("{} - Publishing {}", outerSubscription.hashCode(), item.toString(partition.header.metadata));
                    subscriber.onNext(item);
                }

                request();
            }
            catch (Throwable t)
            {
                onError(t);
            }
        }

        @Override
        public void onComplete()
        {
            //logger.debug("{} - onComplete item", outerSubscription.hashCode());
            try
            {
                if (maybePublishPartition())
                {
                    for (Transformation transformation : outerSubscription.transformations)
                        transformation.onPartitionClose();
                }
            }
            catch (Throwable t)
            {
                onError(t);
            }
            finally
            {
                close();
            }
        }

        @Override
        public void onError(Throwable error)
        {
            //logger.debug("{} - onError item", outerSubscription.hashCode(), error);
            error = addSubscriberChainFromSource(error);

            outerSubscription.onError(error);
            close();
        }

        private boolean maybePublishPartition()
        {
            if (!partitionPublished)
            {
                partitionPublished = true;
                for (Transformation transformation : outerSubscription.transformations)
                {
                    partition = transformation.applyToPartition(partition);
                    if (partition == null)
                    { // the partition was suppressed by a transformation
                        return false;
                    }
                }

                //logger.debug("{} - publishing partition {}", outerSubscription.hashCode(), partition.header.partitionKey);
                subscriber.onNextPartition(new PartitionData(partition.header, partition.staticRow, partition.hasData));
            }

            // the partition was published earlier if it isn't null, otherwise it was suppressed and we return false
            return partition != null;
        }

        public void close()
        {
            if (!closed())
            {
                try
                {
                    super.close();
                }
                catch (Exception ex)
                {
                    logger.warn("Exception when closing: {}", ex.getMessage());
                }

                outerSubscription.onInnerSubscriptionClosed();
            }
        }

        public String toString()
        {
            return CsFlow.formatTrace("innerSubscription");
        }
    }

    /**
     * A utility class to manage the request loop.
     * <p>
     * Requests have to be performed in a loop in order to avoid growing
     * the stack indefinitely because calling {@link CsSubscription#request()} may result
     * in a recursive call to {@link CsSubscriber#onNext(Object)} or {@link CsSubscriber#onComplete()}.
     * Whilst we don't need to request anything when onComplete is called, we need to call request()
     * exactly once when onNext is called.
     * <p>
     * In addition to recursions, we also need to handle races between the request loop and onNext, which
     * could be called from a different thread at any time.
     * <p>
     * This is handled with a small state machine that controls whether we are in the loop and whether an
     * item has been received. The call to request is a no-op if we are already in the loop. Once in the loop,
     * after calling request we try to exit the loop, but if {@link RequestLoop#onNext(Object)} has been called
     * recursively or has raced with the request loop, then we fail to exit the loop and continue with the next
     * request. onNext will try to transition from in_loop_requested to in_loop_ready and if it succeeds then
     * it is done since the loop is still in place, if it instead has failed then we must be out of the loop and
     * hence requestLoop() must be called again.
     */
    private static abstract class RequestLoop<T> implements CsSubscriber<T>, CsSubscription
    {
        private enum State
        {
            OUT_OF_LOOP,
            IN_LOOP_READY,
            IN_LOOP_REQUESTED
        }

        private final AtomicReference<State> state;
        private final CsSubscription subscription;
        private volatile boolean closed;

        public RequestLoop(CsFlow<T> source) throws Exception
        {
            this.state = new AtomicReference<>(State.OUT_OF_LOOP);
            this.subscription = source.subscribe(this);
        }

        public void request()
        {
            // This may be executing concurrently from two threads (this remains while it's trying to loop,
            // another calls it because of onNext).

            // Requests have to be performed in a loop to avoid growing the stack with a full
            // request... -> onNext... -> chain for each new element in the group, which can easily cause stack overflow.
            // So if a request was issued in response to synchronously executed onNext (and thus control
            // will return to the request loop after the onNext and request chains return), mark it and process
            // it when control returns to the loop.
            // When onNext comes asynchronously, the loop has usually completed. If it has, we should process the
            // next request within this call. If not, the loop is still going in another thread and we can still signal
            // it to continue (making sure we don't leave while receiving the signal).

            if (closed)
                return;

            if (state.compareAndSet(State.IN_LOOP_REQUESTED, State.IN_LOOP_READY))
                return;

            if (state.compareAndSet(State.OUT_OF_LOOP, State.IN_LOOP_READY))
                requestLoop();
        }

        private void requestLoop()
        {
            while (!closed)
            {
                // The loop can only be entered in IN_LOOP_REAY
                if (!verifyStateChange(State.IN_LOOP_READY, State.IN_LOOP_REQUESTED))
                    return;

                subscription.request();

                // If we didn't receive item, leave
                if (state.compareAndSet(State.IN_LOOP_REQUESTED, State.OUT_OF_LOOP))
                    break;
            }
        }

        private boolean verifyStateChange(State from, State to)
        {
            State prev = state.getAndSet(to);
            if (prev == from)
                return true;

            onError(new AssertionError("Invalid state " + prev + " in loop of " + this));
            return false;
        }

        public void close() throws Exception
        {
            if (!closed)
            {
                closed = true;
                FileUtils.closeQuietly(subscription);
            }
        }

        public Throwable addSubscriberChainFromSource(Throwable t)
        {
            return subscription.addSubscriberChainFromSource(t);
        }

        public boolean closed()
        {
            return closed;
        }
    }
}
