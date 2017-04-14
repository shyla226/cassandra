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
import java.util.function.Function;

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

    public UnfilteredPartitionIterator toIterator()
    {
       return new IteratorSubscription(this);
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
            subscription.subscribe();
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
    private static final class OuterSubscription implements PartitionsSubscription, CsSubscriber<FlowableUnfilteredPartition>
    {
        /**
         * A stack of transformations to apply to the source before sending downstream
         */
        private final Stack transformations;

        /**
         * Signalled by the transformation when it wants to stop
         */
        public volatile BaseIterator.Stop stop;

        /** The upstream source */
        private final PartitionsSource source;

        /**
         * The downstream subscriber
         */
        private final PartitionsSubscriber<Unfiltered> subscriber;

        /**
         * A subscription to the partitions source, created on subscription, null until then.
         */
        private CsSubscription subscription;

        /**
         * A subscription to the current partition source, null when no subscription exists
         */
        private volatile InnerSubscription innerSubscription = null;

        /** The number of partitions requested */
        private volatile int requested;

        /** The number of partitions received */
        private volatile int received;

        /**
         * Set to true when the downstream has closed the subscription
         */
        private volatile boolean closed;

        /**
         * Set to true when either onComplete or onError have been called.
         */
        private volatile boolean completed;

        /**
         * Set to true when we have released the upstream resources.
         */
        private volatile boolean released;

        private OuterSubscription(PartitionsPublisher publisher, PartitionsSubscriber<Unfiltered> subscriber)
        {
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

        private void subscribe() throws Exception
        {
            subscription = source.get().subscribe(this);

            requested = 1;
            received = 0;
            request();
        }

        private void request()
        {
            if (closed || stop.isSignalled)
            {
                onComplete();
                return;
            }

            while ((requested - received) == 1 && !released)
            {
                //logger.debug("{} - requesting", hashCode());
                assert innerSubscription == null : "Cannot request with a pending inner subscription";

                subscription.request();
                requested++;
            }
        }

        private void onInnerSubscriptionClosed()
        {
            innerSubscription = null;
            received++;

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

            assert innerSubscription == null : "Receive partition before the previous one was completed";
            innerSubscription = new InnerSubscription(this, subscriber, partition);
            innerSubscription.subscribe();
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
                        runWithFailure(transformation::onClose);

                    runWithFailure(subscriber::onComplete);
                }
                finally
                {
                    release();
                }
            }
        }

        @Override
        public void onError(Throwable error)
        {
            if (!completed)
            {
                //logger.debug("{} - onError {}", hashCode(), error);
                completed = true;

                subscriber.onError(error);
                handleFailure(error);
            }
        }

        private void release()
        {
            if (!released)
            {
                //logger.debug("{} - releasing", hashCode());
                released = true;

                if (innerSubscription != null)
                    innerSubscription.close();

                if (subscription != null)
                    FileUtils.closeQuietly(subscription);

                source.close();
            }
        }

        @FunctionalInterface
        public interface RunnableWithFailure
        {
            void run() throws Exception;
        }

        private void runWithFailure(RunnableWithFailure runnable)
        {
            try
            {
                runnable.run();
            }
            catch (Throwable t)
            {
                handleFailure(t);
            }
        }

        private void handleFailure(Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Failed with {}", t.getMessage(), t);

            release();
        }
    }

    /**
     * The subscription to the inner flowable of partition items, e.g. unfiltered items.
     * TODO - support rows as well.
    */
    private final static class InnerSubscription implements CsSubscriber<Unfiltered>
    {
        private final OuterSubscription outerSubscription;
        private final PartitionsSubscriber subscriber;
        private FlowableUnfilteredPartition partition;
        private CsSubscription subscription;
        private boolean partitionPublished;
        private volatile long requested;
        private volatile long received;
        private volatile boolean closed;

        InnerSubscription(OuterSubscription outerSubscription, PartitionsSubscriber subscriber, FlowableUnfilteredPartition partition)
        {
            this.outerSubscription = outerSubscription;
            this.subscriber = subscriber;
            this.partition = partition;
            this.partitionPublished = false;

            // we set this back to true when we get the first item
            this.partition.hasData = false;
        }

        DecoratedKey partitionKey()
        {
            return partition.header.partitionKey;
        }

        // we cannot subscribe in the constructor because the atomic reference is not set yet
        public void subscribe()
        {
            assert this.subscription == null : "onSubscribe should have been called only once";

            try
            {
                subscription = this.partition.content.subscribe(this);
            }
            catch (Throwable t)
            {
                outerSubscription.handleFailure(t);
            }

            requested = 1;
            received = 0;
            request();
        }

        public void request()
        {
            // we start in subscribe() with requested = 1 and received = 0 and we
            // enter the loop. If request() calls onNext() recursively, then both
            // received and requested are incremented and we keep on looping.
            // Otherwise requested is incremented but received is not incremented
            // until onNext() is called, which in turn calls request() again, this
            // time with received incremented and we should enter the loop again
            while ((requested - received) == 1)
            {
                subscription.request(); // this might recursively calls onNext
                requested++;
            }
        }

        @Override
        public void onNext(Unfiltered item)
        {
            //logger.debug("{} - onNext item", outerSubscription.hashCode());

            if (!partition.hasData)
                partition.hasData = true;

            if (!maybePublishPartition())
            { // partition was suppressed
                //logger.debug("{} - partition suppressed", outerSubscription.hashCode());
                close();
                return;
            }

            if (closed || partition.stop.isSignalled)
            {
                //logger.debug("{} - closed or stop signalled {}, {}", outerSubscription.hashCode(), closed, partition.stop.isSignalled);
                onComplete();
                return;
            }

            for (Transformation transformation : outerSubscription.transformations)
            {
                try
                {
                    item = transformation.applyToUnfiltered(item);
                }
                catch (Throwable t)
                {
                    outerSubscription.handleFailure(t);
                }

                if (item == null)
                {
                    //logger.debug("{} - suppressed by {}", outerSubscription.hashCode(), transformation);
                    break;
                }
            }

            if (item != null)
            {
                //logger.debug("{} - Publishing {}", outerSubscription.hashCode(), item.toString(partition.header.metadata));
                try
                {
                    subscriber.onNext(item);
                }
                catch (Throwable t)
                {
                    outerSubscription.handleFailure(t);
                }
            }

            received++;
            request();
        }

        @Override
        public void onComplete()
        {
            //logger.debug("{} - onComplete item", outerSubscription.hashCode());

            if (maybePublishPartition())
            {
                for (Transformation transformation : outerSubscription.transformations)
                    outerSubscription.runWithFailure(transformation::onPartitionClose);
            }

            close();
        }

        @Override
        public void onError(Throwable error)
        {
            //logger.debug("{} - onError item", outerSubscription.hashCode());

            outerSubscription.onError(error);
        }

        private boolean maybePublishPartition()
        {
            if (!partitionPublished)
            {
                partitionPublished = true;
                for (Transformation transformation : outerSubscription.transformations)
                {
                    try { partition = transformation.applyToPartition(partition); }
                    catch (Throwable t) { outerSubscription.handleFailure(t); }

                    if (partition == null)
                    { // the partition was suppressed by a transformation
                        return false;
                    }
                }

                //logger.debug("{} - publishing partition", outerSubscription.hashCode());
                try { subscriber.onNextPartition(new PartitionData(partition.header, partition.staticRow, partition.hasData)); }
                catch (Throwable t)  { outerSubscription.handleFailure(t); }
            }

            // the partition was published, either now or earlier
            return true;
        }

        private void close()
        {
            FileUtils.closeQuietly(subscription);
            outerSubscription.onInnerSubscriptionClosed();
        }
    }

}
