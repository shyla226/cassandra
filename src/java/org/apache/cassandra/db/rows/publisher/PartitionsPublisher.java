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

        /**
         * The request loop keeps track of when we should request the next item;
         */
        private CsFlow.RequestLoop requestLoop;

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
            requestLoop = new CsFlow.RequestLoop(this, subscription);

            requestLoop.request();
        }

        private void onInnerSubscriptionClosed()
        {
            innerSubscription = null;
            requestLoop.onNext();
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
                        transformation.onClose();

                    if (error == null)
                        subscriber.onComplete();
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
            JVMStabilityInspector.inspectThrowable(error);
            //logger.debug("{}/{}", error.getClass().getName(), error.getMessage());

            if (this.error == null)
            {
                this.error = error;
                subscriber.onError(error);
            }

            release();
        }

        private void release()
        {
            if (!requestLoop.isReleased())
            {
                //logger.debug("{} - releasing", hashCode());
                requestLoop.release();

                if (innerSubscription != null)
                    innerSubscription.close();

                if (subscription != null)
                    FileUtils.closeQuietly(subscription);

                source.close();
            }
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
        private CsFlow.RequestLoop requestLoop;
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
                requestLoop = new CsFlow.RequestLoop(this, subscription);
            }
            catch (Throwable t)
            {
                outerSubscription.onError(t);
            }

            requestLoop.request();
        }

        @Override
        public void onNext(Unfiltered item)
        {
            //logger.debug("{} - onNext item", outerSubscription.hashCode());

            if (!partition.hasData)
                partition.hasData = true;

            if (!maybePublishPartition())
            { // partition was suppressed, close silently without calling onComplete
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
                item = transformation.applyToUnfiltered(item);
                if (item == null)
                    break;
            }

            if (item != null)
            {
                //logger.debug("{} - Publishing {}", outerSubscription.hashCode(), item.toString(partition.header.metadata));
                subscriber.onNext(item);
            }

            requestLoop.onNext();
        }

        @Override
        public void onComplete()
        {
            //logger.debug("{} - onComplete item", outerSubscription.hashCode());
            if (maybePublishPartition())
            {
                for (Transformation transformation : outerSubscription.transformations)
                    transformation.onPartitionClose();
            }

            close();
        }

        @Override
        public void onError(Throwable error)
        {
            //logger.debug("{} - onError item", outerSubscription.hashCode(), error);
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

            // the partition was published, either now or earlier
            return true;
        }

        private void close()
        {
            //logger.debug("{} - closing {}", outerSubscription.hashCode(), requestLoop.isReleased());
            if (!requestLoop.isReleased())
            {
                requestLoop.release();
                FileUtils.closeQuietly(subscription);
                outerSubscription.onInnerSubscriptionClosed();
            }
        }
    }

}
