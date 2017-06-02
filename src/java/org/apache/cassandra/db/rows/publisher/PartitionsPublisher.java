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
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.flow.CsFlow;

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
    private static final class OuterSubscription extends CsFlow.RequestLoop<FlowableUnfilteredPartition> implements PartitionsSubscription
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

                    if (error == null)
                        subscriber.onComplete();
                }
                catch(Throwable t)
                {
                    onError(t);
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
            logger.debug("Got exception: {}/{}", error.getClass().getName(), error.getMessage());

            if (this.error == null)
            {
                this.error = error;
                subscriber.onError(error);
            }

            release();
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
    }

    /**
     * The subscription to the inner flowable of partition items, e.g. unfiltered items.
     * TODO - support rows as well.
    */
    private final static class InnerSubscription extends CsFlow.RequestLoop<Unfiltered>
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
    }

}
