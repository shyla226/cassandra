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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.exceptions.Exceptions;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.PartitionTrait;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Subscribe to {@link PartitionsPublisher} to conver asynchronous events back to an iterator.
 * This is required only for backwards compatibility, to avoid making all code asynchronous in one go.
 * It should be limited to tests.
 */
class IteratorSubscription implements UnfilteredPartitionIterator, PartitionsSubscriber<Unfiltered>
{
    private static final Logger logger = LoggerFactory.getLogger(IteratorSubscription.class);

    private final static PartitionIterator POISON_PILL = PartitionIterator.EMPTY;

    private final BlockingQueue<PartitionIterator> queue;

    private PartitionsSubscription subscription;
    private Throwable error = null;
    private PartitionIterator current;
    private PartitionIterator next;

    IteratorSubscription(PartitionsPublisher publisher)
    {
        // unbounded and with poor performance because it should be used for tests only and at most partition publisher will return a page of data
        this.queue = new LinkedBlockingDeque<>();

        publisher.subscribe(this);
    }

    public TableMetadata metadata()
    {
        return null;
    }

    public void close()
    {
        subscription.close();
        Uninterruptibles.putUninterruptibly(queue, POISON_PILL);
    }

    public void onSubscribe(PartitionsSubscription subscription)
    {
        this.subscription = subscription;
    }

    public void onNextPartition(PartitionTrait partition) throws Exception
    {
        if (current != null)
        {
            current.put(PartitionIterator.POISON_PILL);
            current = null;
        }

        current = new PartitionIterator(partition, subscription);
        Uninterruptibles.putUninterruptibly(queue, current);
    }

    public void onNext(Unfiltered item) throws Exception
    {
        assert current != null : "No current inner iterator";
        current.put(item);
    }

    public void onComplete() throws Exception
    {
        if (current != null)
            current.put(PartitionIterator.POISON_PILL);

        Uninterruptibles.putUninterruptibly(queue, POISON_PILL);
    }

    public void onError(Throwable error)
    {
        this.error = error;

        if (current != null)
            current.put(PartitionIterator.POISON_PILL);

        Uninterruptibles.putUninterruptibly(queue, POISON_PILL);
    }

    protected AbstractUnfilteredRowIterator computeNext()
    {
        if (next != null)
            return next;

        next = Uninterruptibles.takeUninterruptibly(queue);

        if (error != null)
            throw Exceptions.propagate(error);

        return next;
    }

    @Override
    public boolean hasNext()
    {
        return computeNext() != POISON_PILL;
    }

    @Override
    public UnfilteredRowIterator next()
    {
        boolean hasNext = hasNext();
        assert hasNext;

        UnfilteredRowIterator ret = next;
        next = null;
        return ret;
    }

    private static class PartitionIterator<T extends Unfiltered> extends AbstractUnfilteredRowIterator
    {
        private final static PartitionIterator EMPTY = new PartitionIterator(PartitionData.EMPTY, null);
        private final static Row POISON_PILL = Rows.EMPTY_STATIC_ROW;

        private final BlockingQueue<T> queue;
        private final PartitionsSubscription subscription;

        private PartitionIterator(PartitionTrait partition, PartitionsSubscription subscription)
        {
            super(partition);
            // unbounded and with poor performance because it should be used for tests only and at most partition publisher will return a page of data
            this.queue = new LinkedBlockingDeque<>();
            this.subscription = subscription;
        }

        public void close()
        {
            if (subscription != null && hasNext())
                subscription.closePartition(partitionKey());
        }

        private void put(T item)
        {
            Uninterruptibles.putUninterruptibly(queue, item);
        }

        protected Unfiltered computeNext()
        {
            Unfiltered ret = Uninterruptibles.takeUninterruptibly(queue);

            if (logger.isTraceEnabled())
                logger.trace("{} - Iterator returns {}", hashCode(), ret.toString(metadata));

            return ret == POISON_PILL ? endOfData() : ret;
        }
    }
}
