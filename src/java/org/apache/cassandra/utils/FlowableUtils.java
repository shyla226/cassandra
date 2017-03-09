package org.apache.cassandra.utils;

import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;

import io.reactivex.Flowable;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class FlowableUtils
{

    public static <T> CloseableIterator<T> closeableIterator(Flowable<T> content)
    {
        IteratorSubscription<T> subscr = new IteratorSubscription<T>();
        content.subscribe(subscr);
        return subscr;
    }

    public static class IteratorSubscription<T> implements Subscriber<T>, CloseableIterator<T>
    {
        static final Object POISON_PILL = new Object();

        Subscription subscription = null;
        BlockingQueue<Object> queue = new ArrayBlockingQueue<>(3);  // onComplete comes with next() sometimes, leave one more for onError
        Object next = null;

        IteratorSubscription()
        {
        }

        public void close()
        {
            subscription.cancel();
        }

        @Override
        public void onComplete()
        {
            Uninterruptibles.putUninterruptibly(queue, POISON_PILL);
        }

        @Override
        public void onError(Throwable arg0)
        {
            Uninterruptibles.putUninterruptibly(queue, arg0);
        }

        @Override
        public void onNext(T arg0)
        {
            Uninterruptibles.putUninterruptibly(queue, arg0);
        }

        @Override
        public void onSubscribe(Subscription arg0)
        {
            assert subscription == null;
            subscription = arg0;
        }

        protected Object computeNext()
        {
            if (next != null)
                return next;

            next = queue.poll();
            if (next instanceof Throwable)
                Throwables.propagate((Throwable) next);
            if (next != null)
                return next;

            subscription.request(1);

            next = Uninterruptibles.takeUninterruptibly(queue);
            if (next instanceof Throwable)
                Throwables.propagate((Throwable) next);
            return next;
        }

        @Override
        public boolean hasNext()
        {
            return computeNext() != POISON_PILL;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T next()
        {
            if (!hasNext())
                throw new NoSuchElementException();
            Object n = next;
            next = null;
            return (T) n;
        }
    }

    public static <T> Flowable<T> fromCloseableIterator(CloseableIterator<T> iter)
    {
        return new FlowableFromIter<>(iter);
    }

    // We have our own iter-to-flowable implementation to take care of closing on cancel and to ensure single use
    // Alternative is to go through single-use iterable, Flowable.fromIterable and doFinally(close).
    private static class FlowableFromIter<T> extends Flowable<T>
    {
        CloseableIterator<T> iter;
        T[] prepend;

        @SafeVarargs
        public FlowableFromIter(CloseableIterator<T> iter, T... prepend)
        {
            this.iter = iter;
            this.prepend = prepend;
        }

        @Override
        protected void subscribeActual(Subscriber<? super T> subscriber)
        {
            assert iter != null : "Can only subscribe once to a closeable iterator.";
            subscriber.onSubscribe(new IterSubscription<T>(subscriber, iter, prepend));
            iter = null;
        }
    }

    private static class IterSubscription<T> implements Subscription
    {
        final Subscriber<? super T> subscriber;
        final CloseableIterator<T> iter;
        final T[] prepend;
        int pos = 0;
        boolean closed = false;

        public IterSubscription(Subscriber<? super T> subscriber, CloseableIterator<T> iter, T[] prepend)
        {
            super();
            this.subscriber = subscriber;
            this.iter = iter;
            this.prepend = prepend;
        }

        @Override
        public void request(long count)
        {
            assert !closed;
            try
            {
                while (--count >= 0)
                {
                    if (pos < prepend.length)
                        subscriber.onNext(prepend[pos++]);
                    else if (iter.hasNext())
                        subscriber.onNext(iter.next());
                    else
                    {
                        cancel();
                        subscriber.onComplete();
                        break;
                    }
                }
            }
            catch (Throwable t)
            {
                cancel();
                subscriber.onError(t);
            }
        }

        @Override
        public void cancel()
        {
            if (!closed)
            {
                iter.close();
                closed = true;
            }
        }
    }
}
