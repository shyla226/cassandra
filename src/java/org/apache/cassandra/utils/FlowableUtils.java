package org.apache.cassandra.utils;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;
import io.reactivex.FlowableOperator;
import io.reactivex.functions.Function;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class FlowableUtils
{
    private static final Logger logger = LoggerFactory.getLogger(FlowableUtils.class);

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

        // synchronization should not be contested
        @Override
        public synchronized void request(long count)
        {
            try
            {
                while (!closed && --count >= 0)
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

        // tpc TODO remove synchronization
        @Override
        public synchronized void cancel()
        {
            if (!closed)
            {
                iter.close();
                closed = true;
            }
        }
    }

    /**
     * Helper class implementing once-only closing.
     */
    public static abstract class OnceCloseable implements Closeable
    {
        /**
         * Called once the first time {@code close} is called.
         */
        abstract public void onClose();

        private volatile int closed = 0;
        private static final AtomicIntegerFieldUpdater closedUpdater =
            AtomicIntegerFieldUpdater.newUpdater(OnceCloseable.class, "closed");

        public void close()
        {
            if (closedUpdater.compareAndSet(this, 0, 1))
                onClose();
        }
    }

    public interface FlowableOp<I, O> extends FlowableOperator<O, I>
    {
        /**
         * Called on next item. Normally calls subscriber.onNext, but could instead finish the stream by calling
         * complete, throw an error using error, or decide to ignore input and request another.
         * For some usage ideas, see SkippingOp and StoppingOp below.
         */
        void onNext(Subscriber<? super O> subscriber, Subscription source, I next);

        /**
         * Call this when onNext needs to terminate the subscription early.
         */
        default void complete(Subscriber<? super O> subscriber, Subscription source)
        {
            source.cancel();
            close();
            subscriber.onComplete();
        }

        /**
         * Call this when onNext identifies an error.
         */
        default void error(Subscriber<? super O> subscriber, Subscription source, Throwable t)
        {
            source.cancel();
            close();
            subscriber.onError(t);
        }

        /**
         * Called on cancel, completion or error.
         * Often called more than once; derive from {@link CloseableFlowableOp} below and override onClose if you need
         * this to be done only once.
         */
        default void close()
        {
        }

        /**
         * Implementation of FlowableOperator to enable direct usage through Flowable.lift().
         */
        default Subscriber<I> apply(Subscriber<? super O> sub)
        {
            return new ApplyOpSubscription<>(sub, this);
        }
    }

    /**
     * Convenience class for use case where we need exactly one final onClose() call.
     */
    public abstract static class CloseableFlowableOp<I, O> extends OnceCloseable implements FlowableOp<I, O>
    {
    }

    public static <I, O> Flowable<O> apply(Flowable<I> source, FlowableOp<I, O> op)
    {
        return source.lift(op);
    }

    private static class ApplyOpSubscription<I, O>
    implements Subscription, Subscriber<I>
    {
        final Subscriber<? super O> subscriber;
        final FlowableOp<I, O> mapper;
        Subscription source;
        boolean alreadyCancelled;

        public ApplyOpSubscription(Subscriber<? super O> subscriber, FlowableOp<I, O> mapper)
        {
            this.subscriber = subscriber;
            this.mapper = mapper;
        }

        public void onSubscribe(Subscription subscription)
        {
            source = subscription;
            subscriber.onSubscribe(this);
            if (alreadyCancelled)
            {
                source.cancel();
                mapper.close();
            }
        }

        public void onNext(I item)
        {
            mapper.onNext(subscriber, source, item);
        }

        public void onError(Throwable throwable)
        {
            mapper.close();
            subscriber.onError(throwable);
        }

        public void onComplete()
        {
            mapper.close();
            subscriber.onComplete();
        }

        public void request(long l)
        {
            if (source != null)
                source.request(l);
        }

        public void cancel()
        {
            if (source != null)
            {
                source.cancel();
                mapper.close();
            }
            alreadyCancelled = true;
        }
        // TODO: This could be a TransformationSubscription...
    }

    // Wrapper allowing simple transformations to extend all the way to FlowableOperator without extra objects being
    // created.
    public interface SkippingOp<I, O> extends Function<I, O>, FlowableOp<I, O>
    {
        default void onNext(Subscriber<? super O> subscriber, Subscription source, I next)
        {
            try
            {
                O out = apply(next);
                if (out != null)
                    subscriber.onNext(out);
                else
                    source.request(1);
            }
            catch (Throwable t)
            {
                error(subscriber, source, t);
            }
        }
    }

    /**
     * Like Flowable.map, but permits mapper to return null, which is treated as an intention to skip the item.
     */
    public static <I, O> Flowable<O> skippingMap(Flowable<I> source, SkippingOp<I, O> mapper)
    {
        return source.lift(mapper);
    }

    public static <I, O> FlowableOperator<O, I> skippingMap(SkippingOp<I, O> mapper)
    {
        return mapper;
    }

    // Wrapper allowing simple transformations to extend all the way to FlowableOperator without extra objects being
    // created.
    public interface StoppingOp<I, O> extends Function<I, O>, FlowableOp<I, O>
    {
        default void onNext(Subscriber<? super O> subscriber, Subscription source, I next)
        {
            try
            {
                O out = apply(next);
                if (out != null)
                    subscriber.onNext(out);
                else
                    complete(subscriber, source);
            }
            catch (Throwable t)
            {
                error(subscriber, source, t);
            }
        }
    }

    /**
     * Like Flowable.map, but permits mapper to return null, which is treated as indication that the stream should stop.
     */
    public static <I, O> Flowable<O> stoppingMap(Flowable<I> source, StoppingOp<I, O> mapper)
    {
        return source.lift(mapper);
    }

    public static <I, O> FlowableOperator<O, I> stoppingMap(StoppingOp<I, O> mapper)
    {
        return mapper;
    }

    /**
     * Operator for grouping elements of a flowable stream. Used with {@link #group(Flowable, GroupOp)} below.
     * <p>
     * Stream is broken up in selections of consecutive elements where {@link #inSameGroup} returns true, passing each
     * collection through {@link #map(List)}.
     */
    public interface GroupOp<I, O> extends FlowableOperator<O, I>
    {
        /**
         * Should return true if l and r are to be grouped together.
         */
        boolean inSameGroup(I l, I r);

        /**
         * Transform the group. May return null, meaning skip.
         */
        O map(List<I> inputs);

        /**
         * Implementation of FlowableOperator to enable direct usage through Flowable.lift().
         */
        default Subscriber<I> apply(Subscriber<? super O> sub)
        {
            return new GroupOpSubscription<I, O>(sub, this);
        }
    }

    private static class GroupOpSubscription<I, O> implements Subscriber<I>, Subscription
    {
        final Subscriber<? super O> subscriber;
        final GroupOp<I, O> mapper;
        Subscription source;
        boolean completed;
        I first;
        List<I> entries;

        public GroupOpSubscription(Subscriber<? super O> subscriber, GroupOp<I, O> mapper)
        {
            this.subscriber = subscriber;
            this.mapper = mapper;
        }

        public void onSubscribe(Subscription subscription)
        {
            source = subscription;
            subscriber.onSubscribe(this);
            if (completed)
                source.cancel();
        }

        public void onNext(I entry)
        {
            boolean needsRequest = true;

            if (first == null || !mapper.inSameGroup(first, entry))
            {
                // Issue previous result
                if (first != null)
                    needsRequest = drain();

                entries = new ArrayList<>();    // create a new collection rather than empty because mapper may hold on to it
                first = entry;
            }

            entries.add(entry);
            if (needsRequest)
                source.request(1);
        }

        /**
         * Send all ready data and return true if a request for new entry needs to be made.
         */
        private boolean drain()
        {
            O out = mapper.map(entries);
            if (out == null)
                return true;

            subscriber.onNext(out);
            return false;
        }

        public void onError(Throwable throwable)
        {
            subscriber.onError(throwable);
        }

        public void onComplete()
        {
            completed = true;
            if (first != null)
                drain();
            subscriber.onComplete();
        }

        public void request(long l)
        {
            if (!completed)
                source.request(l);
        }

        public void cancel()
        {
            if (source != null)
                source.cancel();
            completed = true;
        }
    }

    static public <I, O> Flowable<O> group(Flowable<I> source, GroupOp<I, O> groupMapping)
    {
        return source.lift(groupMapping);
    }

    public static <I, O> FlowableOperator<O, I> group(GroupOp<I, O> mapper)
    {
        return mapper;
    }

    /**
     * Flowable.concat is somewhat eager, in the sense that it makes prefetch requests for the next elements from the
     * iterator before the downstream subscriber has asked for them. This doesn't work well for us (we only want to
     * read if we have to).
     * <p>
     * The method below is a lazy counterpart, only asks for data when it has been requested (and one item at a time).
     */
    public static <T> FlowableOperator<T, Flowable<T>> concatLazy()
    {
        return FlowableConcatLazy.getDirect();
    }

    /**
     * Flowable.concat is somewhat eager, in the sense that it makes prefetch requests for the next elements from the
     * iterator before the downstream subscriber has asked for them. This doesn't work well for us (we only want to
     * read if we have to).
     * <p>
     * The method below is a lazy counterpart, only asks for data when it has been requested (and one item at a time).
     */
    public static <I, O> FlowableOperator<O, I> concatMapLazy(Function<I, Flowable<O>> mapper)
    {
        return new FlowableConcatLazy<I, O>(mapper);
    }
}
