package org.apache.cassandra.utils;

import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;

import io.reactivex.Flowable;
import io.reactivex.FlowableOperator;
import io.reactivex.functions.Function;
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

    /**
     * Flowable.concat is somewhat eager, in the sense that it makes prefetch requests for the next elements from the
     * iterator before the downstream subscriber has asked for them. This doesn't work well for us (we only want to
     * read if we have to).
     *
     * The method below is a lazy counterpart, only asks for data when it has been requested (and one item at a time).
     */
    public static <T> Flowable<T> concatLazy(Iterable<Flowable<T>> sources)
    {
        return new LazyConcat<>(sources);
    }

    private static class LazyConcat<T> extends Flowable<T>
    {
        final Iterable<Flowable<T>> sources;

        public LazyConcat(Iterable<Flowable<T>> sources)
        {
            this.sources = sources;
        }

        protected void subscribeActual(Subscriber<? super T> subscriber)
        {
            LazyConcatSubscription<T> subscription = new LazyConcatSubscription<>(sources.iterator(), subscriber);
            subscriber.onSubscribe(subscription);
        }
    }


    private static class LazyConcatSubscription<T> implements Subscription, Subscriber<T>
    {
        final Iterator<Flowable<T>> iterator;
        final Subscriber<? super T> subscriber;
        volatile long requests = 0;  // requests received from subscriber
        volatile long requested = 0; // requests passed on to sources
        volatile boolean requesting;
        Subscription currentSubscription;

        enum State
        {
            NO_SUBSCRIPTION(null, null, null),
            READY(null, NO_SUBSCRIPTION, null),
            SUBSCRIBING(null, null, READY),
            REQUESTING(READY, NO_SUBSCRIPTION, null);

            final State onNext;
            final State onComplete;
            final State onSubscribe;

            State(State onNext, State onComplete, State onSubscribe)
            {
                this.onNext = onNext;
                this.onComplete = onComplete;
                this.onSubscribe = onSubscribe;
            }
        }
        State state;

        public LazyConcatSubscription(Iterator<Flowable<T>> iterator, Subscriber<? super T> subscriber)
        {
            this.iterator = iterator;
            this.subscriber = subscriber;
            state = State.NO_SUBSCRIPTION;
        }

        void switchState(State newState, String op)
        {
            if (newState == null)
                subscriber.onError(new IllegalStateException(String.format("LazyConcat: invalid %s transition from %s", op, state)));
            state = newState;
        }

        public void onSubscribe(Subscription subscription)
        {
            currentSubscription = subscription;
            switchState(state.onSubscribe, "onSubscribe");
            doRequests();
        }

        public void onNext(T t)
        {
            subscriber.onNext(t);
            switchState(state.onNext, "onNext");
            doRequests();
        }

        public void onError(Throwable throwable)
        {
            subscriber.onError(throwable);
        }

        public void onComplete()
        {
            currentSubscription = null;
            switchState(state.onComplete, "onComplete");
            doRequests();
        }

        public void request(long l)
        {
            requests += l;
            doRequests();
        }

        // TODO: Remove synchronization:
        // -- need to make sure we can't leave loop after onXXX/request call has rejected entering loop due to 'requesting'.
        // -- IN_LOOP variations on State could do the trick, so that we CAS the right one.
        private synchronized void doRequests()
        {
            if (requesting || requested == requests)
                return;

            requesting = true;

            loop:
            while (requested < requests)
            {
                switch (state)
                {
                case NO_SUBSCRIPTION:
                {
                    if (!iterator.hasNext())
                    {
                        subscriber.onComplete();
                        break loop;
                    }
                    state = State.SUBSCRIBING;
                    Flowable<T> next = iterator.next();
                    next.subscribe(this);
                    break;
                }
                case READY:
                    state = State.REQUESTING;
                    ++requested;
                    currentSubscription.request(1);
                    break;
                default:
                    // We are awaiting response. Leave loop, we will be called from onXXX
                    break loop;
                }
            }
            requesting = false;
        }

        public void cancel()
        {
            requests = 0;
            if (currentSubscription != null)
                currentSubscription.cancel();
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
        AtomicIntegerFieldUpdater closedUpdater = AtomicIntegerFieldUpdater.newUpdater(OnceCloseable.class, "closed");

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
}
