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

package org.apache.cassandra.utils.flow;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.Runnables;
import com.google.common.util.concurrent.Uninterruptibles;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.CompletableSource;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.LineNumberInference;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.Throwables;

/**
 * An asynchronous flow of items modelled similarly to Java 9's Flow and RxJava's Flowable with some simplifications.
 */
public abstract class CsFlow<T>
{
    public final static LineNumberInference LINE_NUMBERS = new LineNumberInference();

    /**
     * Create a subscription linking the content of the flow with the given subscriber.
     * The subscriber is expected to call request() on the returned subscription; in response, it will receive an
     * onNext(item), onComplete(), or onError(throwable). To get further items, the subscriber must call request()
     * again _after_ receiving the onNext (usually before returning from the call).
     * When done with the content (regardless of whether onComplete or onError was received), the subscriber must
     * close the subscription.
     */
    abstract public CsSubscription subscribe(CsSubscriber<T> subscriber) throws Exception;

    // Flow manipulation methods and implementations follow

    public static final boolean DEBUG_ENABLED = Boolean.getBoolean("cassandra.debugcsflow");

    /**
     * Interface for abstracting away the gory details of operations with flowables. Used with {@link #apply}.
     */
    public interface FlowableOp<I, O>
    {
        /**
         * Called on next item. Normally calls subscriber.onNext, but could instead finish the stream by calling
         * complete, throw an error using error, or decide to ignore input and request another.
         * For some usage ideas, see {@link SkippingOp} and {@link StoppingOp} below.
         */
        abstract void onNext(CsSubscriber<O> subscriber, CsSubscription source, I next);

        /**
         * Called on close.
         */
        default void close() throws Exception
        {
        }
    }

    public <O> CsFlow<O> apply(FlowableOp<T, O> op)
    {
        return apply(this, "apply", op);
    }

    public <O> CsFlow<O> apply(FlowableOp<T, O> op, String prefix)
    {
        return apply(this, prefix, op);
    }

    public static <I, O> CsFlow<O> apply(CsFlow<I> source, String prefix, FlowableOp<I, O> op)
    {
        class ApplyFlow extends CsFlow<O>
        {
            public CsSubscription subscribe(CsSubscriber<O> subscriber) throws Exception
            {
                return !DEBUG_ENABLED
                       ? new ApplyOpSubscription<>(prefix, subscriber, op, source)
                       : new CheckedApplyOpSubscription<>(prefix, subscriber, op, source);
            }
        }
        return new ApplyFlow();
    }

    /**
     * Apply implementation.
     */
    private static class ApplyOpSubscription<I, O> extends RequestLoop implements CsSubscription, CsSubscriber<I>
    {
        final CsSubscriber<O> subscriber;
        final CsSubscription source;
        final FlowableOp<I, O> mapper;
        final String prefix;

        public ApplyOpSubscription(String prefix, CsSubscriber<O> subscriber, FlowableOp<I, O> mapper, CsFlow<I> source) throws Exception
        {
            this.subscriber = subscriber;
            this.mapper = mapper;
            this.prefix = prefix;
            this.source = source.subscribe(this);
        }

        public void onNext(I item)
        {
            mapper.onNext(subscriber, this, item);
        }

        public void onError(Throwable throwable)
        {
            subscriber.onError(throwable);
        }

        public void onComplete()
        {
            subscriber.onComplete();
        }

        public void request()
        {
            // Note: This is less efficient than optimal. We'd prefer to only do request loops for additional
            // requests, not for ones made by our subscriber.
            requestInLoop(source);
        }

        public void close() throws Exception
        {
            try
            {
                source.close();
            }
            finally
            {
                mapper.close();
            }
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            return source.addSubscriberChainFromSource(throwable);
        }

        public String toString()
        {
            return formatTrace(prefix, mapper, subscriber);
        }
    }

    /**
     * Apply implementation that checks the subscriptions are used correctly (e.g. without concurrent requests).
     */
    private static class CheckedApplyOpSubscription<I, O> extends DebugRequestLoop implements CsSubscription, CsSubscriber<I>
    {
        final CsSubscriber<O> subscriber;
        final FlowableOp<I, O> mapper;
        final CsSubscription source;
        final String prefix;

        enum State
        {
            INITIALIZING,
            READY,
            REQUESTED,
            COMPLETED,
            CLOSED;
        }

        private volatile State state = State.INITIALIZING;
        private static final AtomicReferenceFieldUpdater<CheckedApplyOpSubscription, State> stateUpdater =
            AtomicReferenceFieldUpdater.newUpdater(CheckedApplyOpSubscription.class, State.class, "state");

        boolean verifyStateTransition(Throwable existingError, State from, State to)
        {
            if (!stateUpdater.compareAndSet(this, from, to))
            {
                subscriber.onError(Throwables.merge(existingError,
                        new AssertionError(String.format("Unexpected state %s, required %s to transition to %s\n\t%s",
                                                         state,
                                                         from,
                                                         to,
                                                         this))));
                return false;
            }
            return true;
        }

        void verifyStateTransition(State from, State to) throws Exception
        {
            if (!stateUpdater.compareAndSet(this, from, to))
                throw new AssertionError(String.format("Unexpected state %s, required %s to transition to %s\n\t%s",
                                                       state,
                                                       from,
                                                       to,
                                                       this));
        }

        public CheckedApplyOpSubscription(String prefix, CsSubscriber<O> subscriber, FlowableOp<I, O> mapper, CsFlow<I> source) throws Exception
        {
            super(subscriber);
            this.subscriber = subscriber;
            this.mapper = mapper;
            this.source = source.subscribe(this);
            this.prefix = prefix;
            verifyStateTransition(State.INITIALIZING, State.READY);
        }

        public void onNext(I item)
        {
            // TODO: if the mapper skips too many items it may cause a stack overflow,
            // see Flow/OpsTest. The solution is to use the RequestLoop but we need to
            // integrate the debug states
            if (verifyStateTransition(null, State.REQUESTED, State.READY))
                mapper.onNext(subscriber, this, item);

        }

        public void onError(Throwable throwable)
        {
            if (verifyStateTransition(throwable, State.REQUESTED, State.COMPLETED))
                subscriber.onError(throwable);
        }

        public void onComplete()
        {
            if (verifyStateTransition(null, State.REQUESTED, State.COMPLETED))
                subscriber.onComplete();
        }

        public void request()
        {
            if (verifyStateTransition(null, State.READY, State.REQUESTED))
                requestInLoop(source);
        }

        public void close() throws Exception
        {
            try
            {
                State s = state;
                assert s == State.READY || s == State.COMPLETED : "Unexpected state " + s + " at close.\n\t" + this;
                verifyStateTransition(s, State.CLOSED);
                source.close();
            }
            finally
            {
                mapper.close();
            }
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            return source.addSubscriberChainFromSource(throwable);
        }

        @Override
        public void finalize()
        {
            if (state != State.CLOSED)
                System.err.println("Unclosed flow in state " + state + "\n\t" + this);
        }

        public String toString()
        {
            return formatTrace(prefix, mapper, subscriber);
        }
    }

    /**
     * Op for element-wise transformation.
     *
     * This wrapper allows lambdas to extend to FlowableOp without extra objects being created.
     */
    public interface MappingOp<I, O> extends Function<I, O>, FlowableOp<I, O>
    {
        default void onNext(CsSubscriber<O> subscriber, CsSubscription source, I next)
        {
            try
            {
                O out = apply(next);
                // Calls below should not fail.
                subscriber.onNext(out);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
            }
        }
    }

    /**
     * Tranforms each item in the flow using the specified mapper.
     */
    public <O> CsFlow<O> map(MappingOp<T, O> mapper)
    {
        return apply(mapper, "map");
    }


    /**
     * Op for element-wise transformation which return null, treated as an indication to skip the item.
     *
     * This wrapper allows lambdas to extend to FlowableOp without extra objects being created.
     */
    public interface SkippingOp<I, O> extends Function<I, O>, FlowableOp<I, O>
    {
        default void onNext(CsSubscriber<O> subscriber, CsSubscription source, I next)
        {
            try
            {
                O out = apply(next);
                // Calls below should not fail.
                if (out != null)
                    subscriber.onNext(out);
                else
                    source.request();
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
            }
        }
    }

    /**
     * Like map, but permits mapper to return null, which is treated as an intention to skip the item.
     */
    public <O> CsFlow<O> skippingMap(SkippingOp<T, O> mapper)
    {
        return apply(mapper);
    }

    /**
     * Op for element-wise transformation which return null, treated as an indication to stop the flow.
     *
     * This wrapper allows lambdas to extend to FlowableOp without extra objects being created.
     */
    public interface StoppingOp<I, O> extends Function<I, O>, FlowableOp<I, O>
    {
        default void onNext(CsSubscriber<O> subscriber, CsSubscription source, I next)
        {
            try
            {
                O out = apply(next);
                // Calls below should not fail.
                if (out != null)
                    subscriber.onNext(out);
                else
                    subscriber.onComplete();
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
            }
        }
    }

    /**
     * Like map, but permits mapper to return null, which is treated as indication that the flow should stop.
     */
    public <O> CsFlow<O> stoppingMap(StoppingOp<T, O> mapper)
    {
        return apply(mapper);
    }

    /**
     * Op for stopping the flow once a predicate returns false.
     *
     * This wrapper allows lambdas to extend to FlowableOp without extra objects being created.
     */
    public interface TakeWhileOp<I> extends Predicate<I>, FlowableOp<I, I>
    {
        default void onNext(CsSubscriber<I> subscriber, CsSubscription source, I next)
        {
            try
            {
                // Calls below should not fail.
                if (test(next))
                    subscriber.onNext(next);
                else
                    subscriber.onComplete();
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
            }
        }
    }

    /**
     * Take only the items from from the flow until the tester fails for the first time, not including that item.
     *
     * This applies on the onNext phase of execution, after the item has been produced. If the item holds resources,
     * tester must release these resources as the item will not be passed on downstream.
     */
    public CsFlow<T> takeWhile(TakeWhileOp<T> tester)
    {
        return apply(tester);
    }

    /**
     * Interface for operators that apply to the subscription chain, modifying request() and close().
     */
    public interface SubscriptionLifter<T>
    {
        CsSubscription apply(CsSubscription source, CsSubscriber<T> subscriber);
    }

    /**
     * Applies the given operator to the subscription chain of the given flow, permitting modifications to request()
     * and subscribe().
     *
     * Note: This does not insert any corresponding operation on the subscriber chain, passing the previous subscriber
     * directly to the upstream generator. As a result this operator will not be shown in the subscriber's chain.
     */
    public CsFlow<T> liftSubscription(SubscriptionLifter<T> lifter)
    {
        return liftSubscription(this, lifter);
    }

    public static <T> CsFlow<T> liftSubscription(CsFlow<T> source, SubscriptionLifter<T> lifter)
    {
        return new CsFlow<T>()
        {
            public CsSubscription subscribe(CsSubscriber<T> subscriber) throws Exception
            {
                return lifter.apply(source.subscribe(subscriber), subscriber);
            }
        };
    }

    /**
     * Stops requesting items from the flow when the given predicate succeeds.
     *
     * Unlike takeWhile, this applies on the request phase of execution, before anything has been produced (and as such
     * cannot be given the next item in the flow as argument).
     */
    public CsFlow<T> takeUntil(BooleanSupplier tester)
    {
        return takeUntilAndDoOnClose(tester, Runnables.doNothing());
    }

    /**
     * Apply the operation when the flow is closed.
     */
    public CsFlow<T> doOnClose(Runnable onClose)
    {
        return takeUntilAndDoOnClose(() -> false, onClose);
    }


    /**
     * Combination of takeUntil and doOnClose using a single subscription object.
     *
     * Stops requesting items when the supplied tester returns true, and executes the runnable when the flow is closed.
     */
    public CsFlow<T> takeUntilAndDoOnClose(BooleanSupplier tester, Runnable onClose)
    {
        return liftSubscription((source, subscriber) -> new CsSubscription()
        {
            public void request()
            {
                boolean stop;
                try
                {
                    stop = tester.getAsBoolean();
                }
                catch (Throwable t)
                {
                    subscriber.onError(t);
                    return;
                }

                if (stop)
                    subscriber.onComplete();
                else
                    source.request();
            }

            public void close() throws Exception
            {
                try
                {
                    source.close();
                }
                finally
                {
                    onClose.run();
                }
            }

            public Throwable addSubscriberChainFromSource(Throwable throwable)
            {
                return source.addSubscriberChainFromSource(throwable);
            }
        });
    }

    /**
     * Op for filtering out items.
     *
     * This wrapper allows lambdas to extend to FlowableOp without extra objects being created.
     */
    public interface FilterOp<I> extends Predicate<I>, FlowableOp<I, I>
    {
        default void onNext(CsSubscriber<I> subscriber, CsSubscription source, I next)
        {
            try
            {
                // Calls below should not fail.
                if (test(next))
                    subscriber.onNext(next);
                else
                    source.request();
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
            }
        }
    }

    /**
     * Take only the items from from the flow that pass the supplied tester.
     */
    public CsFlow<T> filter(FilterOp<T> tester)
    {
        return apply(tester, "filter");
    }

    /**
     * Group items using the supplied group op. See {@link GroupOp}
     */
    public <O> CsFlow<O> group(GroupOp<T, O> op)
    {
        return GroupOp.group(this, op);
    }

    /**
     * Pass all elements through the given transformation, then concatenate the results together.
     */
    public <O> CsFlow<O> flatMap(FlatMap.FlatMapper<T, O> op)
    {
        return FlatMap.flatMap(this, op);
    }

    /**
     * Map each element of the flow into CompletableSources, subscribe to them and
     * wait until the upstream and all CompletableSources complete.
     *
     * @param mapper the function that receives each source value and transforms them into CompletableSources.
     *
     * @return the new Completable instance
     * */
    public Completable flatMapCompletable(Function<? super T, ? extends CompletableSource> mapper)
    {
        return FlatMapCompletable.flatMap(this, mapper);
    }

    /**
     * Converts and iterable to a flow. If the iterator is closeable, also closes it with the subscription.
     *
     * onNext() is called synchronously with the next item from the iterator for each request().
     */
    public static <O> CsFlow<O> fromIterable(Iterable<O> o)
    {
        class IterableToFlow extends CsFlow<O>
        {
            public CsSubscription subscribe(CsSubscriber<O> subscriber)
            {
                return new IteratorSubscription(subscriber, o.iterator());
            }
        }
        return new IterableToFlow();
    }

    static class IteratorSubscription<O> implements CsSubscription
    {
        final Iterator<O> iter;
        final CsSubscriber<O> subscriber;

        IteratorSubscription(CsSubscriber<O> subscriber, Iterator<O> iter)
        {
            this.iter = iter;
            this.subscriber = subscriber;
        }

        public void request()
        {
            try
            {
                if (!iter.hasNext())
                    subscriber.onComplete();
                else
                    subscriber.onNext(iter.next());
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
            }

        }

        public void close() throws Exception
        {
            if (iter instanceof AutoCloseable)
                ((AutoCloseable) iter).close();
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            return wrapException(throwable, this);
        }

        @Override
        public String toString()
        {
            return formatTrace("iteratorSubscription", subscriber);
        }
    }

    /**
     * Helper class for implementing requests looping to avoid stack overflows when an operation needs to request
     * many items from its source. In such cases requests have to be performed in a loop to avoid growing the stack with
     * a full request... -> onNext... -> chain for each new requested element, which can easily cause stack overflow
     * if that is not guaranteed to happen only a small number of times for each item generated by the operation.
     *
     * The main idea is that if a request was issued in response to onNext which an ongoing request triggered (and thus
     * control will return to the request loop after the onNext and request chains return), mark it and process
     * it when control returns to the loop.
     *
     * Note that this does not have to be used for _all_ requests served by an operation. It suffices to apply the loop
     * only for requests that the operation itself makes. By convention the downstream subscriber is also issuing
     * requests to the operation in a loop.
     *
     * This is implemented in two versions, RequestLoop and DebugRequestLoop, where the former assumes everything is
     * working correctly while the latter verifies that there is no unexpected behaviour.
     */
    static class RequestLoop
    {
        enum RequestLoopState
        {
            OUT_OF_LOOP,
            IN_LOOP_READY,
            IN_LOOP_REQUESTED
        }

        volatile RequestLoopState state = RequestLoopState.OUT_OF_LOOP;
        static final AtomicReferenceFieldUpdater<RequestLoop, RequestLoopState> stateUpdater =
            AtomicReferenceFieldUpdater.newUpdater(RequestLoop.class, RequestLoopState.class, "state");

        public void requestInLoop(CsSubscription source)
        {
            // See DebugRequestLoop below for a more precise description of the state transitions on which this is built.

            if (stateUpdater.compareAndSet(this, RequestLoopState.IN_LOOP_READY, RequestLoopState.IN_LOOP_REQUESTED))
                // Another call (concurrent or in the call chain) has the loop and we successfully told it to continue.
                return;

            // If the above failed, we must be OUT_OF_LOOP (possibly just concurrently transitioned out of it).
            // Since there can be no other concurrent access, we can grab the loop now.
            do
            {
                state = RequestLoopState.IN_LOOP_READY;
                source.request();
                // If we didn't get another request, leave.
            }
            while (!stateUpdater.compareAndSet(this, RequestLoopState.IN_LOOP_READY, RequestLoopState.OUT_OF_LOOP));
        }
    }

    static class DebugRequestLoop extends RequestLoop
    {
        final CsSubscriber<?> errorRecepient;

        public DebugRequestLoop(CsSubscriber<?> errorRecepient)
        {
            super();
            this.errorRecepient = errorRecepient;
        }

        @Override
        public void requestInLoop(CsSubscription source)
        {
            if (stateUpdater.compareAndSet(this, RequestLoopState.IN_LOOP_READY, RequestLoopState.IN_LOOP_REQUESTED))
                // Another call (concurrent or in the call chain) has the loop and we successfully told it to continue.
                return;

            // If the above failed, we must be OUT_OF_LOOP (possibly just concurrently transitioned out of it).
            // Since there can be no other concurrent access, we can grab the loop now.
            if (!verifyStateChange(RequestLoopState.OUT_OF_LOOP, RequestLoopState.IN_LOOP_REQUESTED))
                return;

            // We got the loop.
            while (true)
            {
                // The loop can only be entered with a pending request; from here, make our state as having issued a
                // request and ready to receive another before performing the request.
                verifyStateChange(RequestLoopState.IN_LOOP_REQUESTED, RequestLoopState.IN_LOOP_READY);

                source.request();

                // If we didn't get another request, leave.
                if (stateUpdater.compareAndSet(this, RequestLoopState.IN_LOOP_READY, RequestLoopState.OUT_OF_LOOP))
                    return;
            }

        }

        private boolean verifyStateChange(RequestLoopState from, RequestLoopState to)
        {
            RequestLoopState prev = stateUpdater.getAndSet(this, to);
            if (prev == from)
                return true;

            errorRecepient.onError(new AssertionError("Invalid state " + prev));
            return false;
        }

    }

    /**
     * Implementation of the reduce operation, used with small variations in {@link #reduceBlocking(Object, ReduceFunction)},
     * {@link #reduce(Object, ReduceFunction)} and {@link #reduceToFuture(Object, ReduceFunction)}.
     */
    abstract static private class ReduceSubscriber<T, O> extends RequestLoop implements CsSubscriber<T>, CsSubscription
    {
        final BiFunction<O, T, O> reducer;
        O current;
        private StackTraceElement[] stackTrace;
        private CsSubscription source;

        ReduceSubscriber(O seed, CsFlow<T> source, BiFunction<O, T, O> reducer) throws Exception
        {
            this.reducer = reducer;
            current = seed;
            this.source = source.subscribe(this);
            this.stackTrace = maybeGetStackTrace();
        }

        public void onNext(T item)
        {
            try
            {
                current = reducer.apply(current, item);
            }
            catch (Throwable t)
            {
                onError(t);
                return;
            }

            request();
        }

        public void request()
        {
            requestInLoop(source);
        }

        public void close() throws Exception
        {
            source.close();
        }

        public String toString()
        {
            return formatTrace("reduce", reducer) + "\n" + stackTraceString(stackTrace);
        }

        @Override
        public Throwable addSubscriberChainFromSource(Throwable t)
        {
            return source.addSubscriberChainFromSource(t);
        }

        public final void onError(Throwable t)
        {
            onErrorInternal(addSubscriberChainFromSource(t));
        }

        protected abstract void onErrorInternal(Throwable t);

        // onComplete and onErrorInternal are to be implemented by the concrete use-case.
    }

    public interface ReduceFunction<ACC, I> extends BiFunction<ACC, I, ACC>
    {
    }

    /**
     * Reduce the flow, blocking until the operation completes.
     *
     * @param seed Initial value for the reduction.
     * @param reducer Called repeatedly with the reduced value (starting with seed and continuing with the result
     *          returned by the previous call) and the next item.
     * @return The final reduced value.
     * @throws Exception
     */
    public <O> O reduceBlocking(O seed, ReduceFunction<O, T> reducer) throws Exception
    {
        CountDownLatch latch = new CountDownLatch(1);

        class ReduceBlockingSubscription extends ReduceSubscriber<T, O>
        {
            Throwable error = null;

            ReduceBlockingSubscription(CsFlow<T> source, ReduceFunction<O, T> reducer) throws Exception
            {
                super(seed, source, reducer);
            }

            public void onComplete()
            {
                latch.countDown();
            }

            public void onErrorInternal(Throwable t)
            {
                error = t;
                latch.countDown();
            }
        };

        ReduceBlockingSubscription s = new ReduceBlockingSubscription(this, reducer);
        s.request();
        Uninterruptibles.awaitUninterruptibly(latch);
        s.close();

        Throwables.maybeFail(s.error);
        return s.current;
    }

    /**
     * Reduce the flow, returning a completable future that is completed when the operations complete.
     *
     * @param seed Initial value for the reduction.
     * @param reducer Called repeatedly with the reduced value (starting with seed and continuing with the result
     *          returned by the previous call) and the next item.
     * @return The final reduced value.
     */
    public <O> CompletableFuture<O> reduceToFuture(O seed, ReduceFunction<O, T> reducer)
    {
        CompletableFuture<O> result = new CompletableFuture<>();

        class ReduceToFutureSubscription extends ReduceSubscriber<T, O>
        {
            ReduceToFutureSubscription(O seed, CsFlow<T> source, ReduceFunction<O, T> reducer) throws Exception
            {
                super(seed, source, reducer);
            }

            public void onComplete()
            {
                try
                {
                    close();
                }
                catch (Throwable t)
                {
                    result.completeExceptionally(t);
                    return;
                }

                result.complete(current);
            }

            protected void onErrorInternal(Throwable t)
            {
                try
                {
                    close();
                }
                catch (Throwable e)
                {
                    t = Throwables.merge(t, e);
                }

                result.completeExceptionally(t);
            }
        };

        ReduceToFutureSubscription s;
        try
        {
            s = new ReduceToFutureSubscription(seed, this, reducer);
        }
        catch (Exception e)
        {
            result.completeExceptionally(e);
            return result;
        }

        s.request();
        return result;
    }

    /**
     * An abstract subscriber used by the reduce operators for rx java. It implements the Disposable
     * interface of rx java, and stops requesting once disposed.
     */
    private static abstract class DisposableReduceSubscriber<T, O> extends ReduceSubscriber<T, O> implements Disposable
    {
        private volatile boolean isDisposed;
        private volatile int isClosed;
        private static AtomicIntegerFieldUpdater<DisposableReduceSubscriber> isClosedUpdater =
                AtomicIntegerFieldUpdater.newUpdater(DisposableReduceSubscriber.class, "isClosed");

        DisposableReduceSubscriber(O seed, CsFlow<T> source, ReduceFunction<O, T> reducer) throws Exception
        {
            super(seed, source, reducer);
            isDisposed = false;
            isClosed = 0;
        }

        public void request()
        {
            if (!isDisposed())
                super.request();
            else
            {
                try
                {
                    close();
                }
                catch (Throwable t)
                {
                    onError(t);
                }
            }
        }

        public void dispose()
        {
            isDisposed = true;
        }

        public boolean isDisposed()
        {
            return isDisposed;
        }

        @Override
        public void onComplete()
        {
            try
            {
                close();
            }
            catch (Throwable t)
            {
                signalError(addSubscriberChainFromSource(t));
                return;
            }
            signalSuccess(current);
        }

        @Override
        public void onErrorInternal(Throwable t)
        {
            try
            {
                close();
            }
            catch (Throwable e)
            {
                t = Throwables.merge(t, e);
            }

            signalError(t);
        }

        @Override
        public void close() throws Exception
        {
            // We may get onComplete/onError after disposal. Make sure we don't double-close in that case.
            if (!isClosedUpdater.compareAndSet(this, 0, 1))
                return;

            assert isClosed == 1;
            super.close();
        }

        abstract void signalError(Throwable t);
        abstract void signalSuccess(O value);

        // onNext/onComplete/onError may arrive after disposal. It's not really possible to prevent consumer
        // receiving these messages after disposal (as it could be paused e.g. at the entrance point of the method), so
        // we aren't really worried about that.
        // Importantly, if onComplete arrives after disposal, it is not because it was triggered by the disposal.
    }


    /**
     * Reduce the flow, returning a completable future that is completed when the operations complete.
     *
     * @param seed Initial value for the reduction.
     * @param reducerToSingle Called repeatedly with the reduced value (starting with seed and continuing with the result
     *          returned by the previous call) and the next item.
     * @return The final reduced value.
     */
    public <O> Single<O> reduceToRxSingle(O seed, ReduceFunction<O, T> reducerToSingle)
    {
        class SingleFromCsFlow extends Single<O>
        {
            protected void subscribeActual(SingleObserver<? super O> observer)
            {
                class ReduceToSingle extends DisposableReduceSubscriber<T, O>
                {
                    ReduceToSingle() throws Exception
                    {
                        super(seed, CsFlow.this, reducerToSingle);
                    }

                    @Override
                    public void signalSuccess(O value)
                    {
                        observer.onSuccess(value);
                    }

                    @Override
                    public void signalError(Throwable t)
                    {
                        observer.onError(t);
                    }
                };

                ReduceToSingle s;
                try
                {
                    s = new ReduceToSingle();
                }
                catch (Exception e)
                {
                    observer.onError(e);
                    return;
                }

                observer.onSubscribe(s);
                s.request();
            }
        }

        return new SingleFromCsFlow();
    }

    public interface RxSingleMapper<I, O> extends Function<I, O>, ReduceFunction<O, I>
    {
        @Override
        default O apply(O prev, I curr) throws Exception
        {
            assert prev == null;
            return apply(curr);
        }
    }

    /**
     * Maps a CsFlow holding a single value into an Rx Single using the supplied mapper.
     */
    public <O> Single<O> mapToRxSingle(RxSingleMapper<T, O> mapper)
    {
        return reduceToRxSingle(null, mapper);
    }

    // Not fully tested -- this is not meant for long-term use
    public Completable processToRxCompletable(ConsumingOp<T> consumer)
    {
        class CompletableFromCsFlow extends Completable
        {
            protected void subscribeActual(CompletableObserver observer)
            {
                class CompletableFromCsFlowSubscriber extends DisposableReduceSubscriber<T, Void>
                {
                    public CompletableFromCsFlowSubscriber() throws Exception
                    {
                        super(null, CsFlow.this, consumer);
                    }

                    @Override
                    public void signalSuccess(Void value)
                    {
                        observer.onComplete();
                    }

                    @Override
                    public void signalError(Throwable t)
                    {
                        observer.onError(t);
                    }
                }

                CompletableFromCsFlowSubscriber cs;
                try
                {
                    cs = new CompletableFromCsFlowSubscriber();
                }
                catch (Throwable t)
                {
                    observer.onError(t);
                    return;
                }
                observer.onSubscribe(cs);
                cs.request();
            }
        }
        return new CompletableFromCsFlow();
    }

    public Completable processToRxCompletable()
    {
        return processToRxCompletable(v -> {});
    }

    /**
     * Reduce the flow and return a CsFlow containing the result.
     *
     * Note: the reduced should not hold resources that need to be released, as the content will be lost on error.
     *
     * @param seed The initial value for the reduction.
     * @param reducer Called repeatedly with the reduced value (starting with seed and continuing with the result
     *          returned by the previous call) and the next item.
     * @return The final reduced value.
     */
    public <O> CsFlow<O> reduce(O seed, ReduceFunction<O, T> reducer)
    {
        CsFlow<T> self = this;
        return new CsFlow<O>()
        {
            public CsSubscription subscribe(CsSubscriber<O> subscriber) throws Exception
            {
                return new ReduceSubscriber<T, O>(seed, self, reducer)
                {
                    volatile boolean completed = false;

                    public void request()
                    {
                        if (completed)
                            subscriber.onComplete();
                        else
                            super.request();
                    }

                    public void onComplete()
                    {
                        completed = true;
                        subscriber.onNext(current); // This should request; if it does we will give it onComplete immediately.
                    }

                    public void onErrorInternal(Throwable t)
                    {
                        subscriber.onError(t);
                    }

                    @Override
                    public String toString()
                    {
                        return formatTrace("reduceWith", reducer);
                    }
                };
            }
        };
    }

    public <I, O, F> CsFlow<F> flatReduce(O seed, BiFunction<O, T, CsFlow<I>> mapper, ReduceFunction<O, I> reducer, MappingOp<O, F> onComplete)
    {
        return this.flatMap(x -> mapper.apply(seed, x))
                   .reduce(seed,
                           reducer)
                   .map(onComplete);
    }

    public interface ConsumingOp<T> extends ReduceFunction<Void, T>
    {
        void accept(T item) throws Exception;

        default Void apply(Void v, T item) throws Exception
        {
            accept(item);
            return v;
        }
    }

    static final ConsumingOp<Object> NO_OP_CONSUMER = (v) -> {};
    static <T> ConsumingOp<T> noOp()
    {
        return (ConsumingOp<T>) NO_OP_CONSUMER;
    }

    public CsFlow<Void> process(ConsumingOp<T> consumer)
    {
        return reduce(null,
                      consumer);
    }

    public CsFlow<Void> process()
    {
        return process(noOp());
    }

    public <I, O> CsFlow<Void> flatProcess(FlatMap.FlatMapper<T, Void> mapper)
    {
        return this.flatMap(mapper)
                   .process();
    }

    /**
     * If the contents are empty, replaces them with the given singleton value.
     */
    public CsFlow<T> ifEmpty(T value)
    {
        return ifEmpty(this, value);
    }

    public static <T> CsFlow<T> ifEmpty(CsFlow<T> source, T value)
    {
        class IfEmptyFlow extends CsFlow<T>
        {
            public CsSubscription subscribe(CsSubscriber<T> subscriber) throws Exception
            {
                return new IfEmptySubscription<>(subscriber, value, source);
            }
        }
        return new IfEmptyFlow();
    }

    static class IfEmptySubscription<T> implements CsSubscription, CsSubscriber<T>
    {
        final CsSubscription source;
        final CsSubscriber<T> subscriber;
        final T value;
        boolean hadItem;
        boolean completed;

        IfEmptySubscription(CsSubscriber<T> subscriber, T value, CsFlow<T> source) throws Exception
        {
            this.subscriber = subscriber;
            this.value = value;
            this.source = source.subscribe(this);
        }

        public void request()
        {
            if (!completed)
                source.request();
            else
                subscriber.onComplete();
        }

        public void close() throws Exception
        {
            source.close();
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            return source.addSubscriberChainFromSource(throwable);
        }

        public void onNext(T item)
        {
            hadItem = true;
            subscriber.onNext(item);
        }

        public void onComplete()
        {
            completed = true;
            if (hadItem)
                subscriber.onComplete();
            else
                subscriber.onNext(value);
        }

        public void onError(Throwable t)
        {
            subscriber.onError(t);
        }

        @Override
        public String toString()
        {
            return formatTrace("ifEmpty", subscriber);
        }
    }

    /**
     * Converts the flow to singleton flow of the list of items.
     */
    public CsFlow<List<T>> toList()
    {
        return group(new GroupOp<T, List<T>>(){
            public boolean inSameGroup(T l, T r)
            {
                return true;
            }

            public List<T> map(List<T> inputs)
            {
                return inputs;
            }
        }).ifEmpty(Collections.emptyList());
    }

    /**
     * Returns the singleton item in the flow, or throws if none/multiple are present. Blocks until operations complete.
     */
    public T blockingSingle()
    {
        try
        {
            T res = reduceBlocking(null, (prev, item) ->
            {
                assert (prev == null) : "Call to blockingSingle with more than one element: " + item;
                return item;
            });
            assert (res != null) : "Call to blockingSingle with empty flow.";
            return res;
        }
        catch (Exception e)
        {
            throw com.google.common.base.Throwables.propagate(e);
        }
    }

    /**
     * Returns the last item in the flow, or the given default if empty. Blocks until operations complete.
     */
    public T blockingLast(T def) throws Exception
    {
        return reduceBlocking(def, (prev, item) -> item);
    }

    /**
     * Runs the flow to completion and blocks until done.
     */
    public void executeBlocking() throws Exception
    {
        blockingLast(null);
    }

    static private CsFlow<?> EMPTY = new CsFlow<Object>()
    {
        public CsSubscription subscribe(CsSubscriber subscriber) throws Exception
        {
            return new CsSubscription()
            {

                public void request()
                {
                    subscriber.onComplete();
                }

                public void close() throws Exception
                {
                }

                public Throwable addSubscriberChainFromSource(Throwable throwable)
                {
                    return CsFlow.wrapException(throwable, this);
                }

                @Override
                public String toString()
                {
                    return formatTrace("empty", subscriber);
                }
            };
        }
    };

    /**
     * Returns an empty flow.
     */
    public static <T> CsFlow<T> empty()
    {
        return (CsFlow<T>) EMPTY;
    }

    /**
     * Returns a singleton flow with the given value.
     */
    public static <T> CsFlow<T> just(T value)
    {
        return new CsFlow<T>()
        {
            public CsSubscription subscribe(CsSubscriber<T> subscriber) throws Exception
            {
                return new SingleSubscription<>(subscriber, value);
            }
        };
    }

    static class SingleSubscription<T> implements CsSubscription
    {
        final CsSubscriber<T> subscriber;
        final T value;
        boolean supplied = false;

        SingleSubscription(CsSubscriber<T> subscriber, T value)
        {
            this.subscriber = subscriber;
            this.value = value;
        }

        public void request()
        {
            if (supplied)
                subscriber.onComplete();
            else
            {
                supplied = true;
                subscriber.onNext(value);
            }
        }

        public void close() throws Exception
        {
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            return throwable;
        }
    }

    /**
     * Performs an ordered merge of the given flows. See {@link Merge}
     */
    public static <I, O> CsFlow<O> merge(List<CsFlow<I>> flows,
                                         Comparator<? super I> comparator,
                                         Reducer<I, O> reducer)
    {
        return Merge.get(flows, comparator, reducer);
    }


    public interface Operator<I, O>
    {
        CsSubscription subscribe(CsFlow<I> source, CsSubscriber<O> subscriber) throws Exception;
    }

    /**
     * Used to apply Operators. See {@link Threads} for usages.
     */
    public <O> CsFlow<O> lift(Operator<T, O> operator)
    {
        CsFlow<T> self = this;
        return new CsFlow<O>()
        {
            public CsSubscription subscribe(CsSubscriber subscriber) throws Exception
            {
                return operator.subscribe(self, subscriber);
            }
        };
    }

    /**
     * Try-with-resources equivalent.
     *
     * The resource supplier is called on subscription, the flow is constructed, and the resource disposer is called
     * when the subscription is closed.
     */
    public static <T, R> CsFlow<T> using(Supplier<R> resourceSupplier, Function<R, CsFlow<T>> flowSupplier, Consumer<R> resourceDisposer)
    {
        return new CsFlow<T>()
        {
            public CsSubscription subscribe(CsSubscriber<T> subscriber) throws Exception
            {
                R resource = resourceSupplier.get();
                try
                {
                    return flowSupplier.apply(resource)
                                       .doOnClose(() -> resourceDisposer.accept(resource))
                                       .subscribe(subscriber);
                }
                catch (Throwable t)
                {
                    resourceDisposer.accept(resource);
                    throw com.google.common.base.Throwables.propagate(t);
                }
            }
        };
    }

    /**
     * Take no more than the first count items from the flow.
     */
    public CsFlow<T> take(long count)
    {
        AtomicLong cc = new AtomicLong(count);
        return stoppingMap(x -> cc.decrementAndGet() >= 0 ? x : null);
    }

    /**
     * Delays the execution of each onNext by the given time.
     * Used for testing.
     */
    public CsFlow<T> delayOnNext(long sleepFor, TimeUnit timeUnit)
    {
        return apply(((subscriber, source, next) ->
                      Schedulers.computation().scheduleDirect(() -> subscriber.onNext(next),
                                                              sleepFor,
                                                              timeUnit)));
    }

    /**
     * Count the items in the flow, blocking until operations complete.
     * Used for tests.
     */
    public long countBlocking() throws Exception
    {
        // Note: using AtomicLong to avoid boxing a long at every iteration.
        return reduceBlocking(new AtomicLong(0),
                              (count, value) ->
                              {
                                  count.incrementAndGet();
                                  return count;
                              }).get();
    }

    /**
     * Returns a flow containing:
     * - the source if it is not empty.
     * - nothing if it is empty.
     *
     * Note: Both resulting flows are single-use.
     */
    public CsFlow<CsFlow<T>> skipEmpty()
    {
        return SkipEmpty.skipEmpty(this);
    }

    /**
     * Returns a flow containing:
     * - the source passed through the supplied mapper if it is not empty.
     * - nothing if it is empty.
     *
     * Note: The flow passed to the mapper, as well as the returned result, are single-use.
     */
    public <U> CsFlow<U> skipMapEmpty(Function<CsFlow<T>, U> mapper)
    {
        return SkipEmpty.skipMapEmpty(this, mapper);
    }

    public static <T> CloseableIterator<T> toIterator(CsFlow<T> source) throws Exception
    {
        return new ToIteratorSubscription<T>(source);
    }

    static class ToIteratorSubscription<T> implements CloseableIterator<T>, CsSubscriber<T>
    {
        static final Object POISON_PILL = new Object();

        final CsSubscription subscription;
        BlockingQueue<Object> queue = new ArrayBlockingQueue<>(1);
        Throwable error = null;
        Object next = null;

        public ToIteratorSubscription(CsFlow<T> source) throws Exception
        {
            subscription = source.subscribe(this);
        }

        public void close()
        {
            try
            {
                subscription.close();
            }
            catch (Exception e)
            {
                throw com.google.common.base.Throwables.propagate(e);
            }
        }

        @Override
        public void onComplete()
        {
            Uninterruptibles.putUninterruptibly(queue, POISON_PILL);
        }

        @Override
        public void onError(Throwable arg0)
        {
            error = org.apache.cassandra.utils.Throwables.merge(error, arg0);
            Uninterruptibles.putUninterruptibly(queue, POISON_PILL);
        }

        @Override
        public void onNext(T arg0)
        {
            Uninterruptibles.putUninterruptibly(queue, arg0);
        }

        protected Object computeNext()
        {
            if (next != null)
                return next;

            assert queue.isEmpty();
            subscription.request();

            next = Uninterruptibles.takeUninterruptibly(queue);
            if (error != null)
                throw com.google.common.base.Throwables.propagate(error);
            return next;
        }

        @Override
        public boolean hasNext()
        {
            return computeNext() != POISON_PILL;
        }

        public String toString()
        {
            return formatTrace("toIterator");
        }

        @Override
        public T next()
        {
            boolean has = hasNext();
            assert has;

            @SuppressWarnings("resource")
            T toReturn = (T) next;
            next = null;
            return toReturn;
        }
    }

    public static StackTraceElement[] maybeGetStackTrace()
    {
        if (CsFlow.DEBUG_ENABLED)
            return Thread.currentThread().getStackTrace();
        return null;
    }

    public static Throwable wrapException(Throwable throwable, Object tag)
    {
        // Avoid re-wrapping an exception if it was already added
        for (Throwable t : throwable.getSuppressed())
        {
            if (t instanceof CsFlowException)
                return throwable;
        }

        // Load lambdas before calling `toString` on the object
        LINE_NUMBERS.preloadLambdas();
        throwable.addSuppressed(new CsFlowException(tag.toString()));
        return throwable;
    }

    private static class CsFlowException extends RuntimeException
    {
        private CsFlowException(Object tag)
        {
            super("CsFlow call chain:\n" + tag.toString());
        }
    }

    public static String stackTraceString(StackTraceElement[] stackTrace)
    {
        if (stackTrace == null)
            return "";

        return " created during\n\t   " + Stream.of(stackTrace)
                                                .skip(4)
                                                .map(Object::toString)
                                                .collect(Collectors.joining("\n\tat "))
               + "\n";
    }
    public static String withLineNumber(Object obj)
    {
        LINE_NUMBERS.maybeProcessClass(obj.getClass());
        Pair<String, Integer> lineNumber = LINE_NUMBERS.getLine(obj.getClass());
        return obj + "(" + lineNumber.left + ":" + lineNumber.right + ")";
    }

    public static String formatTrace(String prefix)
    {
        return String.format("\t%-20s", prefix);
    }

    public static String formatTrace(String prefix, Object tag)
    {
        return String.format("\t%-20s%s", prefix, CsFlow.withLineNumber(tag));
    }

    public static String formatTrace(String prefix, Object tag, CsSubscriber subscriber)
    {
        return String.format("\t%-20s%s\n%s", prefix, CsFlow.withLineNumber(tag), subscriber);
    }

    public static String formatTrace(String prefix, CsSubscriber subscriber)
    {
        return String.format("\t%-20s\n%s", prefix, subscriber);
    }
}
