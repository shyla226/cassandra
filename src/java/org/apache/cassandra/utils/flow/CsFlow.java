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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.Uninterruptibles;

import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.Throwables;

/**
 * An asynchronous flow of items modelled similarly to Java 9's Flow and RxJava's Flowable with some simplifications.
 */
public abstract class CsFlow<T>
{
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

    public static final boolean DEBUG_ENABLED = true;//System.getProperty("cassandra.debugcsflow", "false").equalsIgnoreCase("true");

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
        return apply(this, op);
    }

    public static <I, O> CsFlow<O> apply(CsFlow<I> source, FlowableOp<I, O> op)
    {
        class ApplyFlow extends CsFlow<O>
        {
            public CsSubscription subscribe(CsSubscriber<O> subscriber) throws Exception
            {
                return !DEBUG_ENABLED
                       ? new ApplyOpSubscription<>(subscriber, op, source)
                       : new CheckedApplyOpSubscription<>(subscriber, op, source);
            }
        }
        return new ApplyFlow();
    }

    /**
     * Apply implementation.
     */
    private static class ApplyOpSubscription<I, O>
    implements CsSubscription, CsSubscriber<I>
    {
        final CsSubscriber<O> subscriber;
        final FlowableOp<I, O> mapper;
        final CsSubscription source;

        public ApplyOpSubscription(CsSubscriber<O> subscriber, FlowableOp<I, O> mapper, CsFlow<I> source) throws Exception
        {
            this.subscriber = subscriber;
            this.mapper = mapper;
            this.source = source.subscribe(this);
        }

        public void onNext(I item)
        {
            mapper.onNext(subscriber, source, item);
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
                mapper.close();
            }
        }

        public String toString()
        {
            return "apply(" + mapper.toString() + ")\n\tsubscriber " + subscriber;
        }
    }

    /**
     * Apply implementation that checks the subscriptions are used correctly (e.g. without concurrent requests).
     */
    private static class CheckedApplyOpSubscription<I, O>
    implements CsSubscription, CsSubscriber<I>
    {
        final CsSubscriber<O> subscriber;
        final FlowableOp<I, O> mapper;
        final CsSubscription source;

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

        public CheckedApplyOpSubscription(CsSubscriber<O> subscriber, FlowableOp<I, O> mapper, CsFlow<I> source) throws Exception
        {
            this.subscriber = subscriber;
            this.mapper = mapper;
            this.source = source.subscribe(this);
            verifyStateTransition(State.INITIALIZING, State.READY);
        }

        public void onNext(I item)
        {
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
                source.request();
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

        @Override
        public void finalize()
        {
            if (state != State.CLOSED)
                System.err.println("Unclosed flow in state " + state + "\n\t" + this);
        }

        public String toString()
        {
            return "apply(" + mapper.toString() + ")\n\tsubscriber " + subscriber;
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
        return apply(mapper);
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
     * Take only the items from from the flow until the tester fails for the first time.
     */
    public CsFlow<T> takeWhile(TakeWhileOp<T> tester)
    {
        return apply(tester);
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
        return apply(tester);
    }

    /**
     * Op for applying an operation on close.
     *
     * This wrapper allows lambdas to extend to FlowableOp without extra objects being created.
     */
    public interface OnCloseOp<T> extends FlowableOp<T, T>, Runnable
    {
        default void onNext(CsSubscriber<T> subscriber, CsSubscription source, T next)
        {
            subscriber.onNext(next);
        }

        default void close() throws Exception
        {
            run();
        }
    }

    /**
     * Apply the operation when the flow is closed.
     */
    public CsFlow<T> doOnClose(OnCloseOp onClose)
    {
        return apply(onClose);
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
    public <O> CsFlow<O> flatMap(Function<T, CsFlow<O>> op)
    {
        return FlatMap.flatMap(this, op);
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
    }

    /**
     * Implementation of the reduce operation, used with small variations in {@link #reduceBlocking(Object, BiFunction)},
     * {@link #reduceWith(Supplier, BiFunction)} and {@link #reduceToFuture(Object, BiFunction)}.
     */
    abstract static private class ReduceSubscriber<T, O> implements CsSubscriber<T>, CsSubscription
    {
        final CsSubscription sub;
        final BiFunction<O, T, O> reducer;
        O current;
        private StackTraceElement[] stackTrace;

        enum State {
            OUT_OF_LOOP,
            IN_LOOP_REQUESTED,
            IN_LOOP_READY,
        }
        AtomicReference<State> state = new AtomicReference<>(State.OUT_OF_LOOP);

        ReduceSubscriber(O seed, CsFlow<T> source, BiFunction<O, T, O> reducer) throws Exception
        {
            this.reducer = reducer;
            sub = source.subscribe(this);
            current = seed;
            stackTrace = maybeGetStackTrace();
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

            if (!state.compareAndSet(State.OUT_OF_LOOP, State.IN_LOOP_READY))
                return;
            requestLoop();
        }

        private void requestLoop()
        {
            while (true)
            {
                // The loop can only be entered in IN_LOOP_READY
                if (!verifyStateChange(State.IN_LOOP_READY, State.IN_LOOP_REQUESTED))
                    return;

                sub.request();

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

            if (state.compareAndSet(State.IN_LOOP_REQUESTED, State.IN_LOOP_READY))
                return;

            // We must be out of the loop if the above failed; re-start looping.
            if (verifyStateChange(State.OUT_OF_LOOP, State.IN_LOOP_READY))
                requestLoop();
        }

        public void close() throws Exception
        {
            sub.close();
        }

        public String toString()
        {
            return "reduce(" + reducer + ")" + stackTraceString(stackTrace);
        }

        // onComplete and onError are to be implemented by the concrete use-case.
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
    public <O> O reduceBlocking(O seed, BiFunction<O, T, O> reducer) throws Exception
    {
        CountDownLatch latch = new CountDownLatch(1);

        class ReduceBlockingSubscription extends ReduceSubscriber<T, O>
        {
            Throwable error = null;

            ReduceBlockingSubscription(CsFlow<T> source, BiFunction<O, T, O> reducer) throws Exception
            {
                super(seed, source, reducer);
            }

            public void onComplete()
            {
                latch.countDown();
            }

            public void onError(Throwable t)
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
     * @throws Exception
     */
    public <O> CompletableFuture<O> reduceToFuture(O seed, BiFunction<O, T, O> reducer)
    {
        CompletableFuture<O> result = new CompletableFuture<>();

        class ReduceToFutureSubscription extends ReduceSubscriber<T, O>
        {
            ReduceToFutureSubscription(O seed, CsFlow<T> source, BiFunction<O, T, O> reducer) throws Exception
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

            public void onError(Throwable t)
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
     * Reduce the flow, blocking until the operation completes.
     *
     * Note: the reduced should not hold resources that need to be released, as the content will be lost on error.
     *
     * @param seedSupplier Supplier for the initial value for the reduction.
     * @param reducer Called repeatedly with the reduced value (starting with seed and continuing with the result
     *          returned by the previous call) and the next item.
     * @return The final reduced value.
     * @throws Exception
     */
    public <O> CsFlow<O> reduceWith(Supplier<O> seedSupplier, BiFunction<O, T, O> reducer)
    {
        CsFlow<T> self = this;
        return new CsFlow<O>()
        {
            public CsSubscription subscribe(CsSubscriber<O> subscriber) throws Exception
            {
                return new ReduceSubscriber<T, O>(seedSupplier.get(), self, reducer)
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

                    public void onError(Throwable t)
                    {
                        subscriber.onError(t);
                    }
                };
            }
        };
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
                return flowSupplier.apply(resource)
                                   .doOnClose(() -> resourceDisposer.accept(resource))
                                   .subscribe(subscriber);
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
        return reduceBlocking(new AtomicLong(0),
                              (count, value) ->
                              {
                                  count.incrementAndGet();
                                  return count;
                              }).get();
    }

    public static StackTraceElement[] maybeGetStackTrace()
    {
        if (CsFlow.DEBUG_ENABLED)
            return Thread.currentThread().getStackTrace();
        return null;
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

}
