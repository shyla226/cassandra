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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.CompletableSource;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.BooleanSupplier;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.LineNumberInference;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.Throwables;

/**
 * An asynchronous flow of items modelled similarly to Java 9's Flow and RxJava's Flowable with some simplifications.
 */
public abstract class Flow<T>
{
    private static final Logger logger = LoggerFactory.getLogger(Flow.class);

    public final static LineNumberInference LINE_NUMBERS = new LineNumberInference();

    /**
     * Create a subscription linking the content of the flow with the given subscriber, and request the first item
     * from it.
     *
     * In response, the given subscription recipient's onSubscribe method will be called, supplying the data source,
     * and after that the subscriber will receive an onNext(item), onComplete(), or onError(throwable).
     * To get further items, the subscriber must call requestNext() on the returned subscription.
     *
     * When done with the content (regardless of whether onComplete or onError was received), the subscriber must
     * close the subscription. Closing cannot be done concurrently with any requests.
     *
     * For efficiency, subscription is usually done immediately after a transformation is applied on this flow. Since
     * the resulting flow may remain unused, it is not guaranteed that any open subscription will be requested
     * or closed. However, if the subscriber does perform at least one request, they must close the subscription.
     */
    abstract public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient);

    /**
     * Shorthand for the typical case where the subscriber receives the subscription.
     * @param subscriber
     */
    public void requestFirst(FlowSubscriber<T> subscriber)
    {
        requestFirst(subscriber, subscriber);
    }

    /**
     * This is typically overridden to output a flow chain for debugging.
     *
     * Normally calls one of the formatTrace methods. If this is a transformation with source, it should pass the
     * sourceFlow as argument.
     */
    @Override
    public String toString()
    {
        return formatTrace(getClass().getSimpleName());
    }

    // Flow manipulation methods and implementations follow

    private static final boolean DEBUG_ENABLED = Boolean.getBoolean("dse.debug_flow");

    /**
     * Op for element-wise transformation.
     *
     * This wrapper allows lambdas to extend to FlowableOp without extra objects being created.
     */
    public static class Map<I, O> extends FlowTransformNext<I, O>
    {
        final Function<I, O> mapper;

        public Map(Flow<I> source, Function<I, O> mapper)
        {
            super(source);
            this.mapper = mapper;
        }

        public void onNext(I next)
        {
            O out;
            try
            {
                out = mapper.apply(next);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            subscriber.onNext(out);
        }

        public void onFinal(I next)
        {
            O out;
            try
            {
                out = mapper.apply(next);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            subscriber.onFinal(out);
        }

        public String toString()
        {
            return formatTrace(getClass().getSimpleName(), mapper, sourceFlow);
        }
    }

    /**
     * Tranforms each item in the flow using the specified mapper.
     */
    public <O> Flow<O> map(Function<T, O> mapper)
    {
        return new Map(this, mapper);
    }

    /**
     * Op for element-wise transformation which return null, treated as an indication to skip the item.
     *
     * This wrapper allows lambdas to extend to FlowableOp without extra objects being created.
     */
    public static class SkippingMap<I, O> extends FlowTransformNext<I, O>
    {
        final Function<I, O> mapper;

        public SkippingMap(Flow<I> source, Function<I, O> mapper)
        {
            super(source);
            this.mapper = mapper;
        }

        public void onNext(I next)
        {
            O out;
            try
            {
                out = mapper.apply(next);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            if (out != null)
                subscriber.onNext(out);
            else
                requestInLoop(source);
        }

        public void onFinal(I next)
        {
            O out;
            try
            {
                out = mapper.apply(next);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            if (out != null)
                subscriber.onFinal(out);
            else
                subscriber.onComplete();
        }

        public String toString()
        {
            return formatTrace(getClass().getSimpleName(), mapper, sourceFlow);
        }
    }

    /**
     * Like map, but permits mapper to return null, which is treated as an intention to skip the item.
     */
    public <O> Flow<O> skippingMap(Function<T, O> mapper)
    {
        return new SkippingMap(this, mapper);
    }

    /**
     * Op for filtering out items.
     *
     * This wrapper allows lambdas to extend to FlowableOp without extra objects being created.
     */
    public static class Filter<I> extends FlowTransformNext<I, I>
    {
        final Predicate<I> tester;

        public Filter(Flow<I> source, Predicate<I> tester)
        {
            super(source);
            this.tester = tester;
        }

        public void onNext(I next)
        {
            boolean pass;
            try
            {
                pass = test(next);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            if (pass)
                subscriber.onNext(next);
            else
                requestInLoop(source);
        }

        public void onFinal(I next)
        {
            boolean pass;
            try
            {
                pass = test(next);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            if (pass)
                subscriber.onFinal(next);
            else
                subscriber.onComplete();
        }

        public boolean test(I next) throws Exception
        {
            return tester.test(next);
        }

        public String toString()
        {
            return formatTrace(getClass().getSimpleName(), tester, sourceFlow);
        }
    }

    /**
     * Take only the items from from the flow that pass the supplied tester.
     */
    public Flow<T> filter(Predicate<T> tester)
    {
        return new Filter<>(this, tester);
    }

    /**
     * Op for element-wise transformation which return null, treated as an indication to stop the flow.
     *
     * This wrapper allows lambdas to extend to FlowableOp without extra objects being created.
     */
    static class StoppingMap<I, O> extends FlowTransformNext<I, O>
    {
        final Function<I, O> mapper;

        public StoppingMap(Flow<I> source, Function<I, O> mapper)
        {
            super(source);
            this.mapper = mapper;
        }

        public void onNext(I next)
        {
            O out;
            try
            {
                out = mapper.apply(next);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            if (out != null)
                subscriber.onNext(out);
            else
                subscriber.onComplete();
        }

        public void onFinal(I next)
        {
            O out;
            try
            {
                out = mapper.apply(next);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            if (out != null)
                subscriber.onFinal(out);
            else
                subscriber.onComplete();
        }

        public String toString()
        {
            return formatTrace(getClass().getSimpleName(), mapper, sourceFlow);
        }
    }

    /**
     * Like map, but permits mapper to return null, which is treated as indication that the flow should stop.
     */
    public <O> Flow<O> stoppingMap(Function<T, O> mapper)
    {
        return new StoppingMap<>(this, mapper);
    }

    /**
     * Op for stopping the flow once a predicate returns false.
     *
     * This wrapper allows lambdas to extend to FlowableOp without extra objects being created.
     */
    static class TakeWhile<I> extends FlowTransformNext<I, I>
    {
        final Predicate<I> tester;

        public TakeWhile(Flow<I> source, Predicate<I> tester)
        {
            super(source);
            this.tester = tester;
        }

        public void onNext(I next)
        {
            boolean pass;
            try
            {
                pass = tester.test(next);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            if (pass)
                subscriber.onNext(next);
            else
                subscriber.onComplete();
        }

        public void onFinal(I next)
        {
            boolean pass;
            try
            {
                pass = tester.test(next);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            if (pass)
                subscriber.onFinal(next);
            else
                subscriber.onComplete();
        }

        public String toString()
        {
            return formatTrace(getClass().getSimpleName(), tester, sourceFlow);
        }
    }

    /**
     * Take only the items from the flow until the tester fails for the first time, not including that item.
     *
     * This applies on the onNext phase of execution, after the item has been produced. If the item holds resources,
     * tester must release these resources as the item will not be passed on downstream.
     */
    public Flow<T> takeWhile(Predicate<T> tester)
    {
        return new TakeWhile<>(this, tester);
    }

    static class TakeUntil<T> extends FlowSource<T> implements FlowSubscriptionRecipient
    {
        final Flow<T> sourceFlow;
        final BooleanSupplier tester;
        FlowSubscription source;

        TakeUntil(Flow<T> sourceFlow, BooleanSupplier tester)
        {
            this.sourceFlow = sourceFlow;
            this.tester = tester;
        }

        public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
        {
            subscribe(subscriber, subscriptionRecipient);

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
            {
                subscriber.onComplete();
                return;
            }

            // we are not modifying the subscriber path, let upstream pass directly to our subscriber
            // No need to do this in loop, because we can't intercept the onNext path
            sourceFlow.requestFirst(subscriber, this);
        }

        public void onSubscribe(FlowSubscription source)
        {
            this.source = source;
        }

        public void requestNext()
        {
            assert source != null : "requestNext without onSubscribe";

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
                source.requestNext();
        }

        public void close() throws Exception
        {
            if (source != null)
                source.close();
        }

        public String toString()
        {
            return Flow.formatTrace(getClass().getSimpleName(), tester, sourceFlow);
        }
    };


    /**
     * Stops requesting items from the flow when the given predicate succeeds.
     *
     * Unlike takeWhile, this applies on the request phase of execution, before anything has been produced (and as such
     * cannot be given the next item in the flow as argument).
     */
    public Flow<T> takeUntil(BooleanSupplier tester)
    {
        return new TakeUntil(this, tester);
    }

    /**
     * Apply the operation when the flow is closed.
     */
    public Flow<T> doOnClose(Action onClose)
    {
        Flow<T> sourceFlow = this;

        class DoOnClose extends FlowSource<T> implements FlowSubscriptionRecipient
        {
            FlowSubscription source;

            public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
            {
                subscribe(subscriber, subscriptionRecipient);

                // we are not modifying the subscriber path, let upstream pass directly to our subscriber
                // No need to do this in loop, because we can't intercept the onNext path
                sourceFlow.requestFirst(subscriber, this);
            }

            public void onSubscribe(FlowSubscription source)
            {
                this.source = source;
            }

            public void requestNext()
            {
                source.requestNext();
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

            public String toString()
            {
                return Flow.formatTrace(getClass().getSimpleName(), onClose, sourceFlow);
            }
        };

        return new DoOnClose();
    }

    /**
     * Combination of takeUntil and doOnClose using a single subscription object.
     *
     * Stops requesting items when the supplied tester returns true, and executes the runnable when the flow is closed.
     */
    public Flow<T> takeUntilAndDoOnClose(BooleanSupplier tester, Action onClose)
    {
        return new TakeUntil(this, tester)
        {
            public void close() throws Exception
            {
                try
                {
                    if (source != null)
                        source.close();
                }
                finally
                {
                    onClose.run();
                }
            }
        };
    }

    /**
     * Apply the operation when the flow errors out. If handler throws, exception will be added as suppressed.
     */
    public Flow<T> doOnError(Consumer<Throwable> onError)
    {
        class DoOnError extends FlowTransformNext<T, T>
        {
            protected DoOnError(Flow<T> source)
            {
                super(source);
            }

            public void onNext(T item)
            {
                subscriber.onNext(item);
            }

            public void onFinal(T item)
            {
                subscriber.onFinal(item);
            }

            public void onError(Throwable t)
            {
                try
                {
                    onError.accept(t);
                }
                catch (Throwable mt)
                {
                    t.addSuppressed(mt);
                }

                subscriber.onError(t);
            }

            public String toString()
            {
                return Flow.formatTrace(getClass().getSimpleName(), onError, sourceFlow);
            }
        }

        return new DoOnError(this);
    }

    /**
     * Apply the operation when the flow completes. To avoid having to split onFinal calls, handler is not allowed to throw.
     */
    public Flow<T> doOnComplete(Runnable onComplete)
    {
        class DoOnComplete extends FlowTransformNext<T, T>
        {
            DoOnComplete(Flow<T> source)
            {
                super(source);
            }

            public void onNext(T item)
            {
                subscriber.onNext(item);
            }

            public void onFinal(T item)
            {
                subscriber.onFinal(item);
                onComplete.run();
            }

            public void onComplete()
            {
                subscriber.onComplete();
                onComplete.run();
            }

            public String toString()
            {
                return Flow.formatTrace(getClass().getSimpleName(), onComplete, sourceFlow);
            }
        }

        return new DoOnComplete(this);
    }

    /**
     * Group items using the supplied group op. See {@link GroupOp}
     */
    public <O> Flow<O> group(GroupOp<T, O> op)
    {
        return GroupOp.group(this, op);
    }

    /**
     * Pass all elements through the given transformation, then concatenate the results together.
     */
    public <O> Flow<O> flatMap(Function<T, Flow<O>> op)
    {
        return FlatMap.flatMap(this, op);
    }

    public static <T> Flow<T> concat(Flow<Flow<T>> source)
    {
        return Concat.concat(source);
    }

    /**
     * Concatenate the input flows one after another and return a flow of items.
     * <p>
     * This is currently implemented with {@link #fromIterable} and {@link #flatMap},
     * to be optimized later.
     *
     * @param sources - an iterable of Flow<O></O>
     * @param <T> - the type of the flow items
     *
     * @return a concatenated flow of items
     */
    public static <T> Flow<T> concat(Iterable<Flow<T>> sources)
    {
        return Concat.concat(sources);
    }

    /**
     * Concatenate the input flows one after another and return a flow of items.
     *
     * @param o - an array of Flow<O></O>
     * @param <O> - the type of the flow items
     *
     * @return a concatenated flow of items
     */
    public static <O> Flow<O> concat(Flow<O>... o)
    {
        return Concat.concat(o);
    }

    /**
     * Concatenate this flow with any flow produced by the supplier. After this flow has completed,
     * query the supplier for the next flow and subscribe to it. Continue in this fashion until the
     * supplier returns null, at which point the entire flow is completed.
     *
     * @param supplier - a function that produces a new flow to be concatenated or null when finished
     * @return a flow that will complete when this and all the flows produced by the supplier have
     * completed.
     */
    public Flow<T> concatWith(Callable<Flow<T>> supplier)
    {
        return Concat.concatWith(this, supplier);
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
     * Return a flow that executes normally except that in case of error it fallbacks to the
     * flow provided by the next source supplier.
     *
     * @param nextSourceSupplier - a function that given an error returns a new flow.
     *
     * @return a flow that on error will subscribe to the flow provided by the next source supplier.
     */
    public Flow<T> onErrorResumeNext(Function<Throwable, Flow<T>> nextSourceSupplier)
    {
        class OnErrorResumeNext extends FlowTransform<T, T>
        {
            OnErrorResumeNext(Flow<T> source)
            {
                super(source);
            }

            public void onNext(T item)
            {
                subscriber.onNext(item);
            }

            public void onFinal(T item)
            {
                subscriber.onFinal(item);
            }

            @Override
            public String toString()
            {
                return formatTrace("onErrorResumeNext", nextSourceSupplier, sourceFlow);
            }

            public final void onError(Throwable t)
            {
                Throwable error = wrapException(t, this);

                FlowSubscription prevSubscription = source;
                try
                {
                    sourceFlow = nextSourceSupplier.apply(error);
                    sourceFlow.requestFirst(subscriber, this);
                }
                catch (Exception ex)
                {
                    subscriber.onError(Throwables.merge(t, ex));
                    // Note: close() will take care of closing the original subscription.
                    return;
                }

                try
                {
                    prevSubscription.close();
                }
                catch (Throwable ex)
                {
                    logger.debug("Failed to close previous subscription in onErrorResumeNext: {}/{}", ex.getClass(), ex.getMessage());
                }
            }
        }

        return new OnErrorResumeNext(this);
    }

    public Flow<T> mapError(Function<Throwable, Throwable> mapper)
    {
        class MapError extends FlowTransformNext<T, T>
        {
            protected MapError(Flow<T> source)
            {
                super(source);
            }

            public void onNext(T item)
            {
                subscriber.onNext(item);
            }

            public void onFinal(T item)
            {
                subscriber.onFinal(item);
            }

            public void onError(Throwable t)
            {
                try
                {
                    t = mapper.apply(t);
                }
                catch (Throwable mt)
                {
                    mt.addSuppressed(t);
                    t = mt;
                }

                subscriber.onError(t);
            }

            public String toString()
            {
                return Flow.formatTrace(getClass().getSimpleName(), mapper, sourceFlow);
            }
        }

        return new MapError(this);
    }

    /**
     * Converts and iterable to a flow. If the iterator is closeable, also closes it with the subscription.
     *
     * onNext() is called synchronously with the next item from the iterator for each request().
     */
    public static <O> Flow<O> fromIterable(Iterable<O> o)
    {
        return new IteratorSubscription(o.iterator());
    }

    static class IteratorSubscription<O> extends FlowSource<O>
    {
        final Iterator<O> iter;

        IteratorSubscription(Iterator<O> iter)
        {
            this.iter = iter;
        }

        public void requestNext()
        {
            boolean hasNext = false;
            O next = null;
            try
            {
                if (iter.hasNext())
                {
                    hasNext = true;
                    next = iter.next();
                }
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            if (!hasNext)
                subscriber.onComplete();
            else
                subscriber.onNext(next);
        }

        public void close() throws Exception
        {
            if (iter instanceof AutoCloseable)
                ((AutoCloseable) iter).close();
        }
    }

    enum RequestLoopState
    {
        OUT_OF_LOOP,
        IN_LOOP_READY,
        IN_LOOP_REQUESTED
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
     * This version is used in operations that are not Flow instances (e.g. reduce). Unfortunately we can't extends from
     * both Flow and RequestLoop, so the code is explicitly copied below for Flow operators.
     */
    public static class RequestLoop
    {
        volatile RequestLoopState state = RequestLoopState.OUT_OF_LOOP;
        static final AtomicReferenceFieldUpdater<RequestLoop, RequestLoopState> stateUpdater =
            AtomicReferenceFieldUpdater.newUpdater(RequestLoop.class, RequestLoopState.class, "state");

        public void requestInLoop(FlowSubscription source)
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
                source.requestNext();
                // If we didn't get another request, leave.
            }
            while (!stateUpdater.compareAndSet(this, RequestLoopState.IN_LOOP_READY, RequestLoopState.OUT_OF_LOOP));
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
     * This is implemented in two versions, RequestLoopFlow and DebugRequestLoopFlow, where the former assumes
     * everything is working correctly while the latter verifies that there is no unexpected behaviour.
     */
    public static abstract class RequestLoopFlow<T> extends Flow<T>
    {
        volatile RequestLoopState state = RequestLoopState.OUT_OF_LOOP;
        static final AtomicReferenceFieldUpdater<RequestLoopFlow, RequestLoopState> stateUpdater =
            AtomicReferenceFieldUpdater.newUpdater(RequestLoopFlow.class, RequestLoopState.class, "state");

        public void requestInLoop(FlowSubscription source)
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
                source.requestNext();
                // If we didn't get another request, leave.
            }
            while (!stateUpdater.compareAndSet(this, RequestLoopState.IN_LOOP_READY, RequestLoopState.OUT_OF_LOOP));
        }
    }

    static abstract class DebugRequestLoopFlow<T> extends RequestLoopFlow<T>
    {
        abstract FlowSubscriber<T> errorRecipient();

        @Override
        public void requestInLoop(FlowSubscription source)
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

                source.requestNext();

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

            errorRecipient().onError(new AssertionError("Invalid state " + prev));
            return false;
        }

    }

    /**
     * Implementation of the reduce operation, used with small variations in {@link #reduceBlocking(Object, ReduceFunction)},
     * {@link #reduce(Object, ReduceFunction)} and {@link #reduceToFuture(Object, ReduceFunction)}.
     */
    abstract static private class ReduceSubscriber<T, O> extends RequestLoop implements FlowSubscriber<T>, AutoCloseable
    {
        final BiFunction<O, T, O> reducer;
        final Flow<T> sourceFlow;
        FlowSubscription source;
        O current;
        private final StackTraceElement[] stackTrace;

        ReduceSubscriber(O seed, Flow<T> source, BiFunction<O, T, O> reducer)
        {
            this.reducer = reducer;
            this.sourceFlow = source;
            current = seed;
            this.stackTrace = maybeGetStackTrace();
        }

        public void start()
        {
            sourceFlow.requestFirst(this);
        }

        public void onSubscribe(FlowSubscription source)
        {
            this.source = source;
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

            requestNext();
        }

        public void onFinal(T item)
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

            onComplete();
        }

        public void requestNext()
        {
            requestInLoop(source);
        }

        public void close() throws Exception
        {
            source.close();
        }

        public String toString()
        {
            return formatTrace(getClass().getSimpleName(), reducer, sourceFlow) + "\n" + stackTraceString(stackTrace);
        }

        public final void onError(Throwable t)
        {
            onErrorInternal(wrapException(t, this));
        }

        protected abstract void onErrorInternal(Throwable t);

        // onComplete and onErrorInternal are to be implemented by the concrete use-case.
    }

    public interface ReduceFunction<ACC, I> extends BiFunction<ACC, I, ACC>
    {
    }

    /**
     * Reduce the flow, blocking until the operation completes.
     * Note: The reduced value must not hold resources, because it is lost if an exception occurs.
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

        class ReduceBlocking extends ReduceSubscriber<T, O>
        {
            Throwable error = null;

            ReduceBlocking(Flow<T> source, ReduceFunction<O, T> reducer)
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

        @SuppressWarnings("resource") // subscription is closed right away
        ReduceBlocking s = new ReduceBlocking(this, reducer);
        s.start();
        Uninterruptibles.awaitUninterruptibly(latch);
        Throwable error = s.error;
        try
        {
            s.close();
        }
        catch (Throwable t)
        {
            error = Throwables.merge(error, t);
        }

        Throwables.maybeFail(error);
        return s.current;
    }

    /**
     * Reduce the flow, returning a completable future that is completed when the operations complete.
     * Note: The reduced value must not hold resources, because it is lost if an exception occurs.
     *
     * @param seed Initial value for the reduction.
     * @param reducer Called repeatedly with the reduced value (starting with seed and continuing with the result
     *          returned by the previous call) and the next item.
     * @return The final reduced value.
     */
    @SuppressWarnings("resource") // subscription is closed on future completion
    public <O> CompletableFuture<O> reduceToFuture(O seed, ReduceFunction<O, T> reducer)
    {
        Future<O> future = new Future<>();
        class ReduceToFuture extends ReduceSubscriber<T, O>
        {
            ReduceToFuture(O seed, Flow<T> source, ReduceFunction<O, T> reducer)
            {
                super(seed, source, reducer);
            }

            @Override   // onNext is overridden to enable cancellation
            public void onNext(T item)
            {
                // We may be already cancelled, but we prefer to first pass current to the reducer who may know how to
                // release a resource.
                try
                {
                    current = reducer.apply(current, item);
                }
                catch (Throwable t)
                {
                    onError(t);
                    return;
                }

                if (future.isCancelled())
                {
                    try
                    {
                        close();
                    }
                    catch (Throwable t)
                    {
                        logger.error("Error closing flow after cancellation", t);
                    }
                    return;
                }

                requestInLoop(source);
            }

            public void onComplete()
            {
                try
                {
                    close();
                }
                catch (Throwable t)
                {
                    future.completeExceptionallyInternal(t);
                    return;
                }

                future.completeInternal(current);
            }

            protected void onErrorInternal(Throwable t)
            {
                try
                {
                    close();
                }
                catch (Throwable t2)
                {
                    t.addSuppressed(t2);
                }

                future.completeExceptionallyInternal(t);
            }
        }

        ReduceToFuture s = new ReduceToFuture(seed, this, reducer);
        s.start();

        return future;
    }

    /**
     * A future that is linked to the completion of an underlying flow.
     * <p>
     * This extends and behave like a {@link CompletableFuture}, with the exception that one cannot call the
     * {@link #complete(Object)} and {@link #completeExceptionally(Throwable)} (they throw {@link UnsupportedOperationException}):
     * those are called when the flow this is future on completes. It is however possible to cancel() this future, which
     * will stop the underlying flow at the next request.
     */
    public static class Future<T> extends CompletableFuture<T>
    {
        @Override
        public boolean complete(T t)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean completeExceptionally(Throwable throwable)
        {
            throw new UnsupportedOperationException();
        }

        private void completeInternal(T t)
        {
            super.complete(t);
        }

        private void completeExceptionallyInternal(Throwable throwable)
        {
            super.completeExceptionally(throwable);
        }
    }

    public Flow<T> last()
    {
        return reduce(null, (prev, next) -> next);
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

        DisposableReduceSubscriber(O seed, Flow<T> source, ReduceFunction<O, T> reducer)
        {
            super(seed, source, reducer);
            isDisposed = false;
            isClosed = 0;
        }

        public void requestNext()
        {
            if (!isDisposed())
                super.requestNext();
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
                signalError(wrapException(t, this));
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
     * Note: The reduced value must not hold resources, because it is lost if an exception occurs.
     *
     * @param seed Initial value for the reduction.
     * @param reducerToSingle Called repeatedly with the reduced value (starting with seed and continuing with the result
     *          returned by the previous call) and the next item.
     * @return The final reduced value.
     */
    public <O> io.reactivex.Single<O> reduceToRxSingle(O seed, ReduceFunction<O, T> reducerToSingle)
    {
        @SuppressWarnings("resource") // reduce subscriber is disposed by the Rx observer
        class SingleFromFlow extends io.reactivex.Single<O>
        {
            protected void subscribeActual(SingleObserver<? super O> observer)
            {
                class ReduceToSingle extends DisposableReduceSubscriber<T, O>
                {
                    ReduceToSingle()
                    {
                        super(seed, Flow.this, reducerToSingle);
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

                ReduceToSingle s = new ReduceToSingle();
                observer.onSubscribe(s);
                s.start();
            }
        }

        return new SingleFromFlow();
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
     * Maps a Flow holding a single value into an Rx Single using the supplied mapper.
     */
    public <O> io.reactivex.Single<O> mapToRxSingle(RxSingleMapper<T, O> mapper)
    {
        return reduceToRxSingle(null, mapper);
    }

    /**
     * Maps a Flow holding a single value into an Rx Single using the supplied mapper.
     */
    public io.reactivex.Single<T> mapToRxSingle()
    {
        return mapToRxSingle(x -> x);
    }

    // Not fully tested -- this is not meant for long-term use
    public Completable processToRxCompletable(ConsumingOp<T> consumer)
    {
        @SuppressWarnings("resource") // future is disposed by the Rx observer
        class CompletableFromFlow extends Completable
        {
            protected void subscribeActual(CompletableObserver observer)
            {
                class CompletableFromFlowSubscriber extends DisposableReduceSubscriber<T, Void>
                {
                    private CompletableFromFlowSubscriber()
                    {
                        super(null, Flow.this, consumer);
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

                CompletableFromFlowSubscriber cs = new CompletableFromFlowSubscriber();
                observer.onSubscribe(cs);
                cs.start();
            }
        }
        return new CompletableFromFlow();
    }

    public Completable processToRxCompletable()
    {
        return processToRxCompletable(v -> {});
    }

    /**
     * Subscribe to this flow and return a future on the completion of this flow.
     * <p>
     * Note that the elements of the flow itself are ignored by the underlying subscriber this method creates (which
     * usually suggests some operations with side-effects have been applied to the flow and we now only want to
     * subscribe and wait for completion).
     *
     * @return a future on the completion of this this flow.
     */
    public CompletableFuture<Void> processToFuture()
    {
        return reduceToFuture(null, (v, t) -> null);
    }

    /**
     * Reduce the flow and return a Flow containing the result.
     *
     * Note: the reduced should not hold resources that need to be released, as the content will be lost on error.
     *
     * @param seed The initial value for the reduction.
     * @param reducer Called repeatedly with the reduced value (starting with seed and continuing with the result
     *          returned by the previous call) and the next item.
     * @return The final reduced value.
     */
    public <O> Flow<O> reduce(O seed, ReduceFunction<O, T> reducer)
    {
        class Reduce extends FlowTransform<T, O>
        {
            O current = seed;

            public Reduce(Flow<T> source)
            {
                super(source);
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

                requestInLoop(source);
            }

            public void onFinal(T item)
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

                onComplete();
            }

            public void onComplete()
            {
                subscriber.onFinal(current);
            }

            public String toString()
            {
                return formatTrace(getClass().getSimpleName(), reducer, sourceFlow);
            }
        }
        return new Reduce(this);
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

    private static final ConsumingOp<Object> NO_OP_CONSUMER = (v) -> {};

    @SuppressWarnings("unchecked")
    static <T> ConsumingOp<T> noOp()
    {
        return (ConsumingOp<T>) NO_OP_CONSUMER;
    }

    public Flow<Void> process(ConsumingOp<T> consumer)
    {
        return reduce(null,
                      consumer);
    }

    public Flow<Void> process()
    {
        return process(noOp());
    }

    public <O> Flow<Void> flatProcess(Function<T, Flow<O>> mapper)
    {
        return this.flatMap(mapper)
                   .process();
    }

    /**
     * If the contents are empty, replaces them with the given singleton value.
     */
    public Flow<T> ifEmpty(T value)
    {
        return ifEmpty(this, value);
    }

    public static <T> Flow<T> ifEmpty(Flow<T> source, T value)
    {
        return new IfEmptyFlow<>(source, value);
    }

    static class IfEmptyFlow<T> extends FlowTransform<T, T>
    {
        final T value;
        boolean hadItem;
        boolean completed;

        IfEmptyFlow(Flow<T> source, T value)
        {
            super(source);
            this.value = value;
        }

        public void requestNext()
        {
            if (!completed)
                source.requestNext();
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

        public void onFinal(T item)
        {
            hadItem = true;
            subscriber.onFinal(item);
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

    static class ToList<T> extends FlowTransformNext<T, List<T>>
    {
        List<T> entries;

        public ToList(Flow<T> source)
        {
            super(source);
            entries = new ArrayList<>();
        }

        public void onNext(T entry)
        {
            entries.add(entry);
            requestInLoop(source);
        }

        public void onFinal(T entry)
        {
            entries.add(entry);
            onComplete();
        }

        public void onComplete()
        {
            subscriber.onFinal(entries);
        }
    }

    /**
     * Converts the flow to singleton flow of the list of items.
     */
    public Flow<List<T>> toList()
    {
        return new ToList<>(this);
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

    /**
     * Returns a singleton flow with the given value.
     */
    public static <T> Flow<T> just(T value)
    {
        return new Just<>(value);
    }

    static class Just<T> extends Flow<T>
    {
        final T value;

        Just(T value)
        {
            this.value = value;
        }

        public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
        {
            subscriptionRecipient.onSubscribe(FlowSubscription.DONE);
            subscriber.onFinal(value);
        }
    }

    /**
     * Returns an empty flow.
     */
    public static <T> Flow<T> empty()
    {
        return new EmptyFlow<>();
    }

    static class EmptyFlow<T> extends Flow<T>
    {
        public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
        {
            subscriptionRecipient.onSubscribe(FlowSubscription.DONE);
            subscriber.onComplete();
        }
    };

    /**
     * Returns a flow that simply emits an error as soon as the subscriber
     * requests the first item.
     *
     * @param t - the error to emit
     *
     * @return a flow that emits the error immediately on request
     */
    public static <T> Flow<T> error(Throwable t)
    {
        class ErrorFlow extends Flow<T>
        {
            public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
            {
                subscriptionRecipient.onSubscribe(FlowSubscription.DONE);
                subscriber.onError(t);
            }
        }

        return new ErrorFlow();
    }

    /**
     * Return a Flow that will subscribe to the completable once the first request is received and,
     * when the completable completes, it actually subscribes and passes on the first request to the source flow.
     *
     * @param completable - the completable to execute first
     * @param source - the flow that should execute once the completable has completed
     *
     * @param <T> - the type of the source items
     * @return a Flow that executes the completable, followed by the source flow
     */
    public static <T> Flow<T> concat(Completable completable, Flow<T> source)
    {
        class CompletableFlow extends Flow<T> implements CompletableObserver
        {
            private FlowSubscriber<T> subscriber;
            private FlowSubscriptionRecipient subscriptionRecipient;

            public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
            {
                this.subscriber = subscriber;
                this.subscriptionRecipient = subscriptionRecipient;
                completable.subscribe(this);
            }

            public void onSubscribe(Disposable disposable)
            {
                // We don't cancel
            }

            @Override
            public void onComplete()
            {
                source.requestFirst(subscriber, subscriptionRecipient);
            }

            @Override
            public void onError(Throwable t)
            {
                subscriptionRecipient.onSubscribe(FlowSubscription.DONE);
                subscriber.onError(t);
            }

            @Override
            public String toString()
            {
                return formatTrace("concatCompletableFlow", completable, source);
            }
        }
        return new CompletableFlow();
    }

    /**
     * Return a Flow that will subscribe to the source flow first and then it will delay
     * the final onComplete() by executing the completable first.
     *
     * @param completable - the completable to execute before the final onComplete of the source
     * @param source - the flow that should execute before the final completable
     *
     * @param <T> - the type of the source items
     * @return a Flow that executes the source flow, then the completable
     */
    public static <T> Flow<T> concat(Flow<T> source, Completable completable)
    {
        return new FlowTransform<T, T>(source)
        {
            boolean completeOnNextRequest = false;

            public void requestNext()
            {
                if (completeOnNextRequest)
                    onComplete();
                else
                    super.requestNext();
            }

            public void onNext(T item)
            {
                subscriber.onNext(item);
            }

            public void onFinal(T item)
            {
                // Split item from completion so that we don't start completable before processing last item.
                completeOnNextRequest = true;
                onNext(item);
            }

            public void onComplete()
            {
                completable.subscribe(() -> subscriber.onComplete(), error -> onError(error));
            }

            @Override
            public String toString()
            {
                return formatTrace("concatFlowCompletable", completable, sourceFlow);
            }
        };
    }

    /**
     * Performs an ordered merge of the given flows. See {@link Merge}
     */
    public static <I, O> Flow<O> merge(List<Flow<I>> flows,
                                       Comparator<? super I> comparator,
                                       Reducer<I, O> reducer)
    {
        return Merge.get(flows, comparator, reducer);
    }


    /**
     * Materializes a list of Flow<I> into a Flow with a single List of I
     */
    public static <I> Flow<List<I>> zipToList(List<Flow<I>> flows)
    {
        return Flow.merge(flows, Comparator.comparing((c) -> 0),
                          new Reducer<I, List<I>>()
                            {
                                List<I> list = new ArrayList<>(flows.size());

                                public void reduce(int idx, I current)
                                {
                                    list.add(current);
                                }

                                public List<I> getReduced()
                                {
                                    return list;
                                }
                            });
    }

    public interface Operator<I, O>
    {
        void requestFirst(Flow<I> source, FlowSubscriber<O> subscriber, FlowSubscriptionRecipient subscriptionRecipient);
    }

    /**
     * Used to apply Operators. See {@link Threads} for usages.
     */
    public <O> Flow<O> lift(Operator<T, O> operator)
    {
        Flow<T> self = this;
        return new Flow<O>()
        {
            public void requestFirst(FlowSubscriber<O> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
            {
                operator.requestFirst(self, subscriber, subscriptionRecipient);
            }
        };
    }

    /**
     * Try-with-resources equivalent.
     *
     * The resource supplier is called on first request, the flow is constructed, and the resource disposer is called
     * when the subscription is closed (which is now guaranteed).
     */
    public static <T, R> Flow<T> using(Callable<R> resourceSupplier, Function<R, Flow<T>> flowSupplier, Consumer<R> resourceDisposer)
    {
        class Using extends Flow<T> implements FlowSubscription, FlowSubscriptionRecipient
        {
            Flow<T> sourceFlow;
            FlowSubscription source;
            R resource;

            public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
            {
                subscriptionRecipient.onSubscribe(this);
                try
                {
                    resource = resourceSupplier.call();
                    assert resource != null : "Null resource is not allowed; use defer.";
                    sourceFlow = flowSupplier.apply(resource);
                }
                catch (Throwable t)
                {
                    subscriber.onError(t);
                    return;
                }

                sourceFlow.requestFirst(subscriber, this);
            }

            public void onSubscribe(FlowSubscription source)
            {
                this.source = source;
            }

            public void requestNext()
            {
                source.requestNext();
            }

            public void close() throws Exception
            {
                if (resource != null)
                {
                    try
                    {
                        resourceDisposer.accept(resource);
                    }
                    finally
                    {
                        source.close();
                    }
                }
            }

            public String toString()
            {
                return Flow.formatTrace("using", flowSupplier, sourceFlow);
            }
        }

        return new Using();
    }

    /**
     * Delays construction of the flow until first request, which ensures the flow will be closed.
     */
    public static <T> Flow<T> defer(Callable<Flow<T>> flowSupplier)
    {
        class Defer extends Flow<T>
        {
            Flow<T> sourceFlow;

            public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
            {
                try
                {
                    sourceFlow = flowSupplier.call();
                }
                catch (Throwable t)
                {
                    subscriptionRecipient.onSubscribe(FlowSubscription.DONE);
                    subscriber.onError(t);
                    return;
                }

                sourceFlow.requestFirst(subscriber, subscriptionRecipient);
            }

            public String toString()
            {
                return Flow.formatTrace("defer", flowSupplier, sourceFlow);
            }
        }

        return new Defer();
    }

    /**
     * Take no more than the first count items from the flow.
     */
    public Flow<T> take(long count)
    {
        AtomicLong cc = new AtomicLong(count);
        return takeUntil(() -> cc.decrementAndGet() < 0);
    }

    /**
     * Delays the execution of each onNext by the given time.
     * Used for testing.
     */
    public Flow<T> delayOnNext(long sleepFor, TimeUnit timeUnit, TPCTaskType taskType)
    {
        return new FlowTransformNext<T, T>(this)
        {
            public void onNext(T item)
            {
                TPC.bestTPCScheduler().scheduleDirect(() -> subscriber.onNext(item),
                                                      taskType,
                                                      sleepFor,
                                                      timeUnit);
            }

            public void onFinal(T item)
            {
                TPC.bestTPCScheduler().scheduleDirect(() -> subscriber.onFinal(item),
                                                      taskType,
                                                      sleepFor,
                                                      timeUnit);
            }
        };
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
    public Flow<Flow<T>> skipEmpty()
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
    public <U> Flow<U> skipMapEmpty(Function<Flow<T>, U> mapper)
    {
        return SkipEmpty.skipMapEmpty(this, mapper);
    }

    public static <T> CloseableIterator<T> toIterator(Flow<T> source) throws Exception
    {
        return new ToIteratorSubscriber<T>(source);
    }

    static class ToIteratorSubscriber<T> implements CloseableIterator<T>, FlowSubscriber<T>
    {
        static final Object POISON_PILL = new Object();

        FlowSubscription source;
        BlockingQueue<Object> queue = new ArrayBlockingQueue<>(1);
        // No need for the ones below to be volatile as a modification to the queue always follows their change
        boolean completeOnNextRequest = false;
        Throwable error = null;

        Object next = null;

        public ToIteratorSubscriber(Flow<T> source) throws Exception
        {
            source.requestFirst(this);
            awaitReply(); // await onNext so that we don't call requestNext concurrently
        }

        public void onSubscribe(FlowSubscription source)
        {
            this.source = source;
        }

        public void close()
        {
            try
            {
                source.close();
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

        @Override
        public void onFinal(T arg0)
        {
            // We can't just do a sequence of onNext and onComplete because a thread may catch the first and run through
            // computeNext before we've had a chance to do the latter.
            // Use a separate flag (that we can set before handing over control) instead. Note this can only be set
            // (in response to requestNext) after computeNext has checked it.
            completeOnNextRequest = true;
            Uninterruptibles.putUninterruptibly(queue, arg0);
        }

        protected Object computeNext()
        {
            if (next != null)
                return next;

            assert queue.isEmpty();
            if (completeOnNextRequest)
                return POISON_PILL;

            source.requestNext();
            return awaitReply();
        }

        private Object awaitReply()
        {
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

            @SuppressWarnings("unchecked")
            T toReturn = (T) next;
            next = null;
            return toReturn;
        }
    }

    /**
     * Simple interface for extracting the chilren of a tee operation.
     */
    public interface Tee<T>
    {
        public Flow<T> child(int index);
    }

    /**
     * Splits the flow into multiple resulting flows which can be independently operated and closed. The same
     * items, completions and errors are provided to all of the clients.
     * All resulting flows must be subscribed to and closed.
     *
     * The tee implementation (see {@link TeeImpl} executes by waiting for all clients to act before issuing the act
     * upstream. This means that the use of one client cannot be delayed until the completion of another (as this would
     * have required caching).
     *
     * The typical usage of this operation is to apply a "reduceToFuture" operation to all but one of the resulting
     * flows. This operation subscribes and initiates requests, but will not actually perform anything until the last
     * "controlling" flow subscribes and requests. When that happens onNext calls will be issued to all clients, and
     * any of them can delay (by not requesting) or withdraw from (by closing) futher processing.
     */
    public Tee<T> tee(int count)
    {
        return new TeeImpl<>(this, count);
    }

    /**
     * Splits the flow into two resulting flows which can be independently operated and closed. The same
     * items, completions and errors are provided to both of the clients.
     * Both resulting flows must be subscribed to and closed.
     *
     * The tee implementation (see {@link TeeImpl} executes by waiting for all clients to act before issuing the act
     * upstream. This means that the use of one client cannot be delayed until the completion of another (as this would
     * have required caching).
     *
     * The typical usage of this operation is to apply a "reduceToFuture" operation to all but one of the resulting
     * flows. This operation subscribes and initiates requests, but will not actually perform anything until the last
     * "controlling" flow subscribes and requests. When that happens onNext calls will be issued to all clients, and
     * any of them can delay (by not requesting) or withdraw from (by closing) futher processing.
     */
    public Tee<T> tee()
    {
        return new TeeImpl<>(this, 2);
    }

    public static StackTraceElement[] maybeGetStackTrace()
    {
        if (Flow.DEBUG_ENABLED)
            return Thread.currentThread().getStackTrace();
        return null;
    }

    public static Throwable wrapException(Throwable throwable, Object tag)
    {
        // for well known exceptions that can occur and that are handled downstream,
        // do not attach the subscriber chain as this would slow things down too much
        if (throwable instanceof NonWrappableException)
            return throwable;

        // Avoid re-wrapping an exception if it was already added
        for (Throwable t : throwable.getSuppressed())
        {
            if (t instanceof FlowException && ((FlowException) t).tag == tag)
                return throwable;
        }

        // Load lambdas before calling `toString` on the object
        LINE_NUMBERS.preloadLambdas();
        throwable.addSuppressed(new FlowException(tag));
        return throwable;
    }

    /**
     * An interface to signal to {@link #wrapException(Throwable, Object)}
     * not to wrap these exceptions with the caller stack trace since these
     * exceptions may occur regularly and therefore preloading lambdas would
     * be too slow.
     */
    public interface NonWrappableException
    {

    }

    private static class FlowException extends RuntimeException
    {
        Object tag;

        private FlowException(Object tag)
        {
            super("Flow call chain:\n" + tag.toString());
            this.tag = tag;
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
        LineNumberInference.Descriptor lineNumber = LINE_NUMBERS.getLine(obj.getClass());
        return obj + "(" + lineNumber.source() + ":" + lineNumber.line() + ")";
    }

    public static String formatTrace(String prefix)
    {
        return String.format("\t%-20s", prefix);
    }

    public static String formatTrace(String prefix, Object tag)
    {
        return String.format("\t%-20s%s", prefix, Flow.withLineNumber(tag));
    }

    public static String formatTrace(String prefix, Object tag, Flow<?> sourceFlow)
    {
        return String.format("%s\n\t%-20s%s", sourceFlow, prefix, Flow.withLineNumber(tag));
    }

    public static String formatTrace(String prefix, Flow<?> sourceFlow)
    {
        return String.format("%s\n\t%-20s", sourceFlow, prefix);
    }
}
