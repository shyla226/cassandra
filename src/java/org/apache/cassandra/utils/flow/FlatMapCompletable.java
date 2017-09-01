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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.CompletableSource;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Function;
import io.reactivex.internal.disposables.DisposableHelper;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;

/**
 * Implementation of {@link Flow#flatMapCompletable(Function)}, which applies a method to
 * each item in a flow, where that method will return an rx java completable. Each completable is
 * subscribed to, and no further item in the flow is requested until the current completable successfully
 * completes (depth-first).
 * <p>
 * In case of error, either in the child completables or in the parent flow, the downstream
 * is notified immediately and the operation stops.
 * <p>
 * The completable returned by the flatMap will actually be completed when the flow is consumed and all
 * inner completables have completed.
 */
class FlatMapCompletable<I> extends Flow.RequestLoop implements FlowSubscriber<I>, Disposable
{
    public static <I> Completable flatMap(Flow<I> source, Function<? super I, ? extends CompletableSource> mapper)
    {
        class FlatMapCompletableCompletable extends Completable
        {
            protected void subscribeActual(CompletableObserver observer)
            {
                try
                {
                    FlatMapCompletable<I> subscriber = new FlatMapCompletable<>(observer, mapper);
                    observer.onSubscribe(subscriber);
                    subscriber.requestFirst(source);
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    observer.onError(t);
                }
            }
        }
        return new FlatMapCompletableCompletable();
    }

    /**
     * The downstream observer which will receive the flow using the onXXXX() methods.
     */
    private final CompletableObserver observer;

    /**
     * The mapper converts each input (upstream) item into a CompletableSource
     */
    private final Function<? super I, ? extends CompletableSource> mapper;

    /**
     * Upstream subscription which will be requested to supply source items.
     */
    private FlowSubscription source;

    /**
     * Set to true when the outer completable has been disposed. In this case then next
     * time request is issued it will close the source and complete immediately.
     */
    private final AtomicBoolean isDisposed;

    /**
     * If an item is active, this holds our subscription to the resulting rx completable.
     */
    private volatile FlatMapChild current;

    /**
     * Set to true to indicate an onFinal was received with the previous item.
     */
    boolean completeOnNextRequest = false;

    private FlatMapCompletable(CompletableObserver observer,
                               Function<? super I, ? extends CompletableSource> mapper) throws Exception
    {
        this.observer = observer;
        this.mapper = mapper;
        this.isDisposed = new AtomicBoolean(false);
    }

    void requestFirst(Flow<I> source)
    {
        source.requestFirst(this, this);
    }

    public void onSubscribe(FlowSubscription source)
    {
        this.source = source;
    }

    public void requestNext()
    {
        if (!isDisposed() && !completeOnNextRequest)
            requestInLoop(source);
        else
            close(null);
    }

    public void onFinal(I next)
    {
        completeOnNextRequest = true;
        onNext(next);
    }

    public void onNext(I next)
    {
        if (!verify(current == null, null))
            return;

        try
        {
            CompletableSource child = mapper.apply(next);
            current = new FlatMapChild(child);
            if (isDisposed())
                current.dispose();
            else
                child.subscribe(current);
        }
        catch (Throwable t)
        {
            onError(t);
        }
    }

    public void onError(Throwable throwable)
    {
        onErrorInternal(Flow.wrapException(throwable, this));
    }

    private void onErrorInternal(Throwable throwable)
    {
        if (!verify(current == null, throwable))
            return;

        close(throwable);
    }

    public void onComplete()
    {
        if (!verify(current == null, null))
            return;

        close(null);
    }

    private void close(Throwable t)
    {
        try
        {
            source.close();
        }
        catch (Throwable e)
        {
            t = Throwables.merge(t, e);
        }

        if (isDisposed())
            return; // don't call onXXXX if disposed

        if (t == null)
            observer.onComplete();
        else
            observer.onError(t);
    }

    private boolean verify(boolean test, Throwable existingFail)
    {
        if (!test)
            observer.onError(Throwables.merge(existingFail, new AssertionError("FlatMapCompletable unexpected state\n\t" + this)));
        return test;
    }

    public String toString()
    {
        return Flow.formatTrace("flatMapCompletable", mapper);
    }

    public void dispose()
    {
        if (isDisposed.compareAndSet(false, true))
        {
            FlatMapChild current = this.current;
            if (current != null)
                current.dispose();
        }
    }

    public boolean isDisposed()
    {
        return isDisposed.get();
    }

    class FlatMapChild extends AtomicReference<Disposable> implements CompletableObserver, Disposable
    {
        final CompletableSource source;

        FlatMapChild(CompletableSource source) throws Exception
        {
            this.source = source;
        }

        @Override
        public void dispose() {
            DisposableHelper.dispose(this);
        }

        @Override
        public boolean isDisposed() {
            return DisposableHelper.isDisposed(get());
        }

        @Override
        public void onSubscribe(Disposable d) {
            DisposableHelper.replace(this, d);
        }

        public void onError(Throwable throwable)
        {
            if (!verify(current == this, throwable))
                return;

            current = null;

            close(throwable);
        }

        public void onComplete()
        {
            if (!verify(current == this, null))
                return;

            current = null;

            // request the next child
            FlatMapCompletable.this.requestNext();
        }

        public String toString()
        {
            return FlatMapCompletable.this.toString();
        }
    }
}
