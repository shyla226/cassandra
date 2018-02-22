package org.apache.cassandra.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import io.reactivex.disposables.Disposable;

/**
 * Facility to submit timeout tasks to the {@link org.apache.cassandra.concurrent.TPCTimer} by holding a value and
 * passing such value to a consumer representing the timeout action.
 * <p>
 * The value is "freed" as soon as the timeout is disposed, so this class should be used to optimize GC behaviour.
 */
public class TPCTimeoutTask<T> implements Runnable
{
    private final TPCTimer timer;
    private final AtomicReference<T> valueRef;
    private final AtomicReference<Consumer<T>> actionRef;
    private volatile Disposable disposable;

    public TPCTimeoutTask(T value)
    {
        this(TPC.bestTPCTimer(), value);
    }

    public TPCTimeoutTask(TPCTimer timer, T value)
    {
        if (timer == null || value == null)
            throw new IllegalArgumentException("Timer and value must be both non-null!");

        this.timer = timer;
        this.valueRef = new AtomicReference<>(value);
        this.actionRef = new AtomicReference<>();
    }

    @Override
    public void run()
    {
        T value = valueRef.get();
        if (value != null)
        {
            actionRef.getAndSet(null).accept(value);
            valueRef.set(null);
        }
    }

    public void submit(Consumer<T> action, long timeoutNanos, TimeUnit timeUnit)
    {
        if (actionRef.compareAndSet(null, action))
            disposable = timer.onTimeout(this, timeoutNanos, timeUnit);
        else
            throw new IllegalStateException("Task was already submitted!");
    }

    public void dispose()
    {
        if (disposable != null)
        {
            disposable.dispose();
            if (disposable.isDisposed())
            {
                valueRef.set(null);
                actionRef.set(null);
            }
        }
    }

    public T getValue()
    {
        return valueRef.get();
    }
}
