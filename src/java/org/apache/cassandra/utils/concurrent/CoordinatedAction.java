package org.apache.cassandra.utils.concurrent;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.apache.cassandra.utils.SystemTimeSource;
import org.apache.cassandra.utils.TimeSource;

/**
 * Allows to coordinate a supplied action between different parties: each party asking for the supplied action (via
 * {@link get()} will return a future which will complete:
 * <ul>
 * <li>Successfully when all parties will have invoked {@link get()}.</li>
 * <li>Exceptionally if there's a timeout before all parties are able to invoke {@link get()}, or if the supplied action
 * completes exceptionally itself.</li>
 * </ul>
 * The supplied action itself is a {@link CompletableFuture} supplier, to allow for either sync or async execution,
 * and is guaranteed to be executed at most once.
 */
public class CoordinatedAction implements Supplier<CompletableFuture<Void>>
{
    final Set<CompletableFuture<Void>> futures;
    final Lock lock;
    final Supplier<CompletableFuture<Void>> action;
    final CountDownLatch latch;
    final TimeSource source;
    final long timeoutInMillis;
    final long startTimeInMillis;

    /**
     * Builds a new coordinated action.
     * 
     * @param action The supplied action to execute.
     * @param parties The total number of parties that should invoke {@link get()} before executing the action.
     * @param startTimeInMillis The start time in millis for the action.
     * @param timeout The timeout, computed based on {@code startTimeInMillis).
     * @param unit The time unit.
     */
    public CoordinatedAction(Supplier<CompletableFuture<Void>> action, int parties, long startTimeInMillis, long timeout, TimeUnit unit)
    {
        this(action, parties, startTimeInMillis, timeout, unit, new SystemTimeSource());
    }

    @VisibleForTesting
    CoordinatedAction(Supplier<CompletableFuture<Void>> action, int parties, long startTimeInMillis, long timeout, TimeUnit unit, TimeSource source)
    {
        this.futures = Sets.newConcurrentHashSet();
        this.lock = new ReentrantLock();
        this.latch = new CountDownLatch(parties);
        this.action = action;
        this.source = source;
        this.startTimeInMillis = startTimeInMillis;
        this.timeoutInMillis = TimeUnit.MILLISECONDS.convert(timeout, unit);
    }

    @Override
    public CompletableFuture<Void> get()
    {
        try
        {
            if (source.currentTimeMillis() - startTimeInMillis < timeoutInMillis)
            {
                CompletableFuture<Void> future = new CompletableFuture<>();
                futures.add(future);

                latch.countDown();
                if (latch.await(0, TimeUnit.MILLISECONDS))
                {
                    if (lock.tryLock())
                    {
                        try
                        {
                            if (futures.stream().noneMatch(f -> f.isCompletedExceptionally()))
                            {
                                action.get().whenComplete((r, e) ->
                                {
                                   if (e == null)
                                       futures.stream().forEach(f -> f.complete(r));
                                   else
                                       futures.stream().forEach(f -> f.completeExceptionally(e));
                                });
                            }
                            else
                                futures.stream().forEach(f -> f.completeExceptionally(new TimeoutException()));
                        }
                        finally
                        {
                            lock.unlock();
                        }
                    }
                }
                return future;
            }
            else
            {
                CompletableFuture<Void> future = new CompletableFuture<>();
                futures.add(future);
                futures.stream().forEach(f -> f.completeExceptionally(new TimeoutException()));
                latch.countDown();
                return future;
            }
        }
        catch (InterruptedException ex)
        {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(ex);
            return future;
        }
    }
}
