/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.nodesync;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.math.DoubleMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.NodeSyncConfig;
import org.apache.cassandra.utils.collection.History;
import org.apache.cassandra.utils.units.RateUnit;
import org.apache.cassandra.utils.units.Units;

/**
 * An executor for validations on segments for NodeSync.
 * <p>
 * This class job is to pull {@link Validator} objects from a {@link ValidationScheduler} and run them (using the
 * {@link Validator#executeOn(ValidationExecutor)} method).
 * <p>
 * The main parameter that this class controls is 1) how many concurrent validations we allow and 2) how many threads
 * we use to run those validations. Note that running a validation involves many blocking parts (waiting on disk or
 * replicas) and so we don't dedicate a thread to each validation.
 * <p>
 * This class has an auto-adjusting component to it in that it tries to auto-adjust the number of concurrent validations
 * and threads so as to find the combination that allows us to best achieve our target NodeSync rate
 * ({@link NodeSyncConfig#getRate()}). For that, it checks and uses the following at regular intervals:
 *   - how much time thread spends doing nothing useful: if it's really low, we may benefit from an additional thread,
 *     and it's high, we may do as well without one.
 *   - how much time is spend waiting on the limiter to give us permits: if it's too high, this suggests we have too
 *     much concurrent validations going on.
 *   - the recent rate of validation: if it's below our configured rate, we should be trying to do more concurrent
 *     validations.
 * Of course, to avoid problems, we put limit over how much maximum threads and concurrent validations we're willing to
 * allow (in {@link NodeSyncConfig}): if we can't achieve the configured rate with those max settings, this suggest
 * we either have set a unrealistic NodeSync rate that the node is not able to achieve, or that we've been too
 * conservative in our configured maximums. See the {@link Controller} class for concrete details on that auto-adjustment
 * process.
 */
class ValidationExecutor implements Validator.PageProcessingStatsListener
{
    private static final Logger logger = LoggerFactory.getLogger(ValidationExecutor.class);

    private static final long CONTROLLER_INTERVAL_SEC = Long.getLong("datastax.nodesync.controller_update_interval_sec",
                                                                     TimeUnit.MINUTES.toSeconds(5));

    private static final long CONTROLLER_INTERVAL_MS = TimeUnit.SECONDS.toMillis(CONTROLLER_INTERVAL_SEC);

    // For an update interval, if thread spend more than this time waiting on request, we consider it significant.
    // TODO(Sylvain): this definitely needs testing to check if this is a decent value. May want to make configurable at least for test.
    private static final long THREAD_WAIT_TIME_THRESHOLD_MS = 10 * CONTROLLER_INTERVAL_MS / 100;
    // For an update interval, if we spend more than this waiting on rate limiting, we consider it significant.
    // TODO(Sylvain): this definitely needs testing to check if this is a decent value. May want to make configurable at least for test.
    private static final long LIMITER_WAIT_TIME_THRESHOLD_MS = 5 * CONTROLLER_INTERVAL_MS / 100;

    private enum State
    {
        CREATED, RUNNING, SOFT_STOPPED, HARD_STOPPED;

        public boolean isShutdown()
        {
            return this == HARD_STOPPED || this == SOFT_STOPPED;
        }
    }

    /** The current state of this executor. States are here mainly to handle initialization and shutdown clearly, an
     * executor will simply be in the RUNNING state in general. */
    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);

    private final ValidationScheduler scheduler;
    private final NodeSyncConfig config;

    /** The thread pool at the heart of this executor. */
    private final ThreadPoolExecutor validationExecutor;
    private final Scheduler wrappingScheduler; // Rx Scheduler on top of validationExecutor because that's what Flow use for now
    /** Executor used to schedule the {@link Controller} at regular intervals. */
    private final ScheduledExecutorService updaterExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("NodeSyncController"));

    /** The number of currently processed validations. */
    private final AtomicInteger inFlightValidations = new AtomicInteger();
    /** The maximum number of in-flight validations allowed (this can be updated by the controller to increase/decrease the
     * throughput of the executor). */
    private volatile int maxInFlightValidations;

    /** Keep tracks of which validator exactly are in flight (mainly for the purpose of being able to cancel them
     * on a hard shutdown). */
    private final Set<Validator> inFlightValidators = ConcurrentHashMap.newKeySet();

    /** A future that will only be completed if/once the executor has been fully shutdown. */
    private final CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();

    private final AtomicLong processingWaitTimeNanos = new AtomicLong();
    private final AtomicLong limiterWaitTimeNanos = new AtomicLong();
    private final AtomicLong dataValidatedBytes = new AtomicLong();

    ValidationExecutor(ValidationScheduler scheduler, NodeSyncConfig config)
    {
        this.scheduler = scheduler;
        this.validationExecutor = DebuggableThreadPoolExecutor.createWithFixedPoolSize("NodeSync", config.getMinThreads());
        this.wrappingScheduler = Schedulers.from(validationExecutor);
        this.config = config;
        this.maxInFlightValidations = config.getMinInflightValidations();
    }

    Scheduler asScheduler()
    {
        return wrappingScheduler;
    }

    /**
     * Starts the executor.
     * <p>
     * Once started, the executor will continuously pull validators from the scheduler and execute them.
     */
    void start()
    {
        if (!state.compareAndSet(State.CREATED, State.RUNNING))
        {
            if (state.get().isShutdown())
                throw new IllegalStateException("Cannot restart a stopped ValidationExecutor");
            else
                return; // start() called twice, that's fine
        }

        updaterExecutor.scheduleAtFixedRate(new Controller(), CONTROLLER_INTERVAL_SEC, CONTROLLER_INTERVAL_SEC, TimeUnit.SECONDS);

        for (int i = 0; i < maxInFlightValidations; i++)
            submitNewValidation();
    }

    /**
     * Request the shutdown of the executor and returns a future on the completion of that shutdown.
     * <p>
     * This method is idempotent in the sense that if it called while the executor has been shutdown, it will return
     * a future on the initial shutdown attempt (note however that the {@code interruptValidations} is ignored outside of the
     * first call of this method).
     *
     * @param interruptValidations if {@code true}, the executor will attempt to shutdown as fast as possible, interrupting
     *                             any validations in flight even they are not completed. Otherwise, it will stop executing new
     *                             validations but will complete the currently running ones before signalling the future returned
     *                             by this method.
     * @return a future on the completion of the shutdown process initiated by that method.
     */
    CompletableFuture<Void> shutdown(boolean interruptValidations)
    {
        while (true)
        {
            State current = state.get();
            if (current.isShutdown())
                return shutdownFuture; // Already stopped by someone else

            if (state.compareAndSet(current, interruptValidations ? State.HARD_STOPPED : State.SOFT_STOPPED))
                break;
        }

        // We were the one marking the executor as stopping, proceed with shutdown.

        // We should shutdown the scheduler because some threads could be blocked on a call to
        // scheduler.getNextValidation() and that's required to unblock them.
        scheduler.shutdown();

        // Then, if it's a hard shutdown that was requested, actively cancel any running validations
        if (interruptValidations)
            inFlightValidators.forEach(Validator::cancel);

        // The shutdown future will then be notified once all inflight validations finish, or have been cancelled.
        // It's possible however that we had no in-flight validations in the first place, so signal now in that case
        // (note that our check is racy but signalling the future twice, which is our risk here, is harmless).
        if (inFlightValidations.get() == 0)
            shutdownFuture.complete(null);

        return shutdownFuture;
    }

    /**
     * Whether the executor has been shutdown. Note that this return {@code true} as soon as {@link #shutdown(boolean)}
     * has been called on the executor but doesn't provide indication on whether the shutdown has completed or not.
     */
    public boolean isShutdown()
    {
        return state.get().isShutdown();
    }

    private boolean getValidationPermit()
    {
        while (true)
        {
            int current = inFlightValidations.get();
            if (current + 1 > maxInFlightValidations)
                return false;

            if (inFlightValidations.compareAndSet(current, current + 1))
                return true;
        }
    }

    private void returnValidationPermit()
    {
        int value = inFlightValidations.decrementAndGet();
        if (value < 0)
            logger.warn("More permit for NodeSync validations granted than returned, this is a bug that should be reported as such. "
                        + "However, if NodeSync is still running properly (based on metrics), this has likely little to no "
                        + "practical impact.");

        if (value == 0 && isShutdown())
            shutdownFuture.complete(null);
    }

    private void submitNewValidation()
    {
        if (isShutdown())
            return;

        if (getValidationPermit())
        {
            // Note that getNextValidation() is potentially blocking, so it shouldn't be called on the current thread, that is
            // the 2 submit below mean that we submit the validation creation first, and then submit that for execution.
            validationExecutor.submit(() -> {
                try
                {
                    // We want to block on the acquisition of a new validation if we have nothing else to do (otherwise
                    // we shouldn't block as, given that we can have more threads than segments, we could end up
                    // starving the processing of an ongoing segment otherwise). And we have nothing else to do if
                    // there is no other in-flight validations (keeping in mind we do have a permit ourselves).
                    Validator validator = scheduler.getNextValidation(inFlightValidations.get() <= 1);
                    if (validator != null)
                    {
                        submit(validator);
                    }
                    else
                    {
                        // It means there was not currently available validation and we have other ones in flight. So
                        // release the permit and re-schedule the new validation creation.
                        returnValidationPermit();
                        // We could just call submitNewValidation again, which would basically push the task at the end
                        // of the validationExecutor queue, but in the current implementation, a validation can generate a
                        // lot of new tasks (due to Threads.requestOn() in Validator.executeOn creating a task for each
                        // request() call (so at least each partition), even when we're already on the executor and
                        // request() is not blocking), so if say we have one other in-flight validation and just this one
                        // thread, we'd end up alternative between very short tasks for the validation and calls to
                        // submitNewValidation, which feels unnecessary. Anyway, point is that we get here if we have no
                        // validation to currently do, so potentially waiting 100ms before checking again is totally fine
                        // while it limit the risk of busy spinning.
                        // TODO(Sylvain): obviously not ideal (as indicated by the length of the comment). Probably should
                        // improve Threads.requestOn() so it's able to execute request() calls directly (without re-scheduling)
                        // if it's already on the executor and know the next call won't block (maybe a new method in
                        // FlowSubscription that tells if the next call to request() is guaranteed to not block), as this
                        // is an improvement that goes beyond NodeSync (and in fact, there is already a TODO in Threads.RequestOn
                        // for that).
                        ScheduledExecutors.scheduledTasks.schedule(this::submitNewValidation, 100, TimeUnit.MILLISECONDS);
                    }
                }
                catch (ValidationScheduler.ShutdownException e)
                {
                    assert isShutdown(); // no-one should be shutting down the scheduler behind our back
                    // If we get here, that mean we did got our permit for the validation. We need to return it so the shutdown
                    // future is properly competed.
                    returnValidationPermit();
                }
                catch (Exception e)
                {
                    logger.error("Unexpected error submitting new validation to NodeSync executor. This shouldn't happen "
                                 + "and should be reported but unless this happens repeatedly, this shouldn't prevent "
                                 + "NodeSync general progress", e);
                    returnValidationPermit();
                }
            });
        }
    }

    private void submit(Validator validator)
    {
        if (isShutdown())
        {
            // Since the validator has been created, cancel() it so anyone waiting on it's completion future gets notified
            validator.cancel();
            return;
        }

        inFlightValidators.add(validator);
        validator.executeOn(this)
                 .whenComplete((v, e) -> {
                     // Whatever happens, we should release the validation
                     inFlightValidators.remove(validator);
                     onValidationDone();

                     // Validator handles all it's exception on its side, so this shouldn't happen. Shit happens though.
                     if (e != null)
                         logger.error("Unexpected error reported by NodeSync validator of table {}. "
                                      + "This shouldn't happen and should be reported, but shouldn't have impact outside "
                                      + "of the failure of that particular segment validation");
                 });
    }

    public void onPageProcessing(long processedBytes, long waitedOnLimiterNanos, long waitedForProcessingNanos)
    {
        dataValidatedBytes.addAndGet(processedBytes);
        limiterWaitTimeNanos.addAndGet(waitedOnLimiterNanos);
        processingWaitTimeNanos.addAndGet(waitedForProcessingNanos);
    }

    private void onValidationDone()
    {
        returnValidationPermit();
        submitNewValidation();
    }

    /**
     * Describe an action taken by the controller {@link Controller}: it can increase or decrease the number of threads
     * or number of inflight validations, or simply do nothing. Knowing that we only change one thing at a time.
     */
    private enum Action
    {
        INCREASE_THREADS,
        INCREASE_INFLIGHT_VALIDATIONS,
        DO_NOTHING,
        DECREASE_THREADS,
        DECREASE_INFLIGHT_VALIDATIONS;

        boolean isIncrease()
        {
            return this == INCREASE_THREADS || this == INCREASE_INFLIGHT_VALIDATIONS;
        }

        boolean isDecrease()
        {
            return this == DECREASE_THREADS || this == DECREASE_INFLIGHT_VALIDATIONS;
        }
    }

    /**
     * The controller runs at regular intervals and is in charge of deciding if the number of threads and number of
     * inflight validations of the executor should be increased, decreased, or left as is, and this by using the
     * heuristics described in the {@link ValidationExecutor} javadoc.
     *
     * TODO(Sylvain): we should detect when we're at max allowed capacity but still can't achieve our rate and log a
     * warning in the log. The one issue is that on tiny clusters where we have almost to validate, we're likely going
     * to not achieve our rate, but only because we throttle ourselves from repairing the same segment too often. Plus,
     * with almost empty ranges, our time may be dominated by the reads to the system table, which we don't really
     * account for properly. For the first issue, we should add tracking of the time the executor spends blocking on
     * {@link ValidationScheduler#getNextValidation} and not warn (nor raises capacity like crazy) when that's too
     * big. Less clear how to deal efficiently with the 2nd problem.
     */
    private class Controller implements Runnable
    {
        private final DiffValue processingWaitTimeMsDiff = new DiffValue();
        private final DiffValue limiterWaitTimeMsDiff = new DiffValue();
        private final DiffValue dataValidatedBytesDiff = new DiffValue();

        /** An history of the last 6 actions we took (so covers the last 30 minutes with the default of this running every 5 minutes)  */
        private final History<Action> history = new History<>(6);

        /**
         * Whether we recently attempted a decrease immediately followed by an increase.
         */
        private boolean hasRecentUnsuccessfulDecrease()
        {
            // Look for the last "concrete" (not DO_NOTHING) action we recently did, and check if that was an increase
            // just preceded by a decrease (in which case we want to avoid decreasing again to re-increase next check).
            Iterator<Action> iter = history.iterator();
            while (iter.hasNext())
            {
                Action action = iter.next();
                if (action.isDecrease())
                    break;
                else if (action.isIncrease())
                    return iter.hasNext() && iter.next().isDecrease();
            }
            return false;
        }

        private boolean hasSignificantProcessingWaitTimeSinceLastCheck()
        {
            return processingWaitTimeMsDiff.currentDiff() > THREAD_WAIT_TIME_THRESHOLD_MS;
        }

        private boolean hasSignificantLimiterWaitTimeSinceLastCheck()
        {
            return limiterWaitTimeMsDiff.currentDiff() > LIMITER_WAIT_TIME_THRESHOLD_MS;
        }

        private boolean canIncreaseThreads()
        {
            // We shouldn't go over the configured max, but we also never want more threads than validations (doesn't make sense)
            int count = validationExecutor.getCorePoolSize();
            return count < config.getMaxThreads() && count < maxInFlightValidations;
        }

        private boolean canIncreaseInflightValidations()
        {
            return maxInFlightValidations < config.getMaxInflightValidations();
        }

        private boolean canDecreaseThreads()
        {
            return validationExecutor.getCorePoolSize() > config.getMinThreads();
        }

        private boolean canDecreaseInflightValidations()
        {
            // We shouldn't go under the configured min, but we also never want more threads than validations
            return maxInFlightValidations > config.getMinInflightValidations()
                   && maxInFlightValidations > validationExecutor.getCorePoolSize();
        }

        private Action pickDecreaseAction()
        {
            if (!canDecreaseInflightValidations())
                return Action.DECREASE_THREADS;
            if (!canDecreaseThreads())
                return Action.DECREASE_INFLIGHT_VALIDATIONS;

            // We're achieving our target rate and can decrease both thread or in-flight validations.
            // To choose, we look at our processing wait time: if it's high, it means some validation work sit idle by
            // lack of available thread, so we can lower them.
            return hasSignificantProcessingWaitTimeSinceLastCheck() ? Action.DECREASE_INFLIGHT_VALIDATIONS : Action.DECREASE_THREADS;
        }

        private Action pickIncreaseAction()
        {
            if (!canIncreaseInflightValidations())
                return Action.INCREASE_THREADS;
            if (!canIncreaseThreads())
                return Action.INCREASE_INFLIGHT_VALIDATIONS;

            // We're no achieving our target rate and can increase both thread and in-flights validations.
            // To choose, we look at our processing wait time: if it's high, it means validation work sit idle (most
            // likely) due a lack of available threads so increase those. Otherwise, if it seems validation always
            // processing power ready when they need it, try increasing the number of validations instead.
            return hasSignificantProcessingWaitTimeSinceLastCheck() ? Action.INCREASE_THREADS : Action.INCREASE_INFLIGHT_VALIDATIONS;
        }

        private void updateValues()
        {
            processingWaitTimeMsDiff.update(TimeUnit.NANOSECONDS.toMillis(processingWaitTimeNanos.get()));
            limiterWaitTimeMsDiff.update(TimeUnit.NANOSECONDS.toMillis(limiterWaitTimeNanos.get()));
            dataValidatedBytesDiff.update(dataValidatedBytes.get());
        }

        public void run()
        {
            updateValues();

            double targetRate = (double) config.getRate().in(RateUnit.B_S);
            double recentRate = ((double) dataValidatedBytesDiff.currentDiff()) / CONTROLLER_INTERVAL_SEC;

            Action action = Action.DO_NOTHING;
            if (DoubleMath.fuzzyEquals(targetRate, recentRate, targetRate * 0.05))
            {
                // The recent rate is withing 5% of our target, we're basically good. That said, we might be
                // over-committed. To know if we are, we check how much we've been waiting on the limiter acquire()
                // method. If that's a non-negligible amount of time, it means our current number of in-flight
                // validations and threads generate more work than we want. In that case, we consider lowering one of those.
                // Note however that we want to avoid changing our mind on every "tick", so check our little history to
                // see if we already already tried decreasing our values without success (i.e. we ended up bumping them
                // again afterwards).
                if (!hasRecentUnsuccessfulDecrease() && hasSignificantLimiterWaitTimeSinceLastCheck() && (canDecreaseThreads() || canDecreaseInflightValidations()))
                    action = pickDecreaseAction();
            }
            else
            {
                // We're not achieving our target rate. Unless we're already maxing our allowed number of in-flight
                // validations and threads, bump one of them to (try to) speed things up.
                if (canIncreaseThreads() || canIncreaseInflightValidations())
                    action = pickIncreaseAction();
            }

            if (logger.isDebugEnabled())
                logger.debug("NodeSync executor controller: recent rate={} (configured={}), {} threads and {} in-flight validations: {}",
                             Units.toString((long)recentRate, RateUnit.B_S),
                             Units.toString((long)targetRate, RateUnit.B_S),
                             validationExecutor.getCorePoolSize(),
                             maxInFlightValidations,
                             action);

            switch (action)
            {
                case INCREASE_THREADS:
                    validationExecutor.setCorePoolSize(validationExecutor.getCorePoolSize() + 1);
                    break;
                case INCREASE_INFLIGHT_VALIDATIONS:
                    ++maxInFlightValidations;
                    submitNewValidation();
                    break;
                case DECREASE_THREADS:
                    validationExecutor.setCorePoolSize(validationExecutor.getCorePoolSize() - 1);
                    break;
                case DECREASE_INFLIGHT_VALIDATIONS:
                    --maxInFlightValidations;
                    break;
            }
            history.add(action);
        }
    }

    private static class DiffValue
    {
        private long previousTotal;
        private long currentDiff;

        private void update(long currentTotal)
        {
            currentDiff = currentTotal - previousTotal;
            previousTotal = currentTotal;
        }

        private long currentDiff()
        {
            return currentDiff;
        }
    }
}
