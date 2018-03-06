/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocalThread;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.config.NodeSyncConfig;
import org.apache.cassandra.utils.collection.History;
import org.apache.cassandra.utils.units.RateUnit;
import org.apache.cassandra.utils.units.SizeUnit;
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

    private static final long DEFAULT_CONTROLLER_INTERVAL_SEC = Long.getLong("dse.nodesync.controller_update_interval_sec",
                                                                             TimeUnit.MINUTES.toSeconds(5));

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
    private final DebuggableThreadPoolExecutor validationExecutor;

    private final StagedScheduler wrappingScheduler; // Rx Scheduler on top of validationExecutor because that's what Flow use for now
    /** Executor used to schedule the {@link Controller} at regular intervals. */
    private final ScheduledExecutorService updaterExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("NodeSyncController"));

    private volatile Controller controller;

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
    private final AtomicLong limiterWaitTimeMicros = new AtomicLong();
    private final AtomicLong dataValidatedBytes = new AtomicLong();
    private final AtomicLong blockedOnNewTaskTimeNanos = new AtomicLong();
    private final ConcurrentMap<Thread, Long> waitingOnTaskThreads = new ConcurrentHashMap<>();

    private final long controllerIntervalMs;

    ValidationExecutor(ValidationScheduler scheduler, NodeSyncConfig config)
    {
        this(scheduler, config, TimeUnit.SECONDS.toMillis(DEFAULT_CONTROLLER_INTERVAL_SEC));
    }

    @VisibleForTesting
    ValidationExecutor(ValidationScheduler scheduler, NodeSyncConfig config, long controllerIntervalMs)
    {
        this.scheduler = scheduler;
        this.validationExecutor = DebuggableThreadPoolExecutor.createWithFixedPoolSize(new ValidationThread.Factory(), config.getMinThreads());
        this.wrappingScheduler = new NodeSyncStagedExecutor();
        this.config = config;
        this.maxInFlightValidations = config.getMinInflightValidations();
        this.controllerIntervalMs = controllerIntervalMs;
    }

    StagedScheduler asScheduler()
    {
        return wrappingScheduler;
    }

    TracingAwareExecutorService asExecutor()
    {
        return validationExecutor;
    }

    @VisibleForTesting
    History<Action> controllerHistory()
    {
        if (controller == null)
            throw new IllegalStateException("The executor is not started");

        return controller.history;
    }

    @VisibleForTesting
    long lastMaxedOutWarn()
    {
        if (controller == null)
            throw new IllegalStateException("The executor is not started");

        return controller.lastMaxedOutWarn;
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

        // Setup the controller:
        this.controller = new Controller(controllerIntervalMs);
        controller.updateValues();

        // Submit initial validations:
        for (int i = 0; i < maxInFlightValidations; i++)
            submitNewValidation();

        // Schedule the controller after initial validations so it starts using actual metrics:
        updaterExecutor.scheduleAtFixedRate(controller, controllerIntervalMs, controllerIntervalMs, TimeUnit.MILLISECONDS);
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
        if (scheduler != null) // Only exists because unit tests sometimes keep it null.
            scheduler.shutdown();

        // Then, if it's a hard shutdown that was requested, actively cancel any running validations
        if (interruptValidations)
            inFlightValidators.forEach(v -> v.cancel("Shutting down NodeSync forcefully"));

        // The shutdown future will then be notified once all inflight validations finish, or have been cancelled.
        // It's possible however that we had no in-flight validations in the first place, so signal now in that case
        // (note that our check is racy but signalling the future twice, which is our risk here, is harmless).
        if (inFlightValidations.get() == 0)
            shutdownFuture.complete(null);

        updaterExecutor.shutdown();

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
                    Validator validator;
                    Thread currentThread = Thread.currentThread();
                    waitingOnTaskThreads.put(currentThread, System.nanoTime());
                    try
                    {
                        validator = getNextValidation(inFlightValidations.get() <= 1);
                    }
                    finally
                    {
                        blockedOnNewTaskTimeNanos.addAndGet(System.nanoTime() - waitingOnTaskThreads.remove(currentThread));
                    }
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

    @VisibleForTesting
    protected Validator getNextValidation(boolean blockUntilAvailable)
    {
        return scheduler.getNextValidation(blockUntilAvailable);
    }

    private void submit(Validator validator)
    {
        if (isShutdown())
        {
            // Since the validator has been created, cancel() it so anyone waiting on it's completion future gets notified
            validator.cancel("NodeSync has been shutdown");
            return;
        }

        inFlightValidators.add(validator);
        validator.executeOn(this)
                 .whenComplete((v, e) -> {
                     // Whatever happens, we should release the validation
                     inFlightValidators.remove(validator);
                     onValidationDone();

                     // Validator handles all it's exception (outside of cancellation) on its side, so this shouldn't
                     // happen. Shit happens though so log it.
                     if (e != null && !(e instanceof CancellationException))
                         logger.error("Unexpected error reported by NodeSync validator of table {}. "
                                      + "This shouldn't happen and should be reported, but shouldn't have impact outside "
                                      + "of the failure of that particular segment validation",
                                      validator.segment().table , e);
                 });
    }

    public void onPageProcessing(long processedBytes, long waitedOnLimiterMicros, long waitedForProcessingNanos)
    {
        dataValidatedBytes.addAndGet(processedBytes);
        limiterWaitTimeMicros.addAndGet(waitedOnLimiterMicros);
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
    @VisibleForTesting
    enum Action
    {
        INCREASE_THREADS,
        INCREASE_INFLIGHT_VALIDATIONS,
        MAXED_OUT, // Indicates we wanted to increase, but were already max-ed out so nothing was done in practice
        DO_NOTHING,
        MINED_OUT, // Indicates we wanted to decrease, but were already min-ed out so nothing was done in practice
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
     * <p>
     * Additionally, the controller runs a few checks that allow to warn the user if either we don't seem to be able
     * to achieve the requested rate, or if that rate is simply set too low to meet all tables validation targets (given
     * the current size of the data each table currently hold).
     */
    private class Controller implements Runnable
    {
        // For an update interval, if thread spend more than this time waiting on request, we consider it significant.
        private final long threadWaitTimeThresholdMs;
        // For an update interval, if we spend more than this waiting on rate limiting, we consider it significant.
        private final long limiterWaitTimeThresholdMs;
        // For an update interval, if we spend more than this blocking on getting new task, we consider it significant.
        private final long blockedOnNewTaskThresholdMs;

        private long lastTickNanos = System.nanoTime();
        private long lastIntervalMs;

        private final DiffValue processingWaitTimeMsDiff = new DiffValue();
        private final DiffValue limiterWaitTimeMsDiff = new DiffValue();
        private final DiffValue dataValidatedBytesDiff = new DiffValue();
        private final DiffValue blockedOnNewTaskMsDiff = new DiffValue();

        /** An history of the recent actions we took (covers the last hour with the default of this running every 5 minutes)  */
        private final History<Action> history = new History<>(12);

        /** Timestamp of the last time we warned about the executor being maxed out (without achieving the requested rate
         * that is). Negative if we haven't warned (or should warn unconditionally next time the situation arise). */
        private long lastMaxedOutWarn = -1;

        private Controller(long controllerIntervalMs)
        {
            this.threadWaitTimeThresholdMs = 10 * controllerIntervalMs / 100;
            this.limiterWaitTimeThresholdMs = 5 * controllerIntervalMs / 100;
            this.blockedOnNewTaskThresholdMs = 10 * controllerIntervalMs / 100;
        }

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
            return processingWaitTimeMsDiff.currentDiff() > threadWaitTimeThresholdMs;
        }

        private boolean hasSignificantLimiterWaitTimeSinceLastCheck()
        {
            return limiterWaitTimeMsDiff.currentDiff() > limiterWaitTimeThresholdMs;
        }

        private long perThreadAvgBlockTime()
        {
            // blockedOnNewTaskMsDiff is the total time waited by all threads, so divide by our number of threads so
            // it can be more meaningfully compared to the controller interval
            return blockedOnNewTaskMsDiff.currentDiff() / validationExecutor.getCorePoolSize();
        }

        // What percentage of the interval each thread doing something (not being blocked waiting on task) on average.
        private int threadAvgOccupationPercentage()
        {
            long timeOccupied = lastIntervalMs - perThreadAvgBlockTime();
            float occupationPercentage = 100f * ((float)timeOccupied / (float) lastIntervalMs);
            int value = Math.round(occupationPercentage);
            return value < 0 ? 0 : (value > 100 ? 100 : value);
        }

        private boolean hasSignificantBlockOnNewTaskSinceLastCheck()
        {
            return perThreadAvgBlockTime() > blockedOnNewTaskThresholdMs;
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
                return canDecreaseThreads() ? Action.DECREASE_THREADS : Action.MINED_OUT;
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
                return canIncreaseThreads() ? Action.INCREASE_THREADS : Action.MAXED_OUT;
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
            long nowNanos = System.nanoTime();
            dataValidatedBytesDiff.update(dataValidatedBytes.get());
            processingWaitTimeMsDiff.update(TimeUnit.NANOSECONDS.toMillis(processingWaitTimeNanos.get()));
            limiterWaitTimeMsDiff.update(TimeUnit.MICROSECONDS.toMillis(limiterWaitTimeMicros.get()));

            lastIntervalMs = TimeUnit.NANOSECONDS.toMillis(nowNanos - lastTickNanos);
            lastTickNanos = nowNanos;

            // We want to know how long thread have spend blocking for new task during the interval. This is composed
            // of 2 parts: what has been reported in blockedOnNewTaskTimeNanos, and those thread that are currently
            // waiting and haven't reported how long they are blocked.
            // Note that the following is racy, but the reporter is not so precise that this should really matter.
            blockedOnNewTaskMsDiff.update(TimeUnit.NANOSECONDS.toMillis(blockedOnNewTaskTimeNanos.get()));
            for (long start : waitingOnTaskThreads.values())
            {
                long totalBlockedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                blockedOnNewTaskMsDiff.addToCurrentDiff(Math.min(totalBlockedMs, lastIntervalMs));
            }
        }

        public void run()
        {
            if (isShutdown())
                return;

            updateValues();

            double targetRate = (double) config.getRate().in(RateUnit.B_S);
            double recentRate = ((double) 1000 * dataValidatedBytesDiff.currentDiff()) / lastIntervalMs;

            Action action = Action.DO_NOTHING;
            // Check if we are below the target rate (with a 5% error margin).
            if (recentRate < targetRate * 0.95)
            {
                // We're not achieving our target rate. This can actually have 2 causes:
                // 1) we're not using enough threads and/or in-flight validations to meet our rate goal. Then, assuming
                //    we're not already maxing out both of those resources, we try to increase one to (try to) speed
                //    things up.
                // 2) we haven't had much work to do during the last interval, typically because the cluster has little
                //    to no data to validate. In that case, we may actually want to decrease our resource used unless
                //    we're already to the min.
                // We detect whether we are in case 2) by checking what percentage of the last interval time was spend
                // (on average per thread) simply waiting for work to do. If that's non-negligible, we assume we're in
                // case 2), otherwise, that we're in case 1).
                if (hasSignificantBlockOnNewTaskSinceLastCheck())
                {
                    action = pickDecreaseAction();
                }
                else
                {
                    action = pickIncreaseAction();
                    // It's possible we are already maxing out on both threads and inflight validations. In that case,
                    // this suggests the node is not able to achieve the requested rate within the resources constraint
                    // of NodeSyncConfig.max_threads and max_inflight_validations, and there is nothing we can do, the
                    // user need to change one of those setting (or simply accept the rate he has set isn't being
                    // achieved).
                    if (action == Action.MAXED_OUT)
                        maybeWarnOnMaxedOut(recentRate);
                }
            }
            else if (recentRate > targetRate * 1.05)
            {
                // We're over our rate. This should be happening in general due to the rate limiter, but at least in
                // unit tests where we use a very short controller interval time, it seems to initially happen a bit
                // and no real risk in handling this properly. We do log at debug so that if a bug make that happen
                // a lot, we'll eventually notice the problem.
                logger.debug("Recent effective rate {} is higher than the configured rate {}; this may temporarily happen " +
                             "while the server warm up but shouldn't happen in general. If you see this with some " +
                             "regularity, please report",
                             Units.toString((long)recentRate, RateUnit.B_S),
                             Units.toString((long)targetRate, RateUnit.B_S));
                action = pickDecreaseAction();
            }
            else
            {
                // The recent rate is within 5% of our target, we're basically good. That said, we might be
                // over-committed. To know if we are, we check how much we've been waiting on the limiter acquire()
                // method. If that's a non-negligible amount of time, it means our current number of in-flight
                // validations and threads generate more work than the limiter allows. In that case, we consider lowering
                // one of those.
                // Note however that we want to avoid changing our mind on every "tick", so check our little history to
                // see if we already already tried decreasing our values without success (i.e. we ended up bumping them
                // again afterwards).
                if (!hasRecentUnsuccessfulDecrease() && hasSignificantLimiterWaitTimeSinceLastCheck())
                    action = pickDecreaseAction();
            }

            if (logger.isDebugEnabled())
                logger.debug("NodeSync executor controller: recent rate={} (configured={}), {} thread(s) and {} maximum " +
                             "in-flight validation(s), ~{}% avg thread occupation: {}",
                             Units.toString((long)recentRate, RateUnit.B_S),
                             Units.toString((long)targetRate, RateUnit.B_S),
                             validationExecutor.getCorePoolSize(),
                             maxInFlightValidations,
                             threadAvgOccupationPercentage(),
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

            postRunCleanup();
        }

        /**
         * Called when we're not achieving the requested rate, but we are maxing out threads and in-flight validations
         * so we can't "go faster" so we inform the user of that situation.
         * <p>
         * To avoid premature or repetitive warnings, we use the 2 following heuristics:
         * 1) we only warn once every {@link NodeSyncService#MIN_WARN_INTERVAL_MS}. The idea is to not bother the user
         *    every controller interval while things are maxed out, but with a cap after which we consider that maybe
         *    the user has forgotten and reminding him of the problem may be worth it.
         * 2) we only warn on either 2 consecutive interval being maxed out, or if we detect that more than 1/3 of our
         *    history is maxed out. The general idea being that we want to avoid warning the user on a single interval
         *    fluke (hence the 2 consecutive interval rule), but still want to detect case where we alternate too
         *    much between maxed out and not max out since those would mean our average rate may be genuinely lower
         *    than the configured one.
         */
        private void maybeWarnOnMaxedOut(double recentRate)
        {
            long now = NodeSyncHelpers.time().currentTimeMillis();
            if (lastMaxedOutWarn >= 0 && (now - lastMaxedOutWarn) < NodeSyncService.MIN_VALIDATION_INTERVAL_MS)
                return;

            // As mentioned above, we only log if either the previous interval was also maxed out (we haven't added the
            // current interval action yet when this method is called), or more than 30% of our history is maxed out.
            if (history.last() != Action.MAXED_OUT
                && (history.isAtCapacity() && history.stream().filter(a -> a == Action.MAXED_OUT).count() <= (30 * history.size()) / 100))
                return;

            lastMaxedOutWarn = now;
            logger.warn("NodeSync doesn't seem to be able to sustain the configured rate (over the last {}, the "
                        + "effective rate was {} for a configured rate of {}) and this despite using {} threads and "
                        + "{} parallel range validations (maximums allowed). "
                        + "You may try to improve throughput by increasing the maximum allowed number of threads and/or "
                        + "parallel range validations with the understanding that this may result in NodeSync using "
                        + "more of the node resources. "
                        + "If doing so doesn't help, this suggests the configured rate cannot be sustained by NodeSync "
                        + "on the current hardware.",
                        Units.toString(lastIntervalMs, TimeUnit.MILLISECONDS),
                        Units.toString((long)recentRate, RateUnit.B_S),
                        config.getRate(),
                        validationExecutor.getCorePoolSize(),
                        maxInFlightValidations);
        }

        private void postRunCleanup()
        {
            // If we've recently warned about the executor being maxed out, but we haven't done so in recent history
            // (last hour currently), clear up lastMaxedOutWarn so that if we get maxed out again we log again (without
            // waiting for MIN_WARN_INTERVAL_MS. The rational is that if we haven't max out for an hour, this is a good
            // indication that the user has somehow fixed whatever was making the executor max out (maybe the rate was
            // lowered, or maybe the max threads/inflight validations was upped). In which case, it feels worth warning
            // ASAP if conditions changes.
            if (lastMaxedOutWarn >= 0 && history.isAtCapacity() && history.stream().noneMatch(a -> a == Action.MAXED_OUT))
                lastMaxedOutWarn = -1;
        }

        @Override
        public String toString()
        {
            return String.format("Interval: %s, processing wait time: %s, limiter wait time: %s, data validated; %s, blocked on new task: %s",
                                 Units.toString(lastIntervalMs, TimeUnit.MILLISECONDS),
                                 Units.toString(processingWaitTimeMsDiff.currentDiff, TimeUnit.MILLISECONDS),
                                 Units.toString(limiterWaitTimeMsDiff.currentDiff, TimeUnit.MILLISECONDS),
                                 Units.toString(dataValidatedBytesDiff.currentDiff, SizeUnit.BYTES),
                                 Units.toString(blockedOnNewTaskMsDiff.currentDiff, TimeUnit.MILLISECONDS));
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

        private void addToCurrentDiff(long value)
        {
            this.currentDiff += value;
        }

        private long currentDiff()
        {
            return currentDiff;
        }
    }

    private class NodeSyncStagedExecutor extends StagedScheduler
    {
        private final Scheduler scheduler;

        private NodeSyncStagedExecutor()
        {
            this.scheduler = Schedulers.from(validationExecutor);
        }

        @Override
        public boolean isOnScheduler(Thread thread)
        {
            return thread instanceof ValidationThread;
        }

        @Override
        public int metricsCoreId()
        {
            return TPC.getNumCores(); // with IOScheduler, Unknown, etc..
        }

        @Override
        public void enqueue(TPCRunnable runnable)
        {
            validationExecutor.execute(runnable);
        }

        @Override
        public Worker createWorker()
        {
            return scheduler.createWorker();
        }

        @Override
        public TracingAwareExecutor forTaskType(TPCTaskType type)
        {
            return validationExecutor;
        }
    }

    private static class ValidationThread extends FastThreadLocalThread
    {
        private ValidationThread(String name, Runnable runnable)
        {
            super(null, NamedThreadFactory.threadLocalDeallocator(runnable), name);
            setDaemon(true);
        }

        private static class Factory implements ThreadFactory
        {
            private final AtomicInteger n = new AtomicInteger(1);

            public Thread newThread(Runnable runnable)
            {
                return new ValidationThread("NodeSync-" + n.getAndIncrement(), runnable);
            }
        }
    }
}
