/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.nodesync;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.apollo.nodesync.RateSimulator.Parameters;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.NodeSyncConfig;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.metrics.AbstractMetricNameFactory;
import org.apache.cassandra.metrics.NodeSyncMetrics;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.NodeSyncParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.units.RateUnit;
import org.apache.cassandra.utils.units.RateValue;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.SizeValue;
import org.apache.cassandra.utils.units.TimeValue;

/**
 * The NodeSync service, which continuously and iteratively validates that this node is in sync with any other replica
 * with which it shares a range (for a table on which NodeSync is enabled).
 * <p>
 * Technically, the NodeSync service mainly group a {@link ValidationScheduler} and a {@link ValidationExecutor}. The
 * scheduler will continuously generate new validation to execute, and the executor will execute them.
 */
public class NodeSyncService implements NodeSyncServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(NodeSyncService.class);

    /**
     * The target size for table segments. NodeSync will compute segments so that, in the hypothesis of perfect
     * distribution, segments are lower than this this. Of course, we won't have perfect distribution in practice,
     * so this is more a target size, but distribution should still be good enough in practice (or you will have
     * bigger problem than large NodeSync segments).
     */
    // TODO(Sylvain): Not sure how good of a default it is, could be worth some experimentation (but doesn't seem too bad either)
    static final long SEGMENT_SIZE_TARGET = Long.getLong("dse.nodesync.segment_size_target_bytes", SizeUnit.MEGABYTES.toBytes(200));

    /**
     * The minimum delay we enforce between doing 2 validation on the same segment.
     * <p>
     * This exists because on very small clusters (typically brand new empty ones) we might end up validating everything
     * in a very very short time, and it doesn't feel very meaningful to re-validate the empty system distributed tables
     * every 50ms (which is what happens without this on an empty cluster). Note that the amount of resources devoted to
     * NodeSync is globally guarded by {@link NodeSyncConfig#rateLimiter} so the importance of this shouldn't be
     * over-stated but 1) this feels reasonable and 2) we don't account for validations over empty data with the limiter
     * (since it rate limits validated bytes) but there is costs associated to creating the validation in the first place
     * (reading the {@link SystemDistributedKeyspace#NodeSyncStatus} table mostly) and this prevents this to get out of
     * hand.
     */
    // publicly Visible because we warn if user are setting a deadline lowe than this in NodeSyncParams and that's outside the nodesync package
    public static final String MIN_VALIDATION_INTERVAL_PROP_NAME = "dse.nodesync.min_validation_interval_ms";
    public static final long MIN_VALIDATION_INTERVAL_MS = Long.getLong(MIN_VALIDATION_INTERVAL_PROP_NAME, TimeUnit.MINUTES.toMillis(5));

    private static final long LOG_REPORTING_DELAY_SEC = Long.getLong("dse.nodesync.log_reporter_interval_sec", TimeUnit.MINUTES.toSeconds(10));
    private static final long RATE_CHECKING_DELAY_SEC = Long.getLong("dse.nodesync.rate_checker_interval_sec", TimeUnit.MINUTES.toSeconds(30));

    static final long MIN_WARN_INTERVAL_MS = TimeUnit.SECONDS.toMillis(Long.getLong("dse.nodesync.min_warn_interval_sec",
                                                                                    TimeUnit.HOURS.toSeconds(10)));


    private static final MetricNameFactory factory = new AbstractMetricNameFactory(JMX_GROUP,
                                                                                   "NodeSyncMetrics");
    /**
     * Lifetime metrics (for the node) on the NodeSync service (Per-table metrics are also recorded through {@link TableMetrics}).
     */
    private final NodeSyncMetrics metrics = new NodeSyncMetrics(factory, "NodeSync");

    final NodeSyncConfig config = DatabaseDescriptor.getNodeSyncConfig();

    // Will be null if NodeSync isn't running. Those are set to non-null values only inside synchronized methods
    // so as to ensure we never run 2 instances at once.
    private volatile ValidationScheduler scheduler; // Generate/schedule NodeSync validations as appropriate
    private volatile ValidationExecutor executor;   // Executes the validation generated by the scheduler.
    private volatile RateChecker rateChecker;

    private volatile ScheduledFuture<?> logReporterFuture; // Allows to cancel the LogReporter.

    public NodeSyncService()
    {
        registerJMX();
    }

    public NodeSyncMetrics metrics()
    {
        return metrics;
    }

    private void registerJMX()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            ObjectName jmxName = new ObjectName(MBEAN_NAME);
            mbs.registerMBean(this, jmxName);
        }
        catch (InstanceAlreadyExistsException e)
        {
            logger.error("Cannot register NodeSync through JMX as a prior instance already exists: "
                         + "this shouldn't happen and should be reported to support. "
                         + "It won't prevent NodeSync from running, but it will prevent controlling this instance through JMX");
        }
        catch (Exception e)
        {
            logger.error("Cannot register NodeSync through JMX due to unexpected error: "
                         + "this shouldn't happen and should be reported to support. "
                         + "It won't prevent NodeSync from running, but it will prevent controlling this instance through JMX", e);
        }
    }

    /**
     * Enables the NodeSync service if it is not running already.
     *
     * @return {@code true} if the service was started, {@code false} if it was already running prior to this call (in
     * which case the method was a no-op).
     */
    public synchronized boolean enable()
    {
        if (isRunning())
            return false;

        scheduler = new ValidationScheduler(this);
        executor = new ValidationExecutor(scheduler, config);
        executor.start();

        // Note that we want to register as a schema listener before we compute the initial proposers so that we can't
        // miss a table creation/update due to a race. This might make us create more than one ContinuousTableValidationProposer
        // for a given table, but this is fine as ValidationScheduler will only keep one.
        // Same for the 'endpoint lifecycle' one, we want to set it before we check that there is only one node.
        Schema.instance.registerListener(scheduler);
        StorageService.instance.register(scheduler);

        logReporterFuture = ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(new LogReporter(metrics, scheduler),
                                                                                  LOG_REPORTING_DELAY_SEC,
                                                                                  LOG_REPORTING_DELAY_SEC,
                                                                                  TimeUnit.SECONDS);

        rateChecker = new RateChecker(config);

        // Create initial proposers only once everything else is setup so that on error we leave the service in a
        // functioning (if not exactly right) state.
        try
        {
            // Do a first check now on startup, then schedule following ones.
            rateChecker.checkRate();
            rateChecker.schedule();

            String details;
            if (StorageService.instance.getTokenMetadata().getAllEndpoints().size() == 1)
            {
                // This is a single node cluster so don't create useless validations.
                details = "currently inactive as this is the only node in the cluster; will activate automatically once more nodes join";
            }
            else
            {
                scheduler.createInitialProposers();
                int proposers = scheduler.proposerCount();
                details = proposers == 0
                          ? "currently inactive as no replicated table has NodeSync enabled; will activate automatically once this change"
                          : proposers + " tables have NodeSync enabled";
            }

            logger.info("Enabled NodeSync service ({})", details);
            return true;
        }
        catch (RuntimeException e)
        {
            // If we failed creating proposer and throw an exception here, user will expect the service to not really be
            // running, so make sure that's basically true.
            disableInternal(true, false);
            throw e;
        }
    }

    /**
     * Disable the NodeSync service if it is running.
     *
     * @param force if {@code true}, try to force the shutdown of the service, interrupting any currently running
     *              validation if necessary. If {@code false}, a clean shutdown (where no new segment validation starts
     *              executing but the ongoing one continue to completion) is initiated instead.
     * @return a future on the shutdown of the service. If the service wasn't running in the first place, the returned
     * future will return immediately.
     */
    public synchronized CompletableFuture<Void> disable(boolean force)
    {
        if (!isRunning())
            return CompletableFuture.completedFuture(null);

        return disableInternal(force, true);
    }

    /**
     * Disables the NodeSync service (if it is running) and blocks (indefinitely) on the shutdown completing.
     * <p>
     * This method only exists for the sake of JMX and more precisely JConsole. The {@link #disable(boolean)} variant
     * cannot be used through JMX at all and the {@link #disable(boolean, long, TimeUnit)}, while exposed by JMX, cannot
     * be called from JConsole due to the use of {@link TimeUnit}. As some users may find it convenient to still be
     * able to disable through JConsole (after, you can call {@link #enable()} from there), we expose this variant as
     * well.
     *
     * @return {@code true} if the service was stopped, {@code false} if it wasn't already running.
     */
    public boolean disable()
    {
        try
        {
            return disable(false, Long.MAX_VALUE, TimeUnit.DAYS);
        }
        catch (TimeoutException e)
        {
            throw new AssertionError("I hope the wait wasn't too long");
        }
    }

    /**
     * Disables the NodeSync service (if it is running) and blocks on the shutdown completing.
     * <p>
     * For internal code, the {@link #disable(boolean)} variant should be preferred to this method as it's a bit more
     * flexible (doesn't block by default and the returned future allows for a few conveniences), but this method exists
     * for JMX where we basically have to block (or do something a lot more complex).
     *
     * @param force whether the shutdown should be forced, which means that ongoing validation will be interrupted and the
     *              service is stopped as quickly as possible. if {@code false}, a clean shutdown is performed where
     *              ongoing NodeSync segments validations are left time to finish so no ongoing work is thrown on the floor.
     *              Note that a clean shutdown shouldn't take long in general and is thus recommended.
     * @param timeout how long the method should wait for the service to report proper shutdown. If the service hasn't
     *                finish shutdown within this timeout, a {@link TimeoutException} is thrown.
     * @param timeoutUnit the unit for {@code timeout}.
     * @return {@code true} if the service was stopped, {@code false} if it wasn't already running.
     */
    public boolean disable(boolean force, long timeout, TimeUnit timeoutUnit) throws TimeoutException
    {
        if (!isRunning())
            return false;

        try
        {
            Uninterruptibles.getUninterruptibly(disable(force), timeout, timeoutUnit);
            return true;
        }
        catch (ExecutionException e)
        {
            // We never complete the future returned by disable() exceptionally, so this genuinely shouldn't happen
            throw new AssertionError(e);
        }
    }

    private synchronized CompletableFuture<Void> disableInternal(boolean force, boolean logOnCompletion)
    {
        // We can un-register the scheduler right away.
        Schema.instance.unregisterListener(scheduler);
        StorageService.instance.unregister(scheduler);

        return executor.shutdown(force).thenRun(() -> finishShutdown(logOnCompletion));
    }

    private synchronized void finishShutdown(boolean logOnCompletion)
    {
        this.logReporterFuture.cancel(false);
        this.rateChecker.stop();
        this.scheduler = null;
        this.executor = null;
        this.rateChecker = null;
        this.logReporterFuture = null;
        this.rateChecker = null;
        if (logOnCompletion)
            logger.info("Disabled NodeSync service");
    }

    public boolean isRunning()
    {
        return executor != null;
    }

    /**
     * Updates the global and per-table JMX metrics after a segment validation.
     *
     * @param table the table on which the validation for which we record metrics was.
     * @param validationMetrics the metrics of the validation we performed and that needs to be added to the JMX metrics.
     */
    void updateMetrics(TableMetadata table, ValidationMetrics validationMetrics)
    {
        validationMetrics.addTo(metrics);
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(table.id);
        // We generally should only get there if the table exists, but table drop kind of race with everything so it's
        // possible to get there if the table was effectively removed between the end of the validation and this point.
        // In which case just ignoring the metrics is fine anyway.
        if (cfs != null)
            validationMetrics.addTo(cfs.metric.nodeSyncMetrics);
    }

    /**
     * Sets the validation rate for NodeSync.
     *
     * @param kbPerSecond the new rate to set in kilobytes-per-seconds.
     */
    public void setRate(int kbPerSecond)
    {
        config.setRate(RateValue.of(kbPerSecond, RateUnit.KB_S));
        rateChecker.onRateUpdate();
    }

    /**
     * Returns the currently "configured" validation rate for NodeSync.
     * <p>
     * Please note that this only return the configured "target" rate of NodeSync but may not necessarily correspond
     * to the rate at which NodeSync is currently operating (which cannot be greater that the value returned by this
     * method by definition, but can be lower if there is little to validate in the cluster or if the node is not
     * able to achieve the configured rate). If you want to know said "live" rate, you should look at the
     * {@link NodeSyncMetrics#dataValidated} metric.
     *
     * @return the configured rate in kilobytes-per-seconds.
     */
    public int getRate()
    {
        return (int)config.getRate().in(RateUnit.KB_S);
    }

    public void startUserValidation(Map<String, String> optionMap)
    {
        ValidationScheduler scheduler = this.scheduler;
        if (scheduler == null)
            throw new NodeSyncNotRunningException("Cannot start user validation, NodeSync is not currently running.");

        // TODO: we should use JMX notifications for progress reporting. Not really a priority though.
        scheduler.userValidations().createAndStart(UserValidationOptions.fromMap(optionMap));
    }

    public void startUserValidation(String id, String keyspace, String table, String ranges)
    {
        HashMap<String, String> m = new HashMap<>();
        m.put(UserValidationOptions.ID, id);
        m.put(UserValidationOptions.KEYSPACE_NAME, keyspace);
        m.put(UserValidationOptions.TABLE_NAME, table);
        if (ranges != null && !ranges.isEmpty())
            m.put(UserValidationOptions.REQUESTED_RANGES, ranges);
        startUserValidation(m);
    }

    public void cancelUserValidation(String id)
    {
        ValidationScheduler scheduler = this.scheduler;
        if (scheduler == null)
            throw new NodeSyncNotRunningException("Cannot cancel user validation, NodeSync is not currently running.");

        UserValidationProposer proposer = scheduler.userValidations().get(id);
        if (proposer == null)
            throw new NotFoundValidationException("Cannot find user validation #" + id);

        // We could return the value of cancel() from this method, but as validations are unregistered as soon as they
        // complete, we can only get false here on a race with that removal and the window for that is really small. It
        // follows that returning a boolean where 99.9% of user will always see it return true might be more confusing
        // than anything. So we throw an exception instead: after all, if this had run just a few ms later, we'd have
        // thrown a NoSuchElementException.
        if (!proposer.cancel())
        {
            // Anti-bug protection: if for some reason a validation don't get properly cleared after completion, we'll
            // get here and forcing the removal here may give use a work-around. Otherwise, it's just a no-op.
            scheduler.userValidations().forceRemove(id);
            throw new CancelledValidationException("User validation #" + id + " is already completed");
        }
    }

    public List<Map<String, String>> getRateSimulatorInfo(boolean includeAllTables)
    {
        return RateSimulator.Info.compute(includeAllTables).toJMX();
    }

    /**
     * Simple class in charge of logging progress on regular intervals.
     */
    private static class LogReporter implements Runnable
    {
        private final TimeValue LOG_INTERVAL = TimeValue.of(LOG_REPORTING_DELAY_SEC, TimeUnit.SECONDS);

        // Note: taking a reference to the scheduler rather than making the class non-static and directly accessing
        // the scheduler field of NodeSyncService make sure we won't run into any race while shutting down the service
        private final NodeSyncMetrics metrics;
        private final ValidationScheduler scheduler;

        private int lastProposerCount;
        private long lastQueuedProposalCount;
        private long lastValidatedBytes = 0;
        private long lastRepairedBytes = 0;
        private long lastProcessedPages = 0;
        private long lastPartialPages = 0;
        private long lastUncompletedPages = 0;
        private long lastFailedPages = 0;

        private LogReporter(NodeSyncMetrics metrics, ValidationScheduler scheduler)
        {
            this.metrics = metrics;
            this.scheduler = scheduler;

            this.lastProposerCount = scheduler.proposerCount();
            this.lastQueuedProposalCount = scheduler.queuedProposalCount();
        }

        public void run()
        {
            int currentProposerCount = scheduler.proposerCount();
            long currentQueuedProposalCount = scheduler.queuedProposalCount();
            long currentValidatedBytes = metrics.dataValidated.getCount();
            long currentRepairedBytes = metrics.dataRepaired.getCount();
            long currentProcessedPages = metrics.processedPages.getCount();
            long currentPartialPages = metrics.partialInSyncPages.getCount() + metrics.partialRepairedPages.getCount();
            long currentUncompletedPages = metrics.uncompletedPages.getCount();
            long currentFailedPages = metrics.failedPages.getCount();

            // If we have no table eligible for NodeSync (we're a single node cluster, no keyspace has RF > 1, no table
            // has NodeSync enabled, ...), don't bother logging a message, it's useless and thus confusing.
            if (currentQueuedProposalCount == lastQueuedProposalCount && lastProposerCount == 0 && currentProposerCount == 0)
                return;

            long validatedDiff = currentValidatedBytes - lastValidatedBytes;
            SizeValue validatedBytes = SizeValue.of(validatedDiff, SizeUnit.BYTES);

            long diffProcessedPages = currentProcessedPages - lastProcessedPages;
            long diffPartialPages = currentPartialPages - lastPartialPages;
            long diffUncompletedPages = currentUncompletedPages - lastUncompletedPages;
            long diffFailedPages = currentFailedPages - lastFailedPages;

            List<String> details = new ArrayList<>();
            if (diffPartialPages > 0)
                details.add(String.format("%d%% partial", percent(diffPartialPages, diffProcessedPages)));
            if (diffUncompletedPages > 0)
                details.add(String.format("%d%% uncompleted", percent(diffUncompletedPages, diffProcessedPages)));
            if (diffFailedPages > 0)
                details.add(String.format("%d%% failed", percent(diffFailedPages, diffProcessedPages)));

            String detailStr = details.isEmpty() ? "" : '(' + Joiner.on(',').join(details) + ')';

            logger.info("In last {}: validated {} ({}), {}% was inconsistent{}.",
                        LOG_INTERVAL,
                        validatedBytes,
                        RateValue.compute(validatedBytes, LOG_INTERVAL),
                        percent(currentRepairedBytes - lastRepairedBytes, validatedDiff),
                        detailStr);

            this.lastProposerCount = currentProposerCount;
            this.lastQueuedProposalCount = currentQueuedProposalCount;
            this.lastValidatedBytes = currentValidatedBytes;
            this.lastRepairedBytes = currentRepairedBytes;
            this.lastProcessedPages = currentProcessedPages;
            this.lastPartialPages = currentPartialPages;
            this.lastUncompletedPages = currentUncompletedPages;
            this.lastFailedPages = currentFailedPages;
        }

        private int percent(long value, long total)
        {
            return value == 0 ? 0 : Math.min((int)((value * 100)/total), 100);
        }
    }

    /**
     * Checks if the configured rate is high enough to allow validating all tables within their respective
     * {@link NodeSyncParams#deadlineTarget}. This check is run once when starting the service, but then also at regular
     * intervals because as that check fundamentally depend on the current size of each table, this may change over
     * time and have to be re-evaluated.
     * <p>
     * In practice, this checker uses the {@link RateSimulator} compare the configured rate to compute 2 rates: the
     * theoretical minimum and the minimum recommended. If the configured rate is lower than the theoretical minimum,
     * then we can unequivocally warn than the rate is too low and we do so. If not, but said configured rate is still
     * lower than the minimum recommend, we issue a informational note that the rate may be sufficient but looks a bit
     * on the low end.
     */
    private static class RateChecker implements Runnable
    {
        private final NodeSyncConfig config;
        private volatile ScheduledFuture<?> scheduledFuture; // For cancelling scheduled checks

        /** Timestamps of the last time we _warned_ about the rate being insufficient. */
        private long lastInsufficientRateWarn = -1;
        /** Timestamps of the last time we _inform_ the user about the rate seeming low. */
        private long lastLowRateInfo = -1;

        private RateChecker(NodeSyncConfig config)
        {
            this.config = config;
        }

        public void run()
        {
            checkRate();
        }

        /**
         * Do the actual rate checking and logging, and return if we did any logging/warning.
         */
        private boolean checkRate()
        {
            long now = System.currentTimeMillis();
            // If we warned recently, we're not going to do anything now, so exit early
            if (lastInsufficientRateWarn >= 0 && (now - lastInsufficientRateWarn) < MIN_WARN_INTERVAL_MS)
                return false;

            RateValue rate = config.getRate();
            RateSimulator.Info info = RateSimulator.Info.compute(false);
            RateValue minimumTheoretical = new RateSimulator(info, Parameters.THEORETICAL_MINIMUM).computeRate();
            RateValue minimumRecommended = new RateSimulator(info, Parameters.MINIMUM_RECOMMENDED).computeRate();

            if (rate.compareTo(minimumTheoretical) < 0)
            {
                RateValue recommended = new RateSimulator(info, Parameters.RECOMMENDED).computeRate();
                logger.warn("The configured NodeSync rate on this node ({}) is too low to possibly validate all "
                            + "NodeSync-enabled tables within their respective deadline ('deadline_target_sec' property). "
                            + "This can be fixed by increasing the rate and/or increasing table deadlines. "
                            + "With the current deadlines and current table size, the theoretical minimum rate would "
                            + "be {}, but we would recommend a _minimum_ of {} and ideally {} to account for node "
                            + "failures, temporary slow nodes and future data growth. "
                            + "Please check 'nodetool nodesyncservice ratesimulator' for more details on how those "
                            + "values are computed.",
                            rate, minimumTheoretical, minimumRecommended, recommended);
                lastInsufficientRateWarn = now;
                return true;
            }

            // Otherwise, iff we informed about low rate recently, we're not going to do it again, so exit early
            if (lastLowRateInfo >= 0 && (now - lastLowRateInfo) < MIN_WARN_INTERVAL_MS)
                return false;

            if (rate.compareTo(minimumRecommended) >= 0)
                return false;

            RateValue recommended = new RateSimulator(info, Parameters.RECOMMENDED).computeRate();
            logger.info("The configured NodeSync rate on this node ({}) is barely above the theoretical minimum ({})" +
                        "necessary to validate all NodeSync-enabled tables within their respective deadline "
                        + "('deadline_target_sec' property). This makes it likely those deadline may not be met in the "
                        + "face of relatively normal events like temporary slow or failed nodes, and don't account for"
                        + "future data growth. We would recommend a _minimum_ of {} and ideally {}. Alternatively, you "
                        + "can also relax the deadlines on tables (by updating the 'deadline_target_sec' property). "
                        + "Please check 'nodetool nodesyncservice ratesimulator' for more details on how those rates"
                        + "values are computed.",
                        rate, minimumTheoretical, minimumRecommended, recommended);
            lastLowRateInfo = now;
            return true;
        }

        private void schedule()
        {
            scheduledFuture = ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(this,
                                                                                    RATE_CHECKING_DELAY_SEC,
                                                                                    RATE_CHECKING_DELAY_SEC,
                                                                                    TimeUnit.SECONDS);
        }

        private void stop()
        {
            if (scheduledFuture != null)
                scheduledFuture.cancel(false);
        }

        /**
         * Called when the rate is updated by the user through JMX.
         * <p>
         * This forces a new check so as to give immediate feedback on the impact of the change. In particular, this
         * will inform the user if the new rate is still too low, but also, if the change is one that follows one of
         * our own warning, it acknowledges to the user that the rate is now ok (if it is).
         * <p>
         * Implementation note: we don't really expect users to modify the rate concurrently, but better safe than sorry,
         * hence the synchronization to avoid any issue.
         */
        private synchronized void onRateUpdate()
        {
            // First, cancel any scheduled check since we're going to re-schedule from that point on.
            stop();

            // Then, we want to clear our timestamps because we have a new rate and want to warn/inform immediately if
            // that new rate is not right.
            boolean hadLoggedRecently = lastInsufficientRateWarn >= 0 || lastLowRateInfo >= 0;
            lastInsufficientRateWarn = -1;
            lastLowRateInfo = -1;

            boolean logged = checkRate();
            // If we've logged a warning/info, the user go every feedback on the change it needed. Otherwise, simply
            // acknowledge the rate update.
            if (!logged)
                logger.info("Updated configured rate to {}{}.",
                            config.getRate(),
                            hadLoggedRecently ? " (the new rate is now above the recommend minimum)" : "");

            // Lastly, re-schedule future checks
            schedule();
        }
    }

    static class NodeSyncServiceException extends RuntimeException
    {
        private NodeSyncServiceException(String message)
        {
            super(message);
        }
    }

    /**
     * Thrown by {@link #startUserValidation} and {@link #cancelUserValidation} when the NodeSync service isn't running.
     */
    public final static class NodeSyncNotRunningException extends NodeSyncServiceException
    {
        private NodeSyncNotRunningException(String message)
        {
            super(message);
        }
    }

    /**
     * Thrown by {@link #cancelUserValidation} when the referenced validation is not found.
     */
    public final static class NotFoundValidationException extends NodeSyncServiceException
    {
        private NotFoundValidationException(String message)
        {
            super(message);
        }
    }

    /**
     * Thrown by {@link #cancelUserValidation} when the referenced validation is already cancelled.
     */
    public final static class CancelledValidationException extends NodeSyncServiceException
    {
        private CancelledValidationException(String message)
        {
            super(message);
        }
    }

}
