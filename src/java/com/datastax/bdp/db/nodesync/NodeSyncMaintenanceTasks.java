/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.bdp.db.nodesync.RateSimulator.Parameters;
import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.metrics.NodeSyncMetrics;
import org.apache.cassandra.schema.NodeSyncParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.units.RateValue;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.SizeValue;
import org.apache.cassandra.utils.units.TimeValue;
import org.apache.cassandra.utils.units.Units;

/**
 * Groups maintenance tasks that are necessary to NodeSync (and don't naturally belong to another class).
 */
class NodeSyncMaintenanceTasks
{
    private static final long LOG_REPORTING_DELAY_SEC = Long.getLong("dse.nodesync.log_reporter_interval_sec", TimeUnit.MINUTES.toSeconds(10));
    private static final long RATE_CHECKING_DELAY_SEC = Long.getLong("dse.nodesync.rate_checker_interval_sec", TimeUnit.MINUTES.toSeconds(30));
    private static final long SIZE_CHECKING_DELAY_SEC = Long.getLong("dse.nodesync.size_checker_interval_sec", TimeUnit.HOURS.toSeconds(2));

    private static final Logger logger = LoggerFactory.getLogger(NodeSyncMaintenanceTasks.class);

    private final ScheduledExecutorService scheduledExecutor = new DebuggableScheduledThreadPoolExecutor("NodeSyncMaintenanceTasks");
    private final NodeSyncService.Instance instance;

    private final LogReporter logReporter;
    private final RateChecker rateChecker;
    private final SizeChecker sizeChecker;

    NodeSyncMaintenanceTasks(NodeSyncService.Instance instance)
    {
        this.instance = instance;

        this.logReporter = new LogReporter();
        this.rateChecker = new RateChecker();
        this.sizeChecker = new SizeChecker();
    }

    void start()
    {
        logReporter.start();
        rateChecker.start();
        sizeChecker.start();
    }

    void stop()
    {
        logReporter.stop();
        rateChecker.stop();
        sizeChecker.stop();

        // Shutdown the scheduler for good measure. We don't wait on that shutdown, because we don't really care.
        scheduledExecutor.shutdown();
    }

    void onRateUpdate()
    {
        rateChecker.onRateUpdate();
    }

    private abstract class ScheduledTask implements Runnable
    {
        private volatile ScheduledFuture<?> future;

        abstract long delayInSec();

        void start()
        {
            assert future == null : "Already started";
            long delayInSec = delayInSec();
            future = scheduledExecutor.scheduleAtFixedRate(this, delayInSec, delayInSec, TimeUnit.SECONDS);
        }

        void stop()
        {
            if (future != null)
                future.cancel(true);
            future = null;
        }
    }

    /**
     * Simple class in charge of logging progress on regular intervals.
     */
    private class LogReporter extends ScheduledTask
    {
        private final TimeValue LOG_INTERVAL = TimeValue.of(LOG_REPORTING_DELAY_SEC, TimeUnit.SECONDS);

        private int lastValidatedTables;
        private long lastScheduledValidations;
        private long lastValidatedBytes = 0;
        private long lastRepairedBytes = 0;
        private long lastProcessedPages = 0;
        private long lastPartialPages = 0;
        private long lastUncompletedPages = 0;
        private long lastFailedPages = 0;

        private LogReporter()
        {
            this.lastValidatedTables = scheduler().continuouslyValidatedTables();
            this.lastScheduledValidations = scheduler().scheduledValidations();
        }

        private ValidationScheduler scheduler()
        {
            return instance.scheduler;
        }

        private NodeSyncMetrics metrics()
        {
            return instance.service().metrics();
        }

        long delayInSec()
        {
            return LOG_REPORTING_DELAY_SEC;
        }

        public void run()
        {
            int currentValidatedTables = scheduler().continuouslyValidatedTables();
            long currentScheduledValidations = scheduler().scheduledValidations();
            long currentValidatedBytes = metrics().dataValidated.getCount();
            long currentRepairedBytes = metrics().dataRepaired.getCount();
            long currentProcessedPages = metrics().processedPages.getCount();
            long currentPartialPages = metrics().partialInSyncPages.getCount() + metrics().partialRepairedPages.getCount();
            long currentUncompletedPages = metrics().uncompletedPages.getCount();
            long currentFailedPages = metrics().failedPages.getCount();

            // If we have no table eligible for NodeSync (we're a single node cluster, no keyspace has RF > 1, no table
            // has NodeSync enabled, ...), don't bother logging a message, it's useless and thus confusing.
            if (currentScheduledValidations == lastScheduledValidations && lastValidatedTables == 0 && currentValidatedTables == 0)
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

            this.lastValidatedTables = currentValidatedTables;
            this.lastScheduledValidations = currentScheduledValidations;
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
    private class RateChecker extends ScheduledTask
    {
        /** Timestamps of the last time we _warned_ about the rate being insufficient. */
        private long lastInsufficientRateWarn = -1;
        /** Timestamps of the last time we _inform_ the user about the rate seeming low. */
        private long lastLowRateInfo = -1;

        long delayInSec()
        {
            return RATE_CHECKING_DELAY_SEC;
        }

        @Override
        void start()
        {
            // Do a first check on startup.
            checkRate();
            super.start();
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
            if (lastInsufficientRateWarn >= 0 && (now - lastInsufficientRateWarn) < NodeSyncService.MIN_WARN_INTERVAL_MS)
                return false;

            RateValue rate = instance.service().config().getRate();
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
            if (lastLowRateInfo >= 0 && (now - lastLowRateInfo) < NodeSyncService.MIN_WARN_INTERVAL_MS)
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
                            instance.service().config().getRate(),
                            hadLoggedRecently ? " (the new rate is now above the recommend minimum)" : "");

            // Lastly, re-schedule future checks
            start();
        }
    }

    /**
     * For every table with NodeSync enabled, check the data size on the data to see if we should increase the number
     * of segments used for that table (increase the "depth").
     */
    private class SizeChecker extends ScheduledTask
    {
        long delayInSec()
        {
            return SIZE_CHECKING_DELAY_SEC;
        }

        public void run()
        {
            NodeSyncHelpers.nodeSyncEnabledStores().forEach(this::checkTable);
        }

        private void checkTable(ColumnFamilyStore store)
        {
            // Note that method can execute concurrently with a schema change, which in practice means that the table
            // may stop being "NodeSync-enabled" at any moment. This could show in a few places below:
            //   - The scheduler may not have a proposer for the table (both in the getProposer() call, or in the
            //     replace() calls done if we do attempt an update).
            //   - We could have 0 ranges for the tables (in the case where the keyspace RF just got set to 0).
            //   - ContinuousTableProposer.create() can return an non-present optional.
            // In all of those cases, we simply ignore the table.

            TableMetadata table = store.metadata();
            // Note that this will _not_ load the state if it isn't already, so can return null.
            TableState state = instance.state.get(table);
            if (state == null)
                return;

            long size = NodeSyncHelpers.estimatedSizeOf(store);
            int currentDepth = state.depth();
            Collection<Range<Token>> localRanges = state.localRanges();

            // The naive approach here would be compute the depth for the current size of the table, compare it to the
            // one in the existing proposer, and replace said proposer if the 2 depths differ. In practice however,
            // changing the depth is not something we want to do too often and it would be unproductive to change
            // it all the time due to small size fluctuations, which this naive approach could be prone to.
            //
            // So we take some safety margin: to know if we should increase the depth, we compute the depth for a higher
            // target segment size than we actually want: if the computed depth that way is still bigger than our
            // current one, then it means the data size if substantially bigger than it was when we computed the
            // existing one. Similarly, to know if we should decrease the depth, we use a smaller target segment size.

            // Check if we should increase the depth
            long segSizeIncr = NodeSyncHelpers.segmentSizeTarget() + (NodeSyncHelpers.segmentSizeTarget()/4);
            int newDepth = Segments.depth(size, localRanges.size(), segSizeIncr);
            if (newDepth > currentDepth)
            {
                updateDepth(state, size, currentDepth, newDepth, "Increasing");
                return;
            }

            if (currentDepth == 0)
                return;

            // Check if we should decrease the depth: we take a bigger margin than on increase because 1) depth decrease
            // has more impact than increases (basically, the first post-decrease validations will have a consolidate
            // 2 smaller segments, which is a lossy operation), 2) having data truly decreasing long term should be
            // pretty rare in practice, it's not a natural tendency for a database and 3) having too much segments for
            // the data size is only a slight inefficiency in the first place.
            long segSizeDecr = NodeSyncHelpers.segmentSizeTarget() / 2;
            newDepth = Segments.depth(size, localRanges.size(), segSizeDecr);
            if (newDepth < currentDepth)
                updateDepth(state, size, currentDepth, newDepth, "Decreasing");
        }

        private void updateDepth(TableState state, long tableSize, int currentDepth, int newDepth, String action)
        {
            TableMetadata table = state.table();
            Collection<Range<Token>> localRanges = state.localRanges();
            int currSegCount = Segments.estimateSegments(localRanges, currentDepth);
            int newSegCount = Segments.estimateSegments(localRanges, newDepth);
            SizeValue segmentTarget = SizeValue.of(NodeSyncHelpers.segmentSizeTarget(), SizeUnit.BYTES);
            SizeValue currentPerSegment = SizeValue.of(tableSize / currSegCount, SizeUnit.BYTES);
            SizeValue newPerSegment = SizeValue.of(tableSize / newSegCount, SizeUnit.BYTES);
            logger.info("{} number of segments for table {} (from {} to {}) to account for recent data size change. " +
                        "Table size is {} and current depth is {}, so ~{} per segment for a target of maximum {} per segment; " +
                        "{} depth to {}, so ~{} per segment after update.",
                        action, table, currSegCount, newSegCount,
                        Units.toString(tableSize, SizeUnit.BYTES), currentDepth, currentPerSegment , segmentTarget,
                        action.toLowerCase(), newDepth, newPerSegment);
            state.update(newDepth);
        }
    }

}
