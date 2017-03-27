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

package org.apache.cassandra.locator;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Snapshot;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.metrics.Histogram;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;


/**
 * A dynamic snitch that sorts endpoints by latency with an adapted phi failure detector
 */
public class DynamicEndpointSnitch extends AbstractEndpointSnitch implements ILatencySubscriber, DynamicEndpointSnitchMBean
{
    private static final boolean USE_SEVERITY = !Boolean.getBoolean("cassandra.ignore_dynamic_snitch_severity");

    // legacy value corresponding to a window size of 100 when using com.codahale.metrics.ExponentiallyDecayingReservoi
    static final long MAX_LATENCY = 107964792;

    // we set the histogram update interval to zero, which means the histogram will update itself each time
    // a read is performed, rather than periodically, this means we can only consult histogram values in
    // a thread safe way, which is ok, since we currently only consult values in a periodic task
    private static final int HISTOGRAM_UPDATE_INTERVAL_MILLIS = 0;

    private volatile int dynamicUpdateInterval = DatabaseDescriptor.getDynamicUpdateInterval();
    private volatile int dynamicResetInterval = DatabaseDescriptor.getDynamicResetInterval();
    private volatile double dynamicBadnessThreshold = DatabaseDescriptor.getDynamicBadnessThreshold();

    // the score for a merged set of endpoints must be this much worse than the score for separate endpoints to
    // warrant not merging two ranges into a single range
    private static final double RANGE_MERGING_PREFERENCE = 1.5;

    private String mbeanName;
    private boolean registered = false;

    private volatile Map<InetAddress, Double> scores = Collections.emptyMap();
    private final ConcurrentHashMap<InetAddress, Histogram> samples = new ConcurrentHashMap<>();

    public final IEndpointSnitch subsnitch;

    @Nullable // may be null for testing
    private volatile ScheduledFuture<?> updateSchedular;

    @Nullable // may be null for testing
    private volatile ScheduledFuture<?> resetSchedular;

    private static final double percentile = .9;

    public DynamicEndpointSnitch(IEndpointSnitch snitch)
    {
        this(snitch, null, true);
    }

    public DynamicEndpointSnitch(IEndpointSnitch snitch, String instance, boolean useScheduler)
    {
        mbeanName = "org.apache.cassandra.db:type=DynamicEndpointSnitch";
        if (instance != null)
            mbeanName += ",instance=" + instance;
        subsnitch = snitch;

        if (DatabaseDescriptor.isDaemonInitialized())
        {
            if (useScheduler)
            {
                updateSchedular = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::updateScores, dynamicUpdateInterval, dynamicUpdateInterval, TimeUnit.MILLISECONDS);
                resetSchedular = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::reset, dynamicResetInterval, dynamicResetInterval, TimeUnit.MILLISECONDS);
            }

            registerMBean();
        }
    }

    /**
     * Update configuration from {@link DatabaseDescriptor} and estart the update-scheduler and reset-scheduler tasks
     * if the configured rates for these tasks have changed.
     */
    public void applyConfigChanges()
    {
        if (dynamicUpdateInterval != DatabaseDescriptor.getDynamicUpdateInterval())
        {
            dynamicUpdateInterval = DatabaseDescriptor.getDynamicUpdateInterval();
            if (DatabaseDescriptor.isDaemonInitialized())
            {
                updateSchedular.cancel(false);
                updateSchedular = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::updateScores, dynamicUpdateInterval, dynamicUpdateInterval, TimeUnit.MILLISECONDS);
            }
        }

        if (dynamicResetInterval != DatabaseDescriptor.getDynamicResetInterval())
        {
            dynamicResetInterval = DatabaseDescriptor.getDynamicResetInterval();
            if (DatabaseDescriptor.isDaemonInitialized())
            {
                resetSchedular.cancel(false);
                resetSchedular = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::reset, dynamicResetInterval, dynamicResetInterval, TimeUnit.MILLISECONDS);
            }
        }

        dynamicBadnessThreshold = DatabaseDescriptor.getDynamicBadnessThreshold();
    }

    private void registerMBean()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(mbeanName));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void close()
    {
        if (updateSchedular != null)
            updateSchedular.cancel(false);

        if (resetSchedular != null)
            resetSchedular.cancel(false);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.unregisterMBean(new ObjectName(mbeanName));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void gossiperStarting()
    {
        subsnitch.gossiperStarting();
    }

    public String getRack(InetAddress endpoint)
    {
        return subsnitch.getRack(endpoint);
    }

    public String getDatacenter(InetAddress endpoint)
    {
        return subsnitch.getDatacenter(endpoint);
    }

    public List<InetAddress> getSortedListByProximity(final InetAddress address, Collection<InetAddress> addresses)
    {
        List<InetAddress> list = new ArrayList<>(addresses);
        sortByProximity(address, list);
        return list;
    }

    @Override
    public void sortByProximity(final InetAddress address, List<InetAddress> addresses)
    {
        assert address.equals(FBUtilities.getBroadcastAddress()); // we only know about ourself
        if (dynamicBadnessThreshold == 0)
        {
            sortByProximityWithScore(address, addresses);
        }
        else
        {
            sortByProximityWithBadness(address, addresses);
        }
    }

    private void sortByProximityWithScore(final InetAddress address, List<InetAddress> addresses)
    {
        // Scores can change concurrently from a call to this method. But Collections.sort() expects
        // its comparator to be "stable", that is 2 endpoint should compare the same way for the duration
        // of the sort() call. As we copy the scores map on write, it is thus enough to alias the current
        // version of it during this call.
        Map<InetAddress, Double> scores = this.scores;
        addresses.sort((a1, a2) -> compareEndpoints(address, a1, a2, scores));
    }

    private void sortByProximityWithBadness(final InetAddress address, List<InetAddress> addresses)
    {
        if (addresses.size() < 2)
            return;

        subsnitch.sortByProximity(address, addresses);
        Map<InetAddress, Double> scores = this.scores; // Make sure the score don't change in the middle of the loop below
                                                       // (which wouldn't really matter here but its cleaner that way).
        ArrayList<Double> subsnitchOrderedScores = new ArrayList<>(addresses.size());
        for (InetAddress inet : addresses)
        {
            Double score = scores.get(inet);
            if (score == null)
                continue;
            subsnitchOrderedScores.add(score);
        }

        // Sort the scores and then compare them (positionally) to the scores in the subsnitch order.
        // If any of the subsnitch-ordered scores exceed the optimal/sorted score by dynamicBadnessThreshold, use
        // the score-sorted ordering instead of the subsnitch ordering.
        ArrayList<Double> sortedScores = new ArrayList<>(subsnitchOrderedScores);
        Collections.sort(sortedScores);

        Iterator<Double> sortedScoreIterator = sortedScores.iterator();
        for (Double subsnitchScore : subsnitchOrderedScores)
        {
            if (subsnitchScore > (sortedScoreIterator.next() * (1.0 + dynamicBadnessThreshold)))
            {
                sortByProximityWithScore(address, addresses);
                return;
            }
        }
    }

    // Compare endpoints given an immutable snapshot of the scores
    private int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2, Map<InetAddress, Double> scores)
    {
        double scored1 = scores.getOrDefault(a1, 0.0d);
        double scored2 = scores.getOrDefault(a2, 0.0d);

        int cmp = Double.compare(scored1, scored2);

        return cmp != 0 ? cmp : subsnitch.compareEndpoints(target, a1, a2);
    }

    public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
    {
        // That function is fundamentally unsafe because the scores can change at any time and so the result of that
        // method is not stable for identical arguments. This is why we don't rely on super.sortByProximity() in
        // sortByProximityWithScore().
        throw new UnsupportedOperationException("You shouldn't wrap the DynamicEndpointSnitch (within itself or otherwise)");
    }

    public void receiveTiming(Verb<?, ?> verb, InetAddress host, long latency) // this is cheap
    {
        // We're only tracking reads
        if (verb != Verbs.READS.READ)
            return;

        // prevent the histogram from overflowing
        if (MAX_LATENCY < latency)
            latency = MAX_LATENCY;

        Histogram sample = samples.get(host);
        if (sample == null)
            // using samples.get() before the computeIfAbsent gives a slight perf improvement
            sample = samples.computeIfAbsent(host, (h) -> Histogram.make(true, MAX_LATENCY, HISTOGRAM_UPDATE_INTERVAL_MILLIS, false));

        sample.update(latency);
    }

    /**
     * This is either called by the scheduler every dynamic_snitch_update_interval_in_ms milliseconds
     * or directly by the unit tests.
     */
    @VisibleForTesting
    void updateScores() // this is expensive
    {
        if (!StorageService.instance.isGossipActive())
            return;

        if (!registered)
        {
            if (MessagingService.instance() != null)
            {
                MessagingService.instance().register(this);
                registered = true;
            }

        }
        double maxLatency = 1;

        // First get the entries snapshots, the histogram will update itself so we do this only once,
        // also note that this is not thread safe, which is OK since this method is only called by
        // the scheduled tasks scheduler
        Map<InetAddress, Snapshot> snapshots = samples.entrySet()
                                                      .stream()
                                                      .collect(Collectors.toMap(Map.Entry::getKey,
                                                                                entry -> entry.getValue().getSnapshot()));

        // We're going to weight the latency for each host against the worst one we see, to
        // arrive at sort of a 'badness percentage' for them. First, find the worst for each:
        HashMap<InetAddress, Double> newScores = new HashMap<>();
        for (Map.Entry<InetAddress, Snapshot> entry : snapshots.entrySet())
        {
            double mean = entry.getValue().getValue(percentile);
            if (mean > maxLatency)
                maxLatency = mean;
        }
        // now make another pass to do the weighting based on the maximums we found before
        for (Map.Entry<InetAddress, Snapshot> entry: snapshots.entrySet())
        {
            double score = entry.getValue().getValue(percentile) / maxLatency;
            // finally, add the severity without any weighting, since hosts scale this relative to their own load and the size of the task causing the severity.
            // "Severity" is basically a measure of compaction activity (CASSANDRA-3722).
            if (USE_SEVERITY)
                score += getSeverity(entry.getKey());
            // lowest score (least amount of badness) wins.
            newScores.put(entry.getKey(), score);
        }
        scores = newScores;
    }

    private void reset()
    {
        // we do this so that a host considered bad has a chance to recover, otherwise would we never try
        // to read from it, which would cause its score to never change
       samples.clear();
    }

    public Map<InetAddress, Double> getScores()
    {
        return scores;
    }

    public int getUpdateInterval()
    {
        return dynamicUpdateInterval;
    }
    public int getResetInterval()
    {
        return dynamicResetInterval;
    }
    public double getBadnessThreshold()
    {
        return dynamicBadnessThreshold;
    }

    public String getSubsnitchClassName()
    {
        return subsnitch.getClass().getName();
    }

    public List<Double> dumpTimings(String hostname) throws UnknownHostException
    {
        return dumpTimings(InetAddress.getByName(hostname));
    }

    public List<Double> dumpTimings(InetAddress host) throws UnknownHostException
    {
        ArrayList<Double> timings = new ArrayList<>();
        Histogram sample = samples.get(host);
        if (sample != null)
        {
            long[] values = sample.getSnapshot().getValues();
            long[] bucketOffsets = sample.getOffsets();
            for (int i = 0; i < values.length; i++)
            {
                for (long l = 0; l < values[i]; l++)
                {
                    timings.add((double) bucketOffsets[i]);
                }
            }
        }
        return timings;
    }

    public void setSeverity(double severity)
    {
        Gossiper.instance.addLocalApplicationState(ApplicationState.SEVERITY, StorageService.instance.valueFactory.severity(severity));
    }

    private double getSeverity(InetAddress endpoint)
    {
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (state == null)
            return 0.0;

        VersionedValue event = state.getApplicationState(ApplicationState.SEVERITY);
        if (event == null)
            return 0.0;

        return Double.parseDouble(event.value);
    }

    public double getSeverity()
    {
        return getSeverity(FBUtilities.getBroadcastAddress());
    }

    public boolean isWorthMergingForRangeQuery(List<InetAddress> merged, List<InetAddress> l1, List<InetAddress> l2)
    {
        if (!subsnitch.isWorthMergingForRangeQuery(merged, l1, l2))
            return false;

        // skip checking scores in the single-node case
        if (l1.size() == 1 && l2.size() == 1 && l1.get(0).equals(l2.get(0)))
            return true;

        // Make sure we return the subsnitch decision (i.e true if we're here) if we lack too much scores
        double maxMerged = maxScore(merged);
        double maxL1 = maxScore(l1);
        double maxL2 = maxScore(l2);
        if (maxMerged < 0 || maxL1 < 0 || maxL2 < 0)
            return true;

        return maxMerged <= (maxL1 + maxL2) * RANGE_MERGING_PREFERENCE;
    }

    // Return the max score for the endpoint in the provided list, or -1.0 if no node have a score.
    private double maxScore(List<InetAddress> endpoints)
    {
        double maxScore = -1.0;
        for (InetAddress endpoint : endpoints)
        {
            Double score = scores.get(endpoint);
            if (score == null)
                continue;

            if (score > maxScore)
                maxScore = score;
        }
        return maxScore;
    }
}
