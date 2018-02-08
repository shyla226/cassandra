/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 *
 */
package com.datastax.bdp.db.upgrade;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.utils.*;

/**
 * Keeps track of the lowest and highest {@link org.apache.cassandra.gms.ApplicationState#RELEASE_VERSION release versions},
 * the schema-version and whether there is schema-agreement in the cluster.
 *
 * Callbacks to registered {@link ClusterVersionListener}s are issued, when either the min/max release-versions
 * change, whether there is schema-agreement or not, the schema-version changes.
 *
 * The "production" implementation via {@link org.apache.cassandra.gms.Gossiper#registerUpgradeBarrierListener() Gossiper.registerUpgradeBarrierListener()}
 * triggers {@link #scheduleUpdateVersions()}, which does not block the Gossiper's thread.
 *
 * TODO it would be more convenient in the future to have the DSE version here as well,
 * which is embedded in one "custom" application-state representing all of com.datastax.bdp.gms.DseState.
 * But currently, we do not handle the DSE version in Gossip in Apollo at all.
 */
public class ClusterVersionBarrier
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterVersionBarrier.class);
    public static final CassandraVersion ownOssVersion = new CassandraVersion(FBUtilities.getReleaseVersionString());

    private volatile ClusterVersionInfo current = new ClusterVersionInfo(new CassandraVersion("0.0.0"), new CassandraVersion("0.0.0"), UUID.randomUUID(), true);

    private final CopyOnWriteArrayList<ClusterVersionListener> listeners = new CopyOnWriteArrayList<>();

    // The 'scheduled' atomic flag guarantees that we have at max one outstanding scheduled
    // run of updateVersionsBlocking().
    private volatile int scheduled;
    private static final AtomicIntegerFieldUpdater<ClusterVersionBarrier> scheduledUpdater = AtomicIntegerFieldUpdater.newUpdater(ClusterVersionBarrier.class, "scheduled");

    // Registered listeners will only be called when this flag is set to true.
    // This is to prevent lots of changes being processed during startup.
    private boolean ready;

    /**
     * In production code, this is a method reference to {@link org.apache.cassandra.gms.Gossiper#getAllEndpoints() Gossiper.getAllEndpoints()}.
     * The function reference helps to keep references ot {@link org.apache.cassandra.gms.Gossiper} class away.
     * Unit tests use different functions.
     */
    private final Supplier<Iterable<InetAddress>> endpointsSupplier;

    /**
     * In production code, this is a method reference to {@link org.apache.cassandra.gms.Gossiper#getEndpointInfo(InetAddress) Gossiper.getEndpointInfo(InetAddress)}.
     * The function reference helps to keep references ot {@link org.apache.cassandra.gms.Gossiper} class away.
     * Unit tests use different functions.
     */
    private final Function<InetAddress, EndpointInfo> endpointInfoFunction;

    /**
     * @param endpointsSupplier usually a method reference to {@link org.apache.cassandra.gms.Gossiper#getAllEndpoints() Gossiper.getAllEndpoints()}.
     * @param endpointInfoFunction usually a method reference to {@link org.apache.cassandra.gms.Gossiper#getEndpointInfo(InetAddress) Gossiper.getEndpointInfo(InetAddress)}.
     */
    public ClusterVersionBarrier(Supplier<Iterable<InetAddress>> endpointsSupplier, Function<InetAddress, EndpointInfo> endpointInfoFunction)
    {
        this.endpointsSupplier = endpointsSupplier;
        this.endpointInfoFunction = endpointInfoFunction;
    }

    /**
     * Trigger an async call to {@link #updateVersionsBlocking()}.
     * Safe to be called from the Gossip thread.
     */
    public void scheduleUpdateVersions()
    {
        if (scheduledUpdater.compareAndSet(this, 0, 1))
        {
            ScheduledExecutors.nonPeriodicTasks.execute(() -> {
                scheduledUpdater.set(this, 0);
                updateVersionsBlocking();
            });
        }
    }

    public ClusterVersionInfo currentClusterVersionInfo()
    {
        return current;
    }

    @VisibleForTesting
    boolean hasScheduledUpdate()
    {
        return scheduledUpdater.get(this) != 0;
    }

    /**
     * Called asynchronously after an endpoint state change.
     */
    public synchronized void updateVersionsBlocking()
    {
        ClusterVersionInfo versions = computeClusterVersionInfo();

        if (!current.equals(versions) && ready)
        {
            current = versions;

            // inform via listener ?? call listener on registration !
            // keep booleans around ??
            // tie to "node running in normal mode"

            logger.trace("updateVersionsBlocking - calling listeners");

            for (ClusterVersionListener listener : listeners)
                listener.clusterVersionUpdated(versions);
        }
    }

    private ClusterVersionInfo computeClusterVersionInfo()
    {
        logger.trace("computeClusterVersionInfo - start computing");

        CassandraVersion minOss = null;
        CassandraVersion maxOss = null;
        UUID schema = null;
        boolean schemaAgreement = true;
        for (InetAddress ep : endpointsSupplier.get())
        {
            EndpointInfo epInfo = endpointInfoFunction.apply(ep);

            logger.trace("computeClusterVersionInfo - endpoint {} : {}", ep, epInfo);

            if (epInfo == null)
                continue;

            CassandraVersion epVer = epInfo.version;
            if (epVer != null)
            {
                if (minOss == null || epVer.compareTo(minOss) < 0)
                    minOss = epVer;
                if (maxOss == null || epVer.compareTo(maxOss) > 0)
                    maxOss = epVer;
            }

            UUID schemaVer = epInfo.schemaVersion;
            if (schemaVer != null)
            {
                if (schema == null)
                    schema = schemaVer;
                else if (!schema.equals(schemaVer))
                    schemaAgreement = false;
            }
            else
            {
                // no schema-version from an endpoint - assume that there is no agreement
                schemaAgreement = false;
            }
        }

        // It should not happen, but ... if we get not even one release_version from Gossiper,
        // assume our own release_version.
        if (minOss == null)
            minOss = ownOssVersion;
        if (maxOss == null)
            maxOss = ownOssVersion;

        ClusterVersionInfo clusterVersionInfo = new ClusterVersionInfo(minOss, maxOss, schemaAgreement ? schema : null, schemaAgreement);

        logger.trace("computeClusterVersionInfo - result={}", clusterVersionInfo);

        return clusterVersionInfo;
    }

    /**
     * Call before the daemon starts accepting client requests but after the node has joined the ring and
     * is processing gossip requests. This method blocks.
     */
    public synchronized void onLocalNodeReady()
    {
        if (!ready)
        {
            ready = true;
            updateVersionsBlocking();
        }
    }

    /**
     * Register a new listener.
     * If this instance is "ready" (i.e. {@link #onLocalNodeReady()} has been called before,
     * the listener's {@link ClusterVersionListener#clusterVersionUpdated(ClusterVersionInfo)} method
     * will be called synchronously.
     */
    public synchronized void register(ClusterVersionListener listener)
    {
        if (ready)
            listener.clusterVersionUpdated(current);
        listeners.add(listener);
    }

    @VisibleForTesting
    public synchronized void removeAllListeners()
    {
        listeners.clear();
    }

    /**
     * Information about an endpoint's release-version and schema-version.
     */
    public static final class EndpointInfo
    {
        public final CassandraVersion version;
        public final UUID schemaVersion;

        public EndpointInfo(CassandraVersion version, UUID schemaVersion)
        {
            this.version = version;
            this.schemaVersion = schemaVersion;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EndpointInfo that = (EndpointInfo) o;
            return Objects.equals(version, that.version) &&
                   Objects.equals(schemaVersion, that.schemaVersion);
        }

        public int hashCode()
        {
            return Objects.hash(version, schemaVersion);
        }

        @Override
        public String toString()
        {
            return "EndpointInfo{version=" + version +
                   ", schemaVersion=" + schemaVersion + '}';
        }
    }

    /**
     * Immutable info about the cluster.
     */
    public static final class ClusterVersionInfo
    {
        /**
         * Smallest {@link org.apache.cassandra.gms.ApplicationState#RELEASE_VERSION}.
         */
        public final CassandraVersion minOss;
        /**
         * Highest {@link org.apache.cassandra.gms.ApplicationState#RELEASE_VERSION}.
         */
        public final CassandraVersion maxOss;
        /**
         * Schema version - set if there is schema-agreement, {@code null} if there
         * is schema-disagreement. This field, although maybe unused, helps to trigger
         * a callback if the schema has changed on all nodes without an intermediate
         * schema-disagreement, which might be important for callbacks like the
         * implementation in {@link VersionDependentFeature}.
         */
        public final UUID schemaVersion;
        /**
         * Flag whether there is schema agreement.
         */
        public final boolean schemaAgreement;

        public ClusterVersionInfo(CassandraVersion minOss, CassandraVersion maxOss, UUID schemaVersion, boolean schemaAgreement)
        {
            this.minOss = minOss;
            this.maxOss = maxOss;
            this.schemaVersion = schemaVersion;
            this.schemaAgreement = schemaAgreement;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClusterVersionInfo that = (ClusterVersionInfo) o;
            return Objects.equals(minOss, that.minOss) &&
                   Objects.equals(maxOss, that.maxOss) &&
                   Objects.equals(schemaVersion, that.schemaVersion) &&
                   schemaAgreement == that.schemaAgreement;
        }

        public int hashCode()
        {
            return Objects.hash(minOss, maxOss, schemaVersion, schemaAgreement);
        }

        @Override
        public String toString()
        {
            return "ClusterVersionInfo{minOss=" + minOss + ", maxOss=" + maxOss + ", schemaVersion=" + schemaVersion + ", schemaAgreement=" + schemaAgreement + '}';
        }
    }

    public interface ClusterVersionListener
    {
        /**
         * Called with the minimum and maximum {@link org.apache.cassandra.gms.ApplicationState#RELEASE_VERSION release versions}
         * used by the nodes in the cluster. Only invoked to inform about the initial state and when
         * minimum or maximum version, schema version/schema agreement changes.
         *
         * Implementations should not block.
         */
        void clusterVersionUpdated(ClusterVersionInfo versionInfo);
    }
}
