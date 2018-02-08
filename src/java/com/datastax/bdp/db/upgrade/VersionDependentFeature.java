/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 *
 */
package com.datastax.bdp.db.upgrade;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.utils.CassandraVersion;

/**
 * @param <I> base class for the code using this feature.
 */
public abstract class VersionDependentFeature<I extends VersionDependentFeature.VersionDependent>
{
    private static final Logger logger = LoggerFactory.getLogger(VersionDependentFeature.class);
    private static final long postDdlChangeCheckMillis = Long.getLong("dse.versionDependentFeature.postDdlChangeCheckMillis", 1_000);

    private FeatureStatus status = FeatureStatus.UNKNOWN;

    private final I currentImplementation;
    private final I legacyImplementation;

    private final String name;
    private final CassandraVersion minimumVersion;

    private boolean currentInitialized;
    private boolean legacyInitialized;

    private Future<?> ddlFuture;

    private ClusterVersionBarrier clusterVersionBarrier;

    /**
     * @param name                 some name that appears in log messages
     * @param minimumVersion       the minimum version required on all nodes to activate the feature
     * @param legacyImplementation the legacy implementation
     * @param currentImplementation the current implementation
     */
    public VersionDependentFeature(String name,
                                   CassandraVersion minimumVersion,
                                   I legacyImplementation,
                                   I currentImplementation)
    {
        this.name = name;
        this.minimumVersion = minimumVersion;
        this.legacyImplementation = legacyImplementation;
        this.currentImplementation = currentImplementation;
    }

    /**
     * Convenience method that constructs a {@link VersionDependentFeature} using one {@link SchemaUpgrade} and
     * does some logging.
     *
     * @param name                 some name that appears in log messages
     * @param minimumVersion       the minimum version required on all nodes to activate the feature
     * @param legacyImplementation the legacy implementation
     * @param currentImplementation the current implementation
     * @param schemaUpgrade        schema upgrade instance to use
     * @param logger               logger to use to log the messages
     * @param messageActivating    message to log when activating the feature
     * @param messageActivated     message to log when the feature's been activated
     * @param messageDeactivated   message to log when the feature's been deactivated
     * @param <I>                  implementation of {@link VersionDependent}
     */
    public static <I extends VersionDependentFeature.VersionDependent> VersionDependentFeature<I> createForSchemaUpgrade(String name,
                                                                                                                         CassandraVersion minimumVersion,
                                                                                                                         I legacyImplementation,
                                                                                                                         I currentImplementation,
                                                                                                                         SchemaUpgrade schemaUpgrade,
                                                                                                                         Logger logger,
                                                                                                                         String messageActivating,
                                                                                                                         String messageActivated,
                                                                                                                         String messageDeactivated)
    {
        return new VersionDependentFeature<I>(name,
                                              minimumVersion,
                                              legacyImplementation,
                                              currentImplementation)
        {
            protected boolean ddlChangeRequired()
            {
                return schemaUpgrade.ddlChangeRequired();
            }

            @Override
            protected void executeDDL()
            {
                schemaUpgrade.executeDDL();
            }

            public void onFeatureActivating()
            {
                logger.info(messageActivating);
            }

            public void onFeatureActivated()
            {
                logger.info(messageActivated);
            }

            public void onFeatureDeactivated()
            {
                logger.info(messageDeactivated);
            }
        };
    }

    /**
     * Registers this instance against a {@link ClusterVersionBarrier} and stores
     * the reference for internal purposes (calling the own {@link ClusterVersionBarrier.ClusterVersionListener}
     * with the {@link ClusterVersionBarrier#currentClusterVersionInfo() current ClusterVersionInfo}.
     */
    public void setup(ClusterVersionBarrier clusterVersionBarrier)
    {
        clusterVersionBarrier.register(this::clusterVersionUpdated);
        this.clusterVersionBarrier = clusterVersionBarrier;
    }

    @VisibleForTesting
    public synchronized void clusterVersionUpdated(ClusterVersionBarrier.ClusterVersionInfo versionInfo)
    {
        logger.trace("clusterVersionUpdated for {}/{}: {}", name, status, versionInfo);

        boolean minVersionOK = minimumVersion.compareTo(versionInfo.minOss) <= 0;

        switch (status)
        {
            case UNKNOWN:
            case DEACTIVATED:
                if (minVersionOK && !ddlChangeRequired())
                {
                    // can transition directly to ACTIVATED without going via ACTIVATED if versions are ok and
                    // no DDL change required
                    updateStatus(FeatureStatus.ACTIVATED);
                }
                else
                {
                    if (minVersionOK) // DDL change required
                    {
                        // this change to DEACTIVATED before ACTIVATING usually results in a noop, if the
                        // current status was DEACTIVATED - but we need it in case it was UNKNOWN to initialize
                        // the legacy implementation
                        updateStatus(FeatureStatus.DEACTIVATED);
                        updateStatus(FeatureStatus.ACTIVATING);
                        // Schedule another call this method - although things usually change asynchronously
                        // via gossip callbacks, in a single-node environment and unit-tests we don't get any
                        // state-change-callbacks from Gossiper - so we have to do it on our own by calling
                        // ourselves again.
                        scheduleCallback();
                    }
                    else
                    {
                        // last resort - the new feature is just deactivated
                        updateStatus(FeatureStatus.DEACTIVATED);
                    }
                }
                break;
            case ACTIVATING:
                if (ddlChangeRequired())
                {
                    // DDL change required, wait for schema agreement before triggering DDL change
                    if (versionInfo.schemaAgreement)
                    {
                        maybeScheduleDDL();
                    }
                    // Note: do NOT schedule the DDL change if there is no schema agreement.
                    // Not having schema agreement indicates a non-healthy cluster status and
                    // issuing more DDLs may make the situation even worse. (Better safe than sorry.)
                }
                else
                {
                    // no DDL change required, wait for schema agreement before proceeding to ACTIVATED
                    if (versionInfo.schemaAgreement)
                    {
                        // We have schema agreement after the DDL change - safe to switch to ACTIVATED.
                        updateStatus(FeatureStatus.ACTIVATED);
                    }
                    // Note: do NOT switch to ACTIVATED, if there is no schema agreement.
                    // Not waiting for schema agreement may result in some nodes having the schema
                    // changed via 'executeDDL' and some not having it. This in turn would cause
                    // request failures due to missing/unknown columns.
                }
                break;
            case ACTIVATED:
                if (!minVersionOK)
                {
                    // Feature was activated, but at least one node uses a non-minimum version now,
                    // so we should disable the feature.
                    updateStatus(FeatureStatus.DEACTIVATED);
                }
                break;
        }
    }

    private synchronized void maybeScheduleDDL()
    {
        if (ddlFuture == null)
        {
            logger.info("Scheduling DDL change for '{}'", name);
            // TODO if we want to reduce the chance of concurrent schema changes, this is the
            // right place to do that.
            //
            // Currently, and as in the VersionBarrier class in the bdp repo, as soon as all nodes
            // are on the right version, and there is schema agreement via this class, the DDL
            // necessary to enable the feature will be scheduled by a lot of nodes in the cluster
            // nearly simultaneous. For distributed system keyspace, the change should be idempotent.
            //
            // However, with the dse_* keyspace the schema change is _not_ idempotent, as the schema
            // modifications are submitted using the _current timestamp_ as the writetimestamp. And
            // since the writetimestamp is part of the digest (schema version), the schema mutations
            // from all nodes are different.
            //
            // Other than that, just the number of schema mutations is very high. Each node has to
            // process number-of-nodes schema-pushes - a 100 node cluster therefore 100 schema pushes
            // in a very short timespan.
            //
            // Therefore it would be gentle to reduce the amount of schema pushes. The idea here is
            // simple:
            // * Compute an "execution chance" as an integer
            // * If the current node is a seed node, the "execution chance" is 1/number-of-seed-nodes.
            // * If the current node is not a seed node, the "execution chance" is number-of-seed-nodes+1/number-of-non-seed-nodes
            // * Multiply the computed chance by 100
            // * Use the multipled number as a delay to submit the executeDDL() task
            //
            // The current implementation just blindly submits the executeDDL() task.
            ddlFuture = ScheduledExecutors.nonPeriodicTasks.submit(() ->
                                                                   {
                                                                       try
                                                                       {
                                                                           executeDDL();
                                                                       }
                                                                       finally
                                                                       {
                                                                           ddlFinished();
                                                                           scheduleCallback();
                                                                       }
                                                                   });
        }
    }

    private synchronized void scheduleCallback()
    {
        ScheduledExecutors.nonPeriodicTasks.schedule(() -> clusterVersionUpdated(clusterVersionBarrier.currentClusterVersionInfo()),
                                                     postDdlChangeCheckMillis,
                                                     TimeUnit.MILLISECONDS);
    }

    private synchronized void ddlFinished()
    {
        logger.debug("DDL change for '{}' finished", name);
        ddlFuture = null;
    }

    private synchronized void updateStatus(FeatureStatus newStatus)
    {
        if (status != newStatus)
        {
            status = newStatus;
            logger.debug("New status for '{}': {}", name, newStatus);
            switch (newStatus)
            {
                case ACTIVATED:
                    if (!currentInitialized)
                    {
                        // Initialize the current implementation lazily as it probably relies
                        // on a table structure that's not present before the DDL change.
                        currentImplementation.initialize();
                        currentInitialized = true;
                    }
                    onFeatureActivated();
                    break;
                case ACTIVATING:
                    onFeatureActivating();
                    break;
                case DEACTIVATED:
                    if (!legacyInitialized)
                    {
                        // Initialize the current implementation lazily as it is usually not
                        // used. This is also symmetric to the use of the current implementation.
                        legacyImplementation.initialize();
                        legacyInitialized = true;
                    }
                    onFeatureDeactivated();
                    break;
                default:
                    throw new RuntimeException("Unknown new status " + newStatus);
            }
        }
    }

    public I implementation()
    {
        return status == FeatureStatus.ACTIVATED ? currentImplementation : legacyImplementation;
    }

    public FeatureStatus getStatus()
    {
        return status;
    }

    /**
     * Callback to test whether a DDL change is required.
     * Consider delegating to {@link SchemaUpgrade}.
     * <p>
     * This method should return quite quick.
     */
    protected abstract boolean ddlChangeRequired();

    /**
     * Callback to execute a DDL change. The implementation should check whether
     * the schema update is really necessary before actually executing it.
     * Consider delegating to {@link SchemaUpgrade}.
     * <p>
     * This method is probably called asynchronously.
     */
    protected abstract void executeDDL();

    /**
     * Called when the new feature is {@link FeatureStatus#DEACTIVATED not available).
     * The legacy implementation of {@link VersionDependent} is initialized,
     * when this method is called.
     * Whether current implementation of {@link VersionDependent} is initialized,
     * is undefined.
     */
    public abstract void onFeatureDeactivated();

    /**
     * Called when transitioning from {@link FeatureStatus#DEACTIVATED} to
     * {@link FeatureStatus#ACTIVATED}.
     * DDL changes may happen in this phase.
     * If DDL changes are not necessary, the status may jump directly from
     * {@link FeatureStatus#DEACTIVATED} to * {@link FeatureStatus#ACTIVATED}
     * skipping {@link FeatureStatus#ACTIVATING}.
     */
    public abstract void onFeatureActivating();

    /**
     * Called when the new feature is {@link FeatureStatus#ACTIVATED available).
     * The current implementation of {@link VersionDependent} is initialized,
     * when this method is called.
     * Whether legacy implementation of {@link VersionDependent} is initialized,
     * is undefined.
     */
    public abstract void onFeatureActivated();

    @Override
    public String toString()
    {
        return "VersionDependentFeature{" +
               "name='" + name + '\'' +
               ", minimumVersion=" + minimumVersion +
               ", status=" + status +
               '}';
    }

    public enum FeatureStatus
    {
        /**
         * Initial status when the current status is unknown (before the initial
         * callback to {@link #clusterVersionUpdated(ClusterVersionBarrier.ClusterVersionInfo)}.
         */
        UNKNOWN,
        /**
         * Feature not activated - see {@link #onFeatureDeactivated()}.
         */
        DEACTIVATED,
        /**
         * Feature activating - see {@link #onFeatureActivating()}.
         */
        ACTIVATING,
        /**
         * Feature activated - see {@link #onFeatureActivated()}.
         */
        ACTIVATED
    }

    public interface VersionDependent
    {
        /**
         * Called before either the legacy or current feature-implementation's first use.
         * It is safe to prepare statements in an implementation of this method, if those
         * require a DDL change - e.g. via {@link SchemaUpgrade}.
         * <p>
         * Implementations should not block for a long time. Short-ish blocks to prepare
         * statements are ok.
         */
        void initialize();
    }
}
