/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 *
 */

package com.datastax.bdp.db.upgrade;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;

import org.junit.Test;

import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.*;

public class ClusterVersionBarrierTest
{
    private static final UUID someVersion = UUID.randomUUID();

    @Test
    public void testAllOnSameVersion()
    {
        // ten node cluster - all nodes on the same versios
        ClusterVersionBarrier upgradeBarrier = new ClusterVersionBarrier(ClusterVersionBarrierTest::tenMembers, (ep) -> info(currentVersion()));

        Collector collector = new Collector();
        upgradeBarrier.register(collector);

        assertEquals(0, collector.invocations);

        upgradeBarrier.onLocalNodeReady();
        assertEquals(1, collector.invocations);
        assertEquals(currentVersion(), collector.min);
        assertEquals(currentVersion(), collector.max);

        upgradeBarrier.updateVersionsBlocking();
        assertEquals(1, collector.invocations);
        assertEquals(currentVersion(), collector.min);
        assertEquals(currentVersion(), collector.max);
    }

    @Test
    public void testManyDifferent()
    {
        // Test to ensure that different versions do not confuse the ClusterVersionBarrier implementation

        // ten node cluster - all nodes either
        // - on the same version
        // - on the next major version, but a different patch level
        // - on the previous major version, but a different patch level
        AtomicInteger manyDifferentTrigger = new AtomicInteger(0);
        ClusterVersionBarrier upgradeBarrier = new ClusterVersionBarrier(ClusterVersionBarrierTest::tenMembers,
                                                                         (ep) -> {
                                                                             switch (manyDifferentTrigger.get())
                                                                             {
                                                                                 case 0:
                                                                                     return info(currentVersion());
                                                                                 case 1:
                                                                                     return info(new CassandraVersion((ClusterVersionBarrier.ownOssVersion.major + 1) + "." + ep.getAddress()[3]));
                                                                                 case -1:
                                                                                     return info(new CassandraVersion((ClusterVersionBarrier.ownOssVersion.major - 1) + "." + ep.getAddress()[3]));
                                                                             }
                                                                             throw new AssertionError("oops");
                                                                         });

        Collector collector = new Collector();
        upgradeBarrier.register(collector);

        assertEquals(0, collector.invocations);

        upgradeBarrier.onLocalNodeReady();
        assertEquals(1, collector.invocations);
        assertEquals(currentVersion(), collector.min);
        assertEquals(currentVersion(), collector.max);

        upgradeBarrier.updateVersionsBlocking();
        assertEquals(1, collector.invocations);
        assertEquals(currentVersion(), collector.min);
        assertEquals(currentVersion(), collector.max);

        // up to here, the result should be the same as for testAllOnSameVersion()

        manyDifferentTrigger.set(1);

        upgradeBarrier.updateVersionsBlocking();
        assertEquals(2, collector.invocations);
        assertEquals(new CassandraVersion((ClusterVersionBarrier.ownOssVersion.major + 1) + ".1"), collector.min);
        assertEquals(new CassandraVersion((ClusterVersionBarrier.ownOssVersion.major + 1) + ".10"), collector.max);

        upgradeBarrier.updateVersionsBlocking();
        assertEquals(2, collector.invocations);
        assertEquals(new CassandraVersion((ClusterVersionBarrier.ownOssVersion.major + 1) + ".1"), collector.min);
        assertEquals(new CassandraVersion((ClusterVersionBarrier.ownOssVersion.major + 1) + ".10"), collector.max);

        manyDifferentTrigger.set(-1);

        upgradeBarrier.updateVersionsBlocking();
        assertEquals(3, collector.invocations);
        assertEquals(new CassandraVersion((ClusterVersionBarrier.ownOssVersion.major - 1) + ".1"), collector.min);
        assertEquals(new CassandraVersion((ClusterVersionBarrier.ownOssVersion.major - 1) + ".10"), collector.max);

        upgradeBarrier.updateVersionsBlocking();
        assertEquals(3, collector.invocations);
        assertEquals(new CassandraVersion((ClusterVersionBarrier.ownOssVersion.major - 1) + ".1"), collector.min);
        assertEquals(new CassandraVersion((ClusterVersionBarrier.ownOssVersion.major - 1) + ".10"), collector.max);
    }

    @Test
    public void testManyUpdates() throws InterruptedException
    {
        // There is a mechanism in ClusterVersionBarrier, that prevents running/scheduling
        // ClusterVersionBarrier.updateVersionsBlocking() a lot.
        // To test that mechanism, delay the execution of the suppliers and check the
        // number of these invocations.

        AtomicBoolean waitForLatch = new AtomicBoolean();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger increasingVersion = new AtomicInteger();
        CountDownLatch waiting = new CountDownLatch(1);

        ClusterVersionBarrier upgradeBarrier = new ClusterVersionBarrier(() ->
                                                                         {
                                                                             if (waitForLatch.get())
                                                                             {
                                                                                 waiting.countDown();
                                                                                 try
                                                                                 {
                                                                                     latch.await();
                                                                                 }
                                                                                 catch (InterruptedException e)
                                                                                 {
                                                                                     throw new RuntimeException(e);
                                                                                 }
                                                                             }
                                                                             return tenMembers();
                                                                         },
                                                                         // return a new version for each invocation to trigger callbacks to our Collector instance
                                                                         (ep) -> info(new CassandraVersion("4.0." + increasingVersion.incrementAndGet())));

        Collector collector = new Collector();
        upgradeBarrier.register(collector);

        // Schedules an invocation of Collector.clusterVersionUpdated()
        upgradeBarrier.onLocalNodeReady();

        assertEquals(1, collector.invocations);

        // block execution of ClusterVersionBarrier.updateVersionsBlocking()
        waitForLatch.set(true);

        // Trigger the first update (invocation of Collector.clusterVersionUpdated())
        upgradeBarrier.scheduleUpdateVersions();
        // wait until the first "instance" of ClusterVersionBarrier.updateVersionsBlocking() is blocked
        waiting.await(1, TimeUnit.SECONDS);
        // Trigger another update
        for (int i = 0; i < 100; i++)
            upgradeBarrier.scheduleUpdateVersions();

        // there should be one "instance" of ClusterVersionBarrier.updateVersionsBlocking() running and
        // one scheduled.
        assertTrue(upgradeBarrier.hasScheduledUpdate());

        // unblock ClusterVersionBarrier.updateVersionsBlocking()
        latch.countDown();

        // wait as long as the 2nd scheduled update is scheduled
        waitASec(upgradeBarrier::hasScheduledUpdate);
        assertFalse(upgradeBarrier.hasScheduledUpdate());

        // wait as long as the 2nd scheduled update is not finished
        waitASec(() -> collector.invocations < 3);

        // Our collector should have been called 3 times:
        // - for the invocation via ClusterVersionBarrier.onLocalNodeReady()
        // - via the first ClusterVersionBarrier.scheduleUpdateVersions()
        // - via another ClusterVersionBarrier.scheduleUpdateVersions()
        assertEquals(3, collector.invocations);
    }

    @Test
    public void testRollingUpgradeVersions()
    {
        // ten node cluster - version for node 2 in the reference
        AtomicReference<CassandraVersion> versionForTwo = new AtomicReference<>(currentVersion());
        ClusterVersionBarrier upgradeBarrier = new ClusterVersionBarrier(ClusterVersionBarrierTest::tenMembers,
                                                                         (ep) -> info(ep.getAddress()[3] == 2 ? versionForTwo.get() : ClusterVersionBarrier.ownOssVersion));

        Collector collector = new Collector();
        upgradeBarrier.register(collector);

        assertEquals(0, collector.invocations);

        upgradeBarrier.onLocalNodeReady();
        assertEquals(1, collector.invocations);
        assertEquals(currentVersion(), collector.min);
        assertEquals(currentVersion(), collector.max);

        upgradeBarrier.updateVersionsBlocking();
        assertEquals(1, collector.invocations);
        assertEquals(currentVersion(), collector.min);
        assertEquals(currentVersion(), collector.max);

        // up to here, the result should be the same as for testAllOnSameVersion()

        // node 2 upgraded

        versionForTwo.set(nextMajor());

        upgradeBarrier.updateVersionsBlocking();
        assertEquals(2, collector.invocations);
        assertEquals(currentVersion(), collector.min);
        assertEquals(versionForTwo.get(), collector.max);

        upgradeBarrier.updateVersionsBlocking();
        assertEquals(2, collector.invocations);
        assertEquals(currentVersion(), collector.min);
        assertEquals(versionForTwo.get(), collector.max);

        // node 2 upgraded again

        versionForTwo.set(nextMajorDot5());

        upgradeBarrier.updateVersionsBlocking();
        assertEquals(3, collector.invocations);
        assertEquals(currentVersion(), collector.min);
        assertEquals(versionForTwo.get(), collector.max);

        upgradeBarrier.updateVersionsBlocking();
        assertEquals(3, collector.invocations);
        assertEquals(currentVersion(), collector.min);
        assertEquals(versionForTwo.get(), collector.max);
    }

    @Test
    public void testRollingUpgradeWithSchema()
    {
        // simple simulation of a rolling upgrade with different schema versions

        // This test is similar to testVersionDependentFeature() -
        // that one uses an implementation of VersionDependentFeature instead of the Collector that
        // tests ClusterVersionBarrier.

        UUID schemaInitial = UUID.randomUUID();
        UUID schemaUpgraded1 = UUID.randomUUID();
        UUID schemaUpgraded2 = UUID.randomUUID();
        UUID schemaUpgradedFinal = UUID.randomUUID();

        CassandraVersion versionInitial = ClusterVersionBarrier.ownOssVersion;
        CassandraVersion versionFinal = nextMajor();

        // cluster with three nodes - release-version + schema-version via these arrays
        CassandraVersion[] cassandraVersions = new CassandraVersion[]{ versionInitial, versionInitial, versionInitial };
        UUID[] schemaVersions = new UUID[]{ schemaInitial, schemaInitial, schemaInitial };
        ClusterVersionBarrier upgradeBarrier = new ClusterVersionBarrier(ClusterVersionBarrierTest::threeMembers,
                                                                         (ep) -> new ClusterVersionBarrier.EndpointInfo(cassandraVersions[ep.getAddress()[3] - 1],
                                                                                                                        schemaVersions[ep.getAddress()[3] - 1]));

        Collector collector = new Collector();
        upgradeBarrier.register(collector);

        assertEquals(0, collector.invocations);

        upgradeBarrier.onLocalNodeReady();
        assertEquals(1, collector.invocations);
        assertEquals(versionInitial, collector.min);
        assertEquals(versionInitial, collector.max);
        assertTrue(collector.schemaAgreement);
        assertEquals(schemaInitial, collector.schemaVersion);

        // upgrade node 1
        cassandraVersions[0] = versionFinal;
        schemaVersions[0] = schemaUpgraded1;
        upgradeBarrier.updateVersionsBlocking();
        assertEquals(2, collector.invocations);
        assertEquals(versionInitial, collector.min);
        assertEquals(versionFinal, collector.max);
        assertFalse(collector.schemaAgreement);
        assertNull(collector.schemaVersion);

        // upgrade node 2
        cassandraVersions[1] = versionFinal;
        schemaVersions[1] = schemaUpgraded1;
        upgradeBarrier.updateVersionsBlocking();
        // NO update from ClusterVersionBarrier, as the fields are still the same
        assertEquals(2, collector.invocations);

        // upgrade node 3
        cassandraVersions[2] = versionFinal;
        schemaVersions[2] = schemaUpgraded1;
        upgradeBarrier.updateVersionsBlocking();
        assertEquals(3, collector.invocations);
        assertEquals(versionFinal, collector.min);
        assertEquals(versionFinal, collector.max);
        assertTrue(collector.schemaAgreement);
        assertEquals(schemaUpgraded1, collector.schemaVersion);

        // some schema changes after the upgrade
        schemaVersions[0] = schemaUpgraded2;
        upgradeBarrier.updateVersionsBlocking();
        assertEquals(4, collector.invocations);
        assertEquals(versionFinal, collector.min);
        assertEquals(versionFinal, collector.max);
        assertFalse(collector.schemaAgreement);
        assertNull(collector.schemaVersion);

        schemaVersions[1] = schemaUpgraded2;
        upgradeBarrier.updateVersionsBlocking();
        // NO update from ClusterVersionBarrier - status is still the same (schema disagreement)
        assertEquals(4, collector.invocations);
        schemaVersions[0] = schemaUpgradedFinal;
        upgradeBarrier.updateVersionsBlocking();
        // NO update from ClusterVersionBarrier - status is still the same (schema disagreement)
        assertEquals(4, collector.invocations);

        schemaVersions[2] = schemaUpgradedFinal;
        upgradeBarrier.updateVersionsBlocking();
        // NO update from ClusterVersionBarrier - status is still the same (schema disagreement)
        assertEquals(4, collector.invocations);

        // last node applied the schema
        schemaVersions[1] = schemaUpgradedFinal;
        upgradeBarrier.updateVersionsBlocking();
        assertEquals(5, collector.invocations);
        assertEquals(versionFinal, collector.min);
        assertEquals(versionFinal, collector.max);
        assertTrue(collector.schemaAgreement);
        assertEquals(schemaUpgradedFinal, collector.schemaVersion);
    }

    @Test
    public void testVersionDependentFeatureExpectedVersion()
    {
        CassandraVersion versionFinal = nextMajor();

        ClusterVersionBarrier upgradeBarrier = new ClusterVersionBarrier(ClusterVersionBarrierTest::threeMembers,
                                                                         (ep) -> new ClusterVersionBarrier.EndpointInfo(versionFinal,
                                                                                                                        UUID.randomUUID()));

        LegacyImpl lImpl = new LegacyImpl();
        NativeImpl nImpl = new NativeImpl();
        Feature feature = new Feature(versionFinal, lImpl, nImpl);
        feature.setup(upgradeBarrier);

        // DDL change is not required
        feature.ddlChangeRequired = false;

        assertSame(VersionDependentFeature.FeatureStatus.UNKNOWN, feature.getStatus());
        assertEquals(0, lImpl.initialized);
        assertEquals(0, nImpl.initialized);
        assertEquals(0, feature.executeDDL);
        assertEquals(0, feature.onFeatureDeactivated);
        assertEquals(0, feature.onFeatureActivating);
        assertEquals(0, feature.onFeatureActivated);

        upgradeBarrier.onLocalNodeReady();

        // A cluster where all nodes are on the minimum expected version
        // _AND_ a DDL change is not required, the status must immediately jump to ACTIVATED

        assertSame(VersionDependentFeature.FeatureStatus.ACTIVATED, feature.getStatus());
        assertEquals(0, lImpl.initialized);
        assertEquals(1, nImpl.initialized);
        assertEquals(0, feature.executeDDL);
        assertEquals(0, feature.onFeatureDeactivated);
        assertEquals(0, feature.onFeatureActivating);
        assertEquals(1, feature.onFeatureActivated);
    }

    @Test
    public void testVersionDependentFeatureUpgradeWithoutDDL()
    {
        // This test is similar to testRollingUpgradeWithSchema() -
        // this one uses an implementation of VersionDependentFeature instead of the Collector that
        // tests ClusterVersionBarrier.

        UUID schemaInitial = UUID.randomUUID();

        CassandraVersion versionInitial = ClusterVersionBarrier.ownOssVersion;
        CassandraVersion versionFinal = nextMajor();

        CassandraVersion[] cassandraVersions = new CassandraVersion[]{ versionInitial, versionInitial, versionInitial };
        UUID[] schemaVersions = new UUID[]{ schemaInitial, schemaInitial, schemaInitial };

        ClusterVersionBarrier upgradeBarrier = new ClusterVersionBarrier(ClusterVersionBarrierTest::threeMembers,
                                                                         (ep) -> new ClusterVersionBarrier.EndpointInfo(cassandraVersions[ep.getAddress()[3] - 1],
                                                                                                                        schemaVersions[ep.getAddress()[3] - 1]));

        LegacyImpl lImpl = new LegacyImpl();
        NativeImpl nImpl = new NativeImpl();
        Feature feature = new Feature(versionFinal, lImpl, nImpl);
        feature.setup(upgradeBarrier);

        // DDL change is not required
        feature.ddlChangeRequired = false;

        assertSame(VersionDependentFeature.FeatureStatus.UNKNOWN, feature.getStatus());
        assertEquals(0, lImpl.initialized);
        assertEquals(0, nImpl.initialized);
        assertEquals(0, feature.executeDDL);
        assertEquals(0, feature.onFeatureDeactivated);
        assertEquals(0, feature.onFeatureActivating);
        assertEquals(0, feature.onFeatureActivated);

        upgradeBarrier.onLocalNodeReady();
        assertSame(VersionDependentFeature.FeatureStatus.DEACTIVATED, feature.getStatus());
        assertEquals(1, lImpl.initialized);
        assertEquals(0, nImpl.initialized);
        assertEquals(0, feature.executeDDL);
        assertEquals(1, feature.onFeatureDeactivated);
        assertEquals(0, feature.onFeatureActivating);
        assertEquals(0, feature.onFeatureActivated);

        // upgrade node 1
        cassandraVersions[0] = versionFinal;
        upgradeBarrier.updateVersionsBlocking();
        assertSame(VersionDependentFeature.FeatureStatus.DEACTIVATED, feature.getStatus());
        assertEquals(1, lImpl.initialized);
        assertEquals(0, nImpl.initialized);
        assertEquals(0, feature.executeDDL);
        assertEquals(1, feature.onFeatureDeactivated);
        assertEquals(0, feature.onFeatureActivating);
        assertEquals(0, feature.onFeatureActivated);

        // upgrade node 2
        cassandraVersions[1] = versionFinal;
        upgradeBarrier.updateVersionsBlocking();
        assertSame(VersionDependentFeature.FeatureStatus.DEACTIVATED, feature.getStatus());
        assertEquals(1, lImpl.initialized);
        assertEquals(0, nImpl.initialized);
        assertEquals(0, feature.executeDDL);
        assertEquals(1, feature.onFeatureDeactivated);
        assertEquals(0, feature.onFeatureActivating);
        assertEquals(0, feature.onFeatureActivated);

        // upgrade node 3
        cassandraVersions[2] = versionFinal;
        upgradeBarrier.updateVersionsBlocking();
        assertSame(VersionDependentFeature.FeatureStatus.ACTIVATED, feature.getStatus());
        assertEquals(1, lImpl.initialized);
        assertEquals(1, nImpl.initialized);
        assertEquals(0, feature.executeDDL);
        assertEquals(1, feature.onFeatureDeactivated);
        assertEquals(0, feature.onFeatureActivating);
        assertEquals(1, feature.onFeatureActivated);
    }

    @Test
    public void testVersionDependentFeatureUpgradeWithDDL() throws Exception
    {
        // This test is similar to testRollingUpgradeWithSchema() -
        // this one uses an implementation of VersionDependentFeature instead of the Collector that
        // tests ClusterVersionBarrier.

        UUID schemaInitial = UUID.randomUUID();
        UUID schemaUpgraded1 = UUID.randomUUID();
        UUID schemaUpgraded2 = UUID.randomUUID();
        UUID schemaUpgradedFinal = UUID.randomUUID();

        CassandraVersion versionInitial = ClusterVersionBarrier.ownOssVersion;
        CassandraVersion versionFinal = nextMajor();

        CassandraVersion[] cassandraVersions = new CassandraVersion[]{ versionInitial, versionInitial, versionInitial };
        UUID[] schemaVersions = new UUID[]{ schemaInitial, schemaInitial, schemaInitial };

        ClusterVersionBarrier upgradeBarrier = new ClusterVersionBarrier(ClusterVersionBarrierTest::threeMembers,
                                                                         (ep) -> new ClusterVersionBarrier.EndpointInfo(cassandraVersions[ep.getAddress()[3] - 1],
                                                                                                                        schemaVersions[ep.getAddress()[3] - 1]));

        LegacyImpl lImpl = new LegacyImpl();
        NativeImpl nImpl = new NativeImpl();
        Feature feature = new Feature(versionFinal, lImpl, nImpl);
        feature.setup(upgradeBarrier);

        // DDL change is required
        feature.ddlChangeRequired = true;

        assertSame(VersionDependentFeature.FeatureStatus.UNKNOWN, feature.getStatus());
        assertEquals(0, lImpl.initialized);
        assertEquals(0, nImpl.initialized);
        assertEquals(0, feature.executeDDL);
        assertEquals(0, feature.onFeatureDeactivated);
        assertEquals(0, feature.onFeatureActivating);
        assertEquals(0, feature.onFeatureActivated);

        upgradeBarrier.onLocalNodeReady();
        assertSame(VersionDependentFeature.FeatureStatus.DEACTIVATED, feature.getStatus());
        assertEquals(1, lImpl.initialized);
        assertEquals(0, nImpl.initialized);
        assertEquals(0, feature.executeDDL);
        assertEquals(1, feature.onFeatureDeactivated);
        assertEquals(0, feature.onFeatureActivating);
        assertEquals(0, feature.onFeatureActivated);

        // upgrade node 1
        cassandraVersions[0] = versionFinal;
        schemaVersions[0] = schemaUpgraded1;
        upgradeBarrier.updateVersionsBlocking();
        assertSame(VersionDependentFeature.FeatureStatus.DEACTIVATED, feature.getStatus());
        assertEquals(1, lImpl.initialized);
        assertEquals(0, nImpl.initialized);
        assertEquals(0, feature.executeDDL);
        assertEquals(1, feature.onFeatureDeactivated);
        assertEquals(0, feature.onFeatureActivating);
        assertEquals(0, feature.onFeatureActivated);

        // upgrade node 2
        cassandraVersions[1] = versionFinal;
        schemaVersions[1] = schemaUpgraded1;
        upgradeBarrier.updateVersionsBlocking();
        assertSame(VersionDependentFeature.FeatureStatus.DEACTIVATED, feature.getStatus());
        assertEquals(1, lImpl.initialized);
        assertEquals(0, nImpl.initialized);
        assertEquals(0, feature.executeDDL);
        assertEquals(1, feature.onFeatureDeactivated);
        assertEquals(0, feature.onFeatureActivating);
        assertEquals(0, feature.onFeatureActivated);

        // upgrade node 3
        cassandraVersions[2] = versionFinal;
        schemaVersions[2] = schemaUpgraded1;
        upgradeBarrier.updateVersionsBlocking();
        assertSame(VersionDependentFeature.FeatureStatus.ACTIVATING, feature.getStatus());
        assertEquals(1, lImpl.initialized);
        assertEquals(0, nImpl.initialized);
//        assertEquals(0, feature.executeDDL); - do not test here, as executeDDL() is run asynchronously, see below
        assertEquals(1, feature.onFeatureDeactivated);
        assertEquals(1, feature.onFeatureActivating);
        assertEquals(0, feature.onFeatureActivated);

        // We should have schema-agreement here, so executeDDL() should be called

        // Wait for executeDDL() ...
        // There is a one-second delay for re-executing VersionDependentFeature.clusterVersionUpdated() and
        // another one-second delay after executing executeDDL().
        // Go with 3 seconds and it should be fine.
        for (int i = 0; i < 3_000; i++)
        {
            if (feature.executeDDL == 1)
                break;
            Thread.sleep(1L);
        }

        // some schema changes after the upgrade
        schemaVersions[0] = schemaUpgraded2;
        upgradeBarrier.updateVersionsBlocking();
        assertSame(VersionDependentFeature.FeatureStatus.ACTIVATING, feature.getStatus());
        assertEquals(1, lImpl.initialized);
        assertEquals(0, nImpl.initialized);
        assertEquals(1, feature.executeDDL);
        assertEquals(1, feature.onFeatureDeactivated);
        assertEquals(1, feature.onFeatureActivating);
        assertEquals(0, feature.onFeatureActivated);

        // the DDL change is no longer required
        feature.ddlChangeRequired = false;

        schemaVersions[1] = schemaUpgraded2;
        upgradeBarrier.updateVersionsBlocking();
        assertSame(VersionDependentFeature.FeatureStatus.ACTIVATING, feature.getStatus());
        assertEquals(1, lImpl.initialized);
        assertEquals(0, nImpl.initialized);
        assertEquals(1, feature.executeDDL);
        assertEquals(1, feature.onFeatureDeactivated);
        assertEquals(1, feature.onFeatureActivating);
        assertEquals(0, feature.onFeatureActivated);

        schemaVersions[0] = schemaUpgradedFinal;
        upgradeBarrier.updateVersionsBlocking();
        assertSame(VersionDependentFeature.FeatureStatus.ACTIVATING, feature.getStatus());
        assertEquals(1, lImpl.initialized);
        assertEquals(0, nImpl.initialized);
        assertEquals(1, feature.executeDDL);
        assertEquals(1, feature.onFeatureDeactivated);
        assertEquals(1, feature.onFeatureActivating);
        assertEquals(0, feature.onFeatureActivated);

        schemaVersions[2] = schemaUpgradedFinal;
        upgradeBarrier.updateVersionsBlocking();
        assertSame(VersionDependentFeature.FeatureStatus.ACTIVATING, feature.getStatus());
        assertEquals(1, lImpl.initialized);
        assertEquals(0, nImpl.initialized);
        assertEquals(1, feature.executeDDL);
        assertEquals(1, feature.onFeatureDeactivated);
        assertEquals(1, feature.onFeatureActivating);
        assertEquals(0, feature.onFeatureActivated);

        // last node applied the schema
        schemaVersions[1] = schemaUpgradedFinal;
        upgradeBarrier.updateVersionsBlocking();
        assertSame(VersionDependentFeature.FeatureStatus.ACTIVATED, feature.getStatus());
        assertEquals(1, lImpl.initialized);
        assertEquals(1, nImpl.initialized);
        assertEquals(1, feature.executeDDL);
        assertEquals(1, feature.onFeatureDeactivated);
        assertEquals(1, feature.onFeatureActivating);
        assertEquals(1, feature.onFeatureActivated);

        // DOWNgrade node 2
        cassandraVersions[1] = versionInitial;
        schemaVersions[1] = schemaUpgraded1;
        upgradeBarrier.updateVersionsBlocking();
        assertSame(VersionDependentFeature.FeatureStatus.DEACTIVATED, feature.getStatus());
        assertEquals(1, lImpl.initialized);
        assertEquals(1, nImpl.initialized);
        assertEquals(1, feature.executeDDL);
        assertEquals(2, feature.onFeatureDeactivated);
        assertEquals(1, feature.onFeatureActivating);
        assertEquals(1, feature.onFeatureActivated);

        // UPgrade node 2 again
        cassandraVersions[1] = versionFinal;
        schemaVersions[1] = schemaUpgradedFinal;
        upgradeBarrier.updateVersionsBlocking();
        assertSame(VersionDependentFeature.FeatureStatus.ACTIVATED, feature.getStatus());
        assertEquals(1, lImpl.initialized);
        assertEquals(1, nImpl.initialized);
        assertEquals(1, feature.executeDDL);
        assertEquals(2, feature.onFeatureDeactivated);
        assertEquals(1, feature.onFeatureActivating);
        assertEquals(2, feature.onFeatureActivated);
    }

    // Base class - pretty empty in this test. Real implementations add some (abstract) methods
    static abstract class FeatureImpl implements VersionDependentFeature.VersionDependent
    {
    }

    // The legacy implementation
    static class LegacyImpl extends FeatureImpl
    {
        private int initialized;

        @Override
        public void initialize()
        {
            initialized++;
        }
    }

    // The native implementation
    static class NativeImpl extends FeatureImpl
    {
        private int initialized;

        @Override
        public void initialize()
        {
            initialized++;
        }
    }

    static class Feature extends VersionDependentFeature<FeatureImpl>
    {
        private boolean ddlChangeRequired;
        private int executeDDL;
        private int onFeatureActivating;
        private int onFeatureActivated;
        private int onFeatureDeactivated;

        Feature(CassandraVersion versionFinal, LegacyImpl lImpl, NativeImpl nImpl)
        {
            super("TestFeature",
                  versionFinal,
                  lImpl,
                  nImpl);
        }

        protected boolean ddlChangeRequired()
        {
            return ddlChangeRequired;
        }

        @Override
        protected void executeDDL()
        {
            executeDDL++;
        }

        public void onFeatureActivating()
        {
            onFeatureActivating++;
        }

        public void onFeatureActivated()
        {
            onFeatureActivated++;
        }

        public void onFeatureDeactivated()
        {
            onFeatureDeactivated++;
        }
    }

    // utility stuff

    private void waitASec(BooleanSupplier f) throws InterruptedException
    {
        for (int i = 0; f.getAsBoolean() && i < 1000; i++)
            Thread.sleep(1);
    }

    private CassandraVersion nextMajor()
    {
        return new CassandraVersion((ClusterVersionBarrier.ownOssVersion.major + 1) + ".0.0");
    }

    private CassandraVersion nextMajorDot5()
    {
        return new CassandraVersion((ClusterVersionBarrier.ownOssVersion.major + 1) + ".5.0");
    }

    private CassandraVersion currentVersion()
    {
        return new CassandraVersion(FBUtilities.getReleaseVersionString());
    }

    private ClusterVersionBarrier.EndpointInfo info(CassandraVersion version)
    {
        return new ClusterVersionBarrier.EndpointInfo(version, someVersion);
    }

    private static Set<InetAddress> threeMembers()
    {
        Set<InetAddress> r = new HashSet<>();
        for (int i = 1; i <= 3; i++)
            r.add(ia(i));
        return r;
    }

    private static Set<InetAddress> tenMembers()
    {
        Set<InetAddress> r = new HashSet<>();
        for (int i = 1; i <= 10; i++)
            r.add(ia(i));
        return r;
    }

    private static InetAddress ia(int i)
    {
        try
        {
            return InetAddress.getByAddress(new byte[]{ 127, 0, 0, (byte) i });
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static class Collector implements ClusterVersionBarrier.ClusterVersionListener
    {
        CassandraVersion min;
        CassandraVersion max;
        UUID schemaVersion;
        boolean schemaAgreement;
        int invocations;

        @Override
        public void clusterVersionUpdated(ClusterVersionBarrier.ClusterVersionInfo versionInfo)
        {
            min = versionInfo.minOss;
            max = versionInfo.maxOss;
            schemaVersion = versionInfo.schemaVersion;
            schemaAgreement = versionInfo.schemaAgreement;
            invocations++;
        }
    }
}
