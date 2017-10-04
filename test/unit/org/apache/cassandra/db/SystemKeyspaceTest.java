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
package org.apache.cassandra.db;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SystemKeyspaceTest
{
    @BeforeClass
    public static void prepSnapshotTracker()
    {
        DatabaseDescriptor.daemonInitialization();
        cleanDataDirs();

        if (FBUtilities.isWindows)
            WindowsFailedSnapshotTracker.deleteOldSnapshots();

        SystemKeyspace.finishStartupBlocking();
    }

    private static void cleanDataDirs()
    {
        for (String dirName : DatabaseDescriptor.getAllDataFileLocations())
        {
            File dir = new File(dirName);
            if (dir.exists())
                FileUtils.deleteRecursive(dir);

            FileUtils.createDirectory(dir);
        }
    }

    @Test
    public void testMissingStartup() throws Exception
    {
        final InetAddress address = InetAddress.getByName("127.0.0.2");
        final CassandraVersion version = new CassandraVersion("1.2.3");

        try
        {
            SystemKeyspace.resetStartupBlocking();
            assertNotNull(SystemKeyspace.loadDcRackInfo());

            SystemKeyspace.resetStartupBlocking();
            assertNotNull(SystemKeyspace.getHostIds());

            SystemKeyspace.resetStartupBlocking();
            TPCUtils.blockingAwait(SystemKeyspace.updatePreferredIP(address, address));
            assertEquals(address, SystemKeyspace.getPreferredIP(address));

            SystemKeyspace.resetStartupBlocking();
            TPCUtils.blockingAwait(SystemKeyspace.updatePeerInfo(address, "release_version", "1.2.3"));
            assertEquals(version, SystemKeyspace.getReleaseVersion(address));
        }
        finally
        {
            SystemKeyspace.removeEndpoint(address);
            SystemKeyspace.finishStartupBlocking();
        }
    }

    @Test
    public void testMissingStartupOnTPCThread() throws Exception
    {
        try
        {
            SystemKeyspace.resetStartupBlocking();

            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Throwable> err = new AtomicReference<>(null);
            TPC.getForCore(0).scheduleDirect(() -> {
                try
                {
                    SystemKeyspace.loadDcRackInfo();
                }
                catch (Throwable t)
                {
                    err.set(t);
                }
                finally
                {
                    latch.countDown();
                }
            });

            latch.await(30, TimeUnit.SECONDS);
            assertNotNull(err.get());
            assertEquals(TPCUtils.WouldBlockException.class, err.get().getClass());
        }
        finally
        {
            SystemKeyspace.finishStartupBlocking();
        }
    }

    @Test
    public void testDcRackInfoDuringStartup()
    {
        try
        {
            SystemKeyspace.resetStartupBlocking();
            SystemKeyspace.beginStartupBlocking();
            Map<InetAddress, Map<String,String>> ret = SystemKeyspace.loadDcRackInfo();
            assertNotNull(ret);
        }
        finally
        {
            SystemKeyspace.finishStartupBlocking();
        }
    }

    @Test
    public void testSnitchLoadingDcRackInfo()
    {
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        try
        {
            DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
            {
                @Override public String getRack(InetAddress endpoint) { SystemKeyspace.loadDcRackInfo(); return "rack"; }
                @Override public String getDatacenter(InetAddress endpoint) { SystemKeyspace.loadDcRackInfo(); return "datacenter"; }
                @Override public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2) { return 0; }
            });

            SystemKeyspace.resetStartupBlocking();
            SystemKeyspace.beginStartupBlocking(); // calls SK.persistLocalMetadata(), which calls getRack() and getDatacenter() in the snitch

        }
        finally
        {
            SystemKeyspace.finishStartupBlocking();
            DatabaseDescriptor.setEndpointSnitch(snitch);
        }
    }

    @Test
    public void testMultipleStartupCalls()
    {
        SystemKeyspace.resetStartupBlocking();
        SystemKeyspace.beginStartupBlocking();
        SystemKeyspace.beginStartupBlocking();

        SystemKeyspace.finishStartupBlocking();
        SystemKeyspace.beginStartupBlocking();
        SystemKeyspace.finishStartupBlocking();

    }

    @Test
    public void testMissingBeginStartup()
    {
        SystemKeyspace.resetStartupBlocking();
        SystemKeyspace.finishStartupBlocking();
    }

    @Test
    public void testLocalTokens()
    {
        // Remove all existing tokens
        Collection<Token> current = TPCUtils.blockingGet(SystemKeyspace.loadTokens()).asMap().get(FBUtilities.getLocalAddress());
        if (current != null && !current.isEmpty())
            TPCUtils.blockingAwait(SystemKeyspace.updateTokens(current));

        List<Token> tokens = new ArrayList<Token>()
        {{
            for (int i = 0; i < 9; i++)
                add(new Murmur3Partitioner.LongToken(i));
        }};

        TPCUtils.blockingAwait(SystemKeyspace.updateTokens(tokens));
        int count = 0;

        for (Token tok : TPCUtils.blockingGet(SystemKeyspace.getSavedTokens()))
            assert tokens.get(count++).equals(tok);
    }

    @Test
    public void testNonLocalToken() throws UnknownHostException
    {
        Murmur3Partitioner.LongToken token = new Murmur3Partitioner.LongToken(3);
        InetAddress address = InetAddress.getByName("127.0.0.2");

        TPCUtils.blockingAwait(SystemKeyspace.updateTokens(address, Collections.singletonList(token)));
        assert TPCUtils.blockingGet(SystemKeyspace.loadTokens()).get(address).contains(token);

        TPCUtils.blockingAwait(SystemKeyspace.removeEndpoint(address));
        assert !TPCUtils.blockingGet(SystemKeyspace.loadTokens()).containsValue(token);
    }

    @Test
    public void testLocalHostID()
    {
        UUID firstId = TPCUtils.blockingGet(SystemKeyspace.setLocalHostId());
        UUID secondId = SystemKeyspace.getLocalHostId();
        assertEquals(firstId, secondId);

        UUID anotherId = UUID.randomUUID();
        SystemKeyspace.updateLocalInfo("host_id", anotherId);
        assertEquals(anotherId, SystemKeyspace.getLocalHostId());
    }

    private static List<String> getBuiltIndexes(String keyspace)
    {
        return TPCUtils.blockingGet(SystemKeyspace.getBuiltIndexes(keyspace));
    }

    @Test
    public void testIndexesBuilt()
    {
        final String keyspace1 = "keyspace1";
        final String keyspace2 = "keyspace12";

        final String index1 = "index1";
        final String index2 = "index2";

        assertTrue(getBuiltIndexes(keyspace1).isEmpty());
        assertTrue(getBuiltIndexes(keyspace2).isEmpty());

        TPCUtils.blockingAwait(SystemKeyspace.setIndexBuilt(keyspace1, index1));
        assertEquals(index1, getBuiltIndexes(keyspace1).get(0));
        assertTrue(getBuiltIndexes(keyspace2).isEmpty());

        TPCUtils.blockingAwait(SystemKeyspace.setIndexBuilt(keyspace2, index2));
        assertEquals(index1, getBuiltIndexes(keyspace1).get(0));
        assertEquals(index2, getBuiltIndexes(keyspace2).get(0));

        TPCUtils.blockingAwait(SystemKeyspace.setIndexRemoved(keyspace1, index1));
        TPCUtils.blockingAwait(SystemKeyspace.setIndexRemoved(keyspace2, index2));

        assertTrue(getBuiltIndexes(keyspace1).isEmpty());
        assertTrue(getBuiltIndexes(keyspace2).isEmpty());
    }

    @Test
    public void testGetPreferredIp() throws UnknownHostException
    {
        final InetAddress peer = InetAddress.getByName("127.0.0.2");
        final InetAddress preferredId = InetAddress.getByName("127.0.0.3");

        assertEquals(peer, SystemKeyspace.getPreferredIP(peer));

        TPCUtils.blockingAwait(SystemKeyspace.updatePreferredIP(peer, preferredId));
        assertEquals(preferredId, SystemKeyspace.getPreferredIP(peer));
    }

    @Test
    public void testUpdateDcAndRacks() throws UnknownHostException
    {
        final InetAddress peer = InetAddress.getByName("127.0.0.2");

        final List<Pair<String, String>> dcRacks = new ArrayList<>(2);
        dcRacks.add(Pair.create("dc1", "rack1"));
        dcRacks.add(Pair.create("dc2", "rack2"));

        assertEquals(0, SystemKeyspace.loadDcRackInfo().size());

        for (Pair<String, String> pair : dcRacks)
        {
            TPCUtils.blockingAwait(SystemKeyspace.updatePeerInfo(peer, "rack", pair.right));
            TPCUtils.blockingAwait(SystemKeyspace.updatePeerInfo(peer, "data_center", pair.left));

            Map<InetAddress, Map<String, String>> ret = SystemKeyspace.loadDcRackInfo();
            assertEquals(1, ret.size());

            assertEquals(pair.left, ret.get(peer).get("data_center"));
            assertEquals(pair.right, ret.get(peer).get("rack"));
        }

        TPCUtils.blockingAwait(SystemKeyspace.removeEndpoint(peer));
        assertEquals(0, SystemKeyspace.loadDcRackInfo().size());
    }

    @Test
    public void testUpdateSchemaVersion()
    {
        final String columnName = "schema_version";
        final UUID version = UUID.randomUUID();

        TPCUtils.blockingAwait(SystemKeyspace.updateSchemaVersion(version));

        UntypedResultSet ret = TPCUtils.blockingGet(SystemKeyspace.loadLocalInfo(columnName));
        assertNotNull(ret);
        assertEquals(1, ret.size());

        UntypedResultSet.Row row = ret.one();
        assertTrue(row.has(columnName));
        assertEquals(version, row.getUUID(columnName));
    }

    @Test
    public void testLocalServerId()
    {
        testUpdateLocalInfo("server_id", "123456");
    }

    @Test
    public void testLocalWorkload()
    {
        testUpdateLocalInfo("workload", "analytics");
    }

    @Test
    public void testLocalWorkloads()
    {
        testUpdateLocalInfo("workloads", Collections.singleton("analytics"));
    }

    @Test
    public void testLocalBootstrapState()
    {
        testUpdateLocalInfo("bootstrapped", SystemKeyspace.BootstrapState.IN_PROGRESS);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testLocalTruncationRecords()
    {
        testUpdateLocalInfo("truncated_at", Collections.singletonMap(UUID.randomUUID(), ByteBuffer.allocate(0)));
    }

    private <T> void testUpdateLocalInfo(String columnName, T value)
    {
        UntypedResultSet ret = TPCUtils.blockingGet(SystemKeyspace.loadLocalInfo(columnName));
        assertNotNull(ret);
        assertEquals(1, ret.size());
        assertFalse(ret.one().has(columnName));

        TPCUtils.blockingAwait(SystemKeyspace.updateLocalInfo(columnName, value));

        ret = TPCUtils.blockingGet(SystemKeyspace.loadLocalInfo(columnName));
        assertNotNull(ret);
        assertEquals(1, ret.size());

        UntypedResultSet.Row row = ret.one();
        assertTrue(row.has(columnName));

        if (value instanceof String)
            assertEquals(value, row.getString(columnName));
        else if (value instanceof UUID)
            assertEquals(value, row.getUUID(columnName));
        else if (value instanceof Set)
            assertEquals(value, row.getSet(columnName, UTF8Type.instance));
        else if (value instanceof SystemKeyspace.BootstrapState)
            assertEquals(value, SystemKeyspace.BootstrapState.valueOf(row.getString(columnName)));
        else
            fail("Unsupported value type");
    }

    @Test
    public void testRemoteSchemaVersion() throws UnknownHostException
    {
        final InetAddress peer = InetAddress.getByName("127.0.0.2");
        testUpdatePeerInfo(peer, "schema_version", UUID.randomUUID());
    }

    @Test
    public void testRemoteServerId() throws UnknownHostException
    {
        final InetAddress peer = InetAddress.getByName("127.0.0.2");
        testUpdatePeerInfo(peer, "server_id", "123456");
    }

    @Test
    public void testRemoteWorkload() throws UnknownHostException
    {
        final InetAddress peer = InetAddress.getByName("127.0.0.2");
        testUpdatePeerInfo(peer, "workload", "analytics");
    }

    @Test
    public void testRemoteWorkloads() throws UnknownHostException
    {
        final InetAddress peer = InetAddress.getByName("127.0.0.2");
        testUpdatePeerInfo(peer, "workloads", Collections.singleton("analytics"));
    }

    private <T> void testUpdatePeerInfo(InetAddress peer, String columnName, T value)
    {
        UntypedResultSet ret = TPCUtils.blockingGet(SystemKeyspace.loadPeerInfo(peer, columnName));
        assertNotNull(ret);
        assertEquals(0, ret.size());

        TPCUtils.blockingAwait(SystemKeyspace.updatePeerInfo(peer, columnName, value));

        ret = TPCUtils.blockingGet(SystemKeyspace.loadPeerInfo(peer, columnName));
        assertNotNull(ret);
        assertEquals(1, ret.size());

        UntypedResultSet.Row row = ret.one();
        assertTrue(row.has(columnName));

        if (value instanceof String)
            assertEquals(value, row.getString(columnName));
        else if (value instanceof UUID)
            assertEquals(value, row.getUUID(columnName));
        else if (value instanceof Set)
            assertEquals(value, row.getSet(columnName, UTF8Type.instance));
        else
            fail("Unsupported value type");

        TPCUtils.blockingAwait(SystemKeyspace.removeEndpoint(peer));

        ret = TPCUtils.blockingGet(SystemKeyspace.loadPeerInfo(peer, columnName));
        assertNotNull(ret);
        assertEquals(0, ret.size());
    }

    private void assertDeletedOrDeferred(int expectedCount)
    {
        if (FBUtilities.isWindows)
            assertEquals(expectedCount, getDeferredDeletionCount());
        else
            assertTrue(getSystemSnapshotFiles().isEmpty());
    }

    private int getDeferredDeletionCount()
    {
        try
        {
            Class c = Class.forName("java.io.DeleteOnExitHook");
            LinkedHashSet<String> files = (LinkedHashSet<String>)FBUtilities.getProtectedField(c, "files").get(c);
            return files.size();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void snapshotSystemKeyspaceIfUpgrading() throws IOException
    {
        // First, check that in the absence of any previous installed version, we don't create snapshots
        for (ColumnFamilyStore cfs : Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStores())
            cfs.clearUnsafe();
        Keyspace.clearSnapshot(null, SchemaConstants.SYSTEM_KEYSPACE_NAME);

        int baseline = getDeferredDeletionCount();

        TPCUtils.blockingAwait(SystemKeyspace.snapshotOnVersionChange());
        assertDeletedOrDeferred(baseline);

        // now setup system.local as if we're upgrading from a previous version
        setupReleaseVersion(getOlderVersionString());
        Keyspace.clearSnapshot(null, SchemaConstants.SYSTEM_KEYSPACE_NAME);
        assertDeletedOrDeferred(baseline);

        // Compare versions again & verify that snapshots were created for all tables in the system ks
        TPCUtils.blockingAwait(SystemKeyspace.snapshotOnVersionChange());
        assertEquals(SystemKeyspace.metadata().tables.size(), getSystemSnapshotFiles().size());

        // clear out the snapshots & set the previous recorded version equal to the latest, we shouldn't
        // see any new snapshots created this time.
        Keyspace.clearSnapshot(null, SchemaConstants.SYSTEM_KEYSPACE_NAME);
        setupReleaseVersion(FBUtilities.getReleaseVersionString());

        TPCUtils.blockingAwait(SystemKeyspace.snapshotOnVersionChange());

        // snapshotOnVersionChange for upgrade case will open a SSTR when the CFS is flushed. On Windows, we won't be
        // able to delete hard-links to that file while segments are memory-mapped, so they'll be marked for deferred deletion.
        // 10 files expected.
        assertDeletedOrDeferred(baseline + 10);

        Keyspace.clearSnapshot(null, SchemaConstants.SYSTEM_KEYSPACE_NAME);
    }

    private String getOlderVersionString()
    {
        String version = FBUtilities.getReleaseVersionString();
        CassandraVersion semver = new CassandraVersion(version.contains("-") ? version.substring(0, version.indexOf('-'))
                                                                           : version);
        return (String.format("%s.%s.%s", semver.major - 1, semver.minor, semver.patch));
    }

    private Set<String> getSystemSnapshotFiles()
    {
        Set<String> snapshottedTableNames = new HashSet<>();
        for (ColumnFamilyStore cfs : Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStores())
        {
            if (!cfs.getSnapshotDetails().isEmpty())
                snapshottedTableNames.add(cfs.getTableName());
        }
        return snapshottedTableNames;
    }

    private void setupReleaseVersion(String version)
    {
        // besides the release_version, we also need to insert the cluster_name or the check
        // in SystemKeyspace.checkHealth were we verify it matches DatabaseDescriptor will fail
        QueryProcessor.executeInternal(String.format("INSERT INTO system.local(key, release_version, cluster_name) " +
                                                     "VALUES ('local', '%s', '%s')",
                                                     version,
                                                     DatabaseDescriptor.getClusterName()));
        String r = readLocalVersion();
        assertEquals(String.format("Expected %s, got %s", version, r), version, r);
    }

    private String readLocalVersion()
    {
        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT release_version FROM system.local WHERE key='local'");
        return rs.isEmpty() || !rs.one().has("release_version") ? null : rs.one().getString("release_version");
    }
}
