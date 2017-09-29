/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.mos;


import java.util.Arrays;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.dse.framework.CassandraYamlBuilder;
import com.datastax.dse.framework.DseTestRunner;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.service.StorageServiceMBean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Legacy test for MemoryOnlyStrategy. This test is obsoleted by {@link MemoryOnlyStrategyDiskAccessModeTest} and only
 * exists to test the DSE testing framework, see {@link com.datastax.dse.framework.DseTestRunner}.
 *
 * This test is no longer required for MOS and can be removed if other tests use the DSE testing framework.
 */
public class LegacyMemoryOnlyStrategyDiskAccessModeTest extends DseTestRunner
{
    private static final Logger logger = LoggerFactory.getLogger(MemoryOnlyStrategyDiskAccessModeTest.class);
    private static String KEYSPACE = "ks" + System.nanoTime();

    @After
    public void teardown() throws Exception
    {
        destroyCluster();
    }

    @Test
    public void testDiskAccessModeStandard() throws Exception
    {
        // DiskAccessMode.standard = mmapping is turned off
        startNode(1, CassandraYamlBuilder.newInstance().withDiskAccessMode(Config.DiskAccessMode.standard.name()), false);
        LegacyMemoryOnlyStrategyTestUtil.maybeCreateKeyspace(KEYSPACE);
        insertSomeDataAndVerifyLockedStatus(false);
    }

    @Test
    public void testDiskAccessModeMmapIndexOnly() throws Exception
    {
        startNode(1, CassandraYamlBuilder.newInstance().withDiskAccessMode(Config.DiskAccessMode.mmap_index_only.name()), false);
        LegacyMemoryOnlyStrategyTestUtil.maybeCreateKeyspace(KEYSPACE);
        // we can't really distinguish here between data & index files, so mmapping will work
        insertSomeDataAndVerifyLockedStatus(true);
    }

    @Test
    public void testDiskAccessModeMmap() throws Exception
    {
        startNode(1, CassandraYamlBuilder.newInstance().withDiskAccessMode(Config.DiskAccessMode.mmap.name()), false);
        LegacyMemoryOnlyStrategyTestUtil.maybeCreateKeyspace(KEYSPACE);
        insertSomeDataAndVerifyLockedStatus(true);
    }

    @Test
    public void testDiskAccessModeAuto() throws Exception
    {
        startNode(1, CassandraYamlBuilder.newInstance().withDiskAccessMode(Config.DiskAccessMode.auto.name()), false);
        LegacyMemoryOnlyStrategyTestUtil.maybeCreateKeyspace(KEYSPACE);
        insertSomeDataAndVerifyLockedStatus(true);
    }

    private void insertSomeDataAndVerifyLockedStatus(boolean mmappingShouldWork) throws Exception
    {
        for (String sstableCompressor : Arrays.asList("", "LZ4Compressor"))
        {
            logger.info("insertSomeDataAndVerifyLockedStatus(): " + ("".equals(sstableCompressor) ? "sstable_compression turned off" : String.format("Using sstable_compression: %s", sstableCompressor)));
            String cf = LegacyMemoryOnlyStrategyTestUtil.createNewTable(KEYSPACE, sstableCompressor);
            MBeanServerConnection connection = getMBeanServerConnection();

            MemoryOnlyStatusMXBean mosStatus = LegacyMemoryOnlyStrategyTestUtil.getMosProxy(connection);
            final int numInserts = 4000;
            LegacyMemoryOnlyStrategyTestUtil.insertData(getNativeClientForNode(1).newSession(), numInserts, KEYSPACE, cf);

            String select = String.format("SELECT * FROM %s.%s", KEYSPACE, cf);
            assertEquals(numInserts, sendCql3Native(1, KEYSPACE, select).all().size());

            assertEquals(0, mosStatus.getMemoryOnlyTableInformation(KEYSPACE, cf).getUsed());
            LegacyMemoryOnlyStrategyTestUtil.verifyTotals(mosStatus);

            // Now flush and do it
            // now flush & redo the read. We should read it from the sstable
            StorageServiceMBean ssProxy = JMX.newMBeanProxy(connection,
                                                            new ObjectName("org.apache.cassandra.db:type=StorageService"),
                                                            StorageServiceMBean.class);
            LegacyMemoryOnlyStrategyTestUtil.insertData(getNativeClientForNode(1).newSession(), numInserts, KEYSPACE, cf);
            ssProxy.forceKeyspaceFlush(KEYSPACE, cf);
            LegacyMemoryOnlyStrategyTestUtil.insertData(getNativeClientForNode(1).newSession(), numInserts, KEYSPACE, cf);
            ssProxy.forceKeyspaceFlush(KEYSPACE, cf);
            LegacyMemoryOnlyStrategyTestUtil.waitForCompaction(connection, KEYSPACE, cf);

            if (mmappingShouldWork)
            {
                assertFalse(0 == mosStatus.getMemoryOnlyTableInformation(KEYSPACE, cf).getUsed());
            }
            else
            {
                assertEquals(0, mosStatus.getMemoryOnlyTableInformation(KEYSPACE, cf).getUsed());
                assertFalse(0 == mosStatus.getMemoryOnlyTableInformation(KEYSPACE, cf).getNotAbleToLock());
            }
            LegacyMemoryOnlyStrategyTestUtil.verifyTotals(mosStatus);
        }
    }
}