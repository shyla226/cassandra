/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.mos;


import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.dse.framework.DseTestRunner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageServiceMBean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

/**
 * Legacy test for MemoryOnlyStrategy. This test is obsoleted by {@link MemoryOnlyStrategyTest} and only
 * exists to test the DSE testing framework, see {@link com.datastax.dse.framework.DseTestRunner}.
 *
 * This test is no longer required for MOS and can be removed if other tests use the DSE testing framework.
 */
public class LegacyMemoryOnlyStrategyTest extends DseTestRunner
{
    private static final Logger logger = LoggerFactory.getLogger(LegacyMemoryOnlyStrategyTest.class);
    private static boolean nodeStarted;
    private static String KEYSPACE = "ks" + System.nanoTime();

    @Before
    public void setUp() throws Exception
    {
        if (!nodeStarted)
        {
            startNode(1);
            nodeStarted = true;
        }

        LegacyMemoryOnlyStrategyTestUtil.maybeCreateKeyspace(KEYSPACE);
    }

    @Test
    public void testQueryAfterFlush() throws Exception
    {
        for (String sstableCompressor : Arrays.asList("", "LZ4Compressor"))
        {
            logger.info("testQueryAfterFlush(): " + ("".equals(sstableCompressor) ? "sstable_compression turned off" : String.format("Using sstable_compression: %s", sstableCompressor)));
            String cf = LegacyMemoryOnlyStrategyTestUtil.createNewTable(KEYSPACE, sstableCompressor);
            String insert = String.format("INSERT INTO %s.%s (k,v) VALUES (1,2);", KEYSPACE, cf);
            sendCql3Native(1, KEYSPACE, insert);

            MBeanServerConnection connection = getMBeanServerConnection();

            // perform a read - everything is in the memtable
            String select = String.format("SELECT * FROM %s.%s WHERE k=1", KEYSPACE, cf);
            ResultSet rs = sendCql3Native(1, KEYSPACE, select);
            assertRows(rs, row(1, 2));

            // now flush & redo the read. We should read it from the sstable
            StorageServiceMBean ssProxy = JMX.newMBeanProxy(connection,
                                                            new ObjectName("org.apache.cassandra.db:type=StorageService"),
                                                            StorageServiceMBean.class);
            ssProxy.forceKeyspaceFlush(KEYSPACE, cf);
            rs = sendCql3Native(1, KEYSPACE, select);
            assertRows(rs, row(1, 2));
        }
    }

    @Test
    public void testTruncateClearsData() throws Exception
    {
        for (String sstableCompressor : Arrays.asList("", "LZ4Compressor"))
        {
            logger.info("testTruncateClearsData(): " + ("".equals(sstableCompressor) ? "sstable_compression turned off" : String.format("Using sstable_compression: %s", sstableCompressor)));
            String cf = LegacyMemoryOnlyStrategyTestUtil.createNewTable(KEYSPACE, sstableCompressor);
            MBeanServerConnection connection = getMBeanServerConnection();
            MemoryOnlyStatusMXBean mosStatus = LegacyMemoryOnlyStrategyTestUtil.getMosProxy(connection);
            final int numInserts = 4000;
            LegacyMemoryOnlyStrategyTestUtil.insertData(getNativeClientForNode(1).newSession(), numInserts, KEYSPACE, cf);

            String select = String.format("SELECT * FROM %s.%s", KEYSPACE, cf);
            assertEquals(numInserts, sendCql3Native(1, KEYSPACE, select).all().size());

            assertEquals(0, mosStatus.getMemoryOnlyTableInformation(KEYSPACE, cf).getUsed());
            LegacyMemoryOnlyStrategyTestUtil.verifyTotals(mosStatus);

            String truncate = String.format("TRUNCATE %s.%s", KEYSPACE, cf);
            sendCql3Native(1, null, truncate);

            assertEquals(0, sendCql3Native(1, KEYSPACE, select).all().size());

            assertEquals(0, mosStatus.getMemoryOnlyTableInformation(KEYSPACE, cf).getUsed());
            LegacyMemoryOnlyStrategyTestUtil.verifyTotals(mosStatus);

            // Now flush and do it
            // now flush & redo the read. We should read it from the sstable
            StorageServiceMBean ssProxy = LegacyMemoryOnlyStrategyTestUtil.getProxy(connection, "org.apache.cassandra.db:type=StorageService", StorageServiceMBean.class);
            LegacyMemoryOnlyStrategyTestUtil.insertData(getNativeClientForNode(1).newSession(), numInserts, KEYSPACE, cf);
            ssProxy.forceKeyspaceFlush(KEYSPACE, cf);
            LegacyMemoryOnlyStrategyTestUtil.insertData(getNativeClientForNode(1).newSession(), numInserts, KEYSPACE, cf);
            ssProxy.forceKeyspaceFlush(KEYSPACE, cf);
            LegacyMemoryOnlyStrategyTestUtil.insertData(getNativeClientForNode(1).newSession(), numInserts, KEYSPACE, cf);
            ssProxy.forceKeyspaceFlush(KEYSPACE, cf);
            LegacyMemoryOnlyStrategyTestUtil.waitForCompaction(connection, KEYSPACE, cf);
            LegacyMemoryOnlyStrategyTestUtil.insertData(getNativeClientForNode(1).newSession(), numInserts, KEYSPACE, cf);

            assertNotEquals(0, mosStatus.getMemoryOnlyTableInformation(KEYSPACE, cf).getUsed());
            LegacyMemoryOnlyStrategyTestUtil.verifyTotals(mosStatus);

            assertEquals(numInserts, sendCql3Native(1, KEYSPACE, select).all().size());

            sendCql3Native(1, null, truncate);

            assertEquals(0, sendCql3Native(1, KEYSPACE, select).all().size());

            ssProxy.forceKeyspaceFlush(KEYSPACE, cf);

            assertEquals(0, mosStatus.getMemoryOnlyTableInformation(KEYSPACE, cf).getUsed());
            LegacyMemoryOnlyStrategyTestUtil.verifyTotals(mosStatus);
        }
    }

    @Test
    public void testDropClearsData() throws Exception
    {
        for (String sstableCompressor : Arrays.asList("", "LZ4Compressor"))
        {
            logger.info("testDropClearsData(): " + ("".equals(sstableCompressor) ? "sstable_compression turned off" : String.format("Using sstable_compression: %s", sstableCompressor)));
            String cf = LegacyMemoryOnlyStrategyTestUtil.createNewTable(KEYSPACE, sstableCompressor);
            MBeanServerConnection connection = getMBeanServerConnection();

            MemoryOnlyStatusMXBean mosStatus = LegacyMemoryOnlyStrategyTestUtil.getMosProxy(connection);

            final int numInserts = 4000;
            LegacyMemoryOnlyStrategyTestUtil.insertData(getNativeClientForNode(1).newSession(), numInserts, KEYSPACE, cf);

            String select = String.format("SELECT * FROM %s.%s", KEYSPACE, cf);
            assertEquals(numInserts, sendCql3Native(1, KEYSPACE, select).all().size());

            LegacyMemoryOnlyStrategyTestUtil.verifyTotals(mosStatus);

            String drop = String.format("DROP TABLE %s.%s", KEYSPACE, cf);
            sendCql3Native(1, null, drop);

            LegacyMemoryOnlyStrategyTestUtil.verifyTotals(mosStatus);

            // Now flush and do it
            // now flush & redo the read. We should read it from the sstable
            StorageServiceMBean ssProxy = LegacyMemoryOnlyStrategyTestUtil.getProxy(connection, "org.apache.cassandra.db:type=StorageService", StorageServiceMBean.class);

            cf = LegacyMemoryOnlyStrategyTestUtil.createNewTable(KEYSPACE, sstableCompressor);
            select = String.format("SELECT * FROM %s.%s", KEYSPACE, cf);
            drop = String.format("DROP TABLE %s.%s", KEYSPACE, cf);

            LegacyMemoryOnlyStrategyTestUtil.insertData(getNativeClientForNode(1).newSession(), numInserts, KEYSPACE, cf);
            ssProxy.forceKeyspaceFlush(KEYSPACE, cf);
            LegacyMemoryOnlyStrategyTestUtil.insertData(getNativeClientForNode(1).newSession(), numInserts, KEYSPACE, cf);
            ssProxy.forceKeyspaceFlush(KEYSPACE, cf);
            LegacyMemoryOnlyStrategyTestUtil.insertData(getNativeClientForNode(1).newSession(), numInserts, KEYSPACE, cf);
            ssProxy.forceKeyspaceFlush(KEYSPACE, cf);
            LegacyMemoryOnlyStrategyTestUtil.waitForCompaction(connection, KEYSPACE, cf);
            LegacyMemoryOnlyStrategyTestUtil.insertData(getNativeClientForNode(1).newSession(), numInserts, KEYSPACE, cf);

            LegacyMemoryOnlyStrategyTestUtil.verifyTotals(mosStatus);

            assertNotEquals(0, mosStatus.getMemoryOnlyTableInformation(KEYSPACE, cf).getUsed());
            LegacyMemoryOnlyStrategyTestUtil.verifyTotals(mosStatus);

            assertEquals(numInserts, sendCql3Native(1, KEYSPACE, select).all().size());

            sendCql3Native(1, null, drop);
            // see comment in MemoryOnlyStatus.getMemoryOnlyTableInformation(), if tables are dropped the memory used
            // by the tables is not captured but the totals are still not up-to-date
            Thread.sleep(100);
            LegacyMemoryOnlyStrategyTestUtil.verifyTotals(mosStatus);
        }
    }

    private void testMemoryOnlyStrategyOptions(String minThreshold, String maxthreshold, String active) throws ConfigurationException
    {
        Map<String, String> options = new TreeMap<>();
        options.put(MemoryOnlyStrategyOptions.MOS_MINCOMPACTIONTHRESHOLD, minThreshold);
        options.put(MemoryOnlyStrategyOptions.MOS_MAXCOMPACTIONTHRESHOLD, maxthreshold);
        options.put(MemoryOnlyStrategyOptions.MOS_MAXACTIVECOMPACTIONS, active);
        MemoryOnlyStrategyOptions.validateOptions(options, Collections.<String, String>emptyMap());
    }

    @Test(expected = ConfigurationException.class)
    public void testMemoryOnlyStrategyOptionsOORMinimum() throws ConfigurationException
    {
        testMemoryOnlyStrategyOptions("1", "32", "1");
    }

    @Test(expected = ConfigurationException.class)
    public void testMemoryOnlyStrategyOptionsInvalidMinimum() throws ConfigurationException
    {
        testMemoryOnlyStrategyOptions("LOLWUT", "33", "1");
    }

    @Test(expected = ConfigurationException.class)
    public void testMemoryOnlyStrategyOptionsCrossingMinMax() throws ConfigurationException
    {
        testMemoryOnlyStrategyOptions("5", "4", "1");
    }

    @Test
    public void testMemoryOnlyStrategyOptionsValid() throws ConfigurationException
    {
        testMemoryOnlyStrategyOptions("4", "24", "1");
    }

    @Test(expected = ConfigurationException.class)
    public void testMemoryOnlyStrategyOptionsLowMax() throws ConfigurationException
    {
        testMemoryOnlyStrategyOptions("4", "24", "0");
    }
}