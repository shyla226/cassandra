/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.mos;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;

import static org.junit.Assert.*;

public class MemoryOnlyStrategyDiskAccessModeTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(MemoryOnlyStrategyDiskAccessModeTest.class);
    private MemoryOnlyStrategyTestUtil utils;

    @Before
    public void setUp()
    {
        this.utils = new MemoryOnlyStrategyTestUtil(this);
    }

    @Test
    public void testDiskAccessModeStandard() throws Throwable
    {
        // DiskAccessMode.standard = mmapping is turned off
        DatabaseDescriptor.setDiskAccessMode(Config.AccessMode.standard);
        DatabaseDescriptor.setIndexAccessMode(Config.AccessMode.standard);
        insertSomeDataAndVerifyLockedStatus(false);
    }

    @Test
    public void testDiskAccessModeMmapIndexOnly() throws Throwable
    {
        DatabaseDescriptor.setDiskAccessMode(Config.AccessMode.standard);
        DatabaseDescriptor.setIndexAccessMode(Config.AccessMode.mmap);
        // we can't really distinguish here between data & index files, so mmapping will work
        insertSomeDataAndVerifyLockedStatus(true);
    }

    @Test
    public void testDiskAccessModeMmap() throws Throwable
    {
        DatabaseDescriptor.setDiskAccessMode(Config.AccessMode.mmap);
        DatabaseDescriptor.setIndexAccessMode(Config.AccessMode.mmap);
        insertSomeDataAndVerifyLockedStatus(true);
    }

    private void insertSomeDataAndVerifyLockedStatus(boolean mmappingShouldWork) throws Throwable
    {
        for (String sstableCompressor : Arrays.asList("", "LZ4Compressor"))
        {
            logger.info("insertSomeDataAndVerifyLockedStatus(): " + (sstableCompressor.isEmpty() ? "sstable_compression turned off" : String.format("Using sstable_compression: %s", sstableCompressor)));


            utils.createTable(sstableCompressor);

            ColumnFamilyStore cfs = getCurrentColumnFamilyStore(KEYSPACE_PER_TEST);
            cfs.disableAutoCompaction();

            final int numInserts = 1000;
            utils.insertData(numInserts);

            assertEquals(numInserts, getRows(utils.execute("SELECT * FROM %s")).length); // read from memtable

            MemoryOnlyStatus mosStatus = MemoryOnlyStatus.instance;

            // no memory locked yet because no sstable exists
            assertEquals(0, mosStatus.getMemoryOnlyTableInformation(KEYSPACE_PER_TEST, currentTable()).getUsed());
            utils.verifyTotals(mosStatus);

            flush(KEYSPACE_PER_TEST); // flush the first sstable

            utils.insertData(numInserts);
            flush(KEYSPACE_PER_TEST); // flush another sstable

            assertEquals(numInserts, getRows(utils.execute("SELECT * FROM %s")).length); // read from 2 sstables

            compact(KEYSPACE_PER_TEST); // compact the 2 sstables

            assertEquals(numInserts, getRows(utils.execute("SELECT * FROM %s")).length); // read from compacted sstable

            if (mmappingShouldWork)
            {
                assertFalse(0 == mosStatus.getMemoryOnlyTableInformation(KEYSPACE_PER_TEST, currentTable()).getUsed());
            }
            else
            {
                assertEquals(0, mosStatus.getMemoryOnlyTableInformation(KEYSPACE_PER_TEST, currentTable()).getUsed());
                assertFalse(0 == mosStatus.getMemoryOnlyTableInformation(KEYSPACE_PER_TEST, currentTable()).getNotAbleToLock());
            }

            utils.verifyTotals(mosStatus);
        }
    }
}
