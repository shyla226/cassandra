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

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.*;

public class MemoryOnlyStrategyTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(MemoryOnlyStrategyTest.class);
    private MemoryOnlyStrategyTestUtil utils;

    @Before
    public void setUp()
    {
        this.utils = new MemoryOnlyStrategyTestUtil(this);
    }

    @Test
    public void testQueryAfterFlush() throws Throwable
    {
        for (String sstableCompressor : Arrays.asList("", "LZ4Compressor"))
        {
            logger.info("testQueryAfterFlush(): " + (sstableCompressor.isEmpty() ? "sstable_compression turned off" : String.format("Using sstable_compression: %s", sstableCompressor)));

            // create a table and insert some data
            utils.createTable(sstableCompressor);
            utils.execute("INSERT INTO %s (k,v) VALUES (1,2);");

            // perform a read - everything is in the memtable
            assertRows(utils.execute("SELECT * FROM %s WHERE k=1"), row(1, 2));

            // now flush & redo the read. We should read it from the sstable
            flush(KEYSPACE_PER_TEST);

            assertRows(utils.execute("SELECT * FROM %s WHERE k=1"), row(1, 2));
        }
    }

    @Test
    public void testTruncateClearsData() throws Throwable
    {
        for (String sstableCompressor : Arrays.asList("", "LZ4Compressor"))
        {
            logger.info("testTruncateClearsData(): " + (sstableCompressor.isEmpty() ? "sstable_compression turned off" : String.format("Using sstable_compression: %s", sstableCompressor)));

            utils.createTable(sstableCompressor);

            MemoryOnlyStatus mosStatus = MemoryOnlyStatus.instance;

            final int numInserts = 1000;
            utils.insertData(numInserts);

            assertEquals(numInserts, getRows(utils.execute("SELECT * FROM %s")).length); // read from memtable

            assertEquals(0, mosStatus.getMemoryOnlyTableInformation(KEYSPACE_PER_TEST, currentTable()).getUsed());
            utils.verifyTotals(mosStatus);

            utils.execute("TRUNCATE %s");
            assertEquals(0, getRows(utils.execute("SELECT * FROM %s")).length);

            assertEquals(0, mosStatus.getMemoryOnlyTableInformation(KEYSPACE_PER_TEST, currentTable()).getUsed());
            utils.verifyTotals(mosStatus);

            // Now flush and do it
            // now flush & redo the read. We should read it from the sstable

            utils.insertData(numInserts);
            flush(KEYSPACE_PER_TEST);
            utils.insertData(numInserts);
            flush(KEYSPACE_PER_TEST);
            utils.insertData(numInserts);
            flush(KEYSPACE_PER_TEST);
            compact(KEYSPACE_PER_TEST);
            utils.insertData(numInserts);

            assertFalse(0 == mosStatus.getMemoryOnlyTableInformation(KEYSPACE_PER_TEST, currentTable()).getUsed());
            utils.verifyTotals(mosStatus);

            assertEquals(numInserts, getRows(utils.execute("SELECT * FROM %s")).length);

            utils.execute("TRUNCATE %s");

            assertEquals(0, getRows(utils.execute("SELECT * FROM %s")).length);

            flush(KEYSPACE_PER_TEST);

            assertEquals(0, mosStatus.getMemoryOnlyTableInformation(KEYSPACE_PER_TEST, currentTable()).getUsed());
            utils.verifyTotals(mosStatus);
        }
    }

    @Test
    public void testDropClearsData() throws Throwable
    {
        for (String sstableCompressor : Arrays.asList("", "LZ4Compressor"))
        {
            logger.info("testDropClearsData(): " + (sstableCompressor.isEmpty() ? "sstable_compression turned off" : String.format("Using sstable_compression: %s", sstableCompressor)));

            utils.createTable(sstableCompressor);

            MemoryOnlyStatus mosStatus = MemoryOnlyStatus.instance;

            final int numInserts = 1000;
            utils.insertData(numInserts);

            assertEquals(numInserts, getRows(utils.execute("SELECT * FROM %s")).length);

            assertTrue(0 == mosStatus.getMemoryOnlyTableInformation(KEYSPACE_PER_TEST, currentTable()).getUsed());
            utils.verifyTotals(mosStatus);

            dropTable(KEYSPACE_PER_TEST, "DROP TABLE %s");
            utils.verifyTotals(mosStatus);

            // Test another table but this time with flushing and compaction
            utils.createTable(sstableCompressor);

            utils.insertData(numInserts);
            flush(KEYSPACE_PER_TEST);

            utils.insertData(numInserts);
            flush(KEYSPACE_PER_TEST);

            utils.insertData(numInserts);
            flush(KEYSPACE_PER_TEST);

            compact(KEYSPACE_PER_TEST);

            utils.insertData(numInserts);

            utils.verifyTotals(mosStatus);
            assertFalse(0 == mosStatus.getMemoryOnlyTableInformation(KEYSPACE_PER_TEST, currentTable()).getUsed());

            utils.verifyTotals(mosStatus);
            assertFalse(0 == mosStatus.getMemoryOnlyTableInformation(KEYSPACE_PER_TEST, currentTable()).getUsed());

            assertEquals(numInserts, getRows(utils.execute("SELECT * FROM %s")).length);

            dropTable(KEYSPACE_PER_TEST, "DROP TABLE %s");
            utils.verifyTotals(mosStatus);
        }
    }

    private void testMemoryOnlyStrategyOptions(String minThreshold, String maxthreshold, String active) throws ConfigurationException
    {
        Map<String, String> options = new TreeMap<>();
        options.put(MemoryOnlyStrategyOptions.MOS_MINCOMPACTIONTHRESHOLD, minThreshold);
        options.put(MemoryOnlyStrategyOptions.MOS_MAXCOMPACTIONTHRESHOLD, maxthreshold);
        options.put(MemoryOnlyStrategyOptions.MOS_MAXACTIVECOMPACTIONS, active);
        MemoryOnlyStrategyOptions.validateOptions(options, Collections.emptyMap());
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