/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.mos;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;

import static org.junit.Assert.assertEquals;

class MemoryOnlyStrategyTestUtil
{
    private static final Logger logger = LoggerFactory.getLogger(MemoryOnlyStrategyTestUtil.class);
    private final CQLTester tester;

    MemoryOnlyStrategyTestUtil(CQLTester tester)
    {
        this.tester = tester;
    }

    String createTable(String sstableCompressor)
    {
        String query = String.format("CREATE TABLE IF NOT EXISTS %%s ( k int PRIMARY KEY, v INT) " +
                                     "WITH compaction = {'class': 'MemoryOnlyStrategy', 'mos_min_threshold':'2', 'mos_max_threshold':'32'} " +
                                     "AND compression = {'sstable_compression' : '%s'} " +
                                     "AND caching = {'keys':'NONE', 'rows_per_partition':'NONE'};",
                                     sstableCompressor);

        String ret = tester.createTable(tester.KEYSPACE_PER_TEST, query);

        ColumnFamilyStore cfs = tester.getCurrentColumnFamilyStore(tester.KEYSPACE_PER_TEST);
        cfs.disableAutoCompaction();

        return ret;
    }

    UntypedResultSet execute(String query, Object... values) throws Throwable
    {
        return tester.executeFormattedQuery(tester.formatQuery(tester.KEYSPACE_PER_TEST, query), values);
    }

    void insertData(final int numInserts) throws Throwable
    {
        for (int i = 0; i < numInserts; i++)
            execute("INSERT INTO %s (k,v) VALUES(?,?)", i, i);
    }

    void verifyTotals(MemoryOnlyStatus mosStatus)
    {
        MemoryOnlyStatusMXBean.TotalInfo totals = mosStatus.getMemoryOnlyTotals();
        logger.info(String.format("Max Memory to Lock:                    %10dB\n", totals.getMaxMemoryToLock()));
        logger.info(String.format("Current Total Memory Locked:           %10dB\n", totals.getUsed()));
        logger.info(String.format("Current Total Memory Not Able To Lock: %10dB\n", totals.getNotAbleToLock()));

        long used = 0;
        long notLocked = 0;

        logger.info(String.format("%-30s %-30s %12s %17s %7s\n", "Keyspace", "ColumnFamily", "Size", "Couldn't Lock",
                                  "Usage"));
        for (MemoryOnlyStatusMXBean.TableInfo mi : mosStatus.getMemoryOnlyTableInformation())
        {
            logger.info(String.format("%-30s %-30s %10dB %15dB %6.0f%%\n", mi.getKs(), mi.getCf(), mi.getUsed(),
                                      mi.getNotAbleToLock(), (100.0 * mi.getUsed()) / mi.getMaxMemoryToLock()));
            used += mi.getUsed();
            notLocked += mi.getNotAbleToLock();
        }

        assertEquals(used, totals.getUsed());
        assertEquals(notLocked, totals.getNotAbleToLock());
    }
}