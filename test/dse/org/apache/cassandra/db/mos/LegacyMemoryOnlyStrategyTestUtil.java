/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.mos;


import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.googlecode.junittoolbox.PollingWait;
import com.googlecode.junittoolbox.RunnableAssert;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

import static org.junit.Assert.assertEquals;
import static com.datastax.dse.framework.DseTestRunner.sendCql3Native;

public class LegacyMemoryOnlyStrategyTestUtil
{
    private static final Logger logger = LoggerFactory.getLogger(LegacyMemoryOnlyStrategyTestUtil.class);

    static void maybeCreateKeyspace(String keyspace) throws Exception
    {
        String cql = String.format("CREATE KEYSPACE IF NOT EXISTS %s " +
                                   "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
                                   keyspace);
        sendCql3Native(1, null, cql);
    }

    static String createNewTable(String keyspace, String sstableCompressor) throws Exception
    {
        String name = "cf" + System.nanoTime();
        String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s ( k int PRIMARY KEY, v INT) " +
                                   "WITH compaction = {'class': 'MemoryOnlyStrategy', 'mos_min_threshold':'2', 'mos_max_threshold':'32'} " +
                                   "AND compression = {'sstable_compression' : '%s'} " +
                                   "AND caching = {'keys':'NONE', 'rows_per_partition':'NONE'};",
                                   keyspace, name, sstableCompressor);
        sendCql3Native(1, null, cql);
        return name;
    }

    static void insertData(Session session, int partitions, String ks, String cf) throws Exception
    {
        PreparedStatement ps = session.prepare(String.format("INSERT INTO %s.%s (k,v) VALUES(?,?)",
                                                             ks, cf));
        final CountDownLatch latch = new CountDownLatch(partitions);
        ConcurrentLinkedQueue errors = new ConcurrentLinkedQueue<Throwable>();

        Semaphore maxRunning = new Semaphore(100);

        FutureCallback<Object> callback = new FutureCallback<Object>()
        {
            @Override
            public void onSuccess(Object o)
            {
                latch.countDown();
                maxRunning.release();
            }

            @Override
            public void onFailure(Throwable throwable)
            {
                latch.countDown();
                errors.add(throwable);
                maxRunning.release();
            }
        };

        for (int i = 0; i < partitions; i++)
        {
            maxRunning.acquire();
            Futures.addCallback(session.executeAsync(ps.bind(i, i)), callback);
        }
        latch.await();
        assertEquals("Errors inserting data: " +
                     errors.stream().map(i -> "Error: " + ((Throwable) i).getMessage() + "\n" +
                                              Arrays.toString(((Throwable) i).getStackTrace()).replace(", ", "\n\t")
                     ).collect(Collectors.joining(",\n")),
                     0, errors.size());
    }

    static void verifyTotals(MemoryOnlyStatusMXBean mosStatus)
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

    static void waitForCompaction(MBeanServerConnection connection, String ks, String cf) throws Exception
    {
        ObjectName oName = new ObjectName(String.format("org.apache.cassandra.metrics:type=Table,keyspace=%s,scope=%s,name=LiveSSTableCount", ks, cf));
        CassandraMetricsRegistry.JmxGaugeMBean gauge = JMX.newMBeanProxy(connection, oName, CassandraMetricsRegistry.JmxGaugeMBean.class);

        new PollingWait()
        .timeoutAfter(60, TimeUnit.SECONDS)
        .pollEvery(3, TimeUnit.SECONDS)
        .until(new RunnableAssert("Verify MOS has compacted down to only 2 live sstables")
        {
            public void run() throws Exception
            {
                Assert.assertTrue((Integer) gauge.getValue() <= 2);
            }
        });
    }

    static MemoryOnlyStatusMXBean getMosProxy(MBeanServerConnection connection) throws MalformedObjectNameException
    {
        return getProxy(connection, MemoryOnlyStatusMXBean.MXBEAN_NAME, MemoryOnlyStatusMXBean.class);
    }

    static <T> T getProxy(MBeanServerConnection connection, String mbeanName, Class<T> mbeanClass) throws MalformedObjectNameException
    {
        if (JMX.isMXBeanInterface(mbeanClass))
        {
            return JMX.newMXBeanProxy(connection, new ObjectName(mbeanName), mbeanClass);
        }
        return JMX.newMBeanProxy(connection, new ObjectName(mbeanName), mbeanClass);
    }
}