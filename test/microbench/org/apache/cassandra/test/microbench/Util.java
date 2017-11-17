package org.apache.cassandra.test.microbench;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCMetrics;
import org.apache.cassandra.concurrent.TPCTaskType;

class Util
{
    public static void printTPCStats()
    {
        for (TPCTaskType stage : TPCTaskType.values())
        {
            String v = "";
            for (int i = 0; i < TPC.perCoreMetrics.length; ++i)
            {
                TPCMetrics metrics = TPC.perCoreMetrics[i];
                if (metrics.completedTaskCount(stage) > 0)
                    v += String.format(" %d: %,d(%,d)", i, metrics.completedTaskCount(stage), metrics.blockedTaskCount(stage));
            }
            if (!v.isEmpty())
                System.out.println(stage + ":" + v);
        }
    }
}