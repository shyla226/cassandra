/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.mos;

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.util.List;

/**
 * Return information about any in-memory tables through JMX.  Currently returns the memory used
 * vs total capacity of the table.
 */
public interface MemoryOnlyStatusMXBean
{
    public static final String MXBEAN_NAME = "org.apache.cassandra.db:type=MemoryOnlyStatus";

    // Simple class to serialize a tuple of (keyspace, columnfamily, used memory, not able to lock memory, max memory)
    public class TableInfo implements Serializable
    {
        private final String ks;
        private final String cf;
        private final long used;
        private final long notAbleToLock;
        private final long maxMemoryToLock;

        @ConstructorProperties({ "ks", "cf", "used", "notAbleToLock", "maxMemoryToLock" })
        public TableInfo(String ks, String cf, long used, long notAbleToLock, long maxMemoryToLock)
        {
            this.ks = ks;
            this.cf = cf;
            this.used = used;
            this.notAbleToLock = notAbleToLock;
            this.maxMemoryToLock = maxMemoryToLock;
        }

        // Hello java boilerplate!  Unfortunately this seems necessary to help the MXBean server create
        // a CompositeDataSupport object.
        public String getKs()
        {
            return ks;
        }

        public String getCf()
        {
            return cf;
        }

        public long getUsed()
        {
            return used;
        }

        public long getNotAbleToLock()
        {
            return notAbleToLock;
        }

        public long getMaxMemoryToLock()
        {
            return maxMemoryToLock;
        }
    }

    // Simple class to serialize a tuple of (used memory, not able to lock memory, max memory)
    public class TotalInfo implements Serializable
    {
        private final long used;
        private final long notAbleToLock;
        private final long maxMemoryToLock;

        @ConstructorProperties({ "used", "notAbleToLock", "maxMemoryToLock" })
        public TotalInfo(long used, long notAbleToLock, long maxMemoryToLock)
        {
            this.used = used;
            this.notAbleToLock = notAbleToLock;
            this.maxMemoryToLock = maxMemoryToLock;
        }

        // Hello java boilerplate!  Unfortunately this seems necessary to help the MXBean server create
        // a CompositeDataSupport object.
        public long getUsed()
        {
            return used;
        }

        public long getNotAbleToLock()
        {
            return notAbleToLock;
        }

        public long getMaxMemoryToLock()
        {
            return maxMemoryToLock;
        }
    }

    /**
     * @return A list of TableInfo describing the size of each in-memory table
     */
    public List<TableInfo> getMemoryOnlyTableInformation();

    /**
     * @return A single TableInfo describing the size of an in-memory table
     */
    public TableInfo getMemoryOnlyTableInformation(String ks, String cf);

    /**
     * @return Current memory status of all in-memory tables
     */
    public TotalInfo getMemoryOnlyTotals();

    /**
     * @return Current percent used of all in-memory tables
     */
    public double getMemoryOnlyPercentUsed();
}
