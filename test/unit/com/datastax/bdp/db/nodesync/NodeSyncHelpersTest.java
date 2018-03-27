package com.datastax.bdp.db.nodesync;

import org.apache.cassandra.cql3.CQLTester;
import org.junit.Test;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;

public class NodeSyncHelpersTest extends CQLTester
{

    @Test
    public void testEstimatedSizeOfWithCompression() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, value blob) WITH compression = {'class': 'LZ4Compressor'};");
        testEstimatedSizeOf();
    }

    @Test
    public void testEstimatedSizeOfWithoutCompression() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, value blob) WITH compression = {'enabled':'false'};");
        testEstimatedSizeOf();
    }

    private void testEstimatedSizeOf() throws Throwable
    {
        // no data
        assertEstimatedSize(0);

        // in-memory data 1MB
        int oneMB = 1024 * 1024;
        execute("INSERT INTO %s(key, value) VALUES(1, ?)", ByteBuffer.allocate(oneMB));
        assertEstimatedSize(oneMB);

        // on-disk data 1MB
        flush();
        assertEstimatedSize(oneMB);

        // in-memory data 1MB + on-disk data 1MB
        execute("INSERT INTO %s(key, value) VALUES(1, ?)", ByteBuffer.allocate(oneMB));
        assertEstimatedSize(oneMB * 2);
    }

    private void assertEstimatedSize(long expect)
    {
        long actual = NodeSyncHelpers.estimatedSizeOf(getCurrentColumnFamilyStore());
        double delta = expect * 0.05;
        assertEquals(expect, actual, delta);
    }

}
