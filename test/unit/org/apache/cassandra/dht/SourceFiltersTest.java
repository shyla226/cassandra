/*
 * Copyright DataStax, Inc.
 */
package org.apache.cassandra.dht;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import org.junit.Test;

import org.apache.cassandra.dht.RangeStreamer.ISourceFilter;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.Arrays.asList;

import static org.junit.Assert.*;

public class SourceFiltersTest
{
    private IEndpointSnitch snitch = new AbstractNetworkTopologySnitch()
    {
        public String getRack(InetAddress endpoint)
        {
            return "Rack-" + endpoint.getAddress()[2];
        }

        public String getDatacenter(InetAddress endpoint)
        {
            return "DC-" + endpoint.getAddress()[1];
        }
    };

    private IFailureDetector failureDetector = new IFailureDetector()
    {
        public boolean isAlive(InetAddress endpoint)
        {
            return endpoint.getAddress()[1] != 2;
        }

        public void interpret(InetAddress ep) { throw new UnsupportedOperationException(); }
        public void report(InetAddress ep) { throw new UnsupportedOperationException(); }
        public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
        public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
        public void remove(InetAddress ep) { throw new UnsupportedOperationException(); }
        public void forceConviction(InetAddress ep) { throw new UnsupportedOperationException(); }
    };

    @Test
    public void testIncludeSources()
    {
        ISourceFilter filter = SourceFilters.includeSources(asList(addr(1, 1, 1), addr(1, 1, 2), addr(1, 1, 3)));
        assertTrue(filter.shouldInclude(addr(1, 1,1)));
        assertTrue(filter.shouldInclude(addr(1, 1,2)));
        assertTrue(filter.shouldInclude(addr(1, 1,3)));
        assertFalse(filter.shouldInclude(addr(1, 1,4)));
        assertFalse(filter.shouldInclude(addr(2, 1,1)));
        assertFalse(filter.shouldInclude(addr(1, 2,1)));
    }

    @Test
    public void testExcludeSources()
    {
        ISourceFilter filter = SourceFilters.excludeSources(asList(addr(1, 1, 1), addr(1, 1, 2), addr(1, 1, 3)));
        assertFalse(filter.shouldInclude(addr(1, 1,1)));
        assertFalse(filter.shouldInclude(addr(1, 1,2)));
        assertFalse(filter.shouldInclude(addr(1, 1,3)));
        assertTrue(filter.shouldInclude(addr(1, 1,4)));
        assertTrue(filter.shouldInclude(addr(2, 1,1)));
        assertTrue(filter.shouldInclude(addr(1, 2,1)));
    }

    @Test
    public void testExcludeLocalNode()
    {
        ISourceFilter filter = SourceFilters.excludeLocalNode();
        assertTrue(filter.shouldInclude(addr(1, 1,1)));
        assertTrue(filter.shouldInclude(addr(2, 1,1)));
        assertFalse(filter.shouldInclude(FBUtilities.getBroadcastAddress()));
    }

    @Test
    public void testIncludeRack()
    {
        ISourceFilter filter = SourceFilters.includeRacks(asList("Rack-1", "Rack-3"), snitch);
        assertTrue(filter.shouldInclude(addr(1, 1,1)));
        assertTrue(filter.shouldInclude(addr(1, 1,2)));
        assertFalse(filter.shouldInclude(addr(1, 2,1)));
        assertTrue(filter.shouldInclude(addr(1, 3,1)));
        assertTrue(filter.shouldInclude(addr(2, 1,1)));
        assertFalse(filter.shouldInclude(addr(2, 2,1)));
    }

    @Test
    public void testIncludeSingleDc()
    {
        Map<String, ISourceFilter> filterPerDcs = new HashMap<>();
        filterPerDcs.put("DC-1", SourceFilters.noop());

        ISourceFilter filter = SourceFilters.includeDcs(filterPerDcs, snitch);
        assertTrue(filter.shouldInclude(addr(1, 1,1)));
        assertTrue(filter.shouldInclude(addr(1, 1,2)));
        assertTrue(filter.shouldInclude(addr(1, 2,1)));
        assertTrue(filter.shouldInclude(addr(1, 3,1)));
        assertFalse(filter.shouldInclude(addr(2, 1,1)));
        assertFalse(filter.shouldInclude(addr(2, 2,1)));
        assertFalse(filter.shouldInclude(addr(3, 2,1)));
    }

    @Test
    public void testIncludeSingleRack()
    {
        Map<String, ISourceFilter> filterPerDcs = new HashMap<>();
        filterPerDcs.put("DC-1", SourceFilters.includeRacks(asList("Rack-1"), snitch));

        ISourceFilter filter = SourceFilters.includeDcs(filterPerDcs, snitch);
        assertTrue(filter.shouldInclude(addr(1, 1,1)));
        assertTrue(filter.shouldInclude(addr(1, 1,2)));
        assertFalse(filter.shouldInclude(addr(1, 2,1)));
        assertFalse(filter.shouldInclude(addr(1, 3,1)));
        assertFalse(filter.shouldInclude(addr(2, 1,1)));
        assertFalse(filter.shouldInclude(addr(2, 2,1)));
        assertFalse(filter.shouldInclude(addr(3, 2,1)));
    }

    @Test
    public void testIncludeDcs()
    {
        Map<String, ISourceFilter> filterPerDcs = new HashMap<>();
        filterPerDcs.put("DC-1", SourceFilters.includeRacks(asList("Rack-1", "Rack-3"), snitch));
        filterPerDcs.put("DC-2", SourceFilters.noop());

        ISourceFilter filter = SourceFilters.includeDcs(filterPerDcs, snitch);
        assertTrue(filter.shouldInclude(addr(1, 1,1)));
        assertTrue(filter.shouldInclude(addr(1, 1,2)));
        assertFalse(filter.shouldInclude(addr(1, 2,1)));
        assertTrue(filter.shouldInclude(addr(1, 3,1)));
        assertTrue(filter.shouldInclude(addr(2, 1,1)));
        assertTrue(filter.shouldInclude(addr(2, 2,1)));
        assertFalse(filter.shouldInclude(addr(3, 2,1)));
    }

    @Test
    public void testExcludeSingleDc()
    {
        Map<String, ISourceFilter> filterPerDcs = new HashMap<>();
        filterPerDcs.put("DC-1", SourceFilters.noop());

        ISourceFilter filter = SourceFilters.excludeDcs(filterPerDcs, snitch);
        assertFalse(filter.shouldInclude(addr(1, 1,1)));
        assertFalse(filter.shouldInclude(addr(1, 1,2)));
        assertFalse(filter.shouldInclude(addr(1, 2,1)));
        assertFalse(filter.shouldInclude(addr(1, 3,1)));
        assertTrue(filter.shouldInclude(addr(2, 1,1)));
        assertTrue(filter.shouldInclude(addr(2, 2,1)));
        assertTrue(filter.shouldInclude(addr(3, 2,1)));
    }

    @Test
    public void testExcludeSingleRack()
    {
        Map<String, ISourceFilter> filterPerDcs = new HashMap<>();
        filterPerDcs.put("DC-1", SourceFilters.includeRacks(asList("Rack-1"), snitch));

        ISourceFilter filter = SourceFilters.excludeDcs(filterPerDcs, snitch);
        assertFalse(filter.shouldInclude(addr(1, 1,1)));
        assertFalse(filter.shouldInclude(addr(1, 1,2)));
        assertTrue(filter.shouldInclude(addr(1, 2,1)));
        assertTrue(filter.shouldInclude(addr(1, 3,1)));
        assertTrue(filter.shouldInclude(addr(2, 1,1)));
        assertTrue(filter.shouldInclude(addr(2, 2,1)));
        assertTrue(filter.shouldInclude(addr(3, 2,1)));
    }

    @Test
    public void testExcludeDcs()
    {
        Map<String, ISourceFilter> filterPerDcs = new HashMap<>();
        filterPerDcs.put("DC-1", SourceFilters.includeRacks(asList("Rack-1", "Rack-3"), snitch));
        filterPerDcs.put("DC-2", SourceFilters.noop());

        ISourceFilter filter = SourceFilters.excludeDcs(filterPerDcs, snitch);
        assertFalse(filter.shouldInclude(addr(1, 1,1)));
        assertFalse(filter.shouldInclude(addr(1, 1,2)));
        assertTrue(filter.shouldInclude(addr(1, 2,1)));
        assertFalse(filter.shouldInclude(addr(1, 3,1)));
        assertFalse(filter.shouldInclude(addr(2, 1,1)));
        assertFalse(filter.shouldInclude(addr(2, 2,1)));
        assertTrue(filter.shouldInclude(addr(3, 2,1)));
    }

    @Test
    public void testFailureDetectorFilter()
    {
        ISourceFilter filter = SourceFilters.failureDetectorFilter(failureDetector);
        assertTrue(filter.shouldInclude(addr(1, 1,1)));
        assertTrue(filter.shouldInclude(addr(1, 1,2)));
        assertFalse(filter.shouldInclude(addr(2, 1,1)));
        assertFalse(filter.shouldInclude(addr(2, 2,1)));
        assertTrue(filter.shouldInclude(addr(3, 2,1)));
    }

    @Test
    public void testNoopFilter()
    {
        ISourceFilter filter = SourceFilters.noop();
        Random r = new Random();
        for (int i = 0; i < 20; i++)
        {
            assertTrue(filter.shouldInclude(addr(r.nextInt(255), r.nextInt(255),r.nextInt(255))));
        }
    }

    private static InetAddress addr(int dc, int rack, int node)
    {
        byte[] addr = new byte[]{ 1, (byte) dc, (byte) rack, (byte) node };
        try
        {
            return InetAddress.getByAddress(addr);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }
}
