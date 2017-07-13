/*
 * Copyright DataStax, Inc.
 */
package org.apache.cassandra.dht;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.dht.RangeStreamer.ISourceFilter;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;

import static java.util.Arrays.asList;

import static org.junit.Assert.*;

public class StreamingOptionsTest
{
    private final IEndpointSnitch snitch = new AbstractNetworkTopologySnitch()
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

    private final IFailureDetector failureDetector = new IFailureDetector()
    {
        public boolean isAlive(InetAddress ep)
        {
            return true;
        }

        public void interpret(InetAddress ep) { throw new UnsupportedOperationException(); }
        public void report(InetAddress ep) { throw new UnsupportedOperationException(); }
        public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
        public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
        public void remove(InetAddress ep) { throw new UnsupportedOperationException(); }
        public void forceConviction(InetAddress ep) { throw new UnsupportedOperationException(); }
    };

    private final TokenMetadata tmd = new TokenMetadata() {
        Map<InetAddress, UUID> endpointMap = new HashMap<>();
        Map<String, Multimap<String, InetAddress>> dcRacks = new HashMap<>();

        {
            // add 3 DCs, with 2 racks each, and 3 nodes per rack (6 nodes per DC)
            for (int dc = 1; dc <= 3; dc++)
            {
                Multimap<String, InetAddress> racks = HashMultimap.create();
                dcRacks.put(snitch.getDatacenter(addr(dc, 1, 1)), racks);
                for (int rack = 1; rack <= 2; rack++)
                {
                    for (int node = 1; node <= 3; node++)
                    {
                        InetAddress addr = addr(dc, rack, node);
                        racks.put(snitch.getRack(addr), addr);
                        endpointMap.put(addr, UUID.randomUUID());
                    }
                }
            }
        }

        TokenMetadata.Topology topo = new TokenMetadata.Topology() {
            public Map<String, Multimap<String, InetAddress>> getDatacenterRacks()
            {
                return dcRacks;
            }
        };

        public Topology getTopology()
        {
            return topo;
        }

        public Map<InetAddress, UUID> getEndpointToHostIdMapForReading()
        {
            return endpointMap;
        }
    };

    @After
    public void tearDown()
    {
        System.clearProperty(StreamingOptions.BOOTSTRAP_INCLUDE_DCS);
        System.clearProperty(StreamingOptions.BOOTSTRAP_INCLUDE_DCS);
        System.clearProperty(StreamingOptions.BOOTSTRAP_INCLUDE_SOURCES);
        System.clearProperty(StreamingOptions.BOOTSTRAP_EXCLUDE_SOURCES);
        System.clearProperty(StreamingOptions.BOOTSTRAP_EXCLUDE_KEYSPACES);
    }

    @Test
    public void testInvalidConfigurationForBootStrap()
    {
        for (String property : asList("includeDCs", "excludeDCs"))
        {
            property = Config.PROPERTY_PREFIX + "bootstrap." + property;
            System.setProperty(property, " ");
            assertErrorOnForBootStrap("The " + property + " system property does not specify any datacenter/rack");

            System.setProperty(property, "DC-1, DC-1:Rack-1");
            assertErrorOnForBootStrap("The " + property + " system property contains both a rack restriction and a datacenter restriction for the DC-1 datacenter");

            System.setProperty(property, "DC-1, DC-1, DC-2");
            assertErrorOnForBootStrap("The DC-1 datacenter must be specified only once in the " + property + " system property");

            System.clearProperty(property);
        }

        for (String property : asList("includeSources", "excludeSources"))
        {
            property = Config.PROPERTY_PREFIX + "bootstrap." + property;
            System.setProperty(property, " ");
            assertErrorOnForBootStrap("The " + property + " system property does not specify any source");

            System.setProperty(property, "1.1.1.1, 1.1.1.2, 1.1.1.1");
            assertErrorOnForBootStrap("The 1.1.1.1 source must be specified only once in the " + property + " system property");

            System.clearProperty(property);
        }

        System.setProperty(StreamingOptions.BOOTSTRAP_INCLUDE_SOURCES, "1.1.1.1");
        System.setProperty(StreamingOptions.BOOTSTRAP_EXCLUDE_SOURCES, "1.1.1.1");

        assertErrorOnForBootStrap("The " + StreamingOptions.BOOTSTRAP_INCLUDE_SOURCES
                               + " system property cannot be used together with the "
                               + StreamingOptions.BOOTSTRAP_EXCLUDE_SOURCES + ", "
                               + StreamingOptions.BOOTSTRAP_INCLUDE_DCS + " or "
                               + StreamingOptions.BOOTSTRAP_EXCLUDE_DCS + " system properties");

        System.clearProperty(StreamingOptions.BOOTSTRAP_EXCLUDE_SOURCES );
        System.setProperty(StreamingOptions.BOOTSTRAP_INCLUDE_DCS, "DC-1, DC-2");
        assertErrorOnForBootStrap("The " + StreamingOptions.BOOTSTRAP_INCLUDE_SOURCES
                + " system property cannot be used together with the "
                + StreamingOptions.BOOTSTRAP_EXCLUDE_SOURCES + ", "
                + StreamingOptions.BOOTSTRAP_INCLUDE_DCS + " or "
                + StreamingOptions.BOOTSTRAP_EXCLUDE_DCS + " system properties");

        System.clearProperty(StreamingOptions.BOOTSTRAP_INCLUDE_SOURCES);
        System.setProperty(StreamingOptions.BOOTSTRAP_INCLUDE_DCS, "DC-2");
        System.setProperty(StreamingOptions.BOOTSTRAP_EXCLUDE_DCS, "DC-2");
        assertErrorOnForBootStrap("The " + StreamingOptions.BOOTSTRAP_INCLUDE_DCS + " and " + StreamingOptions.BOOTSTRAP_EXCLUDE_DCS + " system properties are conflicting for the datacenter: DC-2");

        System.clearProperty(StreamingOptions.BOOTSTRAP_INCLUDE_DCS);
        System.setProperty(StreamingOptions.BOOTSTRAP_EXCLUDE_KEYSPACES, " ");
        assertErrorOnForBootStrap("The " + StreamingOptions.BOOTSTRAP_EXCLUDE_KEYSPACES + " system property does not specify any keyspace");

        System.setProperty(StreamingOptions.BOOTSTRAP_EXCLUDE_KEYSPACES, "test2");
        assertErrorOnForBootStrap("The test2 keyspace specified within the " + StreamingOptions.BOOTSTRAP_EXCLUDE_KEYSPACES + " system property is not an existing non local strategy keyspace");
    }

    @Test
    public void testInvalidConfigurationForRebuild()
    {
        assertErrorOnForRebuild("The " + StreamingOptions.ARG_INCLUDE_DC_NAMES + " argument does not specify any datacenter/rack",
                                Collections.emptyList(), null, null, null);

        assertErrorOnForRebuild("DC '' is not a known DC in this cluster",
                                "", null);

        assertErrorOnForRebuild("The specific-sources argument does not specify any source",
                                null, "");

        assertErrorOnForRebuild("DC 'DC-X' is not a known DC in this cluster, DC 'DC-Y' is not a known DC in this cluster",
                                asList("DC-X", "DC-Y:Rack-1"), null, null, null);

        assertErrorOnForRebuild("Rack 'Rack-X' is not a known rack in DC 'DC-2' of this cluster",
                                asList("DC-1", "DC-2:Rack-X"), null, null, null);

        assertErrorOnForRebuild("DC 'DC-X' is not a known DC in this cluster, DC 'DC-Y' is not a known DC in this cluster",
                                null, asList("DC-X", "DC-Y:Rack-1"), null, null);

        assertErrorOnForRebuild("Rack 'Rack-X' is not a known rack in DC 'DC-2' of this cluster",
                                null, asList("DC-1", "DC-2:Rack-X"), null, null);

        assertErrorOnForRebuild("Source '/2.1.1.1' is not a known node in this cluster, Source '/2.1.1.2' is not a known node in this cluster",
                                null, null, asList("1.1.1.1", "2.1.1.1", "2.1.1.2"), null);

        assertErrorOnForRebuild("Source '/2.1.1.1' is not a known node in this cluster, Source '/2.1.1.2' is not a known node in this cluster",
                                null, null, null, asList("1.1.1.1", "2.1.1.1", "2.1.1.2"));

        assertErrorOnForRebuild("The " + StreamingOptions.ARG_INCLUDE_DC_NAMES + " argument contains both a rack restriction and a datacenter restriction for the DC-1 datacenter",
                                asList("DC-1", "DC-1:Rack-1"), null, null, null);

        assertErrorOnForRebuild("The DC-1 datacenter must be specified only once in the " + StreamingOptions.ARG_INCLUDE_DC_NAMES + " argument",
                                asList("DC-1", "DC-1", "DC-2"), null, null, null);

        assertErrorOnForRebuild("The " + StreamingOptions.ARG_EXCLUDE_DC_NAMES + " argument does not specify any datacenter/rack",
                                null, Collections.emptyList(), null, null);

        assertErrorOnForRebuild("The " + StreamingOptions.ARG_EXCLUDE_DC_NAMES + " argument contains both a rack restriction and a datacenter restriction for the DC-1 datacenter",
                                null, asList("DC-1", "DC-1:Rack-1"), null, null);

        assertErrorOnForRebuild("The DC-1 datacenter must be specified only once in the " + StreamingOptions.ARG_EXCLUDE_DC_NAMES + " argument",
                                null, asList("DC-1", "DC-1", "DC-2"), null, null);

        assertErrorOnForRebuild("The " + StreamingOptions.ARG_INCLUDE_SOURCES + " argument does not specify any source",
                                null, null, Collections.emptyList(), null);

        assertErrorOnForRebuild("The 1.1.1.1 source must be specified only once in the " + StreamingOptions.ARG_INCLUDE_SOURCES + " argument",
                                null, null, asList("1.1.1.1", "1.1.1.2", "1.1.1.1"), null);

        assertErrorOnForRebuild("The " + StreamingOptions.ARG_EXCLUDE_SOURCES + " argument does not specify any source",
                                null, null, null, Collections.emptyList());

        assertErrorOnForRebuild("The 1.1.1.1 source must be specified only once in the " + StreamingOptions.ARG_EXCLUDE_SOURCES + " argument",
                                null, null, null, asList("1.1.1.1", "1.1.1.2", "1.1.1.1"));

        assertErrorOnForRebuild("The " + StreamingOptions.ARG_INCLUDE_SOURCES + " argument cannot be used together with the "
                                + StreamingOptions.ARG_EXCLUDE_SOURCES + ", " + StreamingOptions.ARG_INCLUDE_DC_NAMES + " or " + StreamingOptions.ARG_EXCLUDE_DC_NAMES + " arguments",
                                null, null, asList("1.1.1.1"), asList("1.1.1.1"));

        assertErrorOnForRebuild("The " + StreamingOptions.ARG_EXCLUDE_SOURCES + " argument cannot be used together with the "
                                + StreamingOptions.ARG_INCLUDE_SOURCES + ", " + StreamingOptions.ARG_INCLUDE_DC_NAMES + " or " + StreamingOptions.ARG_EXCLUDE_DC_NAMES + " arguments",
                                null, asList("DC-1", "DC-2"), null, asList("1.1.1.1"));

        assertErrorOnForRebuild("The " + StreamingOptions.ARG_INCLUDE_DC_NAMES + " and " + StreamingOptions.ARG_EXCLUDE_DC_NAMES + " arguments are conflicting for the datacenter: DC-2",
                                asList("DC-2"), asList("DC-2"), null, null);
    }

    @Test
    public void testToStringForBootStrap()
    {
        System.clearProperty(StreamingOptions.BOOTSTRAP_INCLUDE_DCS);
        System.clearProperty(StreamingOptions.BOOTSTRAP_EXCLUDE_DCS);
        assertTrue(StreamingOptions.forBootStrap(tmd).toString().isEmpty());

        System.setProperty(StreamingOptions.BOOTSTRAP_INCLUDE_DCS, "DC-1,DC-2");
        assertEquals(" included DCs: DC-1, DC-2", StreamingOptions.forBootStrap(tmd).toString());

        System.setProperty(StreamingOptions.BOOTSTRAP_EXCLUDE_DCS, "DC-1:Rack-1");
        assertEquals(" included DCs: DC-1, DC-2 excluded DCs: DC-1:Rack-1", StreamingOptions.forBootStrap(tmd).toString());

        System.setProperty(StreamingOptions.BOOTSTRAP_EXCLUDE_DCS, "DC-1:Rack-1, DC-1:Rack-2");
        assertEquals(" included DCs: DC-1, DC-2 excluded DCs: DC-1:Rack-1, DC-1:Rack-2", StreamingOptions.forBootStrap(tmd).toString());

        System.clearProperty(StreamingOptions.BOOTSTRAP_INCLUDE_DCS);
        System.clearProperty(StreamingOptions.BOOTSTRAP_EXCLUDE_DCS);
        System.setProperty(StreamingOptions.BOOTSTRAP_INCLUDE_SOURCES, "1.1.1.1, 1.1.1.2");
        assertEquals(" included sources: /1.1.1.1, /1.1.1.2", StreamingOptions.forBootStrap(tmd).toString());

        Schema.instance.load(KeyspaceMetadata.create("test", KeyspaceParams.simpleTransient(1)));
        System.setProperty(StreamingOptions.BOOTSTRAP_EXCLUDE_KEYSPACES, "test");
        assertEquals(" included sources: /1.1.1.1, /1.1.1.2 excluded keyspaces: test", StreamingOptions.forBootStrap(tmd).toString());
    }

    @Test
    public void testAcceptKeyspace()
    {
        StreamingOptions options = StreamingOptions.forBootStrap(tmd);
        assertTrue(options.acceptKeyspace("test"));
        assertTrue(options.acceptKeyspace("test2"));

        Schema.instance.load(KeyspaceMetadata.create("test", KeyspaceParams.simpleTransient(1)));
        System.setProperty(StreamingOptions.BOOTSTRAP_EXCLUDE_KEYSPACES, "test");
        options = StreamingOptions.forBootStrap(tmd);
        assertFalse(options.acceptKeyspace("test"));
        assertTrue(options.acceptKeyspace("test2"));
    }

    @Test
    public void testToSourceFilter()
    {
        ISourceFilter filter;

        // all nodes are allowed for bootstreap
        filter = StreamingOptions.forBootStrap(tmd)
                                 .toSourceFilter(snitch, failureDetector);
        assertTrue(filter.shouldInclude(addr(1, 1, 1)));
        assertTrue(filter.shouldInclude(addr(255, 0, 1)));
        assertTrue(filter.shouldInclude(addr(0, 255, 1)));

        // Stream from a specific DC, any rack
        filter = StreamingOptions.forRebuild(tmd, "DC-1", null)
                                 .toSourceFilter(snitch, failureDetector);
        assertTrue(filter.shouldInclude(addr(1,1,1)));
        assertTrue(filter.shouldInclude(addr(1,2,2)));
        assertFalse(filter.shouldInclude(addr(2,1,2)));
        assertFalse(filter.shouldInclude(addr(2,2,2)));

        // Stream from a specific DC/rack
        filter = StreamingOptions.forRebuild(tmd, "DC-1:Rack-1", null)
                                 .toSourceFilter(snitch, failureDetector);
        assertTrue(filter.shouldInclude(addr(1,1,1)));
        assertTrue(filter.shouldInclude(addr(1,1,2)));
        assertFalse(filter.shouldInclude(addr(1,2,2)));
        assertFalse(filter.shouldInclude(addr(2,1,2)));
        assertFalse(filter.shouldInclude(addr(2,2,2)));

        // Stream from a specific DC, exclude one rack
        filter = StreamingOptions.forRebuild(tmd, asList("DC-1"), asList("DC-1:Rack-1"), null, null)
                                 .toSourceFilter(snitch, failureDetector);
        assertFalse(filter.shouldInclude(addr(1,1,1)));
        assertFalse(filter.shouldInclude(addr(1,1,2)));
        assertTrue(filter.shouldInclude(addr(1,2,1)));
        assertTrue(filter.shouldInclude(addr(1,2,2)));
        assertFalse(filter.shouldInclude(addr(2,1,2)));
        assertFalse(filter.shouldInclude(addr(2,2,2)));

        // Stream from two specific DCs, any rack
        filter = StreamingOptions.forRebuild(tmd, asList("DC-1", "DC-2"), null, null, null)
                                 .toSourceFilter(snitch, failureDetector);
        assertTrue(filter.shouldInclude(addr(1,1,1)));
        assertTrue(filter.shouldInclude(addr(1,1,2)));
        assertTrue(filter.shouldInclude(addr(1,2,1)));
        assertTrue(filter.shouldInclude(addr(1,2,2)));
        assertTrue(filter.shouldInclude(addr(2,1,1)));
        assertTrue(filter.shouldInclude(addr(2,1,2)));
        assertTrue(filter.shouldInclude(addr(2,2,1)));
        assertTrue(filter.shouldInclude(addr(2,2,2)));
        assertFalse(filter.shouldInclude(addr(3,1,1)));
        assertFalse(filter.shouldInclude(addr(3,1,2)));
        assertFalse(filter.shouldInclude(addr(3,2,1)));
        assertFalse(filter.shouldInclude(addr(3,2,2)));

        // Stream from two specific DC, exclude one rack
        filter = StreamingOptions.forRebuild(tmd, asList("DC-1", "DC-2"), asList("DC-1:Rack-1"), null, null)
                                 .toSourceFilter(snitch, failureDetector);
        assertFalse(filter.shouldInclude(addr(1,1,1)));
        assertFalse(filter.shouldInclude(addr(1,1,2)));
        assertTrue(filter.shouldInclude(addr(1,2,1)));
        assertTrue(filter.shouldInclude(addr(1,2,2)));
        assertTrue(filter.shouldInclude(addr(2,1,1)));
        assertTrue(filter.shouldInclude(addr(2,1,2)));
        assertTrue(filter.shouldInclude(addr(2,2,1)));
        assertTrue(filter.shouldInclude(addr(2,2,2)));
        assertFalse(filter.shouldInclude(addr(3,1,1)));
        assertFalse(filter.shouldInclude(addr(3,1,2)));
        assertFalse(filter.shouldInclude(addr(3,2,1)));
        assertFalse(filter.shouldInclude(addr(3,2,2)));

        // Stream from all but one DC
        filter = StreamingOptions.forRebuild(tmd, null, asList("DC-1"), null, null)
                                 .toSourceFilter(snitch, failureDetector);
        assertFalse(filter.shouldInclude(addr(1,1,1)));
        assertFalse(filter.shouldInclude(addr(1,1,2)));
        assertFalse(filter.shouldInclude(addr(1,2,1)));
        assertFalse(filter.shouldInclude(addr(1,2,2)));
        assertTrue(filter.shouldInclude(addr(2,1,1)));
        assertTrue(filter.shouldInclude(addr(2,1,2)));
        assertTrue(filter.shouldInclude(addr(2,2,1)));
        assertTrue(filter.shouldInclude(addr(2,2,2)));
        assertTrue(filter.shouldInclude(addr(3,1,1)));
        assertTrue(filter.shouldInclude(addr(3,1,2)));
        assertTrue(filter.shouldInclude(addr(3,2,1)));
        assertTrue(filter.shouldInclude(addr(3,2,2)));

        // Stream from all but one DC/rack
        filter = StreamingOptions.forRebuild(tmd, null, asList("DC-1:Rack-1"), null, null)
                                 .toSourceFilter(snitch, failureDetector);
        assertFalse(filter.shouldInclude(addr(1,1,1)));
        assertFalse(filter.shouldInclude(addr(1,1,2)));
        assertTrue(filter.shouldInclude(addr(1,2,1)));
        assertTrue(filter.shouldInclude(addr(1,2,2)));
        assertTrue(filter.shouldInclude(addr(2,1,1)));
        assertTrue(filter.shouldInclude(addr(2,1,2)));
        assertTrue(filter.shouldInclude(addr(2,2,1)));
        assertTrue(filter.shouldInclude(addr(2,2,2)));
        assertTrue(filter.shouldInclude(addr(3,1,1)));
        assertTrue(filter.shouldInclude(addr(3,1,2)));
        assertTrue(filter.shouldInclude(addr(3,2,1)));
        assertTrue(filter.shouldInclude(addr(3,2,2)));

        // Stream from all but two DCs
        filter = StreamingOptions.forRebuild(tmd, null, asList("DC-1", "DC-2"), null, null)
                                 .toSourceFilter(snitch, failureDetector);
        assertFalse(filter.shouldInclude(addr(1,1,1)));
        assertFalse(filter.shouldInclude(addr(1,1,2)));
        assertFalse(filter.shouldInclude(addr(1,2,1)));
        assertFalse(filter.shouldInclude(addr(1,2,2)));
        assertFalse(filter.shouldInclude(addr(2,1,1)));
        assertFalse(filter.shouldInclude(addr(2,1,2)));
        assertFalse(filter.shouldInclude(addr(2,2,1)));
        assertFalse(filter.shouldInclude(addr(2,2,2)));
        assertTrue(filter.shouldInclude(addr(3,1,1)));
        assertTrue(filter.shouldInclude(addr(3,1,2)));
        assertTrue(filter.shouldInclude(addr(3,2,1)));
        assertTrue(filter.shouldInclude(addr(3,2,2)));

        // Stream from all but one DC and one DC/rack
        filter = StreamingOptions.forRebuild(tmd, null, asList("DC-1:Rack-1", "DC-2"), null, null)
                                 .toSourceFilter(snitch, failureDetector);
        assertFalse(filter.shouldInclude(addr(1,1,1)));
        assertFalse(filter.shouldInclude(addr(1,1,2)));
        assertTrue(filter.shouldInclude(addr(1,2,1)));
        assertTrue(filter.shouldInclude(addr(1,2,2)));
        assertFalse(filter.shouldInclude(addr(2,1,1)));
        assertFalse(filter.shouldInclude(addr(2,1,2)));
        assertFalse(filter.shouldInclude(addr(2,2,1)));
        assertFalse(filter.shouldInclude(addr(2,2,2)));
        assertTrue(filter.shouldInclude(addr(3,1,1)));
        assertTrue(filter.shouldInclude(addr(3,1,2)));
        assertTrue(filter.shouldInclude(addr(3,2,1)));
        assertTrue(filter.shouldInclude(addr(3,2,2)));

        // all nodes are allowed
        filter = StreamingOptions.forRebuild(tmd, null, null, null, null).toSourceFilter(snitch, failureDetector);
        assertTrue(filter.shouldInclude(addr(1, 1,1)));
        assertTrue(filter.shouldInclude(addr(255, 0,1)));
        assertTrue(filter.shouldInclude(addr(0, 255,1)));

        // Stream from specific nodes
        filter = StreamingOptions.forRebuild(tmd, null, null, asList("1.1.1.1", "1.1.1.2", "1.1.1.3"), null)
                                 .toSourceFilter(snitch, failureDetector);
        assertTrue(filter.shouldInclude(addr(1, 1,1)));
        assertTrue(filter.shouldInclude(addr(1, 1,2)));
        assertTrue(filter.shouldInclude(addr(1, 1,3)));
        assertFalse(filter.shouldInclude(addr(1, 1,4)));
        assertFalse(filter.shouldInclude(addr(2, 1,1)));
        assertFalse(filter.shouldInclude(addr(1, 2,1)));

        // Stream all but some nodes
        filter = StreamingOptions.forRebuild(tmd, null, null, null, asList("1.1.1.1", "1.1.1.2", "1.1.1.3"))
                                 .toSourceFilter(snitch, failureDetector);
        assertFalse(filter.shouldInclude(addr(1, 1,1)));
        assertFalse(filter.shouldInclude(addr(1, 1,2)));
        assertFalse(filter.shouldInclude(addr(1, 1,3)));
        assertTrue(filter.shouldInclude(addr(1, 1,4)));
        assertTrue(filter.shouldInclude(addr(2, 1,1)));
        assertTrue(filter.shouldInclude(addr(1, 2,1)));
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

    private void assertErrorOnForBootStrap(String expectedMsg)
    {
        try
        {
            StreamingOptions.forBootStrap(tmd);
            fail("An error should have been thrown.");
        }
        catch (ConfigurationException e)
        {
            assertEquals("Error msg should be:'" + expectedMsg + "' but was: '" + e.getMessage() + "'" , expectedMsg, e.getMessage());
        }
    }

    private void assertErrorOnForRebuild(String expectedMsg,
                                         List<String> includedDcs,
                                         List<String> excludedDcs,
                                         List<String> includedSources,
                                         List<String> excludedSources)
    {
        try
        {
            StreamingOptions.forRebuild(tmd, includedDcs, excludedDcs, includedSources, excludedSources);
            fail("An error should have been thrown.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Error msg should be:'" + expectedMsg + "' but was: '" + e.getMessage() + "'" , expectedMsg, e.getMessage());
        }
    }

    private void assertErrorOnForRebuild(String expectedMsg,
                                         String sourceDc,
                                         String specificSources)
    {
        try
        {
            StreamingOptions.forRebuild(tmd, sourceDc, specificSources);
            fail("An error should have been thrown.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Error msg should be:'" + expectedMsg + "' but was: '" + e.getMessage() + "'" , expectedMsg, e.getMessage());
        }
    }
}
