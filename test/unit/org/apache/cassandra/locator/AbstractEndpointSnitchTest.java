package org.apache.cassandra.locator;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.cassandra.config.DatabaseDescriptor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.cassandra.utils.FBUtilities;
import org.junit.BeforeClass;
import org.junit.Test;

public class AbstractEndpointSnitchTest
{

    @BeforeClass
    public static void setup() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testGetCrossDcLatency() throws UnknownHostException
    {
        InetAddress localDcHost = InetAddress.getByName("127.0.0.1");
        InetAddress dc1Host = InetAddress.getByName("128.0.0.1");
        InetAddress dc2Host = InetAddress.getByName("129.0.0.1");
        AbstractEndpointSnitch snitch = new AbstractEndpointSnitch()
        {

            @Override
            public String getRack(InetAddress endpoint)
            {
                return "rack1";
            }

            @Override
            public String getDatacenter(InetAddress endpoint)
            {
                if (endpoint.equals(FBUtilities.getBroadcastAddress()))
                    return "datacenter1";
                if (endpoint.toString().startsWith("/128"))
                    return "datacenter2";
                if (endpoint.toString().startsWith("/129"))
                    return "datacenter3";
                return "unknown";
            }

            @Override
            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }

        };

        DatabaseDescriptor.setEndpointSnitch(snitch);

        // 0
        long crossDcLatency = 0;
        DatabaseDescriptor.setCrossDCRttLatency(crossDcLatency);

        assertEquals(0, snitch.getCrossDcRttLatency(localDcHost));
        assertEquals(0, snitch.getCrossDcRttLatency(dc1Host));
        assertEquals(0, snitch.getCrossDcRttLatency(dc2Host));

        // positive
        crossDcLatency = 1000;
        DatabaseDescriptor.setCrossDCRttLatency(crossDcLatency);

        assertEquals(0, snitch.getCrossDcRttLatency(localDcHost));
        assertEquals(crossDcLatency, snitch.getCrossDcRttLatency(dc1Host));
        assertEquals(crossDcLatency, snitch.getCrossDcRttLatency(dc2Host));

        // negative
        crossDcLatency = -1;
        DatabaseDescriptor.setCrossDCRttLatency(crossDcLatency);
        try
        {
            assertEquals(0, snitch.getCrossDcRttLatency(localDcHost));
            fail("expect assertion error");
        }
        catch (AssertionError e)
        {
            // expected
        }
    }
}
