/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.locator;

import java.net.InetAddress;

import com.google.common.net.InetAddresses;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link GossipingPropertyFileSnitch}.
 */
public class GossipingPropertyFileSnitchTest
{
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        DatabaseDescriptor.setDaemonInitialized();
        SchemaLoader.mkdirs();
        SchemaLoader.cleanup();
        Keyspace.setInitialized();
        StorageService.instance.initServer(0);
    }

    @AfterClass
    public static void afterClass()
    {
        StorageService.instance.stopClient();
    }

    @After
    public void after()
    {
        ColumnFamilyStore systemPeers = Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.PEERS);
        systemPeers.truncateBlocking();
    }

    public static void checkEndpoint(final AbstractNetworkTopologySnitch snitch,
                                     final String endpointString, final String expectedDatacenter,
                                     final String expectedRack)
    {
        final InetAddress endpoint = InetAddresses.forString(endpointString);
        assertEquals(expectedDatacenter, snitch.getDatacenter(endpoint));
        assertEquals(expectedRack, snitch.getRack(endpoint));
    }

    @Test
    public void testLoadConfig() throws Exception
    {
        final GossipingPropertyFileSnitch snitch = new GossipingPropertyFileSnitch();
        checkEndpoint(snitch, FBUtilities.getBroadcastAddress().getHostAddress(), "DC1", "RAC1");
    }

    @Test
    public void testPfsCompatibilityEnabled() throws Exception
    {
        testPfsCompatibility(true, false);
    }

    @Test
    public void testPfsCompatibilityEnabledWithSavedInfo() throws Exception
    {
        testPfsCompatibility(true, true);
    }

    @Test
    public void testPfsCompatibilityDisabled() throws Exception
    {
        testPfsCompatibility(false, false);
    }

    public void testPfsCompatibility(boolean enablePfsCompatibility, boolean savedInfo) throws Exception
    {
        final GossipingPropertyFileSnitch snitch = new GossipingPropertyFileSnitch(enablePfsCompatibility);
        if (enablePfsCompatibility)
        {
            if (savedInfo)
            {
                // saved info should have precedence over property file snitch
                SystemKeyspace.updatePeerInfo(InetAddress.getByName("127.0.0.2"), "data_center", "DC3", StageManager.getStage(Stage.MISC)).get();
                SystemKeyspace.updatePeerInfo(InetAddress.getByName("127.0.0.2"), "rack", "RAC3", StageManager.getStage(Stage.MISC)).get();

                checkEndpoint(snitch, "127.0.0.2", "DC3", "RAC3");
            }
            else
            {
                // test/conf/cassandra-topology.properties defines endpoint 127.0.0.2 on DC1/RAC2
                checkEndpoint(snitch, "127.0.0.2", "DC1", "RAC2");
            }
        }
        else
        {
            // when PFS compatibility is not enabled, default rc/rack should be returned
            checkEndpoint(snitch, "127.0.0.2", GossipingPropertyFileSnitch.DEFAULT_DC, GossipingPropertyFileSnitch.DEFAULT_RACK);
        }
    }
}
