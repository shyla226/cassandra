/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.config;

import java.io.File;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(OrderedJUnit4ClassRunner.class)
public class DatabaseDescriptorTest
{
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    // this came as a result of CASSANDRA-995
    @Test
    public void testConfigurationLoader() throws Exception
    {
        // By default, we should load from the yaml
        Config config = DatabaseDescriptor.loadConfig();
        assertEquals("Test Cluster", config.cluster_name);
        Keyspace.setInitialized();

        // Now try custom loader
        ConfigurationLoader testLoader = new TestLoader();
        System.setProperty("cassandra.config.loader", testLoader.getClass().getName());

        config = DatabaseDescriptor.loadConfig();
        assertEquals("ConfigurationLoader Test", config.cluster_name);
    }

    public static class TestLoader implements ConfigurationLoader
    {
        public Config loadConfig() throws ConfigurationException
        {
            Config testConfig = new Config();
            testConfig.cluster_name = "ConfigurationLoader Test";
            return testConfig;
        }
    }

    static NetworkInterface suitableInterface = null;
    static boolean hasIPv4andIPv6 = false;

    /*
     * Server only accepts interfaces by name if they have a single address
     * OS X seems to always have an ipv4 and ipv6 address on all interfaces which means some tests fail
     * if not checked for and skipped
     */
    @BeforeClass
    public static void selectSuitableInterface() throws Exception {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while(interfaces.hasMoreElements()) {
            NetworkInterface intf = interfaces.nextElement();

            System.out.println("Evaluating " + intf.getName());

            if (intf.isLoopback()) {
                suitableInterface = intf;

                boolean hasIPv4 = false;
                boolean hasIPv6 = false;
                Enumeration<InetAddress> addresses = suitableInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    if (addresses.nextElement() instanceof Inet6Address)
                        hasIPv6 = true;
                    else
                        hasIPv4 = true;
                }
                hasIPv4andIPv6 = hasIPv4 && hasIPv6;
                return;
            }
        }
    }

    @Test
    public void testNativeTransportInterface() throws Exception
    {
        testNativeTransportInterface(false);
    }

    @Test
    public void testRpcInterface() throws Exception
    {
        testNativeTransportInterface(true);
    }

    public void testNativeTransportInterface(boolean rpcInterface) throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        if (rpcInterface)
        {
            testConfig.rpc_interface = suitableInterface.getName();
            testConfig.rpc_address = null;
            assertNull(testConfig.native_transport_interface);
        }
        else
        {
            testConfig.native_transport_interface = suitableInterface.getName();
            testConfig.native_transport_address = null;
        }

        DatabaseDescriptor.applyAddressConfig(testConfig);

        if (rpcInterface)
        {
            assertEquals(suitableInterface.getName(), testConfig.native_transport_interface);
        }

        /*
         * Confirm ability to select between IPv4 and IPv6
         */
        if (hasIPv4andIPv6)
        {
            testConfig = DatabaseDescriptor.loadConfig();

            if (rpcInterface)
            {
                testConfig.rpc_interface = suitableInterface.getName();
                testConfig.rpc_address = null;
                testConfig.rpc_interface_prefer_ipv6 = true;
            }
            else
            {
                testConfig.native_transport_interface = suitableInterface.getName();
                testConfig.native_transport_address = null;
                testConfig.native_transport_interface_prefer_ipv6 = true;
            }

            DatabaseDescriptor.applyAddressConfig(testConfig);

            assertEquals(DatabaseDescriptor.getNativeTransportAddress().getClass(), Inet6Address.class);

            testConfig = DatabaseDescriptor.loadConfig();

            if (rpcInterface)
            {
                testConfig.rpc_interface = suitableInterface.getName();
                testConfig.rpc_address = null;
                testConfig.rpc_interface_prefer_ipv6 = false;
            }
            else
            {
                testConfig.native_transport_interface = suitableInterface.getName();
                testConfig.native_transport_address = null;
                testConfig.native_transport_interface_prefer_ipv6 = false;
            }

            DatabaseDescriptor.applyAddressConfig(testConfig);

            assertEquals(DatabaseDescriptor.getNativeTransportAddress().getClass(), Inet4Address.class);
        }
        else
        {
            /*
             * Confirm first address of interface is selected
             */
            assertEquals(DatabaseDescriptor.getNativeTransportAddress(), suitableInterface.getInetAddresses().nextElement());
        }
    }

    @Test
    public void testListenInterface() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.listen_interface = suitableInterface.getName();
        testConfig.listen_address = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);

        /*
         * Confirm ability to select between IPv4 and IPv6
         */
        if (hasIPv4andIPv6)
        {
            testConfig = DatabaseDescriptor.loadConfig();
            testConfig.listen_interface = suitableInterface.getName();
            testConfig.listen_address = null;
            testConfig.listen_interface_prefer_ipv6 = true;
            DatabaseDescriptor.applyAddressConfig(testConfig);

            assertEquals(DatabaseDescriptor.getListenAddress().getClass(), Inet6Address.class);

            testConfig = DatabaseDescriptor.loadConfig();
            testConfig.listen_interface = suitableInterface.getName();
            testConfig.listen_address = null;
            testConfig.listen_interface_prefer_ipv6 = false;
            DatabaseDescriptor.applyAddressConfig(testConfig);

            assertEquals(DatabaseDescriptor.getListenAddress().getClass(), Inet4Address.class);
        }
        else
        {
            /*
             * Confirm first address of interface is selected
             */
            assertEquals(DatabaseDescriptor.getNativeTransportAddress(), suitableInterface.getInetAddresses().nextElement());
        }
    }

    @Test
    public void testListenAddress() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.listen_address = suitableInterface.getInterfaceAddresses().get(0).getAddress().getHostAddress();
        testConfig.listen_interface = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);
    }

    @Test
    public void testNativeTransportAddress() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.native_transport_address = suitableInterface.getInterfaceAddresses().get(0).getAddress().getHostAddress();
        testConfig.native_transport_interface = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);
    }

    @Test
    public void testDeprecatedProperties() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.rpc_address = suitableInterface.getInterfaceAddresses().get(0).getAddress().getHostAddress();
        testConfig.rpc_interface = null;
        testConfig.native_transport_keepalive = false;
        testConfig.rpc_keepalive = true;
        assertNull(testConfig.native_transport_address);
        assertNull(testConfig.native_transport_interface);
        DatabaseDescriptor.applyAddressConfig(testConfig);
        assertEquals(suitableInterface.getInterfaceAddresses().get(0).getAddress().getHostAddress(), testConfig.native_transport_address);
        assertNull(testConfig.native_transport_interface);
        assertTrue(testConfig.native_transport_keepalive); //rpc_keepalive should override native_transport_keepalive if set
        assertFalse(testConfig.native_transport_interface_prefer_ipv6); //test default
    }

    @Test
    public void testTokensFromString()
    {
        assertTrue(DatabaseDescriptor.tokensFromString(null).isEmpty());
        Collection<String> tokens = DatabaseDescriptor.tokensFromString(" a,b ,c , d, f,g,h");
        assertEquals(7, tokens.size());
        assertTrue(tokens.containsAll(Arrays.asList(new String[]{"a", "b", "c", "d", "f", "g", "h"})));
    }

    @Test
    public void testSystemInfoEncryption()
    {
        String someDir = new File(".").getAbsolutePath();

        Config testConfig;
        DatabaseDescriptor.setConfig(testConfig = new Config());
        testConfig.system_info_encryption.enabled = false;
        DatabaseDescriptor.applyEncryptionContext();

        DatabaseDescriptor.setConfig(testConfig = new Config());
        testConfig.system_info_encryption.enabled = true;
        assertConfigException(DatabaseDescriptor::applyEncryptionContext, "system_key_directory is missing and -Dcassandra.storagedir is not set");

        DatabaseDescriptor.setConfig(testConfig = new Config());
        testConfig.system_info_encryption.enabled = true;
        testConfig.system_key_directory = someDir;
        DatabaseDescriptor.applyEncryptionContext();

        DatabaseDescriptor.setConfig(testConfig = new Config());
        testConfig.system_info_encryption.enabled = true;
        testConfig.system_info_encryption.key_provider = "LocalFileSystemKeyProviderFactory";
        testConfig.system_key_directory = someDir;
        DatabaseDescriptor.applyEncryptionContext();

        DatabaseDescriptor.setConfig(testConfig = new Config());
        testConfig.system_info_encryption.enabled = true;
        testConfig.system_key_directory = someDir;
        testConfig.system_info_encryption.chunk_length_kb = 0;
        assertConfigException(DatabaseDescriptor::applyEncryptionContext, "system_info_encryption.chunk_length_kb must be greater than 0");

        DatabaseDescriptor.setConfig(testConfig = new Config());
        testConfig.system_info_encryption.enabled = true;
        testConfig.system_key_directory = someDir;
        testConfig.system_info_encryption.secret_key_strength = 0;
        assertConfigException(DatabaseDescriptor::applyEncryptionContext, "system_info_encryption.secret_key_strength must be greater than 0");

        DatabaseDescriptor.setConfig(testConfig = new Config());
        testConfig.system_info_encryption.enabled = true;
        testConfig.system_key_directory = someDir;
        testConfig.system_info_encryption.key_name = null;
        assertConfigException(DatabaseDescriptor::applyEncryptionContext, "system_info_encryption.key_name must not be empty");

        DatabaseDescriptor.setConfig(testConfig = new Config());
        testConfig.system_info_encryption.enabled = true;
        testConfig.system_key_directory = someDir;
        testConfig.system_info_encryption.key_name = "  ";
        assertConfigException(DatabaseDescriptor::applyEncryptionContext, "system_info_encryption.key_name must not be empty");

        DatabaseDescriptor.setConfig(testConfig = new Config());
        testConfig.system_info_encryption.enabled = true;
        testConfig.system_info_encryption.key_provider = "KmipKeyProviderFactory";
        assertConfigException(DatabaseDescriptor::applyEncryptionContext, "system_info_encryption.kmip_host must be specified for KMIP key provider");

        DatabaseDescriptor.setConfig(testConfig = new Config());
        testConfig.system_info_encryption.enabled = true;
        testConfig.system_info_encryption.key_provider = "com.datastax.bdp.cassandra.crypto.KmipKeyProviderFactory";
        assertConfigException(DatabaseDescriptor::applyEncryptionContext, "system_info_encryption.kmip_host must be specified for KMIP key provider");

        DatabaseDescriptor.setConfig(testConfig = new Config());
        testConfig.system_info_encryption.enabled = true;
        testConfig.system_info_encryption.key_provider = "com.datastax.bdp.cassandra.crypto.KmipKeyProviderFactory";
        testConfig.system_info_encryption.kmip_host = "this-crazy-hostname-hopefully.never-exists-in-the-world.datastax.lan";
        assertConfigException(DatabaseDescriptor::applyEncryptionContext, "system_info_encryption.kmip_host 'this-crazy-hostname-hopefully.never-exists-in-the-world.datastax.lan' " +
                                                                          "is not a valid, resolvable host name: java.net.UnknownHostException: " +
                                                                          "this-crazy-hostname-hopefully.never-exists-in-the-world.datastax.lan: Name or service not known");

        DatabaseDescriptor.setConfig(testConfig = new Config());
        testConfig.system_info_encryption.enabled = true;
        testConfig.system_info_encryption.key_provider = "com.datastax.bdp.cassandra.crypto.KmipKeyProviderFactory";
        testConfig.system_info_encryption.kmip_host = "localhost";
        DatabaseDescriptor.applyEncryptionContext();
    }

    private void assertConfigException(Runnable runnable, String exceptionPattern)
    {
        try
        {
            runnable.run();
            fail("Expected ConfigurationException to be thrown");
        }
        catch (ConfigurationException e)
        {
            assertTrue("Expected '" + exceptionPattern + "', but got '" + e.getMessage() + "'", e.getMessage().matches(exceptionPattern));
        }
    }

    @Test
    public void testLowestAcceptableTimeouts() throws ConfigurationException
    {
        Config testConfig = new Config();
        testConfig.read_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT + 1;
        testConfig.range_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT + 1;
        testConfig.write_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT + 1;
        testConfig.truncate_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT + 1;
        testConfig.cas_contention_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT + 1;
        testConfig.counter_write_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT + 1;
        testConfig.request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT + 1;

        assertTrue(testConfig.read_request_timeout_in_ms > DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.range_request_timeout_in_ms > DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.write_request_timeout_in_ms > DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.truncate_request_timeout_in_ms > DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.cas_contention_timeout_in_ms > DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.counter_write_request_timeout_in_ms > DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.request_timeout_in_ms > DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);

        //set less than Lowest acceptable value
        testConfig.read_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT - 1;
        testConfig.range_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT - 1;
        testConfig.write_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT - 1;
        testConfig.truncate_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT - 1;
        testConfig.cas_contention_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT - 1;
        testConfig.counter_write_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT - 1;
        testConfig.request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT - 1;

        DatabaseDescriptor.checkForLowestAcceptedTimeouts(testConfig);

        assertTrue(testConfig.read_request_timeout_in_ms == DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.range_request_timeout_in_ms == DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.write_request_timeout_in_ms == DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.truncate_request_timeout_in_ms == DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.cas_contention_timeout_in_ms == DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.counter_write_request_timeout_in_ms == DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.request_timeout_in_ms == DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
    }
}
