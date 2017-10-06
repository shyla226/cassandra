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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.AuthMetrics;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Handles native transport server lifecycle and associated resources. Lazily initialized.
 */
public class NativeTransportService
{
    private static final Logger logger = LoggerFactory.getLogger(NativeTransportService.class);

    private final static int ON_CLOSE_WAIT_TIMEOUT_SECS = 5;

    private List<Server> servers = Collections.emptyList();

    private boolean initialized = false;

    private final InetAddress nativeAddr;
    private final int nativePort;

    @VisibleForTesting
    public NativeTransportService(InetAddress nativeAddr, int nativePort)
    {
        this.nativeAddr = nativeAddr;
        this.nativePort = nativePort;
    }

    public NativeTransportService()
    {
        this.nativeAddr = DatabaseDescriptor.getNativeTransportAddress();
        this.nativePort = DatabaseDescriptor.getNativeTransportPort();
    }

    /**
     * Creates netty thread pools and event loops.
     */
    @VisibleForTesting
    synchronized void initialize()
    {
        if (initialized)
            return;

        int nativePortSSL = DatabaseDescriptor.getNativeTransportPortSSL();

        if (!DatabaseDescriptor.getClientEncryptionOptions().enabled)
        {
            servers = new ArrayList<>(1);

            org.apache.cassandra.transport.Server.Builder builder = new org.apache.cassandra.transport.Server.Builder()
                                                                    .withHost(nativeAddr)
                                                                    .withPort(nativePort)
                                                                    .withSSL(false);

            servers.add(builder.build());
        }
        else
        {

            org.apache.cassandra.transport.Server.Builder builder = new org.apache.cassandra.transport.Server.Builder()
                    .withHost(nativeAddr);

            if (nativePort != nativePortSSL)
            {
                // user asked for dedicated ssl port for supporting both non-ssl and ssl connections
                servers = Collections.unmodifiableList(
                    Arrays.asList(
                        builder.withSSL(false).withPort(nativePort).build(),
                        builder.withSSL(true).withPort(nativePortSSL).build()
                    )
                );
            }
            else
            {
                // ssl only mode using configured native port
                servers = Collections.singletonList(builder.withSSL(true).withPort(nativePort).build());
            }
        }

        // register metrics
        ClientMetrics.instance.addCounter("connectedNativeClients", () ->
        {
            int ret = 0;
            for (Server server : servers)
                ret += server.getConnectedClients();
            return ret;
        });

        AuthMetrics.init();

        initialized = true;
    }

    /**
     * Starts native transport servers.
     */
    public void start()
    {
        initialize();

        servers.forEach(Server::start);
    }

    /**
     * Stops currently running native transport servers.
     */
    public void stop()
    {
        try
        {
            stopAsync().get(ON_CLOSE_WAIT_TIMEOUT_SECS, TimeUnit.SECONDS);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Failed to wait for native transport service to stop cleanly", t);
        }

    }

    public CompletableFuture stopAsync()
    {
        return CompletableFuture.allOf(servers.stream().map(Server::stop).toArray(CompletableFuture[]::new));
    }

    /**
     * Ultimately stops servers and closes all resources.
     */
    public void destroy()
    {
        stop();
        servers = Collections.emptyList();
    }

    /**
     * @return true in case native transport server is running
     */
    public boolean isRunning()
    {
        for (Server server : servers)
            if (server.isRunning()) return true;
        return false;
    }

    @VisibleForTesting
    Collection<Server> getServers()
    {
        return servers;
    }
}
