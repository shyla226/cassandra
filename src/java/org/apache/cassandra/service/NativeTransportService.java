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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.nio.NioEventLoopGroup;

import org.apache.cassandra.concurrent.MonitoredEpollEventLoopGroup;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.AuthMetrics;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.transport.Server;

import static org.apache.cassandra.concurrent.NettyRxScheduler.NUM_NETTY_THREADS;

/**
 * Handles native transport server lifecycle and associated resources. Lazily initialized.
 */
public class NativeTransportService
{
    private static final Logger logger = LoggerFactory.getLogger(NativeTransportService.class);
    public static final EventLoopGroup eventLoopGroup = makeWorkerGroup();

    private List<Server> servers = Collections.emptyList();

    private static Integer pIO = Integer.valueOf(System.getProperty("io.netty.ratioIO", "50"));

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
        this.nativeAddr = DatabaseDescriptor.getRpcAddress();
        this.nativePort = DatabaseDescriptor.getNativeTransportPort();
    }

    private static EventLoopGroup makeWorkerGroup()
    {
        if (useEpoll())
        {
            MonitoredEpollEventLoopGroup ret = new MonitoredEpollEventLoopGroup(NUM_NETTY_THREADS);
            logger.info("Using native Epoll event loops");
            return ret;
        }
        else
        {
            NioEventLoopGroup ret = new NioEventLoopGroup(NUM_NETTY_THREADS);
            ret.setIoRatio(pIO);

            logger.info("Using Java NIO event loops");
            logger.info("Netting ioWork ratio to {}", pIO);
            return ret;
        }
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
                                                                    .withEventLoopGroup(eventLoopGroup)
                                                                    .withHost(nativeAddr)
                                                                    .withPort(nativePort)
                                                                    .withSSL(false);

            servers.add(builder.build());
        }
        else
        {

            org.apache.cassandra.transport.Server.Builder builder = new org.apache.cassandra.transport.Server.Builder()
                    .withEventLoopGroup(eventLoopGroup)
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
        servers.forEach(Server::stop);
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
     * @return intend to use epoll bassed event looping
     */
    public static boolean useEpoll()
    {
        final boolean enableEpoll = Boolean.parseBoolean(System.getProperty("cassandra.native.epoll.enabled", "true"));
        return enableEpoll && Epoll.isAvailable();
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
