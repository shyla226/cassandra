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
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import net.openhft.affinity.AffinitySupport;
import org.apache.cassandra.concurrent.NettyRxScheduler;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.AuthMetrics;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Handles native transport server lifecycle and associated resources. Lazily initialized.
 */
public class NativeTransportService
{
    private static final Logger logger = LoggerFactory.getLogger(NativeTransportService.class);

    private List<Server> servers = Collections.emptyList();
    private List<EventLoopGroup> workerGroups = Collections.emptyList();

    private static Integer pIO = Integer.valueOf(System.getProperty("io.netty.ratioIO", "50"));
    private static Boolean affinity = Boolean.valueOf(System.getProperty("io.netty.affinity","false"));

    public static final int NUM_NETTY_THREADS = Integer.valueOf(System.getProperty("io.netty.eventLoopThreads", String.valueOf(FBUtilities.getAvailableProcessors())));

    private boolean initialized = false;
    private boolean tpcInitialized = false;

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

    /**
     * Creates netty thread pools and event loops.
     */
    @VisibleForTesting
    synchronized void initialize()
    {
        if (initialized)
            return;


        if (useEpoll())
            logger.info("Netty using native Epoll event loops");
        else
            logger.info("Netty using Java NIO event loops");

        int nativePortSSL = DatabaseDescriptor.getNativeTransportPortSSL();

        if (!DatabaseDescriptor.getClientEncryptionOptions().enabled)
        {
            workerGroups = new ArrayList<>(NUM_NETTY_THREADS);
            servers = new ArrayList<>(NUM_NETTY_THREADS);
            for (int i = 0; i < NUM_NETTY_THREADS; i++)
            {
                // force one thread per event loop group
                EventLoopGroup loopGroup = useEpoll() ? new EpollEventLoopGroup(1) : new NioEventLoopGroup(1);
                org.apache.cassandra.transport.Server.Builder builder = new org.apache.cassandra.transport.Server.Builder()
                        .withEventLoopGroup(loopGroup)
                        .withHost(nativeAddr)
                        .withPort(nativePort + i)
                        .withSSL(false);

                workerGroups.add(loopGroup);
                servers.add(builder.build());
            }
        }
        else
        {
            // TODO TPC/PPC
            throw new UnsupportedOperationException("SSL isn't supported for PPC yet");
            /*
            org.apache.cassandra.transport.Server.Builder builder = new org.apache.cassandra.transport.Server.Builder()
                    .withEventLoopGroup(workerGroup)
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
            */
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

    private void initializeTPC()
    {
        if (tpcInitialized)
            return;

        logger.info("Netting ioWork ratio to {}", pIO);
        for (int i = 0; i < NUM_NETTY_THREADS; i++)
        {
            final int cpuId = i;
            EventLoopGroup workerGroup = workerGroups.get(i);
            if (useEpoll())
                ((EpollEventLoopGroup)workerGroup).setIoRatio(pIO);
            else
                ((NioEventLoopGroup)workerGroup).setIoRatio(pIO);

            EventLoop loop = workerGroup.next();
            loop.schedule(() -> {
                NettyRxScheduler.instance(loop, cpuId);

                if (affinity)
                {
                    logger.info("Locking {} netty thread to {}, port {}", cpuId, Thread.currentThread().getName(), nativePort + cpuId);
                    AffinitySupport.setAffinity(1L << cpuId);
                }
                {
                    logger.info("Allocated netty {} thread to {}, port {}", workerGroup, Thread.currentThread().getName(), nativePort + cpuId);
                }
            }, 0, TimeUnit.SECONDS);
        }

        tpcInitialized = true;
    }


    /**
     * Starts native transport servers.
     */
    public void start()
    {
        initialize();
        initializeTPC();

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

        // shutdown executors used by netty for native transport server
        for (EventLoopGroup workerGroup : workerGroups)
            workerGroup.shutdownGracefully(3, 5, TimeUnit.SECONDS).awaitUninterruptibly();
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
    List<EventLoopGroup> getWorkerGroups()
    {
        return workerGroups;
    }

    @VisibleForTesting
    Collection<Server> getServers()
    {
        return servers;
    }
}
