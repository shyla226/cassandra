/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework.logback;

import java.net.InetAddress;
import java.net.Socket;

import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.net.LoggingEventPreSerializationTransformer;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.spi.PreSerializationTransformer;

/**
 * Extension of {@link ch.qos.logback.classic.net.SocketAppender} that adds <em>node ID</em> value to events MDC
 */
public class SocketAppender extends AbstractSocketAppender<ILoggingEvent>
{
    // Workaround for https://github.com/reactor/reactor-addons/issues/4
    static
    {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                // stop the logger context on shutdown
                ((LoggerContext) LoggerFactory.getILoggerFactory()).stop();
            }
        }));
    }

    private String nodeId;

    public void setNodeId(String nodeId)
    {
        this.nodeId = nodeId;
    }

    private static final PreSerializationTransformer<ILoggingEvent> pst = new LoggingEventPreSerializationTransformer();

    private boolean includeCallerData = false;

    public SocketAppender()
    {
    }

    @Override
    protected void postProcessEvent(ILoggingEvent event)
    {
        if (includeCallerData)
        {
            event.getCallerData();
        }
        if (nodeId != null)
        {
            event.getMDCPropertyMap().put("nodeid", nodeId);
        }
    }

    public void setIncludeCallerData(boolean includeCallerData)
    {
        this.includeCallerData = includeCallerData;
    }

    public PreSerializationTransformer<ILoggingEvent> getPST()
    {
        return pst;
    }

    @Override
    public void start()
    {
        String hosts = getRemoteHost();
        if (hosts != null && hosts.indexOf(',') != -1)
        {
            // we have specified multiple hostname/addresses for reaching the server
            // pick the first one reachable
            for (String host : StringUtils.split(hosts, ','))
            {
                host = host.trim();
                try (Socket socket = getSocketFactory().createSocket(InetAddress.getByName(host), getPort()))
                {
                    setRemoteHost(host);
                    break;
                }
                catch (Exception e)
                {
                    setRemoteHost(null);
                }
            }
        }
        super.start();
    }
}
