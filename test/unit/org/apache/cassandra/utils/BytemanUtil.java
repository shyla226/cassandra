/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils;

import java.io.IOException;
import java.net.ServerSocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BytemanUtil
{
    private static final Logger logger = LoggerFactory.getLogger(BytemanUtil.class);

    /**
     * Tests are running in parallel on CI, which might lead to byteman port collision.
     * We randomize byteman port to fix it.
     */
    public static void randomizeBytemanPort()
    {
        int port = 0;
        try (ServerSocket serverSocket = new ServerSocket(0))
        {
            port = serverSocket.getLocalPort();
        }
        catch (IOException e)
        {
            // ignore
        }

        System.setProperty("org.jboss.byteman.contrib.bmunit.agent.port", Integer.toString(port));
        logger.info("Setting byteman port to {}.", port);
    }
}
