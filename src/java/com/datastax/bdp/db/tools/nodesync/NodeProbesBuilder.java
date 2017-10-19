/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.tools.nodesync;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Scanner;
import javax.annotation.Nullable;

import com.datastax.driver.core.Session;
import org.apache.cassandra.tools.NodeProbe;

import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * Builder for creating a new JMX {@link NodeProbes} from properties expressed as native types.
 */
class NodeProbesBuilder
{
    private final Session session;
    private String username, password, passwordFilePath;
    private boolean ssl;

    NodeProbesBuilder(Session session)
    {
        this.session = session;
    }

    NodeProbesBuilder withUsername(@Nullable String username)
    {
        this.username = username;
        return this;
    }

    NodeProbesBuilder withPassword(@Nullable String password)
    {
        this.password = password;
        return this;
    }

    NodeProbesBuilder withPasswordFilePath(@Nullable String passwordFilePath)
    {
        this.passwordFilePath = passwordFilePath;
        return this;
    }

    private String readUserPasswordFromFile()
    {
        String password = EMPTY;

        File passwordFile = new File(passwordFilePath);
        try (Scanner scanner = new Scanner(passwordFile).useDelimiter("\\s+"))
        {
            while (scanner.hasNextLine())
            {
                if (scanner.hasNext())
                {
                    String jmxRole = scanner.next();
                    if (jmxRole.equals(username) && scanner.hasNext())
                    {
                        password = scanner.next();
                        break;
                    }
                }
                scanner.nextLine();
            }
        }
        catch (FileNotFoundException e)
        {
            throw new NodeSyncException("JMX password file not found: " + passwordFilePath);
        }

        return password;
    }

    NodeProbesBuilder withSSL(boolean ssl)
    {
        this.ssl = ssl;
        return this;
    }

    NodeProbes build()
    {
        if (passwordFilePath != null)
            password = readUserPasswordFromFile();

        if (ssl)
            System.setProperty("ssl.enable", "true");

        return new NodeProbes(session, this::buildNodeProbe);
    }

    private NodeProbe buildNodeProbe(InetSocketAddress address)
    {
        try
        {
            String host = address.getHostName();
            int port = address.getPort();
            return (username == null) ? new NodeProbe(host, port) : new NodeProbe(host, port, username, password);
        }
        catch (IOException | SecurityException e)
        {
            throw new NodeSyncException(String.format("JMX connection to %s failed: %s", address, e.getMessage()));
        }
    }
}
