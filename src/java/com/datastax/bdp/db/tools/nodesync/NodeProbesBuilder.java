/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.tools.nodesync;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;
import javax.annotation.Nullable;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import org.apache.cassandra.tools.NodeProbe;

import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * Builder for creating a new JMX {@link NodeProbes} from properties expressed as native types.
 */
class NodeProbesBuilder
{
    private final Metadata metadata;
    private String username, password, passwordFilePath;
    private boolean ssl;

    NodeProbesBuilder(Metadata metadata)
    {
        this.metadata = metadata;
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

        return new NodeProbes(metadata, this::buildNodeProbe);
    }

    private NodeProbe buildNodeProbe(Host host)
    {
        String address = host.getBroadcastAddress().getHostAddress();
        int port = host.getJmxPort();

        if (port <= 0)
            throw new NodeSyncException(String.format("Unable to read the JMX port of node %s, this could be because " +
                                                      "JMX is not enabled in that node or it's running a version " +
                                                      "without NodeSync support", address));

        try
        {
            return username == null ? new NodeProbe(address, port) : new NodeProbe(address, port, username, password);
        }
        catch (IOException | SecurityException e)
        {
            throw new NodeSyncException(String.format("JMX connection to %s:%d failed: %s", address, port, e.getMessage()));
        }
    }
}
