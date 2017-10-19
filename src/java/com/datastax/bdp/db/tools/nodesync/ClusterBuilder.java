/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.tools.nodesync;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

/**
 * Builder for creating a new CQL {@link Cluster} from propertiea expressed as native types.
 */
class ClusterBuilder
{
    private final String contactPoint;
    private final int port;
    private AuthProvider authProvider;
    private String username;
    private String password;
    private boolean ssl;

    ClusterBuilder(String contactPoint, int port)
    {
        this.contactPoint = contactPoint;
        this.port = port;
    }

    ClusterBuilder withAuthProvider(@Nullable String className)
    {
        if (className == null)
        {
            authProvider = null;
        }
        else
        {
            try
            {
                Class<?> clazz = Class.forName(className);
                if (!AuthProvider.class.isAssignableFrom(clazz))
                    throw new NodeSyncException(clazz + " is not a valid auth provider");

                if (PlainTextAuthProvider.class.equals(clazz))
                    authProvider = (AuthProvider) clazz.getConstructor(String.class, String.class)
                                                       .newInstance(username, password);
                else
                    authProvider = (AuthProvider) clazz.newInstance();
            }
            catch (Exception e)
            {
                throw new NodeSyncException("Invalid auth provider: " + className);
            }
        }

        return this;
    }

    ClusterBuilder withUsername(@Nullable String username)
    {
        this.username = username;
        return this;
    }

    ClusterBuilder withPassword(@Nullable String password)
    {
        this.password = password;
        return this;
    }

    ClusterBuilder withSSL(boolean ssl)
    {
        this.ssl = ssl;
        return this;
    }

    Cluster build()
    {
        Cluster.Builder builder = Cluster.builder().addContactPoint(contactPoint).withPort(port);

        if (ssl)
            builder.withSSL();

        List<InetSocketAddress> whitelist = Collections.singletonList(new InetSocketAddress(contactPoint, port));
        LoadBalancingPolicy policy = builder.getConfiguration().getPolicies().getLoadBalancingPolicy();
        builder.withLoadBalancingPolicy(new WhiteListPolicy(policy, whitelist));

        if (authProvider != null)
        {
            builder.withAuthProvider(authProvider);
        }
        else if (username != null)
        {
            builder.withCredentials(username, password);
        }

        return builder.build();
    }
}
