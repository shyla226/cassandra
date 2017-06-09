/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework;

import com.googlecode.mobilityrpc.network.ConnectionId;
import com.googlecode.mobilityrpc.quickstart.EmbeddedMobilityServer;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Allow execution of arbitrary code inside node JVM. It uses <a href="https://github.com/npgall/mobility-rpc">Mobility-RPC project</a> under the hood
 * <p>
 * Only available during test execution.
 */
public class CassandraTestingDaemon extends CassandraDaemon
{
    public static void main(String[] args)
    {
        CassandraDaemon.startForDseTesting();
        new DseMobilityControllerImpl().getConnectionManager().bindConnectionListener(new ConnectionId(FBUtilities.getLocalAddress().getHostAddress(), EmbeddedMobilityServer.DEFAULT_PORT));
    }
}
