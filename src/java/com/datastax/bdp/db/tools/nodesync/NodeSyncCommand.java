/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.tools.nodesync;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

import org.apache.cassandra.cql3.ColumnIdentifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

public abstract class NodeSyncCommand implements Runnable
{
    @VisibleForTesting
    static final Pattern TABLE_NAME_PATTERN = Pattern.compile("((?<k>(\\w+|\"\\w+\"))\\.)?(?<t>(\\w+|\"\\w+\"))");

    private static Splitter ON_COMMA = Splitter.on(',').trimResults().omitEmptyStrings();

    @Option(type = OptionType.GLOBAL, name = { "-h", "--host" }, description = "CQL contact point address")
    private String cqlHost = "127.0.0.1";

    @Option(type = OptionType.GLOBAL, name = { "-p", "--port" }, description = "CQL port number")
    private int cqlPort = 9042;

    @Option(type = OptionType.GLOBAL, name = { "-ca", "--cql-auth-provider" }, description = "CQL auth provider class name")
    private String cqlAuthProvider;

    @Option(type = OptionType.GLOBAL, name = { "-cu", "--cql-username" }, description = "CQL username")
    private String cqlUsername;

    @Option(type = OptionType.GLOBAL, name = { "-cp", "--cql-password" }, description = "CQL password")
    private String cqlPassword;

    @Option(type = OptionType.GLOBAL, name = { "-cs", "--cql-ssl" }, description = "Enable SSL for CQL")
    private boolean cqlSSL = false;

    @Option(type = OptionType.GLOBAL, name = { "-ju", "--jmx-username" }, description = "JMX username")
    private String jmxUsername;

    @Option(type = OptionType.GLOBAL, name = { "-jp", "--jmx-password" }, description = "JMX password")
    private String jmxPassword;

    @Option(type = OptionType.GLOBAL, name = { "-jpf", "--jmx-password-file" }, description = "Path to the JMX password file")
    private String jmxPasswordFile;

    @Option(type = OptionType.GLOBAL, name = { "-js", "--jmx-ssl" }, description = "Enable SSL for JMX")
    private boolean jmxSSL = false;

    @Option(type = OptionType.COMMAND, name = { "-v", "--verbose" }, description = "Verbose output")
    private boolean verbose = false;

    @Option(type = OptionType.COMMAND, name = { "-q", "--quiet" }, description = "Quiet output; don't print warnings")
    private boolean quiet = false;

    @Override
    public void run()
    {
        try (Cluster cluster = buildCluster(); Session session = cluster.newSession())
        {
            Metadata metadata = cluster.getMetadata();
            execute(metadata, session, buildNodeProbes(metadata));
        }
    }

    public void validateOptions() throws InvalidOptionException
    {
        if (verbose && quiet)
            throw new InvalidOptionException("Cannot use both --verbose and --quiet at the same time.");
    }

    private Cluster buildCluster()
    {
        return new ClusterBuilder(cqlHost, cqlPort).withAuthProvider(cqlAuthProvider)
                                                   .withUsername(cqlUsername)
                                                   .withPassword(cqlPassword)
                                                   .withSSL(cqlSSL)
                                                   .build();
    }

    private NodeProbes buildNodeProbes(Metadata metadata)
    {
        return new NodeProbesBuilder(metadata).withUsername(jmxUsername)
                                              .withPassword(jmxPassword)
                                              .withPasswordFilePath(jmxPasswordFile)
                                              .withSSL(jmxSSL)
                                              .build();
    }

    protected abstract void execute(Metadata metadata, Session session, NodeProbes nodes);

    /**
     * Returns the metadata of the keyspace identified by the specified keyspace name.
     *
     * @param metadata the cluster metadata
     * @param keyspace the name of the keyspace
     * @return a keyspace metadata
     * @throws NodeSyncException if the metadata doesn't have a keyspace with such name
     */
    static KeyspaceMetadata parseKeyspace(Metadata metadata, String keyspace)
    {
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(keyspace);
        if (keyspaceMetadata == null)
            throw new NodeSyncException(String.format("Keyspace [%s] does not exist.", keyspace));

        return keyspaceMetadata;
    }

    /**
     * Returns the metadata of the table identified by the specifed qualified or unqualified table name. If the table
     * name is unqualified, then the specified default keyspace name will be used.
     *
     * @param metadata the cluster metadata
     * @param defaultKeyspace the default keyspace name to be used if the table name is unqualified
     * @param maybeQualifiedTable a qualified or unqualified table name
     * @return a table metadata
     * @throws NodeSyncException if the metadata doesn't have a table with such name
     */
    static TableMetadata parseTable(Metadata metadata, @Nullable String defaultKeyspace, String maybeQualifiedTable)
    {
        Matcher matcher = TABLE_NAME_PATTERN.matcher(maybeQualifiedTable);
        if (!matcher.matches())
            throw new NodeSyncException("Cannot parse table name: " + maybeQualifiedTable);

        String keyspaceName = matcher.group("k");
        String tableName = matcher.group("t");

        if (keyspaceName == null)
        {
            if (defaultKeyspace == null)
                throw new NodeSyncException("Keyspace required for unqualified table name: " + tableName);
            else
                keyspaceName = defaultKeyspace;
        }

        KeyspaceMetadata keyspaceMetadata = parseKeyspace(metadata, keyspaceName);

        TableMetadata tableMetadata = keyspaceMetadata.getTable(tableName);
        if (tableMetadata == null)
            throw new NodeSyncException(String.format("Table [%s.%s] does not exist.", keyspaceName, tableName));

        return tableMetadata;
    }

    /**
     * Parse a comma-separated list of inet addresses.
     * <p>
     * Note that this method do validate that the addresses are valid, but does not check the liveness of the node
     * corresponding to those addresses in any way.
     *
     * @param addressList the string containing the comma-separated list ot parse.
     * @return the parsed list.
     */
    static Set<InetAddress> parseInetAddressList(String addressList)
    {
        Set<InetAddress> addresses = new HashSet<>();
        for (String s : ON_COMMA.split(addressList))
        {
            try
            {
                addresses.add(InetAddress.getByName(s));
            }
            catch (UnknownHostException e)
            {
                throw new NodeSyncException("Unknown or invalid address: " + s);
            }
        }

        if (addresses.isEmpty())
            throw new NodeSyncException("Invalid empty list of addresses provided.");

        return addresses;
    }

    /**
     * Print a non essential informational message that will only show if the --verbose option is used.
     */
    void printVerbose(String msg, Object... args)
    {
        if (verbose)
            System.out.println(String.format(msg, args));
    }

    /**
     * Print a warning message, which will show by default but can be quiet down by the --quiet option.
     */
    void printWarning(String msg, Object... args)
    {
        if (!quiet)
            System.err.println(String.format("Warning: " + msg, args));
    }

    static String fullyQualifiedTableName(TableMetadata metadata)
    {
        return fullyQualifiedTableName(metadata.getKeyspace().getName(), metadata.getName());
    }

    static String fullyQualifiedTableName(String keyspace, String table)
    {
        return String.format("%s.%s", ColumnIdentifier.maybeQuote(keyspace), ColumnIdentifier.maybeQuote(table));
    }
}
