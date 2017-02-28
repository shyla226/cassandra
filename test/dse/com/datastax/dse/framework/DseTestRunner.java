/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework;


import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.TransportException;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.googlecode.junittoolbox.PollingWait;
import com.googlecode.junittoolbox.RunnableAssert;
import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.auth.AllowAllAuthorizer;
import org.apache.cassandra.auth.AuthConfig;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.auth.PermissionsCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertTrue;
import static com.datastax.dse.framework.LogFileDiscriminator.*;

/**
 * <p>
 * Abstract test class providing the following capabilities:
 * <ul>
 * <li>Start and stop of unlimited number of DSE nodes.</li>
 * <li>Support for multiple Cassandra, Hadoop and Solr nodes.</li>
 * <li>Execution of CQL2/CQL3 queries on each running node.</li>
 * <li>Execution of "test code" to test the state of a specific node.</li>
 * <li>Code injection to simulate failures, trace system state and more.</li>
 * <li>Authentication and authorization: if auth is enabled via
 * setAuthentication()/setAuthorization() methods, each client is configured
 * with the default cassandra/cassandra credentials unless setUser() is
 * called.</li>
 * </ul>
 * </p>
 * <p>
 * Configuration files are generated from the following velocity templates:
 * <ul>
 * <li>test/conf/ng/cassandra.yaml.template -> build/test/ng/<em>INSTANCE_NAME</em>/cassandra.yaml</li>
 * <li>test/conf/ng/dse.yaml.template       -> build/test/ng/<em>INSTANCE_NAME</em>/dse.yaml</li>
 * </ul>
 * </p>
 * <p>
 * Each node binds to 127.0.0.N, where N is the node number: so node 1 binds
 * to 127.0.0.1, node 2 to 127.0.0.2 and so on. Hence, in order to run more than
 * just one node, you need to setup more local network interfaces; in order to
 * verify you can actually run the desired number of nodes, you can call the
 * {@link #verifyCanRunMultipleDseNodes(int)} method. If you are on Linux, usually you do not need
 * to perform anything, on OSX you need to create additional interfaces with:
 * </p>
 * <pre>
 *   sudo ifconfig lo0 alias 127.0.0.2 up
 *   sudo ifconfig lo0 alias 127.0.0.3 up
 *   ...
 * </pre>
 * <p>
 * NOTE: if you run test inside Docker containers (highly recommended; check BUILDING.md for details),
 * then you do not need to worry about network interfaces at all.
 * </p>
 */
public abstract class DseTestRunner
{
    protected final static Logger log = LoggerFactory.getLogger(DseTestRunner.class);
    public final static Logger threadDumpLogger = LoggerFactory.getLogger("threadDumpLogger");

    public static final int TEST_TIMEOUT_MILLIS = 20 * 60 * 1000; // wait 20mins for a test for finish

    protected final static int native_port = 9042;
    protected final static ConcurrentMap<Integer, Credentials> credentials = new ConcurrentHashMap<>();
    protected volatile static Authentication authentication = Authentication.NONE;
    protected volatile static Authorization authorization = Authorization.NONE;
    protected volatile static boolean clientEncryption = false;
    private volatile static boolean requireClientAuth = false;
    private volatile static boolean internodeRequireClientAuth = false;
    private final static String host = "127.0.0.Z";
    private static final String[] dseKerberosHosts = new String[]{ "localhost", "127.0.0.2", "127.0.0.3" };
    private static final String[] httpKerberosHosts = new String[]{ "127.0.0.1", "127.0.0.2", "127.0.0.3" };
    private final static ConcurrentMap<Integer, DseNode> nodes = new ConcurrentHashMap<>();

    protected static volatile boolean useKerberosTicket = false;
    protected static boolean reusableServers = Boolean.parseBoolean(System.getProperty("dse.reuse.servers", "false"));
    protected volatile static boolean serverEncryption = false;
    public static final Duration defaultFutureTimeout = Duration.ofMinutes(5);

    public final static TestEnvironment testEnvironment = new TestEnvironment();

    public enum Authentication
    {
        NONE, PASSWORD
    }

    public enum Authorization
    {
        NONE, CASSANDRA
    }


    protected int getTestTimeout()
    {
        return TEST_TIMEOUT_MILLIS;
    }

    @ClassRule
    public static TestRule classWatcher = new TestWatcher()
    {
        @Override
        protected void starting(Description description)
        {
            logToFile(description.getClassName() + "/before.log");
        }

        @Override
        protected void finished(Description description)
        {
            logToDefaultFile();
            //ForbiddenLogEventsDetector.checkForIssues(description.getClassName()+"/before.log");
            //ForbiddenLogEventsDetector.checkForIssues(description.getClassName()+"/after.log");
        }
    };

    @Rule
    public TestRule watcher = new TestWatcher()
    {

        private String logFile;

        @Override
        protected void starting(Description description)
        {
            logFile = description.getClassName() + "/" + description.getMethodName() + ".log";
            logToFile(logFile);
        }

        @Override
        protected void failed(Throwable e, Description description)
        {
            threadDumpLogger.error("Test {}.{} failed, thread dump:\n{}\n", description.getClassName(),
                                   description.getMethodName(), getThreadDumps());
            logToFile(description.getClassName() + "/after.log");
        }

        @Override
        protected void finished(Description description)
        {
            logToFile(description.getClassName() + "/after.log");
            //ForbiddenLogEventsDetector.checkForIssues(logFile);
        }
    };


    // fail test if it does not complete within the given timeout
    @Rule
    public Timeout testTimeout = new Timeout(getTestTimeout());

    /**
     * Our tests are time-limited and JUnit runs each of them in a separate thread
     * Thus, we should close client connections at the end because they are kept in
     * a thread local variable
     */
    @Rule
    public TestRule closeClientConnectionAtTestEnd = new TestWatcher()
    {
        @Override
        protected void finished(Description description)
        {
            for (DseNode node : nodes.values())
            {
                node.getNativeClient().close();
            }
        }
    };

    static
    {

        if (!Util.isInsideDockerContainer())
        {
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        destroyCluster();
                    }
                    catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            }));
        }
    }

    protected DseTestRunner()
    {
        MDC.put("nodeid", "TEST");
        if (System.getProperty("dse.reuse.servers") == null)
        {
            try
            {
                Category cat = this.getClass().getAnnotation(Category.class);
                if (cat != null)
                {
                    List categories = Arrays.asList(cat.value());
                    reusableServers = true;
                    System.out.println("REUSABLE CASSANDRA NODES");
                }
                else
                {
                    reusableServers = false;
                    System.out.println("NO REUSABLE CASSANDRA NODES");
                }
            }
            catch (Exception e)
            {
                reusableServers = false;
            }
        }
    }

    @Before
    public void setUp() throws Exception
    {
        prepareForTest();
    }

    public static void prepareForTest() throws Exception
    {
        if (!hasReusableServers())
        {
            authentication = Authentication.NONE;
            authorization = Authorization.NONE;
            testEnvironment.clearConfigs();
            credentials.clear();
        }
    }

    @AfterClass
    public static void tearDownClass() throws Exception
    {
        if (hasReusableServers())
        {
            // If we are going to reuse the servers then make sure that we have removed any injection
            // rules from the running nodes
            for (Integer node : nodes.keySet())
            {
                nodes.get(node).inject(Injector.DeleteAll.class);
            }
        }
        else
        {
            classCleanup();
        }

        authentication = Authentication.NONE;
        authorization = Authorization.NONE;
        clientEncryption = false;
        useKerberosTicket = false;
    }

    private static boolean hasReusableServers()
    {
        return reusableServers;
    }

    public static void classCleanup() throws Exception
    {
        destroyCluster();
    }

    public static void verifyCanRunMultipleDseNodes(int total) throws Exception
    {
        for (int i = 1; i <= total; i++)
        {
            ServerSocket socket = new ServerSocket();
            String address = host.replaceFirst("Z", "" + i);
            socket.bind(new InetSocketAddress(address, native_port));
            socket.close();
        }
    }

    /**
     * Set the authentication method to be used for all embedded nodes.
     */
    public static void setAuthentication(Authentication authentication) throws Exception
    {
        DseTestRunner.authentication = authentication;
    }

    public static boolean isPlainTextAuthEnabled()
    {
        return authentication == Authentication.PASSWORD;
    }

    public static Authentication getAuthentication()
    {
        return authentication;
    }

    /**
     * Set the authorization method to be used for all embedded nodes.
     */
    public static void setAuthorization(Authorization authorization) throws Exception
    {
        DseTestRunner.authorization = authorization;
    }

    public static Authorization getAuthorization()
    {
        return authorization;
    }

    /**
     * Set the client encryption state for all embedded nodes. If set to true
     * the embedded nodes will use encrypted connections for thrift and
     * native client connections.
     */
    public static void setClientEncryption(boolean clientEncryption)
    {
        DseTestRunner.clientEncryption = clientEncryption;
    }

    public static boolean getClientEncryption()
    {
        return DseTestRunner.clientEncryption;
    }

    public static void setRequireClientAuth(boolean requireClientAuth)
    {
        DseTestRunner.requireClientAuth = requireClientAuth;
    }

    // Set internode encryption
    public static void setServerEncryption(boolean serverEncryption)
    {
        DseTestRunner.serverEncryption = serverEncryption;
    }

    public static void setInternodeRequireClientAuth(boolean requireClientAuth)
    {
        DseTestRunner.internodeRequireClientAuth = requireClientAuth;
    }

    /**
     * Returns an existing node with the given index or null if such a node does not exist.
     */
    public static DseNode node(int n)
    {
        return nodes.get(n);
    }

    public static Set<Integer> runningNodesIds()
    {
        return nodes.values().stream().filter(DseNode::isRunning).map(DseNode::getId).collect(Collectors.toSet());
    }

    public static Set<Integer> runningNodesIds(String dc)
    {
        return nodes.values().stream()
                    .filter(DseNode::isRunning)
                    .filter(node -> Objects.equal(node.getDatacenterName(), dc))
                    .map(DseNode::getId)
                    .collect(Collectors.toSet());
    }

    public static Collection<DseNode> nodes()
    {
        return nodes.values();
    }

    public static DseNode getOrCreateNode(int n)
    {
        return getOrCreateNode(n, null, null, false);
    }

    public static DseNode getOrCreateNode(int n, CassandraYamlBuilder cassandraYamlBuilder,
                                          Class<? extends Injector> injector, boolean rewrite)
    {
        try
        {
            if (n > 1 && !nodes.containsKey(n - 1))
            {
                throw new IllegalArgumentException(
                                                  "Cannot start node " + n + " if node " + (n - 1) + " has not been started!");
            }

            cassandraYamlBuilder = setupCassandraAuth(cassandraYamlBuilder, n);

            DseNode node = nodes.get(n);

            if (node != null && node.isRunning())
            {
                if (!node.getCassandraYamlBuilder().equals(cassandraYamlBuilder) || injector != null)
                {
                    if (n == 1)
                    {
                        destroyCluster();
                    }
                    else
                    {
                        stopNode(n);
                    }
                }
                log.info("Reusing node {}, instance id: {}", n, node.getCassandraYamlBuilder().getInstanceId());
            }
            if (nodes.get(n) == null)
            {

                String instanceId = UUID.randomUUID().toString();
                cassandraYamlBuilder.setInstanceId(instanceId);

                String address = host.replaceFirst("Z", "" + n);
                // Generate a unique instance name
                Path cassandraConfig = testEnvironment.generateCassandraConfig(getHostForNode(n), n, cassandraYamlBuilder, rewrite, clientEncryption);

                String truststorePath = null;
                if (clientEncryption)
                {
                    truststorePath = testEnvironment.getTruststorePath(n);
                }

                String keystorePath = null;
                if (requireClientAuth)
                {
                    keystorePath = testEnvironment.getKeystorePath(n);
                }
                node = new DseNode(instanceId);
                node.setCassandraYamlBuilder(cassandraYamlBuilder);
                node.setCassandraConfigFileName(cassandraConfig.toFile().toURI().toURL().toString());
                node.setAddress(address);
                node.setTruststorePath(truststorePath);
                node.setKeystorePath(keystorePath);
                node.setId(n);
                node.setBytemanBootstrapInjector(injector);
                node.setAuthentication(authentication);
                node.setAuthorization(authorization);
                nodes.put(n, node);
            }
            else
            {
                if (!node.getAuthentication().equals(authentication) || !node.getAuthorization().equals(authorization))
                {
                    System.out.println("Changed node " + n + " authorization to " + authorization + " and authentication to " + authentication);
                    cassandraYamlBuilder = node.getCassandraYamlBuilder();
                    cassandraYamlBuilder.resetSecurity();
                    cassandraYamlBuilder = setupCassandraAuth(cassandraYamlBuilder, n);
                    node.setAuthentication(authentication);
                    node.setAuthorization(authorization);
                    node.setCassandraConfigFileName(testEnvironment.generateCassandraConfig(getHostForNode(n), n, cassandraYamlBuilder, true, clientEncryption).toFile().toURI().toURL().toString());
                    node.setTruststorePath(testEnvironment.getTruststorePath(n));
                }
            }
            return node;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Start a DSE node, with a given number and type.
     * <br/>
     * The builder is used to pass non-standard configurations to run.
     * If rewrite is set to true then an attempt will be made
     * to reconfigure an existing stopped nodes configuration
     * and then start that node with its existing data.
     * <br/>
     * The injector is used to inject special behaviours enabled since node
     * startup. Once injected, those stay enabled until enableInjectedCode is
     * called to disable them.
     * <br/>
     * Please note you cannot start a node with number n, if
     * another node with number n-1 has not been started: in other words, nodes
     * must be started sequentially,
     * and the same node number cannot be started multiple times without having
     * been stopped first.
     */
    public synchronized static void startNode(int n, CassandraYamlBuilder cassandraYamlBuilder, Class<? extends Injector> injector, boolean rewrite) throws Exception
    {
        getOrCreateNode(n, cassandraYamlBuilder, injector, rewrite).start();
    }


    public synchronized static void startNode(int n, CassandraYamlBuilder builder, boolean rewrite) throws Exception
    {
        startNode(n, builder, null, rewrite);
    }

    public synchronized static void startNode(int n, boolean rewrite) throws Exception
    {
        startNode(n, null, null, rewrite);
    }

    public synchronized static void startNode(int n) throws Exception
    {
        startNode(n, null, null, false);
    }

    protected synchronized static void startNode(int n, Class<? extends Injector> injector) throws Exception
    {
        startNode(n, null, injector, false);
    }

    public synchronized static void reloadAuthConfiguration() throws Exception
    {
        for (int node : nodes.keySet())
        {
            CassandraYamlBuilder cassandraYamlBuilder = testEnvironment.getCassandraYamlBuilder(node);
            cassandraYamlBuilder = setupCassandraAuth(cassandraYamlBuilder, node);
            testEnvironment.generateCassandraConfig(getHostForNode(node), node, cassandraYamlBuilder, true, clientEncryption);

            if (sendTestToNode(node, ReloadAuthConfiguration.class).get().equals("FAILED"))
            {
                throw new RuntimeException("Failed to reload configuration");
            }
            nodes.get(node).getNativeClient().reset();
            if (authentication == Authentication.PASSWORD)
            {
                // need this delay to allow the superuser time to be created
                Thread.sleep(2000);
            }
        }
    }

    public static String getNodeRootDir(int node)
    {
        assertNodeStarted(node);
        return testEnvironment.getCassandraYamlBuilder(node).getRootDir();
    }

    /**
     * Get a remote controller on a background scheduled runnable task on a node.
     *
     * @param n        the node
     * @param runnable the background task runnable
     * @return a controller to use for stepping.
     * @throws ExecutionException   if the task could not be paused
     * @throws InterruptedException when the setup process was interrupted
     */
    public synchronized PauseController getController(int n, Class<? extends Runnable> runnable)
    throws ExecutionException, InterruptedException
    {
        return getController(n, runnable, "run");
    }

    /**
     * Get a remote controller on a background scheduled runnable task on a node.
     *
     * @param n          the node
     * @param runnable   the background task runnable
     * @param methodName the method to step through
     * @return a controller to use for stepping.
     * @throws ExecutionException   if the task could not be paused
     * @throws InterruptedException when the setup process was interrupted
     */
    public synchronized PauseController getController(int n, Class<? extends Runnable> runnable, String methodName)
    throws ExecutionException, InterruptedException
    {
        assertNodeStarted(n);
        return nodes.get(n).getPauseController(runnable, methodName);
    }


    /**
     * Calls {@link #destroyNode(int) } on some number of nodes
     *
     * @param nodes the list of nodes we want to destroy
     */
    public synchronized static void destroyNodes(int... nodes) throws Exception
    {
        for (int node : nodes)
        {
            destroyNode(node);
        }
    }

    /**
     * Calls {@link #destroyNode(int) } on every known node in the cluster
     */
    public synchronized static void destroyCluster() throws Exception
    {
        for (Integer node : Ordering.natural().reverse().immutableSortedCopy(nodes.keySet()))
        {
            destroyNode(node, false);
        }
    }

    /**
     * Calls {@link #stopNode(int)} on every known node in the cluster
     */
    public synchronized static void stopCluster() throws Exception
    {
        for (Integer node : Ordering.natural().reverse().immutableSortedCopy(nodes.keySet()))
        {
            stopNode(node, false);
        }
    }

    public synchronized static void destroyNode(int n) throws Exception
    {
        stopNodeInternal(n, false, false, true);
    }

    /**
     * Stop and remove all configuration from a node. The next time this node is started
     * it will be a clean slate. This is a slower operation compared to {@link #stopNode(int) },
     * so only use it if really necessary to destroy the configuration of a node
     *
     * @param n             The node we want to destroy
     * @param waitForGossip If true, wait for other nodes to detect that the node has been stopped.
     */
    public synchronized static void destroyNode(int n, boolean waitForGossip) throws Exception
    {
        stopNodeInternal(n, false, false, waitForGossip);
    }

    public synchronized static void stopNode(int n) throws Exception
    {
        stopNodeInternal(n, true, true, true);
    }

    /**
     * Stop a node but retain all configuration. The next time this node is started
     * it will have the same configuration as when the node was stopped. This operation does still take
     * take many seconds to complete, so avoid calling if not absolutely necessary
     *
     * @param n             The node we want to stop
     * @param waitForGossip If true, wait for other nodes to detect that the node has been stopped.
     */
    public synchronized static void stopNode(int n, boolean waitForGossip) throws Exception
    {
        stopNodeInternal(n, true, true, waitForGossip);
    }

    private synchronized static void stopNodeInternal(int n, boolean keepConfig, boolean gently, boolean waitForGossip) throws Exception
    {
        final DseNode node = nodes.get(n);
        if (node != null)
        {
            if (gently)
            {
                node.stop();
            }
            else
            {
                node.kill();
            }
            if (!keepConfig)
            {
                nodes.remove(n);
                testEnvironment.removeConfig(n);
            }

            if (nodes.isEmpty())
            {
                testEnvironment.clearConfigs();
            }

            if (waitForGossip)
            {
                // Make sure all live nodes have been notified about the newly stopped node:
                for (Map.Entry<Integer, DseNode> liveNode : nodes.entrySet())
                {
                    if (liveNode.getValue().isRunning())
                    {
                        waitForUnreachableHost(liveNode.getKey(), n);
                    }
                }
            }
        }
    }

    /**
     * Test whether a DSE node is already running
     */
    public synchronized static boolean isNodeRunning(int n)
    {
        return nodes.containsKey(n) ? nodes.get(n).isRunning() : false;
    }

    /**
     * Returns the ID of the first running node or null of no node is running.
     */
    public synchronized static Integer getFirstRunningNodeId()
    {
        for (Integer node : nodes.keySet())
        {
            if (isNodeRunning(node))
            {
                return node;
            }
        }
        return null;
    }

    /**
     * Get the host address for the given node number.
     */
    public static String getHostForNode(int n)
    {
        return host.replaceFirst("Z", "" + n);
    }

    /**
     * Get the host address for the given node number.
     */
    public static InetAddress getInetAddressForNode(int n)
    {
        try
        {
            return InetAddress.getByName(getHostForNode(n));
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the node number for the given host address.
     */
    public static int getNodeForHost(String host)
    {
        return Integer.parseInt(host.substring(host.length() - 1, host.length()));
    }

    public static void cleanupData() throws Exception
    {
        cleanupData(false);
    }

    /**
     * Utility method to cleanup data dirs if forced to do so (otherwise resorts to no-op).
     * <p>
     * See DSP-3300. C* threads can survive a node.stop() so compactions and others could be running in the background.
     * This method could accidentally delete folders and files of these and crash unit tests specially if forced ==
     * true
     */
    public static void cleanupData(boolean force) throws Exception
    {
        if (force)
        {
            for (Integer node : nodes.keySet())
            {
                tryWaitForCompactions(node);
            }
            Path[] pathsToClean = new Path[]{
            testEnvironment.ngDir,
            testEnvironment.buildConfDir
            };

            for (Path path : pathsToClean)
            {
                if (path.toFile().isDirectory())
                {
                    FileUtils.cleanDirectory(path.toFile());
                }
            }
        }
    }

    /**
     * @return {@link MBeanServerConnection} instance for node 1
     * @throws Exception if an error occurs
     * @see #getMBeanServerConnection(int)
     * @deprecated use #getMBeanServerConnection(int) instead
     */
    public static MBeanServerConnection getMBeanServerConnection() throws Exception
    {
        return getMBeanServerConnection(1);
    }

    /**
     * @return {@link MBeanServerConnection} instance for the node with the given node id
     * @throws Exception if an error occurs
     */
    public static MBeanServerConnection getMBeanServerConnection(int nodeId) throws Exception
    {
        return nodes.get(nodeId).getMBeanServerConnection();
    }

    public static Object getMBeanAttribute(MBeanServerConnection connection, ObjectName name, String attribute) throws Exception
    {
        return connection.getAttribute(name, attribute);
    }

    /**
     * Inject special behaviours into the running code of the given node.
     * <br/>
     * Those must be expressed in a publicly accessible class extending
     * Injector.
     * <br/>
     * Once injected, those stay enabled until enableInjectedCodeForNode is called to
     * disable them.
     */
    protected static void sendInjectionToNode(int n, Class<? extends Injector> injector) throws Exception
    {
        assertNodeStarted(n);
        nodes.get(n).inject(injector);
    }

    /**
     * Enable/disable injected code.
     */
    public static void enableInjectedCodeForNode(final int n, final String id, final boolean enabled)
    {
        DseNode node = nodes.get(n);
        if (enabled)
        {
            node.enableInjectedBytemanRule(id);
        }
        else
        {
            node.disableInjectedBytemanRule(id);
        }
    }

    /**
     * Send "test code" to execute in isolation to the DSE node with the given
     * number.
     * <br/>
     * The test code must be contained in a Callable class, publicly accessible
     * and with a no-arg constructor.
     * The call method can return any string output, that can be checked in the
     * main test case.
     */
    public static <T> Future<T> sendTestToNode(int n, Class<? extends Callable<T>> test, Object... args) throws Exception
    {
        assertNodeStarted(n);
        return nodes.get(n).submit(test, args);
    }

    /**
     * Serializes the function provided in {@code code}, executes it inside the given node context,
     * serializes the results sends it back to the testing environment and deserialises it.
     * The provided function needs to be serialisable.
     * In case the invocation of the given function throws an exception, {@link ExecutionException} will
     * be thrown by this method with the original exception as a cause.
     */
    public static <T extends Serializable> Future<T> executeCodeOnNode(int n, SerializableCallable<T> code) throws Exception
    {

        // are we already running on the requested node?
        String nodeId = System.getProperty(DseNode.NODE_ID_PROP);
        if (nodeId != null && nodeId.equals("" + n))
        {
            return Futures.immediateFuture(code.call());
        }
        else
        {
            return nodes.get(n).callAsync(code);
        }
    }

    protected Cluster getNativeClientForNode(int n)
    {
        assertNodeStarted(n);
        return nodes.get(n).getNativeClient().get().getCluster();
    }

    /**
     * Attempt to proxy execute this statement
     *
     * @param n           the node to use
     * @param executingAs the user to proxy execute as (the normal 'login' user must have proxy.execute permission)
     * @param keyspace    keyspace to execute in
     * @param cql         the text of the statement to execute
     */
    public static ResultSet sendCql3NativeAs(int n, String executingAs, String keyspace, String cql) throws Exception
    {
        return sendCql3Native(n, keyspace, cql, executingAs, false, com.datastax.driver.core.ConsistencyLevel.ONE);
    }

    public static ResultSet sendCql3Native(int n, String keyspace, String cql) throws Exception
    {
        return sendCql3Native(n, keyspace, cql, null, false, com.datastax.driver.core.ConsistencyLevel.ONE);
    }

    public static ResultSet sendCql3Native(int n, String keyspace, String cql, boolean tracing) throws Exception
    {
        return sendCql3Native(n, keyspace, cql, null, tracing, com.datastax.driver.core.ConsistencyLevel.ONE);
    }

    protected static ResultSet sendCql3Native(int n, String keyspace, String cql, com.datastax.driver.core.ConsistencyLevel cl) throws Exception
    {
        return sendCql3Native(n, keyspace, cql, null, false, cl);
    }

    protected static ResultSet sendCql3Native(int n, String keyspace, String cql, String executingAs, boolean tracing, com.datastax.driver.core.ConsistencyLevel cl, Object... args) throws Exception
    {
        long retryDuration = 1000;
        int retryNum = 4;
        Exception lastException = null;
        while (retryNum > 0)
        {
            try
            {
                Session session = prepareNativeSession(n, keyspace);
                Statement statement = new SimpleStatement(cql, args);

                if (cl != null)
                {
                    statement.setConsistencyLevel(cl);
                }

                if (executingAs != null)
                {
                    statement.executingAs(executingAs);
                }

                if (tracing)
                {
                    statement = statement.enableTracing();
                }

                return session.execute(statement);
            }
            catch (NoHostAvailableException nhae)
            {
                // sometimes the driver thinks that the node is down, but in case the node is up (according to our knowledge)
                // then retry the query
                if (examineErrorsAndDetermineIfRetryNecessary(nhae, n, cql, keyspace, retryNum))
                {
                    // certain tests that e.g. check credentials cache and such, there's a possible race condition
                    // where the driver successfully creates the control connection, but actually fails to
                    // create the connection pool (because credentials changed).
                    // therefore lets close the driver's connection on retry to prevent that (connection will be opened
                    // once a new cql query will be executed, which in this case is on the next retry cycle)
                    nodes.get(n).getNativeClient().close();
                    Thread.sleep(retryDuration);
                    retryDuration = retryDuration * 2;
                    lastException = nhae;
                }
                else
                {
                    log.error("Query {} failed with", cql, nhae);
                    for (Map.Entry<InetSocketAddress, Throwable> e : nhae.getErrors().entrySet())
                    {
                        log.error("Node {} has thrown", e.getKey().toString(), e.getValue());
                    }
                    throw nhae;
                }
            }
            retryNum--;
        }
        throw lastException;
    }

    private static boolean examineErrorsAndDetermineIfRetryNecessary(NoHostAvailableException nhae, int node, String cql, String keyspace, int retryNum)
    {
        if (nhae.getErrors().isEmpty() && retryNum > 1 && isNodeRunning(node))
        {
            // in some cases we just get a NoHostAvailableException without any details about the real cause
            // In order to make the tests more robust on jenkins, we just retry the query
            log.info("Query failed because the driver believes that node {} is down (got NoHostAvailableException), but according to our knowledge, the node is up, thus retry the query {} on keyspace {}, stacktrace", node, cql, keyspace, nhae);
            return true;
        }

        for (Map.Entry<InetSocketAddress, Throwable> e : nhae.getErrors().entrySet())
        {
            if (retryNum > 1 && isNodeRunning(getNodeForHost(e.getKey().getAddress().getHostAddress())) && isTransportOrDriverExceptionWithTimeout(e.getValue()))
            {
                log.info("Query failed because the driver believes that node {} is down (got NoHostAvailableException.{}), but according to our knowledge, the node is up, thus retry the query {} on keyspace {}, stacktrace", node, e.getValue().getClass().getSimpleName(), cql, keyspace, e.getValue());
                return true;
            }
        }
        return false;
    }

    private static boolean isTransportOrDriverExceptionWithTimeout(Throwable t)
    {
        // due to DSP-11236 we need to do a retry when the driver throws a DriverException & contains a specific timeout string in the error msg
        return TransportException.class.equals(t.getClass()) || (t instanceof DriverException && null != t.getMessage() && t.getMessage().toLowerCase().contains("timeout while trying to acquire available connection"));
    }

    protected ResultSetFuture sendCql3NativeAsync(int n, String keyspace, String cql) throws Exception
    {
        return sendCql3NativeAsync(n, keyspace, cql, false, com.datastax.driver.core.ConsistencyLevel.ONE);
    }

    protected ResultSetFuture sendCql3NativeAsync(int n, String keyspace, String cql, boolean tracing, com.datastax.driver.core.ConsistencyLevel cl) throws Exception
    {
        Session session = prepareNativeSession(n, keyspace);
        Statement statement = new SimpleStatement(cql);
        statement.setConsistencyLevel(cl);

        if (tracing)
        {
            statement = statement.enableTracing();
        }

        return session.executeAsync(statement);
    }

    protected static ResultSet prepareAndExecuteCql3Statement(int n, String keyspace, String cql, Object... values) throws Exception
    {
        Session session = prepareNativeSession(n, keyspace);
        return session.execute(session.prepare(cql).bind(values));
    }

    protected ResultSet sendCql3NativeWithArgs(int n, String keyspace, String cql, Object... args) throws Exception
    {
        return sendCql3Native(n, keyspace, cql, null, false, null, args);
    }

    protected ResultSet sendCql3NativeWithNamedArgs(int n, String keyspace, String cql, Map<String, Object> values) throws Exception
    {
        Session session = prepareNativeSession(n, keyspace);
        return session.execute(cql, values);
    }

    protected void waitForSchemaAgreement(int timeout)
    {
        Cluster cluster = getNativeClientForNode(1);

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeout)
        {
            if (cluster.getMetadata().checkSchemaAgreement())
            {
                break;
            }
        }
    }

    protected void waitForAssert(Runnable runnableAssert, String description, long pollingInterval, long timeout, TimeUnit unit)
    {
        new PollingWait().pollEvery(pollingInterval, unit).timeoutAfter(timeout, unit).until(new RunnableAssert(description)
        {
            @Override
            public void run() throws Exception
            {
                runnableAssert.run();
            }
        });
    }

    protected void assertEmpty(ResultSet result)
    {
        if (result != null && !result.isExhausted())
        {
            Assert.fail(String.format("Expected empty result but got %d rows", result.all().size()));
        }
    }

    protected Object[] row(Object... expected)
    {
        return expected;
    }

    protected void assertRows(ResultSet result, Object[]... rows)
    {
        if (result == null || result.isExhausted())
        {
            if (rows.length > 0)
            {
                Assert.fail(String.format("No rows returned by query but %d expected", rows.length));
            }
            return;
        }

        Cluster cluster = nodes.get(1).getNativeClient().get().getCluster();
        CodecRegistry registry = cluster.getConfiguration().getCodecRegistry();

        final org.apache.cassandra.transport.ProtocolVersion protocolVersion = org.apache.cassandra.transport.ProtocolVersion.CURRENT;
        final ProtocolVersion pv = ProtocolVersion.fromInt(protocolVersion.asInt());

        ColumnDefinitions meta = result.getColumnDefinitions();
        Iterator<Row> iter = result.iterator();
        int i = 0;
        while (iter.hasNext() && i < rows.length)
        {
            Object[] expected = rows[i];
            Row actual = iter.next();

            Assert.assertEquals(String.format("Invalid number of (expected) values provided for row %d (using protocol version %d)",
                                              i, protocolVersion.asInt()),
                                meta.size(), expected.length);

            for (int j = 0; j < meta.size(); j++)
            {
                DataType type = meta.getType(j);
                TypeCodec<Object> codec = CodecRegistry.DEFAULT_INSTANCE.codecFor(type);

                if (expected[j] == null)
                {
                    if (actual.getBytesUnsafe(meta.getName(j)) != null)
                    {
                        ByteBuffer actualValue = actual.getBytesUnsafe(meta.getName(j));
                        int actualBytes = actualValue.remaining();
                        Assert.fail(String.format("Invalid value for row %d column %d (%s of type %s), " +
                                                  "expected <%s> (%d bytes) but got <%s> (%d bytes) " +
                                                  "(using protocol version %d)",
                                                  i, j, meta.getName(j), type,
                                                  "null", 0,
                                                  codec.deserialize(actualValue, pv),
                                                  actualBytes,
                                                  protocolVersion.asInt()));
                    }
                }
                else
                {
                    ByteBuffer expectedByteValue = codec.serialize(expected[j], pv);
                    int expectedBytes = expectedByteValue.remaining();
                    ByteBuffer actualValue = actual.getBytesUnsafe(meta.getName(j));
                    if (actualValue == null)
                    {
                        Assert.fail(String.format("Invalid value for row %d column %d (%s of type %s), " +
                                                  "expected <%s> (%d bytes) but got <%s> (%d bytes) " +
                                                  "(using protocol version %d)",
                                                  i, j, meta.getName(j), type,
                                                  codec.format(expected[j]), expectedBytes,
                                                  "null", 0,
                                                  protocolVersion.asInt()));
                    }
                    int actualBytes = actualValue.remaining();

                    if (!Objects.equal(expectedByteValue, actualValue))
                    {
                        Assert.fail(String.format("Invalid value for row %d column %d (%s of type %s), " +
                                                  "expected <%s> (%d bytes) but got <%s> (%d bytes) " +
                                                  "(using protocol version %d)",
                                                  i, j, meta.getName(j), type,
                                                  codec.format(expected[j]),
                                                  expectedBytes,
                                                  codec.format(codec.deserialize(actualValue, pv)),
                                                  actualBytes,
                                                  protocolVersion.asInt()));
                    }
                }
            }
            i++;
        }

        if (iter.hasNext())
        {
            while (iter.hasNext())
            {
                iter.next();
                i++;
            }
            Assert.fail(String.format("Got less rows than expected. Expected %d but got %d (using protocol version %d).",
                                      rows.length, i, protocolVersion.asInt()));
        }

        assertTrue(String.format("Got %s rows than expected. Expected %d but got %d (using protocol version %d)",
                                 rows.length > i ? "less" : "more", rows.length, i, protocolVersion.asInt()), i == rows.length);
    }

    public static void setUser(int nodeId, String user, String password) throws Exception
    {
        Credentials oldCredentials = credentials.put(nodeId, new Credentials(user, password));
        DseNode node = nodes.get(nodeId);
        if (node != null && oldCredentials != null)
        {
            node.getNativeClient().reset();
        }
    }


    public static void flushMemtable(int node, String keyspace, String table) throws Exception
    {
        assertTrue(Boolean.parseBoolean(sendTestToNode(node, MemtableFlush.class, keyspace, table).get()));
    }

    protected void waitForNormalState(final int node) throws Exception
    {
        PollingWait wait = new PollingWait().timeoutAfter(5, TimeUnit.MINUTES).pollEvery(5, TimeUnit.SECONDS);
        wait.until(new RunnableAssert("Waiting for bootstrap to finish!")
        {
            public void run() throws Exception
            {
                assertTrue(Boolean.parseBoolean(sendTestToNode(node, IsNormal.class).get()));
            }
        });
    }

    protected static void waitForUnreachableHost(int from, int remote)
    {
        String message = "Waiting until node " + remote + " is unreachable from node " + from + "...";

        log.info(message);

        PollingWait wait = new PollingWait().timeoutAfter(5, TimeUnit.MINUTES).pollEvery(5, TimeUnit.SECONDS);
        wait.until(new RunnableAssert(message)
        {
            public void run() throws Exception
            {
                String commaSeparatedUnreachable = sendTestToNode(from, GetUnreachableMembers.class).get();
                ImmutableSet<String> unreachableHosts = ImmutableSet.copyOf(commaSeparatedUnreachable.split(","));
                assertTrue(unreachableHosts.contains(getHostForNode(remote)));
            }
        });
    }

    private static void tryWaitForCompactions(int node) throws Exception
    {
        int seconds = 120;
        for (int i = 0; i < seconds; i++)
        {
            boolean compacting = Boolean.parseBoolean(sendTestToNode(node, IsCompacting.class).get());
            if (!compacting)
            {
                break;
            }
            Thread.sleep(1000);
        }
    }

    private static CassandraYamlBuilder setupCassandraAuth(CassandraYamlBuilder builder, int n) throws Exception
    {
        builder = builder != null ? builder : CassandraYamlBuilder.newInstance();
        credentials.put(n, new Credentials("cassandra", "cassandra"));
        System.setProperty("cassandra.superuser_setup_delay_ms", "1000");
        if (authentication == Authentication.PASSWORD)
        {
            builder.authenticator = PasswordAuthenticator.class.getName();
        }
        else
        {
            builder.authenticator = AllowAllAuthenticator.class.getName();
        }

        if (authorization == Authorization.CASSANDRA)
        {
            builder.authorizer = CassandraAuthorizer.class.getName();
        }
        else
        {
            builder.authorizer = AllowAllAuthorizer.class.getName();
        }

        if (clientEncryption)
        {
            builder.clientEncryption = true;
        }
        if (requireClientAuth)
        {
            builder.requireClientAuth = true;
        }
        if (serverEncryption)
        {
            builder.internodeEncryption = EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.all.name();
        }
        if (internodeRequireClientAuth)
        {
            builder.internodeRequireClientAuth = true;
        }
        return builder;
    }

    protected static Session prepareNativeSession(int n, String keyspace) throws Exception
    {
        assertNodeStarted(n);
        return nodes.get(n).getNativeClient().get().connect(keyspace);
    }

    protected static Session prepareNativeSession(int n) throws Exception
    {
        assertNodeStarted(n);
        return nodes.get(n).getNativeClient().get().connect();
    }

    protected static class ThreadLocalNativeClient extends ThreadLocal<NativeClient>
    {
        private final int nodeNr;
        private final String host;
        private final int port;
        private final String truststorePath;

        public ThreadLocalNativeClient(int nodeNr, String host, int port, String truststorePath)
        {
            this.nodeNr = nodeNr;
            this.host = host;
            this.port = port;
            this.truststorePath = truststorePath;
        }

        private final NativeClient init()
        {
            try
            {
                QueryOptions options = new QueryOptions();
                options.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE);
                options.setDefaultIdempotence(true); // DSP-12199: tell the driver to retry all statements by default
                Cluster.Builder builder = Cluster.builder()
                                                 .addContactPoints(host).withPort(port)
                                                 .withMaxSchemaAgreementWaitSeconds(20)
                                                 .withReconnectionPolicy(new ConstantReconnectionPolicy(1000))
                                                 .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(), Arrays.asList(new InetSocketAddress(host, port))))
                                                 .withQueryOptions(options)
                                                 .withPoolingOptions(new PoolingOptions().setMaxQueueSize(0))
                                                 .withAuthProvider(getAuthProvider(credentials.get(nodeNr)));

                if (clientEncryption)
                {
                    SSLContext ctx = initSSLContext(truststorePath, null);
                    JdkSSLOptions.Builder sslOptionsBuilder = JdkSSLOptions.builder();
                    sslOptionsBuilder.withSSLContext(ctx);
                    builder.withSSL(sslOptionsBuilder.build());
                }

                builder.withSocketOptions(new SocketOptions().setConnectTimeoutMillis(3600000).setReadTimeoutMillis(3600000));

                return new NativeClient(builder.build());
            }
            catch (Exception ex)
            {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }

        public AuthProvider getAuthProvider(Credentials credentials)
        {
            AuthProvider provider = AuthProvider.NONE;

            if (null != credentials && Authentication.PASSWORD.equals(authentication))
            {
                provider = credentials.getPlainTextAuthProvider();
            }
            return provider;
        }

        @Override
        public NativeClient get()
        {
            NativeClient client = super.get();
            if (null == client)
            {
                client = init();
                set(client);
            }
            return client;
        }

        public final void close()
        {
            if (null != super.get())
            {
                super.get().close();
            }
        }

        public final boolean isClosed()
        {
            if (null == super.get())
            {
                // closed if the client wasn't even initialized yet
                return true;
            }
            NativeClient client = get();
            return null == client.session || client.cluster.isClosed() || client.session.isClosed();
        }

        /***
         * Does the same as <code>close()</code>
         */
        public final void reset()
        {
            close();
        }
    }

    private static void assertNodeStarted(int n)
    {
        DseNode node = nodes.get(n);
        checkNotNull(node, "Node " + n + " is not started!");
        checkArgument(node.isRunning(), "Node " + n + " is not started!");
    }

    protected String getThreadDumps()
    {
        StringBuilder sb = new StringBuilder().append(String.format("-----Test JVM thread dump----\n%s\n", Util.getThreadDump()));
        for (Integer nodeId : nodes.keySet())
        {
            sb.append(nodeThreadDump(nodeId));
        }

        return sb.toString();
    }

    public static <T> T get(Future<T> f) throws Exception
    {
        try
        {
            return f.get(defaultFutureTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException e)
        {
            Throwable t = e;
            while (t instanceof ExecutionException) t = t.getCause();
            if (t instanceof Exception) throw (Exception) t;

            throw new Exception(t);
        }
    }

    public static String nodeThreadDump(int nodeId)
    {
        assertNodeStarted(nodeId);
        StringBuilder sb = new StringBuilder();
        DseNode node = nodes.get(nodeId);

        try
        {
            sb.append(String.format("-----Node %d thread dump----\n%s\n", node.getId(), node.callAsync(new ThreadDumpCollector()).get()));
        }
        catch (Throwable e)
        {
            log.warn("Could not collect the thread dump for node {}, reason: {}", node.getId(), e);
        }
        return sb.toString();
    }

    protected static class NativeClient
    {
        private Cluster cluster;
        private String keyspace;
        private Session session;

        public NativeClient(Cluster cluster)
        {
            this.cluster = cluster;
        }

        public Session connect()
        {
            return connect(null);
        }

        public Cluster getCluster()
        {
            return cluster;
        }

        public Session connect(String keyspace)
        {
            if ((session == null) || !StringUtils.equals(this.keyspace, keyspace))
            {
                if (session != null)
                {
                    session.close();
                }
                session = keyspace == null ? cluster.connect() : cluster.connect(keyspace);
                this.keyspace = keyspace;
            }
            return session;
        }

        public void close()
        {
            if (session != null)
            {
                session.close();
                session = null;
            }
            cluster.close();
        }

        @Override
        protected void finalize() throws Throwable
        {
            close();
        }
    }

    public static class ReloadAuthConfiguration implements Callable<String>
    {
        @Override
        public String call() throws Exception
        {
            try
            {
                // TODO: check if this works

                // Setup the new config and apply it.
                Method method = DatabaseDescriptor.class.getDeclaredMethod("setConfig", org.apache.cassandra.config.Config.class);
                method.setAccessible(true);
                method.invoke(null, DatabaseDescriptor.loadConfig());
                method = DatabaseDescriptor.class.getDeclaredMethod("applyAll");
                method.setAccessible(true);
                method.invoke(null);
                Field initializedField = AuthConfig.class.getDeclaredField("initialized");
                initializedField.setAccessible(true);
                initializedField.setBoolean(null, false);
                AuthConfig.applyAuth();

                // Tell the auth classes to set themselves up again.
                DatabaseDescriptor.getRoleManager().setup();
                DatabaseDescriptor.getAuthenticator().setup();
                DatabaseDescriptor.getAuthorizer().setup();

                Field permissionsCacheField = AuthenticatedUser.class.getDeclaredField("permissionsCache");
                permissionsCacheField.setAccessible(true);
                Field modifiersField = Field.class.getDeclaredField("modifiers");
                modifiersField.setAccessible(true);
                modifiersField.setInt(permissionsCacheField, permissionsCacheField.getModifiers() & ~Modifier.FINAL);
                permissionsCacheField.set(null, new PermissionsCache(DatabaseDescriptor.getAuthorizer()));
                return "SUCCEEDED";
            }
            catch (Throwable t)
            {
                t.printStackTrace();
                return "FAILED";
            }
        }
    }

    public static class IsCompacting implements Callable<String>
    {
        @Override
        public String call() throws Exception
        {
            return Boolean.toString(CompactionManager.instance.isCompacting(ColumnFamilyStore.all()));
        }
    }

    public static class IsNormal implements Callable<String>
    {

        @Override
        public String call() throws Exception
        {
            EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddress());
            return Boolean.toString(state.getApplicationState(ApplicationState.STATUS).value.startsWith(VersionedValue.STATUS_NORMAL));
        }
    }

    public static class GetUnreachableMembers implements Callable<String>
    {
        @Override
        public String call() throws Exception
        {
            return Gossiper.instance.getUnreachableMembers().stream().map(InetAddress::getHostAddress).collect(Collectors.joining(","));
        }
    }

    public interface SerializableRunnableWithException extends Serializable
    {
        void run() throws Exception;
    }

    public interface SerializableRunnable extends Runnable, Serializable
    {
    }

    public interface SerializableCallable<T extends Serializable> extends Callable<T>, Serializable
    {
    }

    public static class MemtableFlush implements Callable<String>
    {
        private String keySpace;
        private String columnFamily;

        public MemtableFlush(String keySpace, String columnFamily)
        {
            this.keySpace = keySpace;
            this.columnFamily = columnFamily;
        }

        @Override
        public String call() throws Exception
        {
            StorageService.instance.forceKeyspaceFlush(keySpace, columnFamily);
            return Boolean.toString(Boolean.TRUE);
        }
    }

    public static boolean isServerAvailable(String host, int port)
    {
        Socket socket = null;

        try
        {
            socket = new Socket(host, port);
            return true;
        }
        catch (Throwable e)
        {
        }
        finally
        {
            if (socket != null)
            {
                try
                {
                    socket.close();
                }
                catch (Throwable e)
                {
                }
            }
        }
        return false;
    }

    public static boolean isSslServerAvailable(String host, int port, String truststorePath)
    {
        Socket socket = null;

        try
        {
            SSLContext ctx = DseTestRunner.initSSLContext(truststorePath, null);
            socket = ctx.getSocketFactory().createSocket(host, port);
            // We need to start the handshake here otherwise the server will throw
            // an exception when we close the socket
            ((SSLSocket) socket).startHandshake();
            return true;
        }
        catch (Throwable e)
        {
        }
        finally
        {
            if (socket != null)
            {
                try
                {
                    socket.close();
                }
                catch (Throwable e)
                {
                }
            }
        }
        return false;
    }

    public interface SerializableConsumer<T> extends Serializable
    {
        /**
         * Performs this operation on the given argument.
         *
         * @param t the input argument
         */
        void accept(T t) throws Exception;
    }

    public static class ThreadDumpCollector implements SerializableCallable<String>
    {
        @Override
        public String call() throws Exception
        {
            return Util.getThreadDump();
        }
    }

    static SSLContext initSSLContext(String truststorePath, String keystorePath) throws Exception
    {
        KeyManagerFactory keyManagerFactory = null;
        if (keystorePath != null)
        {
            keyManagerFactory = SSLUtil.initKeyManagerFactory(keystorePath, "JKS", "secret", "secret");
        }
        TrustManagerFactory trustManagerFactory = SSLUtil.initTrustManagerFactory(truststorePath, "JKS", "secret");
        return SSLUtil.initSSLContext(trustManagerFactory, keyManagerFactory, "TLS");
    }
}
