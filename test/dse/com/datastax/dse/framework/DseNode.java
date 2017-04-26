/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.mobilityrpc.network.ConnectionId;
import com.googlecode.mobilityrpc.quickstart.EmbeddedMobilityServer;
import com.googlecode.mobilityrpc.session.MobilitySession;
import jboss.byteman.test.ng.InjectorHelper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SnitchProperties;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static com.datastax.dse.framework.DseTestRunner.native_port;

public class DseNode
{

    public static final String START_MAX_RETRIES = "dse.embedded_container_startup_max_retries";
    public final static String NODE_ID_PROP = "nodeid";

    private static final Logger logger = LoggerFactory.getLogger(DseNode.class);

    private static final long START_TIMEOUT_SECONDS = TimeUnit.MINUTES.toSeconds(5);

    private String name;
    public final String instanceId;
    private String classpath;
    private volatile String address;
    private String cassandraConfigFileName;
    private String rack;
    private String datacenterName;

    private String rackDcFileName;

    private CassandraYamlBuilder cassandraYamlBuilder;

    private String truststorePath;
    private String keystorePath;

    private Class<? extends Injector> bytemanBootstrapInjectorClass;

    private JMXConnector jmxConnector;

    private volatile boolean started;
    private volatile boolean failed;

    private DseTestRunner.Authorization authorization;
    private DseTestRunner.Authentication authentication;

    private final static Class<?> mainClass = CassandraTestingDaemon.class;

    private int id;
    private int jmxPort;

    private Process nodeProcess;

    private DseTestRunner.ThreadLocalNativeClient nativeClient;

    private MobilitySession mobilitySession;

    private final Map<String, String> defaultSystemProperties = ImmutableMap.<String, String>builder()
                                                                .put("cassandra.ring_delay_ms", "3000")
                                                                .put("cassandra.unsafetruncate", "true")
                                                                .put("cassandra-foreground", "true")
                                                                .put("logback.configurationFile", System.getProperty("storage-config") + "/logback-junit-node.xml")
                                                                .put("cassandra.skip_sync", "true")
                                                                .put("cassandra.test.flush_local_schema_changes", "true")
                                                                .put("io.netty.leakDetectionLevel", "paranoid")
                                                                .put("cassandra.migration_task_wait_in_seconds", "10")
                                                                .put("io.netty.allocator.numDirectArenas", "4")
                                                                .put("java.net.preferIPv4Stack", "true")
                                                                .put("system_memory_in_mb", "1024")
                                                                .build();

    private final static Set<String> seenAddresses = new HashSet<>();
    private final static Set<String> seenConfigurations = new HashSet<>();


    private final List<String> defaultArgs = Lists.newArrayList(
    "-ea",
    "-XX:+CMSClassUnloadingEnabled",
    "-XX:ReservedCodeCacheSize=256m",
    "-Xdebug",
    "-Xnoagent",
    "-javaagent:lib/jamm-0.3.0.jar",
    "-Xmx2g"
    );

    private final List<String> inheritedSystemProperties = Lists.newArrayList(
    "storage-config",
    "access.properties",
    "log4j.configuration",
    "java.compiler",
    "org.xerial.snappy.tempdir",
    "tests.cleanthreads",
    "cassandra.triggers_dir",
    "user.uid",
    "java.library.path",
    "cassandra.config.loader",
    "cassandra.custom_tracing_class",
    "system_cpu_cores",
    "system_memory_in_mb",
    // security props
    "cassandra.superuser_setup_delay_ms",
    "java.security.auth.login.config",
    "ssl.truststore",
    "ssl.truststore.password",
    "javax.net.ssl.trustStore",
    "javax.net.ssl.trustStorePassword"
    );

    public final Map<String, String> additionalSystemProperties = new HashMap<>();

    public final Map<String, String> additionalEnvVariables = new HashMap<>();

    public DseNode(String instanceId)
    {
        // add sigar to java lib path before it's being used
        String javaLibraryPath = "lib/sigar-bin:" + System.getProperty("java.library.path");
        System.setProperty("java.library.path", javaLibraryPath);

        // to enable logging and force initialization BEFORE WE START ANY NODE <- very important, do not remove, because
        // some tests tend to break Sigar in a weird way so that it cannot load natives after test execution
        Util.sigar.get().enableLogging(true);
        this.name = instanceId;
        this.instanceId = instanceId;

        this.classpath = MoreObjects.firstNonNull(System.getenv("DSE_NODE_CLASSPATH"), System.getProperty("java.class.path"));
    }

    public DseNode setSystemProperty(String name, String value)
    {
        additionalSystemProperties.put(name, value);
        return this;
    }

    public DseNode setEnvVariable(String name, String value)
    {
        additionalEnvVariables.put(name, value);
        return this;
    }

    public void stop(int signal) throws Exception
    {
        long start = System.currentTimeMillis();

        if (nativeClient != null)
        {
            nativeClient.close();
        }

        if (started || failed)
        {
            if (jmxConnector != null) jmxConnector.close();
            if (nodeProcess != null)
            {
                logger.info("----------------================== STOPPING NODE {} with signal {} ==================----------------", id, signal);
                int pid = Util.getProcessID(nodeProcess);
                if (signal == 9)
                {
                    // in case we send sigkill, the parent process will not destroy child processes, so we need to
                    // force kill the whole tree of processes
                    Util.killProcessTree(pid, signal); // kill the process along with all of its child processes
                }
                else
                {
                    Util.kill(pid, signal);
                }
                nodeProcess.destroy(); // just in case Sigar fails
                nodeProcess.waitFor();
                logger.info("Stopped in {} ms. Exit code of node {} (pid {}) is {}", (System.currentTimeMillis() - start), id, pid, nodeProcess.exitValue());
                nodeProcess = null;
                started = false;
                failed = false;
            }
        }
    }

    /**
     * stops the node
     *
     * @throws Exception if any error occurs
     */
    public void stop() throws Exception
    {
        stop(15);
    }

    /**
     * stops the node - not gracefully
     *
     * @throws Exception if any error occurs
     */
    public void kill() throws Exception
    {
        stop(9);
    }

    public MobilitySession getMobilitySession()
    {
        return mobilitySession;
    }

    /**
     * Submits the code for execution inside node JVM
     *
     * @param callableClass class containing code that should be executed
     * @param args          constructor argiments
     * @param <T>           return type
     * @return value returned by {@link Callable#call()}
     * @see MobilitySession#execute(ConnectionId, Callable)
     * @deprecated Use callAsync or runAsync. Prefer using lambdas for remote calls.
     */
    public <T> Future<T> submit(final Class<? extends Callable<T>> callableClass, final Object... args)
    {
        if (mobilitySession == null)
        {
            throw new RuntimeException("mobilitySession is null! did the node start?");
        }
        return Futures.immediateFuture(mobilitySession.execute(address, DseTestRunner.TEST_TIMEOUT_MILLIS, new RemoteCallable<T>(args, callableClass.getName())));
    }


    /**
     * Submits the code for execution inside node JVM
     *
     * @param runnable callable instance that is about to be executed by calling its {@link Runnable#run()} method
     * @return value returned by {@link Callable#call()}
     */
    public void run(DseTestRunner.SerializableRunnableWithException runnable)
    {
        try
        {
            runAsync(runnable).get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Submits the code for execution inside node JVM
     *
     * @param callable callable instance that is about to be executed by calling its {@link Callable#call()} method
     * @return value returned by {@link Callable#call()}
     */
    public <T extends Serializable> T call(DseTestRunner.SerializableCallable<T> callable)
    {
        try
        {
            return callAsync(callable).get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }


    /**
     * Submits the code for execution inside node JVM. This method is present for potential future async support.
     * Currently however
     * it only supports synchronous operation.
     *
     * @param runnable callable instance that is about to be executed by calling its {@link Runnable#run()} method
     * @return A future of null.
     */
    protected CompletableFuture<Void> runAsync(DseTestRunner.SerializableRunnableWithException runnable)
    {
        return (CompletableFuture) callAsync(() ->
                                             {
                                                 runnable.run();
                                                 return null;
                                             });
    }

    /**
     * Submits the code for execution inside node JVM. This method is present for potential future async support.
     * Currently however
     * it only supports synchronous operation.
     *
     * @param callable callable instance that is about to be executed by calling its {@link Callable#call()} method
     * @param <T>      return type
     * @return value returned by {@link Callable#call()}
     */
    protected <T extends Serializable> CompletableFuture<T> callAsync(DseTestRunner.SerializableCallable<T> callable)
    {
        String nodeId = System.getProperty(DseNode.NODE_ID_PROP);
        if (nodeId != null && nodeId.equals("" + id))
        {
            return CompletableFuture.supplyAsync(() ->
                                                 {
                                                     try
                                                     {
                                                         return callable.call();
                                                     }
                                                     catch (Exception e)
                                                     {
                                                         Throwables.propagate(e);
                                                     }
                                                     return (T) null;
                                                 });
        }
        else
        {
            if (mobilitySession == null)
            {
                throw new RuntimeException("mobilitySession is null! did the node start?");
            }
            return CompletableFuture.completedFuture(mobilitySession.execute(address, DseTestRunner.TEST_TIMEOUT_MILLIS, callable));
        }
    }


    /**
     * Inject Byteman rules into node's JVM
     *
     * @param injector byteman rules
     */
    public void inject(final Class<? extends Injector> injector)
    {
        if (!isRunning()) return;
        try
        {
            Injector instance = injector.getConstructor(String.class).newInstance(address);
            instance.inject();
            enableInjectedBytemanRule(instance.getRules().keySet().toArray(new String[0]));
        }
        catch (Exception e)
        {
            if (isRunning()) throw new RuntimeException(e);
        }
    }

    /**
     * Byteman rules are injected into JVM in disabled state, calling this method enables the given set of rules
     *
     * @param ruleIds ids of rules that should be enabled
     */
    public void enableInjectedBytemanRule(final String... ruleIds)
    {
        try
        {
            call(new BytemanRulesStateChanger(ruleIds, true));
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    /**
     * Disable set of injected Byteman rules
     *
     * @param ruleIds rule ids
     */
    public void disableInjectedBytemanRule(final String... ruleIds)
    {
        try
        {
            call(new BytemanRulesStateChanger(ruleIds, false));
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    private boolean isProcessAlive()
    {
        try
        {
            nodeProcess.exitValue();
            return false;
        }
        catch (Exception e)
        {
            return true;
        }
    }

    private void waitForActivation(int port, String truststorePath, String keystorePath)
    {
        int retries = 0;
        long maxRetries = Long.getLong(START_MAX_RETRIES, START_TIMEOUT_SECONDS);
        boolean active = false;
        String startupErrorMessage = "";
        try
        {
            while (retries < maxRetries)
            {
                if (!isProcessAlive())
                {
                    throw new RuntimeException(String.format("Node %d process is dead", id));
                }

                Socket socket = null;
                try
                {
                    // If the keystore path is not null then the server has encrypted client comms
                    if (truststorePath != null)
                    {
                        SSLContext ctx = DseTestRunner.initSSLContext(truststorePath, keystorePath);
                        socket = ctx.getSocketFactory().createSocket(address, port);
                        // We need to start the handshake here otherwise the server will throw
                        // an exception when we close the socket
                        ((SSLSocket) socket).startHandshake();
                    }
                    else
                    {
                        socket = new Socket(address, port);
                    }
                    active = true;
                    break;
                }
                catch (Exception e)
                {
                    startupErrorMessage = e.getMessage();
                    Thread.sleep(1000);
                    retries++;
                    continue;
                }
                finally
                {
                    if (socket != null)
                    {
                        socket.close();
                    }
                }
            }
        }
        catch (RuntimeException | Error ex)
        {
            failed = true;
            throw ex;
        }
        catch (Throwable ex)
        {
            failed = true;
            throw new RuntimeException(ex.getMessage(), ex);
        }
        if (!active)
        {
            failed = true;
            throw new RuntimeException("Node " + address + ":" + port + " not detected as active within " + maxRetries + "s! Error message: " + startupErrorMessage);
        }
    }

    /**
     * Pause a background runnable task and return a handle to it.
     * The caller is responsible of resuming the task when done.
     *
     * @param runnable   a background task, presumably periodically executed by some scheduler.
     * @param methodName the name of the no-argument method to step through. Make sure there is only one method by that
     *                   name.
     * @return a paused task to be used for stepping.
     * @throws ExecutionException   if the task could not be paused
     * @throws InterruptedException when the setup process was interrupted
     */
    public PauseController getPauseController(Class<? extends Runnable> runnable, String methodName)
    throws ExecutionException, InterruptedException
    {
        String className = runnable.getName();
        try
        {
            if (Class.forName(className).getMethod(methodName) != null)
            {
                PauseController result = new PauseController(address);

                // Hooked method will wait at both start and finish barriers
                submit(PauseController.Setup.class, result.getHost(), result.getId(), className, methodName).get();

                return result;
            }
        }
        catch (NoSuchMethodException | ClassNotFoundException ignored)
        {
            // fall through
        }

        // Provide some feedback when a previously hooked method was removed by a new ticket.
        throw new AssertionError(className + '.' + methodName + "() not found");
    }

    /**
     * @return mbean connection for this node
     * @throws Exception
     */
    public MBeanServerConnection getMBeanServerConnection() throws Exception
    {
        String url = "service:jmx:rmi:///jndi/rmi://" + "127.0.0.1" + ":" + jmxPort + "/jmxrmi";
        jmxConnector = JMXConnectorFactory.connect(new JMXServiceURL(url), null);
        return jmxConnector.getMBeanServerConnection();
    }


    /**
     * Start the node
     *
     * @throws Exception if any exception occurs
     */
    public void start() throws Exception
    {
        if (started)
        {
            logger.info("Node {} is already running, do not start it again", id);
            return;
        }

        launch();
        waitUntilReady();

        seenAddresses.add(address);
        seenConfigurations.add(cassandraConfigFileName);
        updateReplicationFactors();
    }

    private String findBytemanJarPath()
    {
        String path = Iterables.find(Arrays.asList(classpath.split(File.pathSeparator)), Predicates.containsPattern(".*byteman-[\\d\\.]+\\.jar"), null);
        if (null == path)
        {
            throw new RuntimeException("byteman jar must be present on the classpath, otherwise the JVM won't be able to start up!");
        }
        return path;
    }


    /**
     * Launches this node
     *
     * @throws Exception
     */
    private void launch() throws Exception
    {
        Map<String, String> systemProperties = Maps.newHashMap(defaultSystemProperties);
        Map<String, String> envVariables = Maps.newHashMap(System.getenv());

        for (String propName : inheritedSystemProperties)
        {
            String value = System.getProperty(propName);
            if (value != null)
            {
                systemProperties.put(propName, value);
            }
        }

        // we need to set the YamlConfigurationLoader for the Node's JVM
        // because the Junit JVM uses OffsetAwareConfigurationLoader and things will fail
        systemProperties.put("cassandra.config.loader", YamlConfigurationLoader.class.getName());

        systemProperties.put("cassandra.config", cassandraConfigFileName);
        if (rack != null && datacenterName != null)
        {
            rackDcFileName = generateRackDcConfig();
            systemProperties.put(SnitchProperties.RACKDC_PROPERTY_FILENAME, rackDcFileName);
        }

        String[] octets = address.split("\\.");

        if (id == 0)
        {
            id = Integer.parseInt(octets[3]);
        }

        if (id == 1)
        {
            // We have a number of tests that need to parse dse.yaml/cassandra.yaml files
            // and then to connect to the node and perform something.
            // Thus we need to provide their location to test worker JVM as well and reload the configs
            // in the test worker JVM.
            System.setProperty("cassandra.config", cassandraConfigFileName);

            // Remove cached config objects
            FBUtilities.getProtectedField(DatabaseDescriptor.class, "conf").set(null, null);
            FBUtilities.getProtectedField(YamlConfigurationLoader.class, "storageConfigURL").set(null, null);

            // Reinitialize cassandra config
            Field toolInitialized = FBUtilities.getProtectedField(DatabaseDescriptor.class, "toolInitialized");
            toolInitialized.setBoolean(null, false);
            Field clientInitialized = FBUtilities.getProtectedField(DatabaseDescriptor.class, "clientInitialized");
            clientInitialized.setBoolean(null, false);
            Field daemonInitialized = FBUtilities.getProtectedField(DatabaseDescriptor.class, "daemonInitialized");
            daemonInitialized.setBoolean(null, false);
            org.apache.cassandra.config.Config.setClientMode(false);

            // need to use the LenientMBeanServerBuilder for the Junit JVM, otherwise
            // DatabaseDescriptor.daemonInitialization() will fail when trying to register an already registered MBean
            System.setProperty("javax.management.builder.initial", LenientMBeanServerBuilder.class.getName());
            DatabaseDescriptor.daemonInitialization();
        }

        systemProperties.put(NODE_ID_PROP, "" + id);

        jmxPort = 7199 + id - 1;

        if (address.equals("127.0.0.1"))
        {
            seenAddresses.clear();
            seenConfigurations.clear();
        }

        systemProperties.put("cassandra.jmx.local.port", "" + jmxPort);
        systemProperties.putAll(additionalSystemProperties);
        envVariables.putAll(additionalEnvVariables);

        List<String> command = Lists.newArrayList(javaCmd());
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.environment().clear();
        pb.environment().putAll(envVariables);

        command.addAll(defaultArgs);

        for (Map.Entry<String, String> prop : systemProperties.entrySet())
        {
            command.add("-D" + prop.getKey() + "=" + prop.getValue());
        }

        // add byteman
        if (bytemanBootstrapInjectorClass == null)
        {
            command.add(String.format("-javaagent:%s=address:%s", findBytemanJarPath(), address));
        }
        else
        {
            File bytemanScriptFile = new File(new File(cassandraYamlBuilder.getRootDir()), cassandraConfigFileName + ".btm");
            Injector bytemanBootstrapInjector = bytemanBootstrapInjectorClass.getConstructor(String.class).newInstance(address);
            String bytemanScript = bytemanBootstrapInjector.script();
            logger.debug("Injecting Byteman script at bootstrap:\n{}\n", bytemanScript);
            FileUtils.write(bytemanScriptFile, bytemanScript, false);
            command.add(String.format("-javaagent:%s=address:%s,script:%s", findBytemanJarPath(), address, bytemanScriptFile.getAbsolutePath()));
        }

        String jacocoAgent = detectJacocoAgentParam();
        if (jacocoAgent != null)
        {
            command.add(jacocoAgent);
        }

        String debugArg = System.getProperty("node" + id + "DebugArg");
        if (debugArg != null)
        {
            command.add(debugArg);
            System.out.println(String.format("Debugging ON for node %d, passed debug arg: %s", id, debugArg));
            if (debugArg.contains("server=y"))
            {
                System.out.println(String.format("Debugger agent listens on %s addresses", Iterables.toString(debuggerAgentAddresses())));
            }
        }
        else
        {
            if (isDebugMode())
            {
                System.out.println(String.format("WARN: You have turned debugging ON for test worker JVM, but NO debug args are provided for node %d", id));
                System.out.println(String.format("Node %d will run in separate JVM. If you want to turn debugging on for the node,", id));
                System.out.println(String.format("please provide the debug args in node%dDebugArg Gradle property", id));
                System.out.println(String.format("on the command line via -Pnode%dDebugArg=DEBUG_ARGS or put it in gradle.properties file", id));
                System.out.println("--------");
            }
        }

        // add classpath
        command.add("-cp");
        command.add(classpath);

        // main class
        command.add(mainClass.getName());


        logger.debug("Invoking {} with env vars {}", StringUtils.join(command, " "), pb.environment());
        logger.info("----------------================== STARTING NODE {} ==================----------------\nwith configuration: \nCassandra: {}\nRackDC:    {}",
                    id, cassandraConfigFileName, rackDcFileName);

        nodeProcess = pb.start();
        logger.info("Node {} PID is {}", id, Util.getProcessID(nodeProcess));

        redirectNodeProcessStreams();
    }


    private void redirectNodeProcessStreams()
    {
        new StreamPrinter(nodeProcess.getInputStream(), System.out, String.format("Node %d stdout redirector", id)).start();
        new StreamPrinter(nodeProcess.getErrorStream(), System.err, String.format("Node %d stderr redirector", id)).start();
    }

    protected String detectJacocoAgentParam()
    {
        return Iterables.find(java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments(), Predicates.containsPattern("jacoco"), null);
    }

    protected boolean isDebugMode()
    {
        return Iterables.find(java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments(), new Predicate<String>()
        {
            @Override
            public boolean apply(@Nullable String input)
            {
                return input.startsWith("-Xrunjdwp") || input.startsWith("-agentlib:jdwp");
            }
        }, null) != null;
    }

    protected Iterable<InetAddress> debuggerAgentAddresses()
    {
        Iterable<InetAddress> addresses = Util.allIpAddresses();
        if (Util.isInsideDockerContainer())
        {
            // do not report loopback addresses when test are running inside docker container
            // because debugger agent cannot be accessed on them from the outside
            addresses = Iterables.filter(addresses, new Predicate<InetAddress>()
            {
                @Override
                public boolean apply(@Nullable InetAddress addr)
                {
                    return !addr.isLoopbackAddress();
                }
            });
        }
        return addresses;
    }

    protected void waitUntilReady() throws Exception
    {
        waitForActivation(native_port, truststorePath, keystorePath);
        waitForActivation(EmbeddedMobilityServer.DEFAULT_PORT, null, null);

        mobilitySession = new DseMobilityControllerImpl().newSession();
        started = Boolean.parseBoolean(submit(StartupCheck.class).get());
        long numOfTries = START_TIMEOUT_SECONDS;
        while (!started && numOfTries > 0)
        {
            Thread.sleep(1000);
            started = Boolean.parseBoolean(submit(StartupCheck.class).get());
            numOfTries--;
        }
        if (!started)
        {
            failed = true;
            throw new IllegalStateException(String.format("Node %d has been not detected as started after %d s", id, START_TIMEOUT_SECONDS));
        }
        else
        {
            logger.info("Node {} ({}) detected as ready", id, address);
        }

        if (authentication == DseTestRunner.Authentication.PASSWORD)
        {
            // need this delay to allow the superuser to be created
            Thread.sleep(2000);
        }
    }


    private String javaCmd()
    {
        String javaBinDir = System.getProperty("java.home") + File.separator + "bin" + File.separator;
        return SystemUtils.IS_OS_WINDOWS ? javaBinDir + "java.exe" : javaBinDir + "java";
    }

    public void setCassandraConfigFileName(String cassandraConfigFileName)
    {
        this.cassandraConfigFileName = cassandraConfigFileName;
    }

    public void setRackDcFileName(String rackDcFileName)
    {
        this.rackDcFileName = rackDcFileName;
    }

    public void setTruststorePath(String truststorePath)
    {
        this.truststorePath = truststorePath;
    }

    public String getAddress()
    {
        return address;
    }

    public void setAddress(String address)
    {
        this.address = address;
    }

    public boolean isStarted()
    {
        return started;
    }

    public boolean isFailed()
    {
        return failed;
    }

    public CassandraYamlBuilder getCassandraYamlBuilder()
    {
        return cassandraYamlBuilder;
    }

    public void setCassandraYamlBuilder(CassandraYamlBuilder cassandraYamlBuilder)
    {
        this.cassandraYamlBuilder = cassandraYamlBuilder;
    }

    /**
     * @return node ID, first node has id=1, second equal to 2, etc.
     */
    public int getId()
    {
        return id;
    }

    public void setId(int id)
    {
        this.id = id;
    }

    public boolean isRunning()
    {
        return nodeProcess == null ? false : isProcessAlive();
    }

    public Process getProcess()
    {
        return nodeProcess;
    }

    public DseTestRunner.Authentication getAuthentication()
    {
        return authentication;
    }

    public void setAuthentication(DseTestRunner.Authentication authentication)
    {
        this.authentication = authentication;
    }


    public DseTestRunner.Authorization getAuthorization()
    {
        return authorization;
    }

    public void setAuthorization(DseTestRunner.Authorization authorization)
    {
        this.authorization = authorization;
    }

    public DseTestRunner.ThreadLocalNativeClient getNativeClient()
    {
        if (null == nativeClient || nativeClient.isClosed())
        {
            nativeClient = new DseTestRunner.ThreadLocalNativeClient(id, address, native_port, truststorePath);
        }
        return nativeClient;
    }

    public String getKeystorePath()
    {
        return keystorePath;
    }

    public void setKeystorePath(String keystorePath)
    {
        this.keystorePath = keystorePath;
    }

    /**
     * Sets Byteman rules that should be injected at node JVM bootstrap time
     *
     * @param injectorClass
     */
    public void setBytemanBootstrapInjector(Class<? extends Injector> injectorClass)
    {
        this.bytemanBootstrapInjectorClass = injectorClass;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getRack()
    {
        return rack;
    }

    public void setRack(String rack)
    {
        this.rack = rack;
    }

    public String getDatacenterName()
    {
        return datacenterName;
    }

    public void setDatacenterName(String datacenterName)
    {
        this.datacenterName = datacenterName;
    }

    /**
     * Returns the PID for the project
     */
    public int getPID()
    {
        return Util.getProcessID(nodeProcess);
    }

    private String generateRackDcConfig() throws IOException
    {
        Properties rackDcProps = new Properties();
        rackDcProps.put("dc", datacenterName);
        rackDcProps.put("rack", rack);

        File rackDcConfigFile = new File(cassandraYamlBuilder.getRootDirPath().toFile(), "cassandra-rackdc.properties");

        try (OutputStream os = FileUtils.openOutputStream(rackDcConfigFile))
        {
            rackDcProps.store(os, "generated by DSE test framework");
        }

        return rackDcConfigFile.toURI().toString();
    }

    private void updateReplicationFactors() throws Exception
    {
        if (StringUtils.isNotBlank(getDatacenterName()))
        {
            submit(ReplicationFactorUpdate.class).get();
        }
    }

    public static class ReplicationFactorUpdate implements Callable<Boolean>
    {
        @Override
        public Boolean call() throws Exception
        {
            String[] keyspaces = new String[]{
            //"dse_security",
            "cfs",
            "cfs_archive",
            "system_auth"
            };
            for (String keyspace : keyspaces)
            {
                KeyspaceMetadata ks = keyspace == null ? null : Schema.instance.getKeyspaceMetadata(keyspace);
                if (ks != null && NetworkTopologyStrategy.class.isAssignableFrom(ks.params.replication.klass))
                {
                    Map<String, String> replicationOptions = ks.params.replication.asMap();
                    if (replicationOptions.putIfAbsent(DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress()), "1") == null)
                    {
                        ReplicationFactorUpdate.maybeAlterKeyspace(ks.withSwapped(KeyspaceParams.create(ks.params.durableWrites, replicationOptions)));
                    }
                }
            }
            return true;
        }

        public static void maybeAlterKeyspace(KeyspaceMetadata ksm)
        {
            try
            {
                MigrationManager.announceKeyspaceUpdate(ksm);
            }
            catch (ConfigurationException e)
            {
                logger.debug(String.format("Keyspace %s doesn't exist", ksm.name));
            }
            catch (Exception e)
            {
                throw new AssertionError(e);
            }
        }
    }

    public static class StartupCheck implements Callable<String>
    {
        private final static Logger log = LoggerFactory.getLogger(StartupCheck.class);

        @SuppressWarnings("unchecked")
        @Override
        public String call() throws Exception
        {
            try
            {
                return String.valueOf(StorageService.instance.isNativeTransportRunning());
            }
            catch (Throwable t)
            {
                log.warn("Error by startup check ", t);
                return String.valueOf(false);
            }
        }
    }

    /**
     * Callable that gets instantiated in remote JVM and its method {@link #call()} get invoked.
     *
     * @param <T>
     */
    static class RemoteCallable<T> implements Callable<T>
    {
        private final Object[] args;
        private final String className;

        /**
         * @param args      constructor arguments of className
         * @param className the name of class that implements {@link Callable} interface
         */
        public RemoteCallable(Object[] args, String className)
        {
            this.args = args;
            this.className = className;
        }

        /**
         * Instantiates the callable, invokes its call method
         *
         * @return the return value of called callable
         * @throws Exception if any exception occurs
         */
        @Override
        public T call() throws Exception
        {
            Class<Callable<T>> callableClass = (Class<Callable<T>>) Thread.currentThread().getContextClassLoader().loadClass(className);
            if (args.length > 0)
            {
                Class<?>[] types = new Class<?>[args.length];
                for (int i = 0; i < args.length; i++) types[i] = args[i].getClass();
                return callableClass.getConstructor(types).newInstance(args).call();
            }
            else
            {
                return callableClass.newInstance().call();
            }
        }
    }

    public static class BytemanRulesStateChanger implements DseTestRunner.SerializableCallable<Boolean>
    {
        private final String[] ruleIds;
        private final boolean newState;

        public BytemanRulesStateChanger(String[] ruleIds, boolean newState)
        {
            this.ruleIds = ruleIds;
            this.newState = newState;
        }

        @Override
        public Boolean call() throws Exception
        {
            for (String ruleId : ruleIds)
            {
                InjectorHelper.setEnabled(ruleId, newState);
            }
            return true;
        }
    }

    class StreamPrinter extends Thread
    {
        private final BufferedReader reader;
        private final PrintStream printer;

        public StreamPrinter(InputStream in, PrintStream printer, String threadName)
        {
            this.reader = new BufferedReader(new InputStreamReader(in));
            this.printer = printer;
            setName(threadName);
        }

        @Override
        public void run()
        {
            String line;
            try
            {
                while ((line = reader.readLine()) != null)
                {
                    printer.println(line);
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }
}
