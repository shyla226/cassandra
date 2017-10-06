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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.concurrent.TimeoutException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.management.remote.JMXConnectorServer;

import com.addthis.metrics3.reporter.config.ReporterConfig;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.functions.ThreadAwareSecurityManager;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.schema.TriggerMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.*;

/**
 * The <code>CassandraDaemon</code> is an abstraction for a Cassandra daemon
 * service, which defines not only a way to activate and deactivate it, but also
 * hooks into its lifecycle methods (see {@link #setup()}, {@link #start()},
 * {@link #stop()} and {@link #setup()}).
 */
public class CassandraDaemon
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=NativeAccess";

    private static final Logger logger;
    static
    {
        LineNumberInference.init();

        // Need to register metrics before instrumented appender is created(first access to LoggerFactory).
        SharedMetricRegistries.getOrCreate("logback-metrics").addListener(new MetricRegistryListener.Base()
        {
            @Override
            public void onMeterAdded(String metricName, Meter meter)
            {
                // Given metricName consists of appender name in logback.xml + "." + metric name.
                // We first separate appender name
                int separator = metricName.lastIndexOf('.');
                String appenderName = metricName.substring(0, separator);
                String metric = metricName.substring(separator + 1); // remove "."
                ObjectName name = DefaultNameFactory.createMetricName(appenderName, metric, null).getMBeanName();
                CassandraMetricsRegistry.Metrics.registerMBean(meter, name);
            }
        });
        logger = LoggerFactory.getLogger(CassandraDaemon.class);
    }

    static public BiConsumer<Thread, Throwable> defaultExceptionHandler = new BiConsumer<Thread, Throwable>()
    {
        public void accept(Thread t, Throwable e)
        {
            if (e instanceof RequestExecutionException ||
                (e.getCause() != null && e.getCause() instanceof RequestExecutionException))
            {
                // Completable.merge() or .concat() (with prefetch > 0) will pass to the subscriber
                // only the exception generated by the first observable to complete, the remaining
                // exceptions will end up here. Likewise a future completing after an observable linked to it was
                // disposed, will cause an unhandled exception (see WriteHandler.toObservable()).
                // We should be able to remove this special handling once all the Completable.merge()
                // and WriteHandler.toObservable() calls have been removed from StorageProxy.
                // We basically want to remove rx-java from the read and write path and use completable
                // futures instead, see APOLLO-1038.
                logger.debug("Got duplicated request execution exception: {}", e.getMessage());
                return;
            }

            StorageMetrics.uncaughtExceptions.inc();
            logger.error("Exception in thread " + t, e);
            Tracing.trace("Exception in thread {}", t, e);

            JVMStabilityInspector.inspectThrowable(e);
        }
    };

    private void maybeInitJmx()
    {
        System.setProperty("java.rmi.server.randomIDs", "true");

        // If a remote port has been specified then use that to set up a JMX
        // connector server which can be accessed remotely. Otherwise, create
        // a server which is bound only to the loopback address. Auth options
        // are applied to both remote and local-only servers, but currently SSL
        // is only available for remote.
        // If neither is remote nor local port is set in cassandra-env.(sh|ps)
        // then JMX is effectively disabled.
        DatabaseDescriptor.getJMXPort()
                          .ifPresent(port ->
                                     {
                                         boolean localOnly = DatabaseDescriptor.isJMXLocalOnly();
                                         try
                                         {
                                             jmxServer = JMXServerUtils.createJMXServer(port, localOnly);
                                         }
                                         catch (IOException e)
                                         {
                                             exitOrFail(StartupException.ERR_WRONG_MACHINE_STATE,
                                                        e.getMessage(),
                                                        e.getCause());
                                         }
                                     });
    }

    static final CassandraDaemon instance = new CassandraDaemon();

    @VisibleForTesting
    NativeTransportService nativeTransportService;
    private JMXConnectorServer jmxServer;

    private final boolean runManaged;
    protected final StartupChecks startupChecks;
    private boolean setupCompleted;
    private final CountDownLatch isRunning = new CountDownLatch(1);

    public CassandraDaemon()
    {
        this(false);
    }

    public CassandraDaemon(boolean runManaged)
    {
        this.runManaged = runManaged;
        this.startupChecks = new StartupChecks().withDefaultTests();
        this.setupCompleted = false;
    }

    /**
     * Initialize the thread-per-core workers.
     */
    public void initializeTPC()
    {
        TPC.ensureInitialized();
    }

    /**
     * This is a hook for concrete daemons to initialize themselves suitably.
     *
     * Subclasses should override this to finish the job (listening on ports, etc.)
     */
    protected void setup()
    {
        FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());

        // Delete any failed snapshot deletions on Windows - see CASSANDRA-9658
        if (FBUtilities.isWindows)
            WindowsFailedSnapshotTracker.deleteOldSnapshots();

        maybeInitJmx();

        Mx4jTool.maybeLoad();

        ThreadAwareSecurityManager.install();

        logSystemInfo();

        NativeLibrary.tryMlockall();

        try
        {
            startupChecks.verify();
        }
        catch (StartupException e)
        {
            exitOrFail(e.returnCode, e.getMessage(), e.getCause());
        }

        TPCUtils.blockingAwait(SystemKeyspace.snapshotOnVersionChange()
                                                 .whenComplete((v, e) -> {
                                                     if (e != null)
                                                     {
                                                         if (e instanceof CompletionException && e.getCause() != null)
                                                             e = e.getCause();
                                                         if (e instanceof IOException)
                                                            exitOrFail(3, e.getMessage(), e.getCause());
                                                     }
                                                 }));

        SystemKeyspace.beginStartupBlocking();

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            public void uncaughtException(Thread t, Throwable e)
            {
                defaultExceptionHandler.accept(t, e);
            }
        });

        // Populate token metadata before flushing, for token-aware sstable partitioning (#6696)
        StorageService.instance.populateTokenMetadata();

        // load schema from disk
        Schema.instance.loadFromDisk();

        // clean up debris in the rest of the keyspaces and pre-load triggers to catch any load errors
        for (String keyspaceName : Schema.instance.getKeyspaces())
        {
            // Skip system as we've already cleaned it
            if (keyspaceName.equals(SchemaConstants.SYSTEM_KEYSPACE_NAME))
                continue;

            for (TableMetadata cfm : Schema.instance.getTablesAndViews(keyspaceName))
            {
                // Pre-load triggers to make sure they can be loaded correctly
                for (TriggerMetadata trigger : cfm.triggers)
                {
                    try
                    {
                        TriggerExecutor.instance.loadTriggerInstance(trigger.classOption);
                    }
                    catch(Throwable t)
                    {
                        exitOrFail(StartupException.ERR_WRONG_CONFIG,
                                   String.format("Could not load class '%s' from trigger '%s' from %s.%s. " +
                                                 "Cannot continue startup. Trigger load error stack " +
                                                 "trace: %s", trigger.classOption, trigger.name,
                                                 cfm.keyspace, cfm.name, ExceptionUtils.getStackTrace(t)),
                                   t.getCause());
                    }
                }
                try
                {
                    ColumnFamilyStore.scrubDataDirectories(cfm);
                }
                catch (StartupException e)
                {
                    exitOrFail(e.returnCode, e.getMessage(), e.getCause());
                }
            }
        }

        Keyspace.setInitialized();

        // initialize keyspaces
        for (String keyspaceName : Schema.instance.getKeyspaces())
        {
            if (logger.isDebugEnabled())
                logger.debug("opening keyspace {}", keyspaceName);
            // disable auto compaction until commit log replay ends
            for (ColumnFamilyStore cfs : Keyspace.open(keyspaceName).getColumnFamilyStores())
            {
                for (ColumnFamilyStore store : cfs.concatWithIndexes())
                {
                    store.disableAutoCompaction();
                }
            }
        }


        try
        {
            loadRowAndKeyCacheAsync().get();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn("Error loading key or row cache", t);
        }

        try
        {
            GCInspector.register();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn("Unable to start GCInspector (currently only supported on the Sun JVM)");
        }

        // Replay any CommitLogSegments found on disk
        try
        {
            CommitLog.instance.recoverSegmentsOnDisk();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        // Finish startup
        SystemKeyspace.finishStartupBlocking();

        // Re-populate token metadata after commit log recover (new peers might be loaded onto system keyspace #10293)
        StorageService.instance.populateTokenMetadata();

        // Enable active repair service: this *must* happen prior to enabling compactions (see APOLLO-821)
        ActiveRepairService.instance.start();

        // Enable auto compaction
        for (Keyspace keyspace : Keyspace.all())
        {
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            {
                for (final ColumnFamilyStore store : cfs.concatWithIndexes())
                {
                    if (store.getCompactionStrategyManager().shouldBeEnabled())
                        store.enableAutoCompaction();
                }
            }
        }

        // Prepared statements
        QueryProcessor.preloadPreparedStatementBlocking();

        // Metrics
        String metricsReporterConfigFile = System.getProperty("cassandra.metricsReporterConfigFile");
        if (metricsReporterConfigFile != null)
        {
            logger.info("Trying to load metrics-reporter-config from file: {}", metricsReporterConfigFile);
            try
            {
                // enable metrics provided by metrics-jvm.jar
                CassandraMetricsRegistry.Metrics.register("jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
                CassandraMetricsRegistry.Metrics.register("jvm.gc", new GarbageCollectorMetricSet());
                CassandraMetricsRegistry.Metrics.register("jvm.memory", new MemoryUsageGaugeSet());
                CassandraMetricsRegistry.Metrics.register("jvm.fd.usage", new FileDescriptorRatioGauge());
                // initialize metrics-reporter-config from yaml file
                URL resource = CassandraDaemon.class.getClassLoader().getResource(metricsReporterConfigFile);
                if (resource == null)
                {
                    logger.warn("Failed to load metrics-reporter-config, file does not exist: {}", metricsReporterConfigFile);
                }
                else
                {
                    String reportFileLocation = resource.getFile();
                    ReporterConfig.loadFromFile(reportFileLocation).enableAll(CassandraMetricsRegistry.Metrics);
                }
            }
            catch (Exception e)
            {
                logger.warn("Failed to load metrics-reporter-config, metric sinks will not be activated", e);
            }
        }

        // start server internals
        StorageService.instance.registerDaemon(this);
        try
        {
            StorageService.instance.initServer();
        }
        catch (ConfigurationException e)
        {
            System.err.println(e.getMessage() + "\nFatal configuration error; unable to start server.  See log for stacktrace.");
            exitOrFail(1, "Fatal configuration error", e);
        }

        // Because we are writing to the system_distributed keyspace, this should happen after that is created, which
        // happens in StorageService.instance.initServer()
        Runnable viewRebuild = () -> {
            for (Keyspace keyspace : Keyspace.all())
            {
                keyspace.viewManager.buildAllViews();
            }
            logger.debug("Completed submission of build tasks for any materialized views defined at startup");
        };

        ScheduledExecutors.optionalTasks.schedule(viewRebuild, StorageService.RING_DELAY, TimeUnit.MILLISECONDS);

        Gossiper.waitToSettle("accepting client requests");

        // schedule periodic background compaction task submission. this is simply a backstop against compactions stalling
        // due to scheduling errors or race conditions
        ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(ColumnFamilyStore.getBackgroundCompactionTaskSubmitter(), 5, 1, TimeUnit.MINUTES);

        // schedule periodic dumps of table size estimates into SystemKeyspace.SIZE_ESTIMATES_CF
        // set cassandra.size_recorder_interval to 0 to disable
        int sizeRecorderInterval = Integer.getInteger("cassandra.size_recorder_interval", 5 * 60);
        if (sizeRecorderInterval > 0)
            ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(SizeEstimatesRecorder.instance, 30, sizeRecorderInterval, TimeUnit.SECONDS);

        // Native transport
        nativeTransportService = new NativeTransportService();

        // During initialization memtables do not know the correct token boundaries to use.
        // Flush all to make sure they get updated and don't get stuck on core 0 (APOLLO-939).
        // There should be no data in the tables at this time, so this should complete very quickly.
        // Wait for it before signalling we are ready.
        FBUtilities.waitOnFutures(Iterables.concat(Iterables.transform(Keyspace.all(),
                                                                       Keyspace::flush)));

        completeSetup();
    }

    /*
     * Asynchronously load the row and key cache in one off threads and return a compound future of the result.
     * Error handling is pushed into the cache load since cache loads are allowed to fail and are handled by logging.
     */
    private ListenableFuture<?> loadRowAndKeyCacheAsync()
    {
        final ListenableFuture<Integer> keyCacheLoad = CacheService.instance.keyCache.loadSavedAsync();

        final ListenableFuture<Integer> rowCacheLoad = CacheService.instance.rowCache.loadSavedAsync();

        @SuppressWarnings("unchecked")
        ListenableFuture<List<Integer>> retval = Futures.successfulAsList(keyCacheLoad, rowCacheLoad);

        return retval;
    }

    @VisibleForTesting
    public void completeSetup()
    {
        setupCompleted = true;
    }

    public boolean setupCompleted()
    {
        return setupCompleted;
    }

    private void logSystemInfo()
    {
    	if (logger.isInfoEnabled())
    	{
	        try
	        {
	            logger.info("Hostname: {}", InetAddress.getLocalHost().getHostName());
	        }
	        catch (UnknownHostException e1)
	        {
	            logger.info("Could not resolve local host");
	        }

	        logger.info("JVM vendor/version: {}/{}", System.getProperty("java.vm.name"), System.getProperty("java.version"));
	        logger.info("Heap size: {}/{}",
                        FBUtilities.prettyPrintMemory(Runtime.getRuntime().totalMemory()),
                        FBUtilities.prettyPrintMemory(Runtime.getRuntime().maxMemory()));

	        for(MemoryPoolMXBean pool: ManagementFactory.getMemoryPoolMXBeans())
	            logger.info("{} {}: {}", pool.getName(), pool.getType(), pool.getPeakUsage());

	        logger.info("Classpath: {}", System.getProperty("java.class.path"));

            logger.info("JVM Arguments: {}", ManagementFactory.getRuntimeMXBean().getInputArguments());
    	}
    }

    /**
     * Initialize the Cassandra Daemon based on the given <a
     * href="http://commons.apache.org/daemon/jsvc.html">Commons
     * Daemon</a>-specific arguments. To clarify, this is a hook for JSVC.
     *
     * @param arguments
     *            the arguments passed in from JSVC
     * @throws IOException
     */
    public void init(String[] arguments) throws IOException
    {
        setup();
    }

    /**
     * Start the Cassandra Daemon, assuming that it has already been
     * initialized via {@link #init(String[])}
     *
     * Hook for JSVC
     */
    public void start()
    {
        String nativeFlag = System.getProperty("cassandra.start_native_transport");
        if ((nativeFlag != null && Boolean.parseBoolean(nativeFlag)) || (nativeFlag == null && DatabaseDescriptor.startNativeTransport()))
        {
            startNativeTransport();
            StorageService.instance.setNativeTransportReady(true);
        }
        else
            logger.info("Not starting native transport as requested. Use JMX (StorageService->startNativeTransport()) or nodetool (enablebinary) to start it");

        if (DatabaseDescriptor.getNodeSyncConfig().isEnabled())
        {
            try
            {
                StorageService.instance.nodeSyncService.enable();
            }
            catch (Exception e)
            {
                logger.error("Unexpected error starting the NodeSync service. No tables will be validated by NodeSync.", e);
            }
        }
    }

    /**
     * Stop the daemon, ideally in an idempotent manner.
     *
     * Hook for JSVC / Procrun
     */
    public void stop()
    {
        // On linux, this doesn't entirely shut down Cassandra, just the RPC server.
        // jsvc takes care of taking the rest down
        logger.info("Cassandra shutting down...");
        if (nativeTransportService != null)
            nativeTransportService.destroy();

        StorageService.instance.setNativeTransportReady(false);

        // On windows, we need to stop the entire system as prunsrv doesn't have the jsvc hooks
        // We rely on the shutdown hook to drain the node
        if (FBUtilities.isWindows)
            System.exit(0);

        if (jmxServer != null)
        {
            try
            {
                jmxServer.stop();
            }
            catch (IOException e)
            {
                logger.error("Error shutting down local JMX server: ", e);
            }
        }

        try
        {
            StorageService.instance.nodeSyncService.disable(false, 2, TimeUnit.MINUTES);
        }
        catch (TimeoutException e)
        {
            logger.error("Timed-out (after 2 minutes) while waiting on the NodeSync service to stop");
        }
    }


    /**
     * Clean up all resources obtained during the lifetime of the daemon. This
     * is a hook for JSVC.
     */
    public void destroy()
    {}

    /**
     * A convenience method to initialize and start the daemon in one shot.
     */
    public void activate(boolean wait)
    {
        // Do not put any references to DatabaseDescriptor above the forceStaticInitialization call.
        try
        {
            applyConfig();

            initializeTPC();

            try
            {
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                mbs.registerMBean(new StandardMBean(new NativeAccess(), NativeAccessMBean.class), new ObjectName(MBEAN_NAME));
            }
            catch (Exception e)
            {
                logger.error("error registering MBean {}", MBEAN_NAME, e);
                //Allow the server to start even if the bean can't be registered
            }

            if (FBUtilities.isWindows)
            {
                // We need to adjust the system timer on windows from the default 15ms down to the minimum of 1ms as this
                // impacts timer intervals, thread scheduling, driver interrupts, etc.
                WindowsTimer.startTimerPeriod(DatabaseDescriptor.getWindowsTimerInterval());
            }

            setup();

            String pidFile = System.getProperty("cassandra-pidfile");

            if (pidFile != null)
            {
                new File(pidFile).deleteOnExit();
            }

            if (System.getProperty("cassandra-foreground") == null)
            {
                System.out.close();
                System.err.close();
            }

            start();

            if (wait)
            {
                // as long as the process is running, keep the main thread around so we don't exist prematurely, e.g. on DRAIN
                Uninterruptibles.awaitUninterruptibly(isRunning);
            }
        }
        catch (Throwable e)
        {
            boolean logStackTrace =
                    e instanceof ConfigurationException ? ((ConfigurationException)e).logStackTrace : true;

            System.out.println("Exception (" + e.getClass().getName() + ") encountered during startup: " + e.getMessage());

            if (logStackTrace)
            {
                if (runManaged)
                    logger.error("Exception encountered during startup", e);
                // try to warn user on stdout too, if we haven't already detached
                e.printStackTrace();
                exitOrFail(3, "Exception encountered during startup", e);
            }
            else
            {
                if (runManaged)
                    logger.error("Exception encountered during startup: {}", e.getMessage());
                // try to warn user on stdout too, if we haven't already detached
                System.err.println(e.getMessage());
                exitOrFail(3, "Exception encountered during startup: " + e.getMessage());
            }
        }
    }

    public void applyConfig()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    public void startNativeTransport()
    {
        if (nativeTransportService == null)
            throw new IllegalStateException("setup() must be called first for CassandraDaemon");
        else
            nativeTransportService.start();
    }

    public void stopNativeTransport()
    {
        if (nativeTransportService != null)
            nativeTransportService.stop();
    }

    public CompletableFuture stopNativeTransportAsync()
    {
        if (nativeTransportService != null)
            return nativeTransportService.stopAsync();

        return CompletableFuture.completedFuture(null);
    }

    public boolean isNativeTransportRunning()
    {
        return nativeTransportService != null ? nativeTransportService.isRunning() : false;
    }


    /**
     * A convenience method to stop and destroy the daemon in one shot.
     */
    public void deactivate()
    {
        stop();
        destroy();
        // completely shut down cassandra
        isRunning.countDown();
        if(!runManaged)
        {
            System.exit(0);
        }
    }

    public static void stop(String[] args)
    {
        instance.deactivate();
    }

    public static void main(String[] args)
    {
        instance.activate(true);
    }

    public static void startForDseTesting()
    {
        instance.activate(false);
    }

    private void exitOrFail(int code, String message)
    {
        exitOrFail(code, message, null);
    }

    private void exitOrFail(int code, String message, Throwable cause)
    {
        if (runManaged)
        {
            RuntimeException t = cause!=null ? new RuntimeException(message, cause) : new RuntimeException(message);
            throw t;
        }
        else
        {
            logger.error(message, cause);
            System.exit(code);
        }
    }

    static class NativeAccess implements NativeAccessMBean
    {
        public boolean isAvailable()
        {
            return NativeLibrary.isAvailable();
        }

        public boolean isMemoryLockable()
        {
            return NativeLibrary.jnaMemoryLockable();
        }
    }

    public interface Server
    {
        /**
         * Start the server.
         * This method shoud be able to restart a server stopped through stop().
         * Should throw a RuntimeException if the server cannot be started
         */
        public void start();

        /**
         * Stop the server.
         * This method should be able to stop server started through start().
         * Should throw a RuntimeException if the server cannot be stopped
         */
        public CompletableFuture stop();

        /**
         * Returns whether the server is currently running.
         */
        public boolean isRunning();
    }
}
