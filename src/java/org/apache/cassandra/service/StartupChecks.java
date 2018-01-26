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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.*;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.bdp.db.util.CGroups;
import io.netty.channel.unix.FileDescriptor;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.JavaUtils;
import org.apache.cassandra.utils.SigarLibrary;

/**
 * Verifies that the system and environment is in a fit state to be started.
 * Used in CassandraDaemon#setup() to check various settings and invariants.
 *
 * Each individual test is modelled as an implementation of StartupCheck, these are run
 * at the start of CassandraDaemon#setup() before any local state is mutated. The default
 * checks are a mix of informational tests (inspectJvmOptions), initialization
 * (initSigarLibrary, checkCacheServiceInitialization) and invariant checking
 * (checkValidLaunchDate, checkSystemKeyspaceState, checkSSTablesFormat).
 *
 * In addition, if checkSystemKeyspaceState determines that the release version has
 * changed since last startup (i.e. the node has been upgraded) it snapshots the system
 * keyspace to make it easier to back out if necessary.
 *
 * If any check reports a failure, then the setup method exits with an error (after
 * logging any output from the tests). If all tests report success, setup can continue.
 * We should be careful in future to ensure anything which mutates local state (such as
 * writing new sstables etc) only happens after we've verified the initial setup.
 */
public class StartupChecks
{
    // List of checks to run before starting up. If any test reports failure, startup will be halted.
    private final List<StartupCheck> preFlightChecks = new ArrayList<>();

    // The default set of pre-flight checks to run. Order is somewhat significant in that we probably
    // always want the system keyspace check run last, as this actually loads the schema for that
    // keyspace. All other checks should not require any schema initialization.
    private final List<StartupCheck> DEFAULT_TESTS = ImmutableList.of(checkJemalloc,
                                                                      checkValidLaunchDate,
                                                                      checkInvalidJmxProperty,
                                                                      inspectJvmOptions,
                                                                      checkNativeLibraryInitialization,
                                                                      initSigarLibrary,
                                                                      checkMaxMapCount,
                                                                      checkDataDirs,
                                                                      checkSSTablesFormat,
                                                                      checkSystemKeyspaceState,
                                                                      checkDatacenter,
                                                                      checkRack,
                                                                      checkLegacyAuthTables,
                                                                      checkObsoleteAuthTables,
                                                                      warnOnUnsupportedPlatform,
                                                                      warnOnLackOfAIO,
                                                                      checkVirtualization,
                                                                      checkClockSource,
                                                                      checkCgroupCpuSets,
                                                                      checkCpu,
                                                                      checkPCID,
                                                                      checkZoneReclaimMode,
                                                                      checkUlimits,
                                                                      checkThpDefrag,
                                                                      checkFilesystems);

    public StartupChecks withDefaultTests()
    {
        preFlightChecks.addAll(DEFAULT_TESTS);
        return this;
    }

    /**
     * Add system test to be run before schema is loaded during startup
     * @param test the system test to include
     */
    public StartupChecks withTest(StartupCheck test)
    {
        preFlightChecks.add(test);
        return this;
    }

    /**
     * Run the configured tests and return a report detailing the results.
     * @throws StartupException if any test determines that the
     * system is not in an valid state to startup
     */
    public void verify() throws StartupException
    {
        Logger logger = LoggerFactory.getLogger(StartupChecks.class);
        for (StartupCheck test : preFlightChecks)
        {
            try
            {
                test.execute(logger);
            }
            catch (StartupException e)
            {
                throw e;
            }
            catch (Exception e)
            {
                logger.warn("Failed to execute a startup check", e);
            }
        }
    }

    public static final StartupCheck checkJemalloc = new StartupCheck()
    {
        public void execute(Logger logger)
        {
            if (FBUtilities.isWindows)
                return;
            String jemalloc = System.getProperty("cassandra.libjemalloc");
            if (jemalloc == null)
                logger.warn("jemalloc shared library could not be preloaded to speed up memory allocations");
            else if ("-".equals(jemalloc))
                logger.info("jemalloc preload explicitly disabled");
            else
                logger.info("jemalloc seems to be preloaded from {}", jemalloc);
        }
    };

    public static final StartupCheck checkValidLaunchDate = new StartupCheck()
    {
        /**
         * The earliest legit timestamp a casandra instance could have ever launched.
         * Date roughly taken from http://perspectives.mvdirona.com/2008/07/12/FacebookReleasesCassandraAsOpenSource.aspx
         * We use this to ensure the system clock is at least somewhat correct at startup.
         */
        private static final long EARLIEST_LAUNCH_DATE = 1215820800000L;
        public void execute(Logger logger) throws StartupException
        {
            long now = System.currentTimeMillis();
            if (now < EARLIEST_LAUNCH_DATE)
                throw new StartupException(StartupException.ERR_WRONG_MACHINE_STATE,
                                           String.format("current machine time is %s, but that is seemingly incorrect. exiting now.",
                                                         new Date(now).toString()));
        }
    };

    public static final StartupCheck checkInvalidJmxProperty = new StartupCheck()
    {
        public void execute(Logger logger) throws StartupException
        {
            // If the standard com.sun.management.jmxremote.port property has been set
            // then the JVM agent will have already started up a default JMX connector
            // server, so we don't accept this property anymore.
            // See: CASSANDRA-11540 & CASSANDRA-11725 & DB-1040
            if (System.getProperty("com.sun.management.jmxremote.port") != null)
            {
                    throw new StartupException(StartupException.ERR_WRONG_CONFIG,
                                               "The JVM property 'com.sun.management.jmxremote.port' is not allowed. " +
                                               "Please use cassandra.jmx.remote.port instead and refer to cassandra-env.(sh|ps1) " +
                                               "for JMX configuration info.");
            }
        }
    };

    public static final StartupCheck inspectJvmOptions = new StartupCheck()
    {
        public void execute(Logger logger)
        {
            // log warnings for different kinds of sub-optimal JVMs.  tldr use 64-bit Oracle >= 1.6u32
            if (!DatabaseDescriptor.hasLargeAddressSpace())
                logger.warn("32bit JVM detected.  It is recommended to run Cassandra on a 64bit JVM for better performance.");

            String javaVmName = System.getProperty("java.vm.name");
            if (!(javaVmName.contains("HotSpot") || javaVmName.contains("OpenJDK")))
            {
                logger.warn("Non-Oracle JVM detected.  Some features, such as immediate unmap of compacted SSTables, may not work as intended");
            }
            else
            {
                checkOutOfMemoryHandling(logger);
            }
        }

        /**
         * Checks that the JVM is configured to handle OutOfMemoryError
         */
        private void checkOutOfMemoryHandling(Logger logger)
        {
            if (JavaUtils.supportExitOnOutOfMemory(System.getProperty("java.version")))
            {
                if (!jvmOptionsContainsOneOf("-XX:OnOutOfMemoryError=", "-XX:+ExitOnOutOfMemoryError", "-XX:+CrashOnOutOfMemoryError"))
                    logger.warn("The JVM is not configured to stop on OutOfMemoryError which can cause data corruption."
                                + " Use one of the following JVM options to configure the behavior on OutOfMemoryError: "
                                + " -XX:+ExitOnOutOfMemoryError, -XX:+CrashOnOutOfMemoryError, or -XX:OnOutOfMemoryError=\"<cmd args>;<cmd args>\"");
            }
            else
            {
                if (!jvmOptionsContainsOneOf("-XX:OnOutOfMemoryError="))
                    logger.warn("The JVM is not configured to stop on OutOfMemoryError which can cause data corruption."
                            + " Either upgrade your JRE to a version greater or equal to 8u92 and use -XX:+ExitOnOutOfMemoryError/-XX:+CrashOnOutOfMemoryError"
                            + " or use -XX:OnOutOfMemoryError=\"<cmd args>;<cmd args>\" on your current JRE.");
            }
        }

        /**
         * Checks if one of the specified options is being used.
         * @param optionNames The name of the options to check
         * @return {@code true} if one of the specified options is being used, {@code false} otherwise.
         */
        private boolean jvmOptionsContainsOneOf(String... optionNames)
        {
            RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
            List<String> inputArguments = runtimeMxBean.getInputArguments();
            for (String argument : inputArguments)
            {
                for (String optionName : optionNames)
                    if (argument.startsWith(optionName))
                        return true;
            }
            return false;
        }
    };

    public static final StartupCheck checkNativeLibraryInitialization = new StartupCheck()
    {
        public void execute(Logger logger) throws StartupException
        {
            // Fail-fast if the native library could not be linked.
            if (!NativeLibrary.isAvailable())
                throw new StartupException(StartupException.ERR_WRONG_MACHINE_STATE, "The native library could not be initialized properly. ");
        }
    };

    public static final StartupCheck initSigarLibrary = new StartupCheck()
    {
        public void execute(Logger logger)
        {
            SigarLibrary.instance.warnIfRunningInDegradedMode();
        }
    };

    public static final StartupCheck checkMaxMapCount = new StartupCheck()
    {
        private final long EXPECTED_MAX_MAP_COUNT = 1048575;
        private final String MAX_MAP_COUNT_PATH = "/proc/sys/vm/max_map_count";

        private long getMaxMapCount(Logger logger)
        {
            final Path path = Paths.get(MAX_MAP_COUNT_PATH);
            try (final BufferedReader bufferedReader = Files.newBufferedReader(path))
            {
                final String data = bufferedReader.readLine();
                if (data != null)
                {
                    try
                    {
                        return Long.parseLong(data);
                    }
                    catch (final NumberFormatException e)
                    {
                        logger.warn("Unable to parse {}.", path, e);
                    }
                }
            }
            catch (final IOException e)
            {
                logger.warn("IO exception while reading file {}.", path, e);
            }
            return -1;
        }

        public void execute(Logger logger)
        {
            if (!FBUtilities.isLinux)
                return;

            if (DatabaseDescriptor.getDiskAccessMode() == Config.AccessMode.standard &&
                DatabaseDescriptor.getIndexAccessMode() == Config.AccessMode.standard)
                return; // no need to check if disk access mode is only standard and not mmap

            //TPC: We only care about mmap when used with the MemoryOnlyStrategy
            //but we can't detect if MOS is being used till later, so leaving this in place
            //just incase.
            long maxMapCount = getMaxMapCount(logger);
            if (maxMapCount < EXPECTED_MAX_MAP_COUNT)
                logger.warn("Maximum number of memory map areas per process (vm.max_map_count) {} " +
                            "is too low, recommended value: {}, you can change it with sysctl.",
                            maxMapCount, EXPECTED_MAX_MAP_COUNT);
        }
    };

    public static final StartupCheck checkDataDirs = (logger) ->
    {
        // check all directories(data, commitlog, saved cache) for existence and permission
        Iterable<String> dirs = Iterables.concat(Arrays.asList(DatabaseDescriptor.getAllDataFileLocations()),
                                                 Arrays.asList(DatabaseDescriptor.getCommitLogLocation(),
                                                               DatabaseDescriptor.getSavedCachesLocation(),
                                                               DatabaseDescriptor.getHintsDirectory().getAbsolutePath()));
        for (String dataDir : dirs)
        {
            logger.debug("Checking directory {}", dataDir);
            File dir = new File(dataDir);

            // check that directories exist.
            if (!dir.exists())
            {
                logger.warn("Directory {} doesn't exist", dataDir);
                // if they don't, failing their creation, stop cassandra.
                if (!dir.mkdirs())
                    throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                               "Has no permission to create directory "+ dataDir);
            }

            // if directories exist verify their permissions
            if (!Directories.verifyFullPermissions(dir, dataDir))
                throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                           "Insufficient permissions on directory " + dataDir);
        }
    };

    public static final StartupCheck checkSSTablesFormat = new StartupCheck()
    {
        public void execute(Logger logger) throws StartupException
        {
            final Set<String> invalid = new HashSet<>();
            final Set<String> nonSSTablePaths = new HashSet<>();
            nonSSTablePaths.add(FileUtils.getCanonicalPath(DatabaseDescriptor.getCommitLogLocation()));
            nonSSTablePaths.add(FileUtils.getCanonicalPath(DatabaseDescriptor.getSavedCachesLocation()));
            nonSSTablePaths.add(FileUtils.getCanonicalPath(DatabaseDescriptor.getHintsDirectory()));

            FileVisitor<Path> sstableVisitor = new SimpleFileVisitor<Path>()
            {
                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs)
                {
                    File file = path.toFile();
                    if (!Descriptor.isValidFile(file))
                        return FileVisitResult.CONTINUE;

                    try
                    {
                        if (!Descriptor.fromFilename(file).isCompatible())
                            invalid.add(file.toString());
                    }
                    catch (Exception e)
                    {
                        invalid.add(file.toString());
                    }
                    return FileVisitResult.CONTINUE;
                }

                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException
                {
                    String name = dir.getFileName().toString();
                    return (name.equals(Directories.SNAPSHOT_SUBDIR)
                            || name.equals(Directories.BACKUPS_SUBDIR)
                            || nonSSTablePaths.contains(dir.toFile().getCanonicalPath()))
                           ? FileVisitResult.SKIP_SUBTREE
                           : FileVisitResult.CONTINUE;
                }
            };

            for (String dataDir : DatabaseDescriptor.getAllDataFileLocations())
            {
                try
                {
                    Files.walkFileTree(Paths.get(dataDir), sstableVisitor);
                }
                catch (IOException e)
                {
                    throw new StartupException(3, "Unable to verify sstable files on disk", e);
                }
            }

            if (!invalid.isEmpty())
                throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                           String.format("Detected unreadable sstables %s, please check " +
                                                         "NEWS.txt and ensure that you have upgraded through " +
                                                         "all required intermediate versions, running " +
                                                         "upgradesstables",
                                                         Joiner.on(",").join(invalid)));

        }
    };

    public static final StartupCheck checkSystemKeyspaceState = new StartupCheck()
    {
        public void execute(Logger logger) throws StartupException
        {
            // check the system keyspace to keep user from shooting self in foot by changing partitioner, cluster name, etc.
            // we do a one-off scrub of the system keyspace first; we can't load the list of the rest of the keyspaces,
            // until system keyspace is opened.

            for (TableMetadata cfm : Schema.instance.getTablesAndViews(SchemaConstants.SYSTEM_KEYSPACE_NAME))
                ColumnFamilyStore.scrubDataDirectories(cfm);

            try
            {
                TPCUtils.blockingAwait(SystemKeyspace.checkHealth());
            }
            catch (ConfigurationException e)
            {
                throw new StartupException(StartupException.ERR_WRONG_CONFIG, "Fatal exception during initialization", e);
            }
        }
    };

    public static final StartupCheck checkDatacenter = new StartupCheck()
    {
        public void execute(Logger logger) throws StartupException
        {
            if (!Boolean.getBoolean("cassandra.ignore_dc"))
            {
                String storedDc = TPCUtils.blockingGet(SystemKeyspace.getDatacenter());
                if (storedDc != null)
                {
                    String currentDc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
                    if (!storedDc.equals(currentDc))
                    {
                        String formatMessage = "Cannot start node if snitch's data center (%s) differs from previous data center (%s). " +
                                               "Please fix the snitch configuration, decommission and rebootstrap this node or use the flag -Dcassandra.ignore_dc=true.";

                        throw new StartupException(StartupException.ERR_WRONG_CONFIG, String.format(formatMessage, currentDc, storedDc));
                    }
                }
            }
        }
    };

    public static final StartupCheck checkRack = new StartupCheck()
    {
        public void execute(Logger logger) throws StartupException
        {
            if (!Boolean.getBoolean("cassandra.ignore_rack"))
            {
                String storedRack = TPCUtils.blockingGet(SystemKeyspace.getRack());
                if (storedRack != null)
                {
                    String currentRack = DatabaseDescriptor.getLocalRack();
                    if (!storedRack.equals(currentRack))
                    {
                        String formatMessage = "Cannot start node if snitch's rack (%s) differs from previous rack (%s). " +
                                               "Please fix the snitch configuration, decommission and rebootstrap this node or use the flag -Dcassandra.ignore_rack=true.";

                        throw new StartupException(StartupException.ERR_WRONG_CONFIG, String.format(formatMessage, currentRack, storedRack));
                    }
                }
            }
        }
    };

    public static final StartupCheck checkLegacyAuthTables = (logger) ->
    {
        List<String> existingTables = getExistingAuthTablesFrom(SchemaConstants.LEGACY_AUTH_TABLES);
        if (!existingTables.isEmpty())
        {
            String msg = String.format("Legacy auth tables %s in keyspace %s still exist and have not been properly migrated.",
                                       Joiner.on(", ").join(existingTables),
                                       SchemaConstants.AUTH_KEYSPACE_NAME);
            throw new StartupException(StartupException.ERR_WRONG_CONFIG, msg);
        }
    };

    public static final StartupCheck checkObsoleteAuthTables = (logger) ->
    {
        List<String> existingTables = getExistingAuthTablesFrom(SchemaConstants.OBSOLETE_AUTH_TABLES);
        if (!existingTables.isEmpty())
            logger.warn("Auth tables {} in keyspace {} exist but can safely be dropped.",
                        Joiner.on(", ").join(existingTables),
                        SchemaConstants.AUTH_KEYSPACE_NAME);
    };

    private static final StartupCheck warnOnUnsupportedPlatform = (logger) ->
    {
        if (FBUtilities.isLinux)
            return;

        String url = "(see http://docs.datastax.com/en/landing_page/doc/landing_page/supportedPlatforms.html for details)";
        String warning = "this could result in instabilities, degraded performance and/or unsupported features.";

        if (FBUtilities.isWindows)
            logger.warn("Please note that Microsoft Windows is not officially supported by DataStax {}; {}", url, warning);
        else if (FBUtilities.isMacOSX)
            logger.warn("Please note that Mac OS X is only supported by DataStax for development, not production {}", url);
        else
            logger.warn("Please note that you operating system ({}) does not seem to be officially supported by DataStax {}; {}",
                        FBUtilities.OPERATING_SYSTEM, url, warning);
    };

    /**
     * If (true) AIO isn't available/enabled (possibly only on some data directory due to not supporting O_DIRECT),
     * performance is going to be rather bad so we warn the user about it.
     * <p>
     * Note: this check <b>must</b> run after the {@link #checkDataDirs} one as it assumes all data directories exists.
     */
    private static final StartupCheck warnOnLackOfAIO = (logger) ->
    {
        // Only linux is supported in production and we already have warned if this isn't linux in
        // warnOnUnsupportedPlatform. No point in piling on here.
        if (!FBUtilities.isLinux)
            return;

        // Not using EPoll on Linux is sub-optimal 1) it's known to perform better at least for network I/O and 2) this
        // is the event loop we've spend time optimizing/perf testing.
        if (!TPC.USE_EPOLL)
        {
            if (!Boolean.parseBoolean(System.getProperty("cassandra.native.epoll.enabled", "true")))
                logger.warn("EPoll has been manually disabled (through the 'cassandra.native.epoll.enabled' system property). "
                            + "This may result in subpar performance.");
            else
                logger.warn("EPoll doesn't seem to be available: this may result in subpar performance.");
        }
        else if (DatabaseDescriptor.assumeDataDirectoriesOnSSD())
        {
            if (TPC.USE_AIO)
            {
                // Even if (true) AIO is available/enabled on the system, it will only work properly on file-systems that support
                // O_DIRECT (see http://lse.sourceforge.net/io/aio.html). We will properly fall-back to java fake-asynchronous IO
                // when that is the case ({@link AsynchronousChannelProxy#openFileChannel}) but as it means the user may not
                // experience full AIO performance, we're warning him here.
                warnOnDataDirNotSupportingODirect(logger);
            }
            else
            {
                if (!Boolean.parseBoolean(System.getProperty("dse.io.aio.enabled", "true")))
                    logger.warn("Asynchronous I/O has been manually disabled (through the 'dse.io.aio.enabled' system property). "
                                + "This may result in subpar performance.");
                else // Means !Aio.isAvailable()
                    logger.warn("Asynchronous I/O doesn't seem to be available: this may result in subpar performance.");
            }
        }
        else
        {
            // We don't want to use Asynchronous I/O on HDD, because it doesn't provide benefits _but_ risk filling the
            // I/O queue if not throttled properly. Also note that since AIO is disabled on HDD by default (see TPC.USE_AIO
            // definition), having it enabled means the user forced it.
            if (TPC.USE_AIO)
                logger.warn("Forcing Asynchronous I/O as requested with the 'dse.io.aio.force' system property "
                            + " despite not using SSDs; please note that this is not the recommended configuration.");
        }
    };

    private static void warnOnDataDirNotSupportingODirect(Logger logger)
    {
        Set<String>  locationsWithoutODirect = new HashSet<>();
        for (String dataDir : DatabaseDescriptor.getAllDataFileLocations())
        {
            File dir = new File(dataDir);
            try
            {
                File tmp = File.createTempFile("dse-db", null, dir);
                try
                {
                    // This is part where Netty opens the file
                    FileDescriptor.from(tmp, FileDescriptor.O_DIRECT).close();
                }
                catch (IOException e)
                {
                    if (e.getMessage().contains("Invalid argument"))
                        locationsWithoutODirect.add(dataDir);
                    else
                        throw e;
                }
                finally
                {
                    // Because this run after the checkDataDirs check, we should have proper permission and deletion
                    // shouldn't fail. But better safe than sorry, so if it happens, inform the user it's safe to remove.
                    if (!tmp.delete())
                        logger.warn("Wasn't able to delete empty temporary file {} for an unknown reason; "
                                    + "while this shouldn't happen, this is of no consequence outside of the fact that "
                                    + "you will need to delete this (empty and now unused) file manually.", tmp);
                }
            }
            catch (IOException e)
            {
                // Also shouldn't happen, but if it does, no point in freaking people out with an obscure error, we're
                // no doing anything important here (if, for instance, there is something very wrong with that data
                // directories, user will get more meaningful error soon enough). Just mark the dir without O_DIRECT
                // since we haven't validated it does support it.
                logger.debug("Unexpected error while trying to read empty file for O_DIRECT check", e);
                locationsWithoutODirect.add(dataDir);
            }
        }

        if (locationsWithoutODirect.isEmpty())
            return;

        boolean noODirect = locationsWithoutODirect.size() == DatabaseDescriptor.getAllDataFileLocations().length;
        logger.warn("Asynchronous I/O is available/enabled but {} of the configured data directories{} are on "
                    + "file systems supporting O_DIRECT; This will result in subpar performance{}.",
                    noODirect ? "none" : "some",
                    noODirect ? "" : String.format(" (%s)", locationsWithoutODirect),
                    noODirect ? "" : " for operations involving the aforementioned data directories");
    }

    /**
     * Returns the specified tables that exists in the Auth keyspace.
     * @param tables the tables to check for existence
     * @return the specified tables that exists in the Auth keyspace
     */
    private static List<String> getExistingAuthTablesFrom(List<String> tables)
    {
        return tables.stream().filter((table) ->
        {
            String cql = String.format("SELECT table_name FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s'",
                                       SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                       "tables",
                                       SchemaConstants.AUTH_KEYSPACE_NAME,
                                       table);
            UntypedResultSet result = TPCUtils.blockingGet(QueryProcessor.executeOnceInternal(cql));
            return result != null && !result.isEmpty();
        }).collect(Collectors.toList());
    }

    public static final StartupCheck checkPCID = (logger) ->
    {
        if (!FBUtilities.isLinux)
            return;

        Set<String> flags = cpuinfoFlags(logger);
        if (!flags.contains("pcid"))
            logger.warn("CPU does not have PCID (enabled). This will cause an unnecessary performance regression with " +
                        "a kernel having kernel-page-tables-isolation enabled, which should be the case to since " +
                        "CVE-2017-5754 (\"Meltdown\").");
    };

    public static final StartupCheck checkClockSource = (logger) ->
    {
        if (!FBUtilities.isLinux)
            return;

        File fClockSource = new File("/sys/devices/system/clocksource/clocksource0/current_clocksource");

        if (!fClockSource.exists())
            logger.warn("Could not find {}", fClockSource);
        else
        {
            // See:
            // https://0xax.gitbooks.io/linux-insides/content/Timers/timers-6.html
            // http://pzemtsov.github.io/2017/07/23/the-slow-currenttimemillis.html
            //
            // https://github.com/yangoliver/linux/blob/master/Documentation/virtual/kvm/timekeeping.txt
            // http://oliveryang.net/2015/09/pitfalls-of-TSC-usage/#332-implementations-on-various-hypervisors

            List<String> clocksource = FileUtils.readLines(fClockSource);
            if (clocksource.size() != 1)
                logger.warn("Unknown content in {}: {}", fClockSource, clocksource);
            else
            {
                Set<String> flags = cpuinfoFlags(logger);

                String cs = clocksource.get(0);
                switch (cs)
                {
                    case "tsc":
                        if (!flags.contains("constant_tsc") || !flags.contains("nonstop_tsc"))
                            logger.warn("Detected TSC clocksource may perform suboptimal: constant_tsc={}, nonstop_tsc={}", flags.contains("constant_tsc"), flags.contains("nonstop_tsc"));
                        else
                            logger.info("Detected TSC clocksource (constant_tsc={}, nonstop_tsc={}).", flags.contains("constant_tsc"), flags.contains("nonstop_tsc"));
                        break;
                    case "kvm-clock":
                        logger.info("Detected KVM clocksource (constant_tsc={}, nonstop_tsc={}).", flags.contains("constant_tsc"), flags.contains("nonstop_tsc"));
                        break;
                    case "hpet":
                        logger.warn("Detected HPET clocksource. Consider using TSC as the clocksource (constant_tsc={}, nonstop_tsc={}).", flags.contains("constant_tsc"), flags.contains("nonstop_tsc"));
                        break;
                    case "acpi_pm":
                        logger.warn("Detected ACPI-power-management clocksource, which is known to cause severe performance issues. Stongly consider configuring TSC or HPET as the clocksource (constant_tsc={}, nonstop_tsc={}).", flags.contains("constant_tsc"), flags.contains("nonstop_tsc"));
                        break;
                    case "jiffies":
                        logger.warn("Detected jiffies clocksource. Consider configuring TSC or HPET as the clocksource.");
                        break;
                    default:
                        logger.warn("Detected unknown clocksource '{}'.", cs);
                        break;
                }
            }
        }
    };

    private static Set<String> cpuinfoFlags(Logger logger)
    {
        Set<String> flags = Collections.emptySet();
        try
        {
            FBUtilities.CpuInfo cpuInfo = FBUtilities.CpuInfo.load();
            flags = cpuInfo.getProcessors().get(0).getFlags();
        }
        catch (Exception e)
        {
            logger.warn("Could not read /proc/cpuinfo");
        }
        return flags;
    }

    public static final StartupCheck checkVirtualization = (logger) ->
    {
        if (!FBUtilities.isLinux)
            return;

        String virutalization = FileUtils.detectVirtualization();
        if (virutalization == null)
            logger.info("No virtualization/hypervisor detected");
        else
            logger.info("Virtualization/hypervisor '{}' detected. " +
                        "Make sure the disk_optimization_strategy settings reflect the actual hardware. " +
                        "Be aware that certain startup checks may return wrong results due to virtualization/hypervisors. " +
                        "Be also aware that running on virtualized environments can lead to serious performance penalties.",
                        virutalization);
    };

    public static final StartupCheck checkCgroupCpuSets = (logger) ->
    {
        if (!FBUtilities.isLinux)
            return;

        FBUtilities.CpuInfo cpuInfo = FBUtilities.CpuInfo.load();
        if (CGroups.blkioThrottled())
            logger.warn("Block I/O is throttled for this process via Linux cgroups");
        Integer cpus = CGroups.countCpus();
        if (cpus != null && cpus != cpuInfo.cpuCount())
            logger.warn("Some CPUs are not usable because their usage has been restricted via Linux cgroups. Can only use {} of {} CPUs.",
                        CGroups.countCpus(), cpuInfo.cpuCount());
        if (CGroups.memoryLimit() != CGroups.MEM_UNLIMITED)
            logger.warn("Not all memory is accessable by this process as it has been restricted via Linux cgroups. Can only use {}.",
                        FileUtils.stringifyFileSize(CGroups.memoryLimit()));
    };

    public static final StartupCheck checkCpu = (logger) ->
    {
        if (!FBUtilities.isLinux)
            return;

        try
        {
            verifyCpu(logger, FBUtilities.CpuInfo::load);
        }
        catch (Exception e)
        {
            logger.debug("Could not read /proc/cpuinfo", e);
            logger.warn("Could not read /proc/cpuinfo");
        }
    };

    static void verifyCpu(Logger logger, Supplier<FBUtilities.CpuInfo> cpuInfoSupplier)
    {
        FBUtilities.CpuInfo cpuInfo = cpuInfoSupplier.get();

        if (cpuInfo == null || cpuInfo.getProcessors().isEmpty())
        {
            logger.warn("Could not get any CPU information from /proc/cpuinfo");
            return;
        }

        logger.info("CPU information: {} physical processors: {}",
                    cpuInfo.getProcessors().size(),
                    cpuInfo.getProcessors().stream()
                                      .map(p -> p.getName() + " (" +
                                                p.getCores() + " cores, " +
                                                p.getThreadsPerCore() + " threads-per-core, " +
                                                p.getCacheSize() + " cache)")
                                      .collect(Collectors.joining(", ")));

        Map<String, List<Integer>> governors = cpuInfo.getProcessors().stream()
                                                      .flatMap(p -> p.cpuIds().boxed())
                                                      .collect(Collectors.groupingBy(cpuInfo::fetchCpuScalingGovernor));
        governors = new TreeMap<>(governors); // need to make it deterministic - at least for unit tests
        logger.info("CPU scaling governors: {}", governors.entrySet().stream()
                                                          .map(e -> "CPUs " + FBUtilities.CpuInfo.niceCpuIdList(e.getValue()) + ": " + e.getKey())
                                                          .collect(Collectors.joining(", ")));
        if (governors.size() == 1)
        {
            String governor = governors.keySet().iterator().next();
            switch (governor)
            {
                case "performance":
                    // fine, that's what we want to see
                    break;

                // Virtual machines usually have no scaling governor support, so we cannot verify these.
                case "no_cpufreq": // if directory containing scaling-governor file in sysfs cannot be read
                case "no_scaling_governor": // if scaling-governor file in sysfs cannot be read
                    // do not log anything in this case - probably KVM or some other virtualization
                    logger.warn("CPU scaling governors could not be inquired, as /sys/devices/system/cpu/cpu*/cpufreq is not readable.");
                    break;

                case "unknown": // if scaling-governor file in sysfs cannot be read (empty file for example)
                default:
                    logger.warn("CPU scaling governors are all set to {}, but 'performance' is recommended (see above)",
                                governor);
            }
        }
        else
        {
            logger.warn(governors.containsKey("performance")
                        ? "Not all CPU scaling governors not set to 'performance' (see above)"
                        : "None of the CPU scaling governors are set to 'performance' (see above)");
        }
    }

    public static final StartupCheck checkZoneReclaimMode = (logger) ->
    {
        if (!FBUtilities.isLinux)
            return;

        try
        {
            String reclaimMode = FileUtils.readLine(new File("/proc/sys/vm/zone_reclaim_mode"));
            verifyZoneReclaimMode(logger, reclaimMode);
        }
        catch (Exception e)
        {
            logger.debug("Unable to read /sys/kernel/mm/transparent_hugepage/defrag", e);
            logger.warn("Unable to read /sys/kernel/mm/transparent_hugepage/defrag");
        }
    };

    static void verifyZoneReclaimMode(Logger logger, String reclaimMode)
    {
        if ("0".equals(reclaimMode))
            logger.info("Linux NUMA zone-reclaim-mode is set to '0'");
        else
            logger.warn("Linux NUMA zone-reclaim-mode is set to '{}', but should be '0'", reclaimMode);
    }

    public static final StartupCheck checkUlimits = (logger) ->
    {
        if (!FBUtilities.isLinux)
            return;

        // typical contents of /proc/self/limits
        // Limit                     Soft Limit           Hard Limit           Units
        // Max cpu time              unlimited            unlimited            seconds
        // Max file size             unlimited            unlimited            bytes
        // Max data size             unlimited            unlimited            bytes
        // Max stack size            8388608              unlimited            bytes
        // Max core file size        unlimited            unlimited            bytes
        // Max resident set          unlimited            unlimited            bytes
        // Max processes             32768                32768                processes
        // Max open files            100000               100000               files
        // Max locked memory         unlimited            unlimited            bytes
        // Max address space         unlimited            unlimited            bytes
        // Max file locks            unlimited            unlimited            locks
        // Max pending signals       256677               256677               signals
        // Max msgqueue size         819200               819200               bytes
        // Max nice priority         0                    0
        // Max realtime priority     0                    0
        // Max realtime timeout      unlimited            unlimited            us

        try
        {
            List<String> limits = FileUtils.readLines(new File("/proc/self/limits"));
            verifyLimits(logger, limits);
        }
        catch (Exception e)
        {
            logger.debug("Unable to read /proc/self/limits", e);
            logger.warn("Unable to read /proc/self/limits");
        }
    };

    private static boolean verifyLimit(Logger logger, Map<String, List<String>> limitsMap,
                                    String name, String shortName, int limit)
    {
        List<String> lst = limitsMap.get(name);
        String value = lst.get(0);
        int intValue = "unlimited".equals(value)
                       ? Integer.MAX_VALUE
                       : Integer.parseInt(value);

        if (intValue < limit)
        {
            logger.warn("Limit for '{}' ({}) is recommended to be '{}', but is '{}'",
                        shortName,
                        name,
                        limit == Integer.MAX_VALUE ? "unlimited" : Integer.toString(limit),
                        "soft:" + lst.get(0) + ", hard:" + lst.get(1) + " [" + lst.get(2) + ']');
            return false;
        }
        return true;
    }

    static void verifyLimits(Logger logger, List<String> limits)
    {
        Pattern twoSpaces = Pattern.compile("[ ][ ]+");
        Map<String, List<String>> limitsMap = limits.stream().skip(1)
                                                    .map(twoSpaces::split)
                                                    .map(arr -> Arrays.stream(arr).map(String::trim).collect(Collectors.toList()))
                                                    .collect(Collectors.toMap(l -> l.get(0), l -> l.subList(1, l.size())));

        // DataStax recommended prod settings page recommends:
        // <cassandra_user> - memlock unlimited
        // <cassandra_user> - nofile 100000
        // <cassandra_user> - nproc 32768
        // <cassandra_user> - as unlimited

        boolean ok;
        ok = verifyLimit(logger, limitsMap, "Max locked memory", "memlock", Integer.MAX_VALUE);
        ok &= verifyLimit(logger, limitsMap, "Max open files", "nofile", 100000);
        ok &= verifyLimit(logger, limitsMap, "Max processes", "nproc", 32768);
        ok &= verifyLimit(logger, limitsMap, "Max address space", "as", Integer.MAX_VALUE);
        if (ok)
            logger.info("Limits configured according to recommended production settings");
    }

    public static final StartupCheck checkThpDefrag = (logger) ->
    {
        if (!FBUtilities.isLinux)
            return;

        try
        {
            String defrag = FileUtils.readLine(new File("/sys/kernel/mm/transparent_hugepage/defrag"));
            verifyThpDefrag(logger, defrag);
        }
        catch (Exception e)
        {
            logger.debug("Unable to read /sys/kernel/mm/transparent_hugepage/defrag", e);
            logger.warn("Unable to read /sys/kernel/mm/transparent_hugepage/defrag");
        }
    };

    static void verifyThpDefrag(Logger logger, String defrag)
    {
        Matcher m = Pattern.compile(".*\\[(.+)].*").matcher(defrag);
        if (m.matches())
        {
            defrag = m.group(1);
            switch (defrag)
            {
                case "never":
                    logger.info("Linux THP defrag is set to 'never'");
                    // OK
                    break;
                default:
                    logger.warn("Linux THP defrag is set to '{}', but should be 'never'", defrag);
                    break;
            }
        }
        else
        {
            logger.warn("Unable to parse content '{}' of  /sys/kernel/mm/transparent_hugepage/defrag", defrag);
        }
    }

    public static final StartupCheck checkFilesystems = (logger) ->
    {
        if (!FBUtilities.isLinux)
            return;

        try
        {
            checkMountpoint(logger, "saved caches", DatabaseDescriptor.getSavedCachesLocation());
            checkMountpoint(logger, "commitlog", DatabaseDescriptor.getCommitLogLocation());

            for (String dataDirectory : DatabaseDescriptor.getAllDataFileLocations())
                checkMountpoint(logger, "data", dataDirectory);
        }
        catch (Exception e)
        {
            logger.debug("Unable to inspect mounted partitions", e);
            logger.warn("Unable to inspect mounted partitions");
        }
    };

    private static void checkMountpoint(Logger logger, String type, String directory) throws IOException
    {
        FileUtils.MountPoint mountpoint = FileUtils.mountPointForDirectory(directory);
        if (mountpoint == null)
        {
            logger.warn("Could not detect mountpoint for directory {} {}", type, directory);
            return;
        }
        boolean onSsd = FileUtils.isPartitionOnSSD(mountpoint.device);
        String ssd = onSsd ? "SSD" : "rotational";
        switch (mountpoint.fstype)
        {
            case "ext4":
            case "xfs":
                logger.info("{} directory {} is on device {} (appears to be {}), formatted using {} file system",
                            type, directory, mountpoint.device, ssd, mountpoint.fstype);
                break;
            default:
                logger.warn("{} directory {} on device {} (appears to be {})) uses a not recommended file system type '{}' (mounted as {})",
                            type, directory, mountpoint.device, ssd, mountpoint.fstype, mountpoint.mountpoint);
                break;
        }
    }
}
