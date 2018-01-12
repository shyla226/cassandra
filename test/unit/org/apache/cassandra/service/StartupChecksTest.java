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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ForwardingLogger;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StartupChecksTest
{
    public static final String INVALID_LEGACY_SSTABLE_ROOT_PROP = "invalid-legacy-sstable-root";
    StartupChecks startupChecks;
    Path sstableDir;

    @BeforeClass
    public static void setupServer()
    {
        SchemaLoader.prepareServer();
    }

    @Before
    public void setup() throws IOException
    {
        for (ColumnFamilyStore cfs : Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStores())
            cfs.clearUnsafe();
        for (File dataDir : Directories.getKSChildDirectories(SchemaConstants.SYSTEM_KEYSPACE_NAME))
            FileUtils.deleteRecursive(dataDir);

        File dataDir = new File(DatabaseDescriptor.getAllDataFileLocations()[0]);
        sstableDir = Paths.get(dataDir.getAbsolutePath(), "Keyspace1", "Standard1");
        Files.createDirectories(sstableDir);

        startupChecks = new StartupChecks();
    }

    @After
    public void tearDown() throws IOException
    {
        if (sstableDir != null)
            FileUtils.deleteRecursive(sstableDir.toFile());
    }

    @Test
    public void failStartupIfInvalidJmxPropertyFound() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkInvalidJmxProperty);

        // When com.sun.management.jmxremote.port is unsert, it should not throw StartupException
        assertNull(System.getProperty("com.sun.management.jmxremote.port"));
        startupChecks.verify();

        System.setProperty("com.sun.management.jmxremote.port", "7199");
        verifyFailure(startupChecks, "The JVM property 'com.sun.management.jmxremote.port' is not allowed. " +
                                     "Please use cassandra.jmx.remote.port instead and refer to cassandra-env.(sh|ps1) " +
                                     "for JMX configuration info.");
        System.clearProperty("com.sun.management.jmxremote.port");
    }


    @Test
    public void failStartupIfInvalidSSTablesFound() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkSSTablesFormat);

        copyInvalidLegacySSTables(sstableDir);

        verifyFailure(startupChecks, "Detected unreadable sstables");

        // we should ignore invalid sstables in a snapshots directory
        FileUtils.deleteRecursive(sstableDir.toFile());
        Path snapshotDir = sstableDir.resolve("snapshots");
        Files.createDirectories(snapshotDir);
        copyInvalidLegacySSTables(snapshotDir);
        startupChecks.verify();

        // and in a backups directory
        FileUtils.deleteRecursive(sstableDir.toFile());
        Path backupDir = sstableDir.resolve("backups");
        Files.createDirectories(backupDir);
        copyInvalidLegacySSTables(backupDir);
        startupChecks.verify();
    }

    @Test
    public void compatibilityCheckIgnoresNonDbFiles() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkSSTablesFormat);

        copyLegacyNonSSTableFiles(sstableDir);
        assertFalse(sstableDir.toFile().listFiles().length == 0);

        startupChecks.verify();
    }

    @Test
    public void maxMapCountCheck() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkMaxMapCount);
        startupChecks.verify();
    }

    private void copyLegacyNonSSTableFiles(Path targetDir) throws IOException
    {

        Path legacySSTableRoot = Paths.get(System.getProperty(INVALID_LEGACY_SSTABLE_ROOT_PROP),
                                           "Keyspace1",
                                           "Standard1");
        for (String filename : new String[]{ "Keyspace1-Standard1-ic-0-TOC.txt",
                                             "Keyspace1-Standard1-ic-0-Digest.sha1",
                                             "legacyleveled.json" })
            Files.copy(Paths.get(legacySSTableRoot.toString(), filename), targetDir.resolve(filename));
    }

    private void copyInvalidLegacySSTables(Path targetDir) throws IOException
    {
        File legacySSTableRoot = Paths.get(System.getProperty(INVALID_LEGACY_SSTABLE_ROOT_PROP),
                                           "Keyspace1",
                                           "Standard1").toFile();
        for (File f : legacySSTableRoot.listFiles())
            Files.copy(f.toPath(), targetDir.resolve(f.getName()));
    }

    private void verifyFailure(StartupChecks tests, String message)
    {
        try
        {
            tests.verify();
            fail("Expected a startup exception but none was thrown");
        }
        catch (StartupException e)
        {
            assertTrue(e.getMessage().contains(message));
        }
    }

    @Test
    public void testNiceCpuIdList()
    {
        assertEquals("0,2,4-8", FBUtilities.CpuInfo.niceCpuIdList(Arrays.asList(0, 2, 4, 5, 6, 7, 8)));
        assertEquals("0,2,4-8,10", FBUtilities.CpuInfo.niceCpuIdList(Arrays.asList(0, 2, 4, 5, 6, 7, 8, 10)));
        assertEquals("0-2,4-8,10", FBUtilities.CpuInfo.niceCpuIdList(Arrays.asList(0, 1, 2, 4, 5, 6, 7, 8, 10)));
        assertEquals("2", FBUtilities.CpuInfo.niceCpuIdList(Arrays.asList(2)));
    }

    @Test
    public void testCheckCpu()
    {
        ForwardingLogger.MockLogger logger = new ForwardingLogger.MockLogger();
        StartupChecks.verifyCpu(logger, () -> wrapForTest(FBUtilities.CpuInfo.loadFrom(generateProcCpuInfo(1, 8, 2)),
                                                          "performance"));
        logger.assertWarnings();
        logger.assertInfos("CPU information: 1 physical processors: Intel(R) Core(TM) i7-6900K CPU @ 3.20GHz (8 cores, 2 threads-per-core, 20480 KB cache)",
                           "CPU scaling governors: CPUs 0-15: performance");

        logger = new ForwardingLogger.MockLogger();
        StartupChecks.verifyCpu(logger, () -> wrapForTest(FBUtilities.CpuInfo.loadFrom(generateProcCpuInfo(2, 24, 2)),
                                                          "performance", "powersave", "powersave", "powersave", "powersave", "powersave", "powersave"));
        logger.assertWarnings("CPU scaling governors not set go 'performance' (see above)");
        logger.assertInfos("CPU information: 2 physical processors: Intel(R) Core(TM) i7-6900K CPU @ 3.20GHz (24 cores, 2 threads-per-core, 20480 KB cache), Intel(R) Core(TM) i7-6900K CPU @ 3.20GHz (24 cores, 2 threads-per-core, 20480 KB cache)",
                           "CPU scaling governors: CPUs 0,7-95: performance, CPUs 1-6: powersave");

        logger = new ForwardingLogger.MockLogger();
        StartupChecks.verifyCpu(logger, () -> wrapForTest(FBUtilities.CpuInfo.loadFrom(generateProcCpuInfo(4, 10, 1)),
                                                          "powersave", "powersave", "powersave", "powersave", "powersave", "powersave", "powersave", "powersave", "powersave", "powersave",
                                                          "performance", "performance", "performance", "performance", "performance", "performance", "performance", "performance", "performance", "performance",
                                                          "powersave", "powersave", "powersave", "powersave", "powersave", "powersave", "powersave", "powersave", "powersave", "powersave",
                                                          "performance", "performance", "performance", "performance", "performance", "performance", "performance", "performance", "performance", "performance"));
        logger.assertWarnings("CPU scaling governors not set go 'performance' (see above)");
        logger.assertInfos("CPU information: 4 physical processors: Intel(R) Core(TM) i7-6900K CPU @ 3.20GHz (10 cores, 1 threads-per-core, 20480 KB cache), Intel(R) Core(TM) i7-6900K CPU @ 3.20GHz (10 cores, 1 threads-per-core, 20480 KB cache), Intel(R) Core(TM) i7-6900K CPU @ 3.20GHz (10 cores, 1 threads-per-core, 20480 KB cache), Intel(R) Core(TM) i7-6900K CPU @ 3.20GHz (10 cores, 1 threads-per-core, 20480 KB cache)",
                           "CPU scaling governors: CPUs 10-19,30-39: performance, CPUs 0-9,20-29: powersave");
    }

    private FBUtilities.CpuInfo wrapForTest(FBUtilities.CpuInfo cpuInfo, String... governors)
    {
        return new FBUtilities.CpuInfo() {
            public List<PhysicalProcessor> getProcessors()
            {
                return cpuInfo.getProcessors();
            }

            public String fetchCpuScalingGovernor(int cpuId)
            {
                return governors.length > cpuId ? governors[cpuId] : governors[0];
            }
        };
    }

    @Test
    public void testCheckZoneReclaimMode()
    {
        ForwardingLogger.MockLogger logger = new ForwardingLogger.MockLogger();
        StartupChecks.verifyZoneReclaimMode(logger, "0");
        logger.assertWarnings();
        logger.assertInfos("Linux NUMA zone-reclaim-mode is set to '0'");

        logger = new ForwardingLogger.MockLogger();
        StartupChecks.verifyZoneReclaimMode(logger, "234");
        logger.assertWarnings("Linux NUMA zone-reclaim-mode is set to '234', but should be '0'");
        logger.assertInfos();

        logger = new ForwardingLogger.MockLogger();
        StartupChecks.verifyZoneReclaimMode(logger, "foobarbaz");
        logger.assertWarnings("Linux NUMA zone-reclaim-mode is set to 'foobarbaz', but should be '0'");
        logger.assertInfos();
    }

    @Test
    public void testCheckUlimits()
    {
        ForwardingLogger.MockLogger logger = new ForwardingLogger.MockLogger();
        StartupChecks.verifyLimits(logger, generateLimits("32768", "32768",
                                                          "100000", "100000",
                                                          "unlimited", "unlimited",
                                                          "unlimited", "unlimited"));
        logger.assertWarnings();
        logger.assertInfos("Limits configured according to recommended production settings");

        logger = new ForwardingLogger.MockLogger();
        StartupChecks.verifyLimits(logger, generateLimits("50000", "50000",
                                                          "500000", "500000",
                                                          "5000", "unlimited",
                                                          "unlimited", "unlimited"));
        logger.assertWarnings("Limit for 'memlock' (Max locked memory) is recommended to be 'unlimited', but is 'soft:5000, hard:unlimited [bytes]'");
        logger.assertInfos();

        logger = new ForwardingLogger.MockLogger();
        StartupChecks.verifyLimits(logger, generateLimits("500", "500",
                                                          "500000", "500000",
                                                          "unlimited", "unlimited",
                                                          "unlimited", "unlimited"));
        logger.assertWarnings("Limit for 'nproc' (Max processes) is recommended to be '32768', but is 'soft:500, hard:500 [processes]'");
        logger.assertInfos();

        logger = new ForwardingLogger.MockLogger();
        StartupChecks.verifyLimits(logger, generateLimits("50000", "50000",
                                                          "500000", "500000",
                                                          "unlimited", "unlimited",
                                                          "99999", "888888"));
        logger.assertWarnings("Limit for 'as' (Max address space) is recommended to be 'unlimited', but is 'soft:99999, hard:888888 [bytes]'");
        logger.assertInfos();

        logger = new ForwardingLogger.MockLogger();
        StartupChecks.verifyLimits(logger, generateLimits("500", "5000",
                                                          "500000", "500000",
                                                          "5000", "unlimited",
                                                          "7777", "unlimited"));
        logger.assertWarnings("Limit for 'memlock' (Max locked memory) is recommended to be 'unlimited', but is 'soft:5000, hard:unlimited [bytes]'",
                              "Limit for 'nproc' (Max processes) is recommended to be '32768', but is 'soft:500, hard:5000 [processes]'",
                              "Limit for 'as' (Max address space) is recommended to be 'unlimited', but is 'soft:7777, hard:unlimited [bytes]'");
        logger.assertInfos();
    }

    @Test
    public void testCheckThpDefrag()
    {
        ForwardingLogger.MockLogger logger = new ForwardingLogger.MockLogger();
        StartupChecks.verifyThpDefrag(logger, "always defer defer+madvise madvise [never]");
        logger.assertWarnings();
        logger.assertInfos("Linux THP defrag is set to 'never'");

        logger = new ForwardingLogger.MockLogger();
        StartupChecks.verifyThpDefrag(logger, "always defer [defer+madvise] madvise never");
        logger.assertWarnings("Linux THP defrag is set to 'defer+madvise', but should be 'never'");
        logger.assertInfos();

        logger = new ForwardingLogger.MockLogger();
        StartupChecks.verifyThpDefrag(logger, "always");
        logger.assertWarnings("Unable to parse content 'always' of  /sys/kernel/mm/transparent_hugepage/defrag");
        logger.assertInfos();

        logger = new ForwardingLogger.MockLogger();
        StartupChecks.verifyThpDefrag(logger, "");
        logger.assertWarnings("Unable to parse content '' of  /sys/kernel/mm/transparent_hugepage/defrag");
        logger.assertInfos();
    }

    public static final List<String> generateProcCpuInfo(int physical, int coresPerSocket, int threadsPerCore)
    {
        int cpuId = 0;
        List<String> l = new ArrayList<>();
        for (int p = 0; p < physical; p++)
        {
            for (int c = 0; c < coresPerSocket; c++)
            {
                for (int t = 0; t < threadsPerCore; t++, cpuId++)
                {
                    l.addAll(Arrays.asList(String.format("processor\t: %d\n" +
                                                         "vendor_id\t: GenuineIntel\n" +
                                                         "cpu family\t: 6\n" +
                                                         "model\t\t: 79\n" +
                                                         "model name\t: Intel(R) Core(TM) i7-6900K CPU @ 3.20GHz\n" +
                                                         "stepping\t: 1\n" +
                                                         "microcode\t: 0xb000021\n" +
                                                         "cpu MHz\t\t: 3200.312\n" +
                                                         "cache size\t: 20480 KB\n" +
                                                         "physical id\t: %d\n" +
                                                         "siblings\t: %d\n" +
                                                         "core id\t\t: %d\n" +
                                                         "cpu cores\t: %d\n" +
                                                         "apicid\t\t: 0\n" +
                                                         "initial apicid\t: 0\n" +
                                                         "fpu\t\t: yes\n" +
                                                         "fpu_exception\t: yes\n" +
                                                         "cpuid level\t: 20\n" +
                                                         "wp\t\t: yes\n" +
                                                         "flags\t\t: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx pdpe1gb rdtscp lm constant_tsc arch_perfmon pebs bts rep_good nopl xtopology nonstop_tsc cpuid aperfmperf pni pclmulqdq dtes64 monitor ds_cpl vmx est tm2 ssse3 sdbg fma cx16 xtpr pdcm pcid dca sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand lahf_lm abm 3dnowprefetch cpuid_fault cat_l3 cdp_l3 intel_ppin intel_pt tpr_shadow vnmi flexpriority ept vpid fsgsbase tsc_adjust bmi1 hle avx2 smep bmi2 erms invpcid rtm cqm rdt_a rdseed adx smap xsaveopt cqm_llc cqm_occup_llc cqm_mbm_total cqm_mbm_local dtherm ida arat pln pts\n" +
                                                         "bugs\t\t:\n" +
                                                         "bogomips\t: 6400.62\n" +
                                                         "clflush size\t: 64\n" +
                                                         "cache_alignment\t: 64\n" +
                                                         "address sizes\t: 46 bits physical, 48 bits virtual\n" +
                                                         "power management:",
                                                         cpuId,
                                                         p,
                                                         coresPerSocket * threadsPerCore,
                                                         c,
                                                         coresPerSocket
                    ).split("\n")));
                    l.add("");
                }
            }
        }
        return l;
    }

    private static List<String> generateLimits(String nproc, String nprocHard,
                                               String nofile, String nofileHard,
                                               String memlock, String memlockHard,
                                               String as, String asHard)
    {
        return Arrays.asList(String.format("Limit                     Soft Limit           Hard Limit           Units     \n" +
                                           "Max cpu time              unlimited            unlimited            seconds   \n" +
                                           "Max file size             unlimited            unlimited            bytes     \n" +
                                           "Max data size             unlimited            unlimited            bytes     \n" +
                                           "Max stack size            8388608              unlimited            bytes     \n" +
                                           "Max core file size        unlimited            unlimited            bytes     \n" +
                                           "Max resident set          unlimited            unlimited            bytes     \n" +
                                           "Max processes             %s                   %s                   processes \n" +
                                           "Max open files            %s                   %s                   files     \n" +
                                           "Max locked memory         %s                   %s                   bytes     \n" +
                                           "Max address space         %s                   %s                   bytes     \n" +
                                           "Max file locks            unlimited            unlimited            locks     \n" +
                                           "Max pending signals       256677               256677               signals   \n" +
                                           "Max msgqueue size         819200               819200               bytes     \n" +
                                           "Max nice priority         0                    0                    \n" +
                                           "Max realtime priority     0                    0                    \n" +
                                           "Max realtime timeout      unlimited            unlimited            us        \n",
                                           nproc, nprocHard,
                                           nofile, nofileHard,
                                           memlock, memlockHard,
                                           as, asHard
        ).split("\n"));
    }
}
