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
package org.apache.cassandra.cql3;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.reactivex.functions.Consumer;

import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.Config.DiskFailurePolicy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogSegment;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BytemanUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.*;

/**
 * Test flush and compactions with all disk failure policies when the
 * sstable directory cannot be written to, or in case of other write errors.
 */
@RunWith(BMUnitRunner.class)
//@BMUnitConfig(bmunitVerbose=true, debug=true, verbose=true, dumpGeneratedClasses=true)
public class OutOfSpaceTest extends CQLTester
{
    @BeforeClass
    public static void setupClass()
    {
        BytemanUtil.randomizeBytemanPort();
        DatabaseDescriptor.daemonInitialization();

        // We need to initialize CassandraDaemon for the transport and gossip services in order to check that they get
        // stopped as dictated by the disk failure policy. Also, the test requires the default exception handler installed
        // by CassandraDaemon, which is responsible for calling JVMStabilityInspector.inspectThrowable().
        // Note that whilst the compaction executor will use this exception handler, for flushing CFS.switchMemtable() and
        // Flush.run will call JVMStabilityInspector.inspectThrowable() directly. So using the correct exception handler is
        // only important for compactions. CQLTester.prepareServer() will install a default exception handler if none are
        // found, which DOES NOT call JVMStabilityInspector.inspectThrowable().
        CassandraDaemon d = new CassandraDaemon();
        d.completeSetup();
        d.nativeTransportService = new NativeTransportService();
        StorageService.instance.registerDaemon(d);
    }

    @Before
    public void setup()
    {
        // restart the services in case a previous test has stopped them

        if (!StorageService.instance.getDaemon().isNativeTransportRunning())
            StorageService.instance.getDaemon().nativeTransportService.start();

        if (!StorageService.instance.isGossipActive())
            StorageService.instance.startGossiping();
    }

    /**
     * The reason why a write will fail.
     */
    private enum WriteFailureReason
    {
        UNWRITABLE, // the directory has been blacklisted and is therefore not writable
        FS_WRITE_ERROR // byteman will make a method throw FSWriteError
    }

    @Test
    public void testFlushUnwriteableDie() throws Throwable
    {
        testFlush(DiskFailurePolicy.die, WriteFailureReason.UNWRITABLE);
    }

    @Test
    @BMRule(name="throw_fs_write_error_in_table_writer",
    targetClass = "TrieIndexSSTableWriter",
    targetMethod = "append",
    targetLocation = "ENTRY",
    condition = "!$iterator.metadata().keyspace.contains(\"system\")",
    action = "throw org.apache.cassandra.io.FSWriteError(new java.io.IOException(\"Failed to write for testing\"),\"\");")
    public void testFlushWriteErrorInTableWriterDie() throws Throwable
    {
        testFlush(DiskFailurePolicy.die, WriteFailureReason.FS_WRITE_ERROR);
    }

    @Test
    @BMRule(name="throw_fs_write_error_in_txn_log",
    targetClass = "FileUtils",
    targetMethod = "write",
    targetLocation = "ENTRY",
    condition = "!$file.toPath().toString().contains(\"system\")",
    action = "throw org.apache.cassandra.io.FSWriteError(new java.io.IOException(\"Failed to write for testing\"),\"\");")
    public void testFlushWriteErrorInTxnLogDie() throws Throwable
    {
        testFlush(DiskFailurePolicy.die, WriteFailureReason.FS_WRITE_ERROR);
    }

    @Test
    public void testFlushUnwriteableStop() throws Throwable
    {
        testFlush(DiskFailurePolicy.stop, WriteFailureReason.UNWRITABLE);
    }

    @Test
    @BMRule(name="throw_fs_write_error_in_table_writer",
    targetClass = "TrieIndexSSTableWriter",
    targetMethod = "append",
    targetLocation = "ENTRY",
    condition = "!$iterator.metadata().keyspace.contains(\"system\")",
    action = "throw org.apache.cassandra.io.FSWriteError(new java.io.IOException(\"Failed to write for testing\"),\"\");")
    public void testFlushWriteErrorInTableWriterStop() throws Throwable
    {
        testFlush(DiskFailurePolicy.stop, WriteFailureReason.FS_WRITE_ERROR);
    }

    @Test
    @BMRule(name="throw_fs_write_error_in_txn_log",
    targetClass = "FileUtils",
    targetMethod = "write",
    targetLocation = "ENTRY",
    condition = "!$file.toPath().toString().contains(\"system\")",
    action = "throw org.apache.cassandra.io.FSWriteError(new java.io.IOException(\"Failed to write for testing\"),\"\");")
    public void testFlushWriteErrorInTxnLogStop() throws Throwable
    {
        testFlush(DiskFailurePolicy.stop, WriteFailureReason.FS_WRITE_ERROR);
    }

    @Test
    public void testFlushUnwriteableStopParanoid() throws Throwable
    {
        testFlush(DiskFailurePolicy.stop_paranoid, WriteFailureReason.UNWRITABLE);
    }

    @Test
    public void testFlushUnwriteableBestEffort() throws Throwable
    {
        testFlush(DiskFailurePolicy.best_effort, WriteFailureReason.UNWRITABLE);
    }

    @Test
    public void testFlushUnwriteableIgnore() throws Throwable
    {
        testFlush(DiskFailurePolicy.ignore, WriteFailureReason.UNWRITABLE);
    }

    private void testFlush(DiskFailurePolicy policy, WriteFailureReason failureReason) throws Throwable
    {
        makeTable();

        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
        DiskFailurePolicy oldPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        Closeable blackListDirectory =  failureReason == WriteFailureReason.UNWRITABLE
                                        ? Util.markDirectoriesUnwriteable(getCurrentColumnFamilyStore(KEYSPACE_PER_TEST))
                                        : null;
        try
        {
            DatabaseDescriptor.setDiskFailurePolicy(policy);
            flushAndExpectError(FSWriteError.class);
            verifyDiskFailurePolicy(policy);
        }
        finally
        {
            if (blackListDirectory != null)
                blackListDirectory.close(); // restore directory as non-blacklisted

            DatabaseDescriptor.setDiskFailurePolicy(oldPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
        }

        if (policy == DiskFailurePolicy.ignore)
            flush(KEYSPACE_PER_TEST); //next flush should succeed
    }

    @Test
    public void testCompactionUnwriteableDie() throws Throwable
    {
        testCompaction(DiskFailurePolicy.die, WriteFailureReason.UNWRITABLE);
    }

    @Test
    @BMRule(name="throw_fs_write_error_in_table_writer",
    targetClass = "TrieIndexSSTableWriter",
    targetMethod = "append",
    targetLocation = "ENTRY",
    condition = "!$iterator.metadata().keyspace.contains(\"system\") && Thread.currentThread().getName().contains(\"CompactionExecutor\")",
    action = "throw org.apache.cassandra.io.FSWriteError(new java.io.IOException(\"Failed to write for testing\"),\"\");")
    public void testCompactionWriteErrorInTableWriterDie() throws Throwable
    {
        testCompaction(DiskFailurePolicy.die, WriteFailureReason.FS_WRITE_ERROR);
    }

    @Test
    @BMRule(name="throw_fs_write_error_in_txn_log",
    targetClass = "FileUtils",
    targetMethod = "write",
    targetLocation = "ENTRY",
    condition = "!$file.toPath().toString().contains(\"system\") && $file.toPath().toString().contains(\"compaction\")",
    action = "throw org.apache.cassandra.io.FSWriteError(new java.io.IOException(\"Failed to write for testing\"),\"\");")
    public void testCompactionWriteErrorInTxnLogDie() throws Throwable
    {
        testCompaction(DiskFailurePolicy.die, WriteFailureReason.FS_WRITE_ERROR);
    }

    @Test
    public void testCompactionUnwriteableStop() throws Throwable
    {
        testCompaction(DiskFailurePolicy.stop, WriteFailureReason.UNWRITABLE);
    }

    @Test
    @BMRule(name="throw_fs_write_error_in_table_writer",
    targetClass = "TrieIndexSSTableWriter",
    targetMethod = "append",
    targetLocation = "ENTRY",
    condition = "!$iterator.metadata().keyspace.contains(\"system\") && Thread.currentThread().getName().contains(\"CompactionExecutor\")",
    action = "throw org.apache.cassandra.io.FSWriteError(new java.io.IOException(\"Failed to write for testing\"),\"\");")
    public void testCompactionWriteErrorInTableWriterStop() throws Throwable
    {
        testCompaction(DiskFailurePolicy.stop, WriteFailureReason.FS_WRITE_ERROR);
    }

    @Test
    @BMRule(name="throw_fs_write_error_in_txn_log",
    targetClass = "FileUtils",
    targetMethod = "write",
    targetLocation = "ENTRY",
    condition = "!$file.toPath().toString().contains(\"system\") && $file.toPath().toString().contains(\"compaction\")",
    action = "throw org.apache.cassandra.io.FSWriteError(new java.io.IOException(\"Failed to write for testing\"),\"\");")
    public void testCompactionWriteErrorInTxnLogStop() throws Throwable
    {
        testCompaction(DiskFailurePolicy.stop, WriteFailureReason.FS_WRITE_ERROR);
    }

    @Test
    public void testCompactionUnwriteableStopParanoid() throws Throwable
    {
        testCompaction(DiskFailurePolicy.stop_paranoid, WriteFailureReason.UNWRITABLE);
    }

    @Test
    public void testCompactionUnwriteableBestEffort() throws Throwable
    {
        testCompaction(DiskFailurePolicy.best_effort, WriteFailureReason.UNWRITABLE);
    }

    @Test
    public void testCompactionUnwriteableIgnore() throws Throwable
    {
        testCompaction(DiskFailurePolicy.ignore, WriteFailureReason.UNWRITABLE);
    }

    private void testCompaction(DiskFailurePolicy policy, WriteFailureReason failureReason) throws Throwable
    {
        makeTable();

        execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column1', 'val');");
        flush(KEYSPACE_PER_TEST);

        execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column2', 'val');");
        flush(KEYSPACE_PER_TEST);

        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
        DiskFailurePolicy oldPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        Closeable blackListDirectory = failureReason == WriteFailureReason.UNWRITABLE
                                       ? Util.markDirectoriesUnwriteable(getCurrentColumnFamilyStore(KEYSPACE_PER_TEST))
                                       : null;

        try
        {
            DatabaseDescriptor.setDiskFailurePolicy(policy);
            compactAndExpectError(failureReason == WriteFailureReason.UNWRITABLE
                                  ? RuntimeException.class // RuntimeException because of CASSANDRA-12385, see comment below
                                  : FSWriteError.class);

            // the compaction future is returned before handling the exception so wait for a bit (can we do better?)
            // see DebuggableThreadPoolExecutor.afterExecute() -> handleOrLog()
            FBUtilities.sleepQuietly(10);

            if (failureReason == WriteFailureReason.UNWRITABLE)
            {
                // regardless of the disk failure policy, marking a directory as unwritable will result in a
                // FSDiskFullWriteError being thrown by CompactionTask.buildCompactionCandidatesForAvailableDiskSpace(),
                // which then gets converted into a RuntimeException by AbstractCompactionTask.execute() because of CASSANDRA-12385
                // therefore ignoring the disk failure policy, this is the intended behavior because we can recover from
                // out of disk space during compactions (e.g. too many parallel compactions or a big one ongoing), see also DB-1833
                verifyDiskFailurePolicyIgnore();
            }
            else
            {
                // FSWriteErrors that are not FSDiskFullWriteError will not be converted by CASSANDRA-12385 and hence
                // the failure policy will be applied. Note that in theory the sequential writer and file utils write methods should
                // check for out-of-space and throw FSDiskFullWriteError instead of FSWriteErrors if out of space, but that's not been the case
                // so far and DB-1833 is not changing this. In practice CompactionTask.buildCompactionCandidatesForAvailableDiskSpace()
                // should catch out-of-space errors beforehand and so it shouldn't happen too frequently that the failure policy is
                // applied for compactions on out of space as well.
                verifyDiskFailurePolicy(policy);
            }
        }
        finally
        {
            if (blackListDirectory != null)
                blackListDirectory.close(); // restore directory as non black-listed

            DatabaseDescriptor.setDiskFailurePolicy(oldPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
        }

        if (policy == DiskFailurePolicy.ignore)
            compact(KEYSPACE_PER_TEST); //next compact should succeed
    }

    private void makeTable() throws Throwable
    {
        createTable(KEYSPACE_PER_TEST, "CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");

        // insert exactly the amount of tombstones that shouldn't trigger an exception
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column" + i + "', null);");
    }

    @Override
    public UntypedResultSet execute(String query, Object... values) throws Throwable
    {
        return executeFormattedQuery(formatQuery(KEYSPACE_PER_TEST, query), values);
    }

    private void flushAndExpectError(Class<? extends Throwable> error)
    {
        performOpAndExpectError(error, cfs -> cfs.forceFlush().join());

        // Make sure commit log wasn't discarded.
        TableId tableId = currentTableMetadata(KEYSPACE_PER_TEST).id;
        for (CommitLogSegment segment : CommitLog.instance.segmentManager.getActiveSegments())
            if (segment.getDirtyTableIds().contains(tableId))
                return;
        fail("Expected commit log to remain dirty for the affected table.");
    }

    private void compactAndExpectError(Class<? extends Throwable> error)
    {
        performOpAndExpectError(error, ColumnFamilyStore::forceMajorCompaction);
    }

    private void performOpAndExpectError(Class<? extends Throwable> error, Consumer<ColumnFamilyStore> op)
    {
        try
        {
            op.accept(Keyspace.open(KEYSPACE_PER_TEST).getColumnFamilyStore(currentTable()));
            fail(error.getSimpleName() + " expected.");
        }
        catch (AssertionError e)
        {
            throw e; // the fail above threw this one, let it pass and fail the test
        }
        catch (Throwable e)
        {
            // Correct path.
            Throwable t = e.getCause();
            while (t.getCause() != null && (t.getClass() == ExecutionException.class || t.getClass() == RuntimeException.class))
                t = t.getCause();
            assertTrue(String.format("%s is not an instance of %s", t.getClass().getName(), error.getName()),
                       error.isInstance(t));
        }
    }

    private void verifyDiskFailurePolicy(DiskFailurePolicy policy)
    {
        switch (policy)
        {
            case stop:
            case stop_paranoid:
                verifyDiskFailurePolicyStop();
                break;
            case die:
                verifyDiskFailurePolicyDie();
                break;
            case best_effort:
                verifyDiskFailurePolicyBestEffort();
                break;
            case ignore:
                verifyDiskFailurePolicyIgnore();
                break;
            default:
                fail("Unsupported disk failure policy: " + policy);
                break;
        }
    }

    private void verifyDiskFailurePolicyStop()
    {
        verifyGossip(false);
        verifyNativeTransports(false);
        verifyJVMWasKilled(false);
    }

    private void verifyDiskFailurePolicyDie()
    {
        verifyJVMWasKilled(true);
    }

    private void verifyDiskFailurePolicyBestEffort()
    {
        assertFalse(Util.getDirectoriesWriteable(getCurrentColumnFamilyStore(KEYSPACE_PER_TEST)));
        FBUtilities.sleepQuietly(10); // give them a chance to stop before verifying they were not stopped
        verifyGossip(true);
        verifyNativeTransports(true);
        verifyJVMWasKilled(false);
    }

    private void verifyDiskFailurePolicyIgnore()
    {
        FBUtilities.sleepQuietly(10); // give them a chance to stop before verifying they were not stopped
        verifyGossip(true);
        verifyNativeTransports(true);
        verifyJVMWasKilled(false);
    }

    private void verifyJVMWasKilled(boolean killed)
    {
        KillerForTests killer = (KillerForTests) JVMStabilityInspector.killer();
        assertEquals(killed, killer.wasKilled());
        if (killed)
            assertFalse(killer.wasKilledQuietly()); // true only on startup
    }

    private void verifyGossip(boolean isEnabled)
    {
        // Gossip is stopped asynchronously on the GOSSIP stage by StorageService.instance.stopTransportsAsync(),
        // because the GOSSIP stage is single threaded, by checking on the same thread we get the correct result
        CompletableFuture<Boolean> fut = CompletableFuture.supplyAsync(Gossiper.instance::isEnabled, StageManager.getStage(Stage.GOSSIP));
        assertEquals(isEnabled, TPCUtils.blockingGet(fut));
    }

    private void verifyNativeTransports(boolean isRunning)
    {
        // Native transports are also stopped asynchronously, but isRunning is set synchronously
        assertEquals(isRunning, StorageService.instance.getDaemon().isNativeTransportRunning());

        // if the transport has been stopped, we wait for it to be fully stopped so that restarting it for
        // the next test will not fail due to the port being already in use
        if (!isRunning)
            TPCUtils.blockingGet(StorageService.instance.getDaemon().stopNativeTransportAsync());
    }
}
