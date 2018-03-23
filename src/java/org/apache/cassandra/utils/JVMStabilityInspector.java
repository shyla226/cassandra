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
package org.apache.cassandra.utils;

import java.io.FileNotFoundException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * Responsible for deciding whether to kill the JVM if it gets in an "unstable" state (think OOM).
 */
public final class JVMStabilityInspector
{
    private static final Logger logger = LoggerFactory.getLogger(JVMStabilityInspector.class);
    private static Killer killer = new Killer();
    private static final List<Pair<Thread, Runnable>> shutdownHooks = new ArrayList<>(1);
    private static AtomicBoolean printingHeapHistogram = new AtomicBoolean(false);
    private static ErrorHandler diskHandler;
    private static ErrorHandler globalHandler = new GlobalHandler();

    private JVMStabilityInspector() {}

    public static void setFSErrorHandler(ErrorHandler handler)
    {
        diskHandler = handler;
    }

    /**
     * Inspects the error recursively for disk and global errors.
     */
    public static void inspectThrowable(Throwable error)
    {
        inspectThrowable(error, diskHandler, globalHandler);
    }

    /**
     * Inspect the error recursively by applying the specified handler first, and the global handler afterwards.
     * The handler passed-in can be null, which means that only the global handler will be applied, this can be
     * used for the cases where the disk failure handler should not be applied.
     */
    public static void inspectThrowable(Throwable error, @Nullable ErrorHandler handler)
    {
        inspectThrowable(error, handler, globalHandler);
    }

    /**
     * Inspect the error recursively until it finds a handler that has handled the error.
     * @param error
     * @param handlers
     */
    public static void inspectThrowable(Throwable error, ErrorHandler ... handlers)
    {
        while (error != null)
        {
            for (ErrorHandler handler : handlers)
            {
                if (handler != null)
                    handler.handleError(error);
            }

            error = error.getCause();
        }
    }

    /**
     * A global error handler to handle certain Throwables and Exceptions that represent "Die"
     * conditions for the server.
      */
    private static class GlobalHandler implements ErrorHandler
    {
        @Override
        public void handleError(Throwable t)
        {
            boolean isUnstable = false;
            if (logger.isTraceEnabled())
                logger.trace("Inspecting {}/{}", t.getClass(), t.getMessage(), t);

            if (t instanceof OutOfMemoryError)
            {
                if (printHeapHistogramOnOutOfMemoryError())
                {
                    // We want to avoid printing multiple time the heap histogram if multiple OOM errors happen in a short time span.
                    if (printingHeapHistogram.compareAndSet(false, true))
                        HeapUtils.logHeapHistogram();
                    else
                        return; // TODO - previous behavior, is it correct to suppress the exception?
                }

                logger.error("OutOfMemory error letting the JVM handle the error:", t);

                removeShutdownHooks();
                // We let the JVM handle the error. The startup checks should have warned the user if it did not configure
                // the JVM behavior in case of OOM (CASSANDRA-13006).
                throw (OutOfMemoryError) t;
            }

            if (t instanceof LinkageError)
                // Anything that's wrong with the class files, dependency jars and also static initializers that fail.
                isUnstable = true;

            // Check for file handle exhaustion
            if (t instanceof FileNotFoundException || t instanceof SocketException)
                if (t.getMessage().contains("Too many open files"))
                    isUnstable = true;

            if (isUnstable)
                killer.killCurrentJVM(t);
        }
    }

    /**
     * Checks if an heap histogram must be printed on OutOfMemoryError.
     * <p>By default we always print an heap histogram as for large heap it can save us a huge amount of time.
     * If the user want to turn it down he should use the {@code cassandra.printHeapHistogramOnOutOfMemoryError}
     * property.</p>
     * @return {@code true} if an heap histogram must be printed on OutOfMemoryError, {@code false} otherwise.
     */
    private static boolean printHeapHistogramOnOutOfMemoryError()
    {
        String property = System.getProperty("cassandra.printHeapHistogramOnOutOfMemoryError");

        if (property == null)
            return true;

        return Boolean.parseBoolean(property);
    }

    public static void killCurrentJVM(Throwable t, boolean quiet)
    {
        killer.killCurrentJVM(t, quiet);
    }

    public static void userFunctionTimeout(Throwable t)
    {
        switch (DatabaseDescriptor.getUserFunctionTimeoutPolicy())
        {
            case die:
                // policy to give 250ms grace time to
                ScheduledExecutors.nonPeriodicTasks.schedule(() -> killer.killCurrentJVM(t), 250, TimeUnit.MILLISECONDS);
                break;
            case die_immediate:
                killer.killCurrentJVM(t);
                break;
            case ignore:
                logger.error(t.getMessage());
                break;
        }
    }

    public static void registerShutdownHook(Thread hook, Runnable runOnHookRemoved)
    {
        Runtime.getRuntime().addShutdownHook(hook);
        shutdownHooks.add(Pair.create(hook, runOnHookRemoved));
    }

    @VisibleForTesting
    public static Killer replaceKiller(Killer newKiller)
    {
        Killer oldKiller = JVMStabilityInspector.killer;
        JVMStabilityInspector.killer = newKiller;
        return oldKiller;
    }

    @VisibleForTesting
    public static Killer killer()
    {
        return killer;
    }

    public static void removeShutdownHooks()
    {
        Throwable err = null;
        for (Pair<Thread, Runnable> hook : shutdownHooks)
        {
            err = Throwables.perform(err,
                                     () -> Runtime.getRuntime().removeShutdownHook(hook.left),
                                     hook.right::run);
        }

        if (err != null)
            logger.error("Got error when removing shutdown hook(s): {}", err.getMessage(), err);

        shutdownHooks.clear();
    }

    @VisibleForTesting
    public static class Killer
    {
        private final AtomicBoolean killing = new AtomicBoolean();

        /**
        * Certain situations represent "Die" conditions for the server, and if so, the reason is logged and the current JVM is killed.
        *
        * @param t
        *      The Throwable to log before killing the current JVM
        */
        protected void killCurrentJVM(Throwable t)
        {
            killCurrentJVM(t, false);
        }

        protected void killCurrentJVM(Throwable t, boolean quiet)
        {
            if (!quiet)
            {
                t.printStackTrace(System.err);
                logger.error("JVM state determined to be unstable.  Exiting forcefully due to:", t);
            }
            if (killing.compareAndSet(false, true))
            {
                removeShutdownHooks();
                System.exit(100);
            }
        }
    }
}
