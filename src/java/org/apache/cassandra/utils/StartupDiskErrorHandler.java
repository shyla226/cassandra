/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;

/**
 * The error handler for disk errors during startup, see {@link JVMStabilityInspector}.
 */
final class StartupDiskErrorHandler implements ErrorHandler
{
    private static final Logger logger = LoggerFactory.getLogger(StartupDiskErrorHandler.class);

    private final JVMKiller killer;

    StartupDiskErrorHandler(JVMKiller killer)
    {
        this.killer = killer;
    }

    @Override
    public void handleError(Throwable error)
    {
        if (!(error instanceof FSError) && !(error instanceof CorruptSSTableException))
            return;

        switch (DatabaseDescriptor.getDiskFailurePolicy())
        {
            case stop_paranoid:
            case stop:
            case die:
                logger.error("Exiting forcefully due to file system exception on startup, disk failure policy \"{}\"",
                             DatabaseDescriptor.getDiskFailurePolicy(),
                             error);
                killer.killJVM(error, true);
                break;
            default:
                break;
        }
    }
}
