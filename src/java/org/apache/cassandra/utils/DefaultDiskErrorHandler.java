/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BlacklistedDirectories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.service.StorageService;

/**
 * The error handler for disk errors, see {@link JVMStabilityInspector}.
 */
public final class DefaultDiskErrorHandler implements ErrorHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultDiskErrorHandler.class);

    private final JVMKiller killer;
    private final StorageService storageService;

    public DefaultDiskErrorHandler(JVMKiller killer, StorageService storageService)
    {
        this.killer = killer;
        this.storageService = storageService;
    }

    @Override
    public void handleError(Throwable error)
    {
        if (error instanceof FSError)
            handleFSError((FSError) error);
        else if (error instanceof CorruptSSTableException)
            handleCorruptSSTable((CorruptSSTableException) error);
    }

    private void handleCorruptSSTable(CorruptSSTableException e)
    {
        switch (DatabaseDescriptor.getDiskFailurePolicy())
        {
            case stop_paranoid:
                storageService.stopTransportsAsync();
                break;
            case die:
                killer.killJVM(e, false);
                break;
        }
    }

    private void handleFSError(FSError e)
    {
        switch (DatabaseDescriptor.getDiskFailurePolicy())
        {
            case stop_paranoid:
            case stop:
                storageService.stopTransportsAsync();
                break;
            case best_effort:
                // for both read and write errors mark the path as unwritable, if available
                if (e.path.isPresent())
                {
                    BlacklistedDirectories.maybeMarkUnwritable(e.path.get());
                    if (e instanceof FSReadError)
                    {
                        File directory = BlacklistedDirectories.maybeMarkUnreadable(e.path.get());
                        if (directory != null)
                            Keyspace.removeUnreadableSSTables(directory);
                    }
                }
                break;
            case ignore:
                logger.error("Ignoring file system error {}/{} as per ignore disk failure policy", e.getClass(), e.getMessage());
                break;
            case die:
                killer.killJVM(e, false);
                break;
            default:
                throw new IllegalStateException();
        }
    }
}
