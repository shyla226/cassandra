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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BlacklistedDirectories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.utils.ErrorHandler;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.JVMStabilityInspector;

public class DefaultFSErrorHandler implements ErrorHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultFSErrorHandler.class);

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
        if (!StorageService.instance.isDaemonSetupCompleted())
            handleStartupFSError(e);

        switch (DatabaseDescriptor.getDiskFailurePolicy())
        {
            case stop_paranoid:
                StorageService.instance.stopTransportsAsync();
                break;
            case die:
                JVMStabilityInspector.killCurrentJVM(e, false);
                break;
        }
    }

    private void handleFSError(FSError e)
    {
        if (!StorageService.instance.isDaemonSetupCompleted())
            handleStartupFSError(e);

        switch (DatabaseDescriptor.getDiskFailurePolicy())
        {
            case stop_paranoid:
            case stop:
                StorageService.instance.stopTransportsAsync();
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
                JVMStabilityInspector.killCurrentJVM(e, false);
                break;
            default:
                throw new IllegalStateException();
        }
    }

    private static void handleStartupFSError(Throwable t)
    {
        switch (DatabaseDescriptor.getDiskFailurePolicy())
        {
            case stop_paranoid:
            case stop:
            case die:
                logger.error("Exiting forcefully due to file system exception on startup, disk failure policy \"{}\"",
                             DatabaseDescriptor.getDiskFailurePolicy(),
                             t);
                JVMStabilityInspector.killCurrentJVM(t, true);
                break;
            default:
                break;
        }
    }
}
