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

package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.util.concurrent.CompletionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.IndexFileEntry;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.flow.FlowSource;

/**
 * A flow of {@link IndexFileEntry} for the
 * legacy big table format.
 */
public class BigIndexFileFlow extends FlowSource<IndexFileEntry>
{
    private static final Logger logger = LoggerFactory.getLogger(BigIndexFileFlow.class);

    private final BigTableReader sstable;
    private final FileHandle ifile;
    private final RandomAccessReader reader;
    private final BigRowIndexEntry.IndexSerializer rowIndexEntrySerializer;
    private final IPartitioner partitioner;
    private final Version version;
    private final PartitionPosition left;
    private final int inclusiveLeft;
    private final PartitionPosition right;
    private final int exclusiveRight;
    private final TPCScheduler onReadyExecutor;

    private boolean firstPublished;
    private long position;

    public BigIndexFileFlow(BigTableReader sstable,
                            PartitionPosition left,
                            int inclusiveLeft,
                            PartitionPosition right,
                            int exclusiveRight)
    {
        this.sstable = sstable;
        this.ifile = sstable.ifile.sharedCopy();
        this.reader = ifile.createReader(Rebufferer.ReaderConstraint.ASYNC);
        this.rowIndexEntrySerializer = sstable.rowIndexEntrySerializer;
        this.partitioner = sstable.getPartitioner();
        this.version = sstable.descriptor.version;
        this.left = left;
        this.inclusiveLeft = inclusiveLeft;
        this.right = right;
        this.exclusiveRight = exclusiveRight;
        this.onReadyExecutor = TPC.bestTPCScheduler();
        this.firstPublished = false;
        this.position = -1;
    }

    public void requestNext()
    {
        readWithRetry(false);
    }

    private void readWithRetry(boolean isRetry)
    {
        try
        {
            IndexFileEntry current = !firstPublished ? readFirst(isRetry) : readNext(isRetry);
            if (current != IndexFileEntry.EMPTY)
                subscriber.onNext(current);
            else
                subscriber.onComplete();
        }
        catch (Rebufferer.NotInCacheException e)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} - isRetry? {}, firstPublished? {}  NotInCacheException: {}", hashCode(), isRetry, firstPublished, e.getMessage());

            // Retry the request once data is in the cache
            e.accept(this.getClass(),
                     () -> readWithRetry(true),
                     (t) ->
                     {
                         logger.error("Failed to retry due to exception", t);

                         // Calling completeExceptionally() wraps the original exception into a CompletionException even
                         // though the documentation says otherwise
                         if (t instanceof CompletionException && t.getCause() != null)
                             t = t.getCause();

                         subscriber.onError(t);
                         return null;
                     },
                     onReadyExecutor);
        }
        catch (CorruptSSTableException | IOException e)
        {
            sstable.markSuspect();
            subscriber.onError(new CorruptSSTableException(e, sstable.getFilename()));
        }
        catch (Throwable t)
        {
            subscriber.onError(t);
        }
    }

    private IndexFileEntry readFirst(boolean isRetry) throws IOException
    {
        if (!isRetry)
        {
            assert position == -1 : "readFirst called multiple times with retry set to false";
            position = sstable.getIndexScanPosition(left);
        }

        reader.seek(position);

        while (!reader.isEOF())
        {
            DecoratedKey indexDecoratedKey = partitioner.decorateKey(ByteBufferUtil.readWithShortLength(reader));
            if (indexDecoratedKey.compareTo(left) > inclusiveLeft)
            {
                if (indexDecoratedKey.compareTo(right) > exclusiveRight)
                    break;

                firstPublished = true; // from now on it's safe to compare only with the right
                //logger.debug("Publishing {} - {} / {}", indexDecoratedKey, left, right);
                return new IndexFileEntry(indexDecoratedKey,
                                          rowIndexEntrySerializer.deserialize(reader, reader.getFilePointer()));
            }
            else
            {
                BigRowIndexEntry.Serializer.skip(reader, version);
            }

            position = reader.getPosition();
        }

        return IndexFileEntry.EMPTY;
    }

    private IndexFileEntry readNext(boolean isRetry) throws IOException
    {
        if (isRetry)
            reader.seek(position);
        else
            position = reader.getPosition();

        if (!reader.isEOF())
        {
            DecoratedKey indexDecoratedKey = partitioner.decorateKey(ByteBufferUtil.readWithShortLength(reader));
            if (right == null || indexDecoratedKey.compareTo(right) <= exclusiveRight)
            {
                //logger.debug("Publishing {} - {} / {}, firstPublished? {}", indexDecoratedKey, left, right, firstPublished);
                return new IndexFileEntry(indexDecoratedKey, rowIndexEntrySerializer.deserialize(reader, reader.getFilePointer()));
            }
        }

        return IndexFileEntry.EMPTY;
    }

    public void close() throws Exception
    {
        Throwables.maybeFail(Throwables.closeNonNull(null, reader, ifile));
    }
}
