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
package org.apache.cassandra.io.sstable;

import java.io.*;

import io.reactivex.Flowable;
import io.reactivex.Observable;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.reactivestreams.Subscription;

public class SSTableIdentityIterator implements Comparable<SSTableIdentityIterator>, UnfilteredRowIterator
{
    private final SSTableReader sstable;
    private DecoratedKey key;
    private DeletionTime partitionLevelDeletion;
    private final FileDataInput dfile;
    private final String filename;
    private boolean shouldClose;

    protected final SSTableSimpleIterator iterator;
    private Row staticRow;

    public SSTableIdentityIterator(SSTableReader sstable, DecoratedKey key, DeletionTime partitionLevelDeletion,
                                   FileDataInput dfile, boolean shouldClose, SSTableSimpleIterator iterator) throws IOException
    {
        super();
        this.sstable = sstable;
        this.key = key;
        this.partitionLevelDeletion = partitionLevelDeletion;
        this.dfile = dfile;
        this.filename = dfile.getPath();
        this.iterator = iterator;
        this.staticRow = iterator.readStaticRow();
        this.shouldClose = shouldClose;
    }

    public static SSTableIdentityIterator create(SSTableReader sstable, long partitionStartPosition, DecoratedKey key)
    {
        FileDataInput file = sstable.getFileDataInput(partitionStartPosition);
        try
        {
            if (key != null)
                ByteBufferUtil.skipShortLength(file); // we already know the key, skip creating unnecessary copy
            else
                key = sstable.decorateKey(ByteBufferUtil.readWithShortLength(file));
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, file.getPath());
        }
        return create(sstable, file, key, true);
    }

    public static SSTableIdentityIterator create(SSTableReader sstable, FileDataInput file, DecoratedKey key)
    {
        return create(sstable, file, key, false);
    }

    static SSTableIdentityIterator create(SSTableReader sstable, FileDataInput file, DecoratedKey key, boolean shouldClose)
    {
        try
        {
            DeletionTime partitionLevelDeletion = DeletionTime.serializer.deserialize(file);
            SerializationHelper helper = new SerializationHelper(sstable.metadata, sstable.descriptor.version.correspondingMessagingVersion(), SerializationHelper.Flag.LOCAL);
            SSTableSimpleIterator iterator = SSTableSimpleIterator.create(sstable.metadata, file, sstable.header, helper, partitionLevelDeletion);
            return new SSTableIdentityIterator(sstable, key, partitionLevelDeletion, file, shouldClose, iterator);
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, file.getPath());
        }
    }

    public static SSTableIdentityIterator create(SSTableReader sstable, FileDataInput dfile, RowIndexEntry<?> indexEntry, DecoratedKey key, boolean tombstoneOnly)
    {
        try
        {
            dfile.seek(indexEntry.position);
            ByteBufferUtil.skipShortLength(dfile); // Skip partition key
            DeletionTime partitionLevelDeletion = DeletionTime.serializer.deserialize(dfile);
            SerializationHelper helper = new SerializationHelper(sstable.metadata, sstable.descriptor.version.correspondingMessagingVersion(), SerializationHelper.Flag.LOCAL);
            SSTableSimpleIterator iterator = tombstoneOnly
                                             ? SSTableSimpleIterator.createTombstoneOnly(sstable.metadata, dfile, sstable.header, helper, partitionLevelDeletion)
                                             : SSTableSimpleIterator.create(sstable.metadata, dfile, sstable.header, helper, partitionLevelDeletion);
            return new SSTableIdentityIterator(sstable, key, partitionLevelDeletion, dfile, false, iterator);
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, dfile.getPath());
        }
    }

    public CFMetaData metadata()
    {
        return sstable.metadata;
    }

    public PartitionColumns columns()
    {
        return metadata().partitionColumns();
    }

    public boolean isReverseOrder()
    {
        return false;
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public boolean hasNext()
    {
        try
        {
            return iterator.hasNext();
        }
        catch (IndexOutOfBoundsException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, filename);
        }
        catch (IOError e)
        {
            if (e.getCause() instanceof IOException)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException((Exception) e.getCause(), filename);
            }
            else
            {
                throw e;
            }
        }
    }

    public Unfiltered next()
    {
        try
        {
            return doCompute();
        }
        catch (IndexOutOfBoundsException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, filename);
        }
        catch (IOError e)
        {
            if (e.getCause() instanceof IOException)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException((Exception) e.getCause(), filename);
            }
            else
            {
                throw e;
            }
        }
    }

    protected Unfiltered doCompute()
    {
        return iterator.next();
    }

    public void close()
    {
        if (shouldClose)
        {
            FileUtils.closeQuietly(dfile);
            shouldClose = false;
        }
    }

    public String getPath()
    {
        return filename;
    }

    public EncodingStats stats()
    {
        return sstable.stats();
    }

    public int compareTo(SSTableIdentityIterator o)
    {
        return key.compareTo(o.key);
    }

    public Flowable<Unfiltered> asObservable()
    {
        return Flowable.fromIterable(() -> this).doAfterTerminate(() -> close());
    }
}
