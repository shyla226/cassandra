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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;

class ChecksummedRebufferer extends BufferManagingRebufferer
{
    private final DataIntegrityMetadata.ChecksumValidator validator;

    @SuppressWarnings("resource") // chunk reader is closed by super::close()
    ChecksummedRebufferer(AsynchronousChannelProxy channel, DataIntegrityMetadata.ChecksumValidator validator)
    {
        super(new SimpleChunkReader(channel, channel.size(), BufferType.OFF_HEAP, validator.chunkSize));
        this.validator = validator;
    }

    @Override
    public CompletableFuture<BufferHolder> rebufferAsync(long position)
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public BufferHolder rebuffer(long desiredPosition)
    {
        if (desiredPosition != validator.getFilePointer())
            validator.seek(desiredPosition);

        // align with buffer size, as checksums were computed in chunks of buffer size each.
        BufferHolder ret = newBufferHolder(alignedPosition(desiredPosition));

        try
        {
            source.readChunk(ret.offset(), ret.buffer()).join();
        }
        catch (CompletionException t)
        {
            ret.release();
            if (t.getCause() != null && t.getCause() instanceof RuntimeException)
                throw (RuntimeException) t.getCause();

            throw t;
        }

        try
        {
            validator.validate(ByteBufferUtil.getArray(ret.buffer()), 0, ret.buffer().remaining());
        }
        catch (IOException e)
        {
            ret.release();
            throw new CorruptFileException(e, channel().filePath());
        }

        return ret;
    }

    @Override
    public void close()
    {
        try
        {
            source.close();
        }
        finally
        {
            validator.close();
        }
    }

    @Override
    long alignedPosition(long desiredPosition)
    {
        return (desiredPosition / source.chunkSize()) * source.chunkSize();
    }
}
