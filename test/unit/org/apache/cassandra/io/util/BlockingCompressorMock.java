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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.schema.CompressionParams;

/**
 * This test compressor simulates the DSE Transparency Data Encryption Compressor implementation, which
 * may needs to block in order to retrieve remote keys or read them from disk, hence occasionally throwing
 * {@link org.apache.cassandra.concurrent.TPCUtils.WouldBlockException} when invoked on a TPC thread.
 */
public class BlockingCompressorMock implements ICompressor
{
    private final static String THROW_AT = "throwAt";
    private final int throwAt;
    private int numInvocations;

    private final ICompressor realCompressor = SnappyCompressor.instance;

    private BlockingCompressorMock(int throwAt)
    {
        this.throwAt = throwAt;
        this.numInvocations = 0;
    }

    public static BlockingCompressorMock create(Map<String, String> args) throws ConfigurationException
    {
        int throwAt = args.containsKey(THROW_AT) ? Integer.parseInt(args.get(THROW_AT)) : -1;
        return new BlockingCompressorMock(throwAt);
    }

    static CompressionParams compressionParams(int chunkSize, int throwAt)
    {
        Map<String, String> options = new HashMap<>();
        options.put(THROW_AT, Integer.toString(throwAt));
        return new CompressionParams(BlockingCompressorMock.class.getCanonicalName(), options, chunkSize, 1.1);
    }

    private void maybeThrow()
    {
        if (TPC.isTPCThread() && throwAt >= 0 && numInvocations++ >= throwAt)
            throw new TPCUtils.WouldBlockException("Would block TPC thread " + Thread.currentThread().getName());
    }

    public int initialCompressedBufferLength(int chunkLength)
    {
        maybeThrow();
        return realCompressor.initialCompressedBufferLength(chunkLength);
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        maybeThrow();
        return realCompressor.uncompress(input, inputOffset, inputLength, output, outputOffset);
    }

    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        maybeThrow();
        realCompressor.compress(input, output);
    }

    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        maybeThrow();
        realCompressor.uncompress(input, output);
    }

    public BufferType preferredBufferType()
    {
        return realCompressor.preferredBufferType();
    }

    public boolean supports(BufferType bufferType)
    {
        return realCompressor.supports(bufferType);
    }

    public Set<String> supportedOptions()
    {
        Set<String> ret = new HashSet<>();
        ret.addAll(realCompressor.supportedOptions());
        ret.add(THROW_AT);
        return ret;
    }
}
