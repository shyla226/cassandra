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
import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SafeMemoryWriterTest
{
    Random rand = new Random();
    static final int CHUNK = 54321;

    @Test
    public void testTrim() throws IOException
    {
        testSafeMemoryWriter(CHUNK * 5, CHUNK);
    }

    @Test
    public void testOver2GBuffer() throws IOException
    {
        testSafeMemoryWriter(Integer.MAX_VALUE * 5L / 4, CHUNK);
    }

    public void testSafeMemoryWriter(long toSize, int chunkSize) throws IOException
    {
        byte[] data = new byte[chunkSize];
        rand.nextBytes(data);
        try (SafeMemoryWriter writer = new SafeMemoryWriter(65536))
        {

            long l;
            for (l = 0; l < toSize; l += data.length)
            {
                writer.write(data);
            }
            writer.trim();

            try (SafeMemory written = writer.currentBuffer().sharedCopy())
            {
                assertEquals(l, written.size);

                byte[] writtenBytes = new byte[chunkSize];
                for (l = 0; l < toSize; l += writtenBytes.length)
                {
                    written.getBytes(l, writtenBytes, 0, writtenBytes.length);
                    Assert.assertTrue(Arrays.equals(data, writtenBytes));   // assertArrayEquals is too slow for this
                }
            }
        }
    }
}
