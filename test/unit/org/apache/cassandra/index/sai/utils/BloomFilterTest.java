/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.index.sai.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.hash.BloomFilter;
import com.google.common.primitives.UnsignedBytes;
import org.junit.Test;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.IOUtils;

public class BloomFilterTest extends NdiRandomizedTest
{
    @Test
    public void testBloom() throws Exception
    {
        RAMDirectory ramDirectory = new RAMDirectory();

        int num = 100;
        int start = 10000;

        BloomFilter<Long> bloomFilter = BloomFilter.create((token, sink) ->
                                                                   sink.putLong(token.longValue()), num, 0.01);
        for (int x = start; x < start+num; x++)
        {
            //long token = tokenValues.get(x);
            bloomFilter.put((long)x);
            //            tokenBloomWriter.addHash(token); // the token is a hash(?) so add it as is, with no additional hashing
        }

        final IndexOutput out = ramDirectory.createOutput("name", IOContext.DEFAULT);

        bloomFilter.writeTo(new OutputStream()
        {
            @Override
            public void write(int b) throws IOException
            {
                out.writeByte((byte)b);
            }
        });
        IOUtils.close(out);

        final IndexInput input = ramDirectory.openInput("name", IOContext.DEFAULT);

        final BloomFilter<Long> tokenBloom = BloomFilter.readFrom(new InputStream()
        {
            @Override
            public int read() throws IOException
            {
                //return input.readByte();
                return UnsignedBytes.toInt(input.readByte());
            }
        }, (obj, sink) -> sink.putLong(obj.longValue()));

        // bloomFilter = (value) -> tokenBloom.mightContain(value);

        for (int x = start; x < start+num; x++)
        {
            boolean contains = tokenBloom.mightContain((long)x);
            System.out.println(x+" contains="+contains);
        }
        int v = start+num+8888;
        boolean contains = tokenBloom.mightContain(new Long(v));
        System.out.println(v+" contains="+contains);
        input.close();
    }
}
