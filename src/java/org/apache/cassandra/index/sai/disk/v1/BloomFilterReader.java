package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;

import org.apache.cassandra.index.sai.utils.LongBloomFilter;
import org.apache.lucene.store.IndexInput;

public class BloomFilterReader implements LongBloomFilter
{
    final int kOrNumberOfHashFunctions;
    final int numBitsRequired;
    final int size;
    final IndexInput input;
    final long startPos;
    final int numBytes;

    public BloomFilterReader(IndexInput input) throws IOException
    {
        this.input = input;
        kOrNumberOfHashFunctions = input.readInt();
        numBitsRequired = input.readInt();
        size = input.readInt();
        numBytes = input.readInt();
        startPos = input.getFilePointer();
    }

    public boolean maybeContains(long value)
    {
        return containsHash(value);
    }

    public boolean get(int index)
    {
        int pos = index >> 3; // div 8
        int bit = 1 << (index & 0x7);
        try
        {
            input.seek(startPos+pos);
            byte bite = input.readByte();
            return (bite & bit) != 0;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to read bitset from disk");
        }
    }

    public final boolean contains(byte[] bytes)
    {
        long hash64 = BloomFilterWriter.hash(bytes);
        return containsHash(hash64);
    }

    public final boolean containsHash(long hash64)
    {
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);
        for (int i = 1; i <= kOrNumberOfHashFunctions; i++)
        {
            int nextHash = hash1 + i * hash2;
            if (nextHash < 0)
            {
                nextHash = ~nextHash;
            }
            if (!get(nextHash % numBytes))
            {
                return false;
            }
        }
        return true;
    }
}
