package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.StringHelper;

public class BloomFilterWriter
{
    private static final int SEED = 0x7f3a2;
    public static final double LOG_2 = Math.log(2);
    public static final double LOG_2_SQUARE = LOG_2 * LOG_2;
    protected final int kOrNumberOfHashFunctions;
    protected final int numBitsRequired;
    final int numBytes;
    final byte[] bytes;
    final int size;

    public BloomFilterWriter(int size, double falsePositiveProbability)
    {
        this.size = size;

        if (size == 0) size = 1;

        this.numBitsRequired = optimalBitSizeOrM(size, falsePositiveProbability);
        this.kOrNumberOfHashFunctions = optimalNumberofHashFunctionsOrK(size, numBitsRequired);
        numBytes = (size >> 3) + 1;
        bytes = new byte[numBytes];
    }

    public boolean setBit(int index)
    {
        if (index > size)
        {
            throw new IndexOutOfBoundsException("Index is greater than max elements permitted");
        }
        int pos = index >> 3; // div 8
        int bit = 1 << (index & 0x7);
        byte bite = bytes[pos];
        bite = (byte) (bite | bit);
        bytes[pos] = bite;
        return true;
    }

    public void write(IndexOutput out) throws IOException
    {
        out.writeInt(kOrNumberOfHashFunctions);
        out.writeInt(numBitsRequired);
        out.writeInt(size);
        out.writeInt(numBytes);
        out.writeBytes(bytes, bytes.length);
    }

    public static int optimalBitSizeOrM(final double n, final double p)
    {
        return (int) (-n * Math.log(p) / (LOG_2_SQUARE));
    }

    public static int optimalNumberofHashFunctionsOrK(final long n, final long m)
    {
        return Math.max(1, (int) Math.round(m / n * Math.log(2)));
    }

    public static long hash(byte[] bytes)
    {
        return StringHelper.murmurhash3_x86_32(bytes, 0, bytes.length, SEED);
    }

    public final void addHash(byte[] bytes)
    {
        long hash = hash(bytes);
        addHash(hash);
    }

    public final void addHash(long hash64)
    {
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= this.kOrNumberOfHashFunctions; i++)
        {
            int nextHash = hash1 + i * hash2;
            if (nextHash < 0)
            {
                nextHash = ~nextHash;
            }
            setBit(nextHash % numBytes);
        }
    }
}
