package com.google.common.hash;

import java.lang.reflect.Field;

import com.google.common.primitives.Longs;

public class BloomFilterAccessor
{
    final BloomFilter bloomFilter;
    final BloomFilterStrategies.LockFreeBitArray bits;
    final int numHashFunctions;

    public BloomFilterAccessor(BloomFilter bloomFilter)
    {
        this.bloomFilter = bloomFilter;

        try
        {
            Field bitsField = BloomFilter.class.getDeclaredField("bits");
            bitsField.setAccessible(true);
            bits = (BloomFilterStrategies.LockFreeBitArray) bitsField.get(bloomFilter);

            Field numHashFunctionsField = BloomFilter.class.getDeclaredField("numHashFunctions");
            numHashFunctionsField.setAccessible(true);
            numHashFunctions = (Integer) numHashFunctionsField.get(bloomFilter);
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    public static long[] getHash(Long object, Funnel<Long> funnel)
    {
        final byte[] bytes = Hashing.murmur3_128().hashObject(object, funnel).getBytesInternal();
        final long hash1 = lowerEight(bytes);
        final long hash2 = upperEight(bytes);
        return new long[] {hash1, hash2};
    }

    public <T> boolean mightContain(
            long hash1,
            long hash2)
    {
        long bitSize = bits.bitSize();
//        byte[] bytes = Hashing.murmur3_128().hashObject(object, funnel).getBytesInternal();
//        long hash1 = lowerEight(bytes);
//        long hash2 = upperEight(bytes);

        long combinedHash = hash1;
        for (int i = 0; i < numHashFunctions; i++)
        {
            // Make the combined hash positive and indexable
            if (!bits.get((combinedHash & Long.MAX_VALUE) % bitSize))
            {
                return false;
            }
            combinedHash += hash2;
        }
        return true;
    }

    private static long lowerEight(byte[] bytes)
    {
        return Longs.fromBytes(
                bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
    }

    private static long upperEight(byte[] bytes)
    {
        return Longs.fromBytes(
                bytes[15], bytes[14], bytes[13], bytes[12], bytes[11], bytes[10], bytes[9], bytes[8]);
    }
}
