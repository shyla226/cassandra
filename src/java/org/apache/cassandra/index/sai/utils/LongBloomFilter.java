package org.apache.cassandra.index.sai.utils;

public interface LongBloomFilter
{
    boolean maybeContains(long value);
}
