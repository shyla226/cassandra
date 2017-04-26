/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.mos;

/**
 * Contains information on a memory mapped buffer that was locked in RAM.
 */
public class MemoryLockedBuffer
{
    /**
     * The address of the byte buffer.
     */
    public final long address;

    /**
     * The number of bytes locked in memory.
     */
    public final long amount;

    /**
     * True when the memory was successfully locked, false otherwise.
     */
    public final boolean succeeded;

    MemoryLockedBuffer(long address, long amount, boolean succeeded)
    {
        this.address = address;
        this.amount = amount;
        this.succeeded = succeeded;
    }

    public long locked()
    {
        return succeeded ? amount : 0;
    }

    public long notLocked()
    {
        return succeeded ? 0 : amount;
    }

    public static MemoryLockedBuffer succeeded(long address, long amount)
    {
        return new MemoryLockedBuffer(address, amount, true);
    }

    public static MemoryLockedBuffer failed(long address, long amount)
    {
        return new MemoryLockedBuffer(address, amount, false);
    }
}
