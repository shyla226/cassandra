/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils;

public interface Flags
{
    static boolean isEmpty(int flags)
    {
        return flags == 0;
    }

    static boolean containsAll(int flags, int testFlags)
    {
        return (flags & testFlags) == testFlags;
    }

    static boolean contains(int flags, int testFlags)
    {
        return (flags & testFlags) != 0;
    }

    static int add(int flags, int toAdd)
    {
        return flags | toAdd;
    }

    static int remove(int flags, int toRemove)
    {
        return flags & ~toRemove;
    }
}