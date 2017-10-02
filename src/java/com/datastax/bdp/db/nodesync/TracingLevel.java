/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

/**
 * Tracing level, the higher it is, the most things are traced (and the more verbose it is).
 */
public enum TracingLevel
{
    /**
     * Only thing traced is which segments are validated (or skipped) and the result of those validations. It provides
     * a non cluttered vision of what happens.
     */
    LOW,

    /**
     * Most events related to NodeSync are traced.
     */
    HIGH;

    public static TracingLevel parse(String str)
    {
        return valueOf(str.toUpperCase());
    }
}
