/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.io.util;

/**
 * The type of file access. This is currently used as a hint for read-ahead.
 */
public enum FileAccessType
{
    /** Random file access disables read-ahead. It's currently used for index files and for single partition
     * queries accessing data files. */
    RANDOM,
    /** Sequential file access enables read-head. It's currently used for partition range queries accessing
     * data files. */
    SEQUENTIAL,
    /** Full file access enables read ahead. It's currently used for operations that scan the entire file
     * such as compactions and upgrades. */
    FULL_FILE
}
