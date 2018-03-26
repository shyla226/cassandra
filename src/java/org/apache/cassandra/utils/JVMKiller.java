/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils;

/**
 * An interface implemented to kill the JVM abnormally.
 *
 * It's currently implemented by {@link JVMStabilityInspector}
 * or tests.
 */
public interface JVMKiller
{
    /**
     * Kills the JVM in a verbose fashion.
     *
     * @param error - the error that has caused the JVM to be killed
     */
    default void killJVM(Throwable error)
    {
        killJVM(error, false);
    }

    /**
     * Kills the JVM.
     *
     * @param error - the error that has caused the JVM to be killed
     * @param quiet - whether the error should be logged verbosely
     */
    public void killJVM(Throwable error, boolean quiet);
}
