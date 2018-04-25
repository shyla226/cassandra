/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.cql3.continuous.paging;

import org.apache.cassandra.utils.flow.Flow;

/**
 * This exception is thrown to signal that the client is not keeping up or does not want
 * to receive any more pages just yet.
 */
public class ContinuousBackPressureException extends RuntimeException implements Flow.NonWrappableException
{
    public ContinuousBackPressureException(String message)
    {
        super(message);
    }
}
