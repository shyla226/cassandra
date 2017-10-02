/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.tools.nodesync;

public class InvalidOptionException extends Exception
{
    InvalidOptionException(String msg)
    {
        super(msg);
    }
}
