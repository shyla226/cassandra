/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.util.Locale;

import org.apache.cassandra.exceptions.ConfigurationException;

public enum AuditableEventCategory
{
    QUERY, DML, DDL, DCL, AUTH, ADMIN, ERROR, UNKNOWN;

    public static AuditableEventCategory fromString(String value)
    {
        try
        {
            return AuditableEventCategory.valueOf(value.toUpperCase(Locale.ENGLISH));
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException("Unknown audit event category:  " + value, false);
        }
    }
}
