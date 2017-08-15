/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.audit;

import java.util.HashSet;
import java.util.Set;

public class AuditLoggingOptions
{
    public boolean enabled = false;

    // the class name of the logger
    public String logger = SLF4JAuditWriter.class.getName();
    public String included_categories = null;
    public String excluded_categories = null;
    public String included_keyspaces = null;
    public String excluded_keyspaces = null;
    public int retention_time = 0;

    public CassandraAuditWriterOptions cassandra_audit_writer_options = new CassandraAuditWriterOptions();

    private static Set<String> commaSeparatedStrToSet(String valueString)
    {
        if (valueString == null)
            return new HashSet<>();

        if (valueString.trim().length() == 0)
            return new HashSet<>();

        Set<String> values = new HashSet<>();
        for (String value : valueString.split(","))
            if (value.trim().length() > 0)
                values.add(value.trim());

        return values;
    }

    public Set<String> getAuditIncludedKeyspaces()
    {
        return commaSeparatedStrToSet(included_keyspaces);
    }

    public Set<String> getAuditExcludedKeyspaces()
    {
        return commaSeparatedStrToSet(excluded_keyspaces);
    }

    public Set<String> getAuditIncludedCategories()
    {
        return commaSeparatedStrToSet(included_categories);
    }

    public Set<String> getAuditExcludedCategories()
    {
        return commaSeparatedStrToSet(excluded_categories);
    }
}
