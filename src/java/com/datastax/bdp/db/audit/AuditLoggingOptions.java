/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class AuditLoggingOptions
{
    public boolean enabled = false;

    // the class name of the logger
    public String logger = SLF4JAuditWriter.class.getName();
    public String included_categories = null;
    public String excluded_categories = null;
    public String included_keyspaces = null;
    public String excluded_keyspaces = null;
    public String included_roles = null;
    public String excluded_roles = null;
    public int retention_time = 0;

    public CassandraAuditWriterOptions cassandra_audit_writer_options = new CassandraAuditWriterOptions();

    public void validateFilters()
    {
        if (isNotBlank(excluded_keyspaces) && isNotBlank(included_keyspaces))
            throw new ConfigurationException("Can't specify both included and excluded keyspaces for audit logger", false);

        if (isNotBlank(excluded_categories) && isNotBlank(included_categories))
            throw new ConfigurationException("Can't specify both included and excluded categories for audit logger", false);

        if (isNotBlank(excluded_roles) && isNotBlank(included_roles))
            throw new ConfigurationException("Can't specify both included and excluded roles for audit logger", false);
    }
}
