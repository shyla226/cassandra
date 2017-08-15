/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.config;

import org.apache.cassandra.audit.SLF4JAuditWriter;

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
}
