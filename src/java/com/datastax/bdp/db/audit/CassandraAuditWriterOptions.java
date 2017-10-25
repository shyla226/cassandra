/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import org.apache.cassandra.db.ConsistencyLevel;

public class CassandraAuditWriterOptions
{
    public String mode = "sync";
    public String num_writers = "10";
    public String batch_size = "50";
    public String flush_time = "500";
    public String queue_size = "10000";
    public String day_partition_millis = Integer.valueOf(1000 * 60 * 60).toString();  // partition days into hours by default
    public String write_consistency = ConsistencyLevel.QUORUM.toString();
    public String dropped_event_log = "/var/log/cassandra/dropped_audit_events.log";
}
