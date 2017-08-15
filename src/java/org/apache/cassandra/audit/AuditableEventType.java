/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.audit;

import static org.apache.cassandra.audit.AuditableEventCategory.*;

public enum AuditableEventType
{

    // QUERY
    GET(QUERY),
    GET_SLICE(QUERY),
    GET_COUNT(QUERY),
    MULTIGET_SLICE(QUERY),
    MULTIGET_COUNT(QUERY),
    GET_RANGE_SLICES(QUERY),
    GET_INDEXED_SLICES(QUERY),
    GET_PAGED_SLICE(QUERY),
    CQL_SELECT(QUERY),
    SOLR_QUERY(QUERY),
    GRAPH_TINKERPOP_TRAVERSAL(QUERY),
        
    // DML
    INSERT(DML),
    REMOVE(DML),
    ADD(DML),
    REMOVE_COUNTER(DML),
    BATCH_MUTATE(DML),
    SET_KS(DML),
    TRUNCATE(DML),
    CQL_UPDATE(DML),
    CQL_DELETE(DML),
    CQL_PREPARE_STATEMENT(DML),
    SOLR_UPDATE(DML),
    COMPARE_AND_SET(DML),
    
    // DDL
    DESC_SCHEMA(DDL),
    DESC_KS(DDL),
    ADD_CF(DDL),
    DROP_CF(DDL),
    UPDATE_CF(DDL),
    ADD_KS(DDL),
    DROP_KS(DDL),
    UPDATE_KS(DDL),
    CREATE_INDEX(DDL),
    DROP_INDEX(DDL),
    CREATE_TRIGGER(DDL),
    DROP_TRIGGER(DDL),
    SOLR_GET_RESOURCE(DDL),
    SOLR_UPDATE_RESOURCE(DDL),
    
    // DCL
    CREATE_ROLE(DCL),
    ALTER_ROLE(DCL),
    DROP_ROLE(DCL),
    LIST_ROLES(DCL),
    GRANT(DCL),
    REVOKE(DCL),
    LIST_PERMISSIONS(DCL),
    
    // AUTH
    LOGIN(AUTH),
    LOGIN_ERROR(AUTH),
    UNAUTHORIZED_ATTEMPT(AUTH),

    // ADMIN
    DESC_SCHEMA_VERSIONS(ADMIN),
    DESC_CLUSTER_NAME(ADMIN),
    DESC_VERSION(ADMIN),
    DESC_RING(ADMIN),
    DESC_LOCAL_RING(ADMIN),
    DESC_TOKEN_MAP(ADMIN),
    DESC_PARTITIONER(ADMIN),
    DESC_SNITCH(ADMIN),
    DESC_SPLITS(ADMIN),
    DESC_SPARK_MASTER_ADDRESS(ADMIN),
    DESC_HADOOP_JOB_TRACKER_ADDRESS(ADMIN),
    DESC_SPARK_METRICS_CONFIG(ADMIN),
    DESC_IP_MIRROR(ADMIN),

    // ERROR
    REQUEST_FAILURE(ERROR);

    private final AuditableEventCategory category;
    private AuditableEventType(AuditableEventCategory category)
    {
      this.category = category;
    }
    
    public AuditableEventCategory getCategory()
    {
        return category;
    }
}
