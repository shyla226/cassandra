/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

public enum AuditableEventType
{

    // QUERY
    GET(AuditableEventCategory.QUERY),
    GET_SLICE(AuditableEventCategory.QUERY),
    GET_COUNT(AuditableEventCategory.QUERY),
    MULTIGET_SLICE(AuditableEventCategory.QUERY),
    MULTIGET_COUNT(AuditableEventCategory.QUERY),
    GET_RANGE_SLICES(AuditableEventCategory.QUERY),
    GET_INDEXED_SLICES(AuditableEventCategory.QUERY),
    GET_PAGED_SLICE(AuditableEventCategory.QUERY),
    CQL_SELECT(AuditableEventCategory.QUERY),
    SOLR_QUERY(AuditableEventCategory.QUERY),
    GRAPH_TINKERPOP_TRAVERSAL(AuditableEventCategory.QUERY),
        
    // DML
    INSERT(AuditableEventCategory.DML),
    REMOVE(AuditableEventCategory.DML),
    ADD(AuditableEventCategory.DML),
    REMOVE_COUNTER(AuditableEventCategory.DML),
    BATCH_MUTATE(AuditableEventCategory.DML),
    SET_KS(AuditableEventCategory.DML),
    TRUNCATE(AuditableEventCategory.DML),
    CQL_UPDATE(AuditableEventCategory.DML),
    CQL_DELETE(AuditableEventCategory.DML),
    CQL_PREPARE_STATEMENT(AuditableEventCategory.DML),
    SOLR_UPDATE(AuditableEventCategory.DML),
    COMPARE_AND_SET(AuditableEventCategory.DML),
    
    // DDL
    DESC_SCHEMA(AuditableEventCategory.DDL),
    DESC_KS(AuditableEventCategory.DDL),
    ADD_CF(AuditableEventCategory.DDL),
    DROP_CF(AuditableEventCategory.DDL),
    UPDATE_CF(AuditableEventCategory.DDL),
    ADD_KS(AuditableEventCategory.DDL),
    DROP_KS(AuditableEventCategory.DDL),
    UPDATE_KS(AuditableEventCategory.DDL),
    CREATE_INDEX(AuditableEventCategory.DDL),
    DROP_INDEX(AuditableEventCategory.DDL),
    CREATE_TRIGGER(AuditableEventCategory.DDL),
    DROP_TRIGGER(AuditableEventCategory.DDL),
    SOLR_GET_RESOURCE(AuditableEventCategory.DDL),
    SOLR_UPDATE_RESOURCE(AuditableEventCategory.DDL),
    
    // DCL
    CREATE_ROLE(AuditableEventCategory.DCL),
    ALTER_ROLE(AuditableEventCategory.DCL),
    DROP_ROLE(AuditableEventCategory.DCL),
    LIST_ROLES(AuditableEventCategory.DCL),
    GRANT(AuditableEventCategory.DCL),
    REVOKE(AuditableEventCategory.DCL),
    LIST_PERMISSIONS(AuditableEventCategory.DCL),
    
    // AUTH
    LOGIN(AuditableEventCategory.AUTH),
    LOGIN_ERROR(AuditableEventCategory.AUTH),
    UNAUTHORIZED_ATTEMPT(AuditableEventCategory.AUTH),

    // ADMIN
    DESC_SCHEMA_VERSIONS(AuditableEventCategory.ADMIN),
    DESC_CLUSTER_NAME(AuditableEventCategory.ADMIN),
    DESC_VERSION(AuditableEventCategory.ADMIN),
    DESC_RING(AuditableEventCategory.ADMIN),
    DESC_LOCAL_RING(AuditableEventCategory.ADMIN),
    DESC_TOKEN_MAP(AuditableEventCategory.ADMIN),
    DESC_PARTITIONER(AuditableEventCategory.ADMIN),
    DESC_SNITCH(AuditableEventCategory.ADMIN),
    DESC_SPLITS(AuditableEventCategory.ADMIN),
    DESC_SPARK_MASTER_ADDRESS(AuditableEventCategory.ADMIN),
    DESC_HADOOP_JOB_TRACKER_ADDRESS(AuditableEventCategory.ADMIN),
    DESC_SPARK_METRICS_CONFIG(AuditableEventCategory.ADMIN),
    DESC_IP_MIRROR(AuditableEventCategory.ADMIN),

    // ERROR
    REQUEST_FAILURE(AuditableEventCategory.ERROR);

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
