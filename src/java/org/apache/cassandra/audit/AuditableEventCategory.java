/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.audit;

public enum AuditableEventCategory
{
    QUERY, DML, DDL, DCL, AUTH, ADMIN, ERROR
}
