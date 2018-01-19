/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

public enum AuditableEventCategory
{
    QUERY, DML, DDL, DCL, AUTH, ADMIN, ERROR
}
