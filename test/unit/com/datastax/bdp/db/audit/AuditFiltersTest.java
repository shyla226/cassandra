/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import org.junit.Test;

import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static com.datastax.bdp.db.audit.AuditFilters.*;
import static com.datastax.bdp.db.audit.CoreAuditableEventType.CQL_SELECT;
import static com.datastax.bdp.db.audit.CoreAuditableEventType.CQL_UPDATE;
import static com.datastax.bdp.db.audit.CoreAuditableEventType.REQUEST_FAILURE;

public class AuditFiltersTest
{
    @Test
    public void checkIncludedKeyspaceFilter() throws Exception
    {
        IAuditFilter filter;

        // check empty filtering
        filter = AuditFilters.acceptEverything();
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks1")));
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks2")));

        // check filtering
        filter = includeKeyspaces("ks2");
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1")));
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks2")));
    }

    @Test
    public void checkIncludedRegex()
    {
        IAuditFilter filter;

        filter = includeKeyspaces("ks1");
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks111")));
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1_stuff")));
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks1")));

        filter = includeKeyspaces("ks[1]{1,}");
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks111")));
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1_stuff")));
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks1")));
    }

    @Test
    public void checkExcludedKeyspaceFilter() throws Exception
    {
        IAuditFilter filter;

        // check empty filtering
        filter = AuditFilters.acceptEverything();
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks1")));
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks2")));

        // check filtering
        filter = excludeKeyspace("ks1");
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1")));
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks2")));
    }

    @Test
    public void checkExcludedRegex()
    {
        IAuditFilter filter;

        filter = excludeKeyspace("ks1");
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks111")));
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks1_stuff")));
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1")));

        filter = excludeKeyspace("ks[1]{1,}");
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks111")));
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks1_stuff")));
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1")));
    }

    @Test
    public void checkIncludedCategoryFilter() throws Exception
    {
        IAuditFilter filter;

        // check empty filtering
        filter = AuditFilters.acceptEverything();
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks1")));
        assertTrue(filter.accept(newEvent(CQL_UPDATE, "ks1")));

        // check filtering
        filter = includeCategory(AuditableEventCategory.DML);
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1")));
        assertTrue(filter.accept(newEvent(CQL_UPDATE, "ks1")));
    }

    @Test
    public void checkExcludedCategoryFilter() throws Exception
    {
        IAuditFilter filter;

        // check empty filtering
        filter = AuditFilters.acceptEverything();
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks1")));
        assertTrue(filter.accept(newEvent(CQL_UPDATE, "ks1")));

        // check filtering
        filter = excludeCategory(AuditableEventCategory.QUERY);
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1")));
        assertTrue(filter.accept(newEvent(CQL_UPDATE, "ks1")));
    }

    /**
     * operations that don't touch a specific keyspace shouldn't NPE
     */
    @Test
    public void nullKeyspaceMatch()
    {
        IAuditFilter filter = includeKeyspaces("dse_system");
        assertFalse(filter.accept(newEvent(REQUEST_FAILURE, null)));
    }

    private AuditableEvent newEvent(AuditableEventType type, String keyspace)
    {
        return new AuditableEvent(UserRolesAndPermissions.ANONYMOUS,
                                  type,
                                  "127.0.0.1",
                                  UUIDGen.getTimeUUID(),
                                  null,
                                  keyspace,
                                  "table",
                                  "..CQL statement..",
                                  null);
    }
}
