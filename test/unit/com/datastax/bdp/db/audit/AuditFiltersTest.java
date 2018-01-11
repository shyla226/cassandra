/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.datastax.bdp.db.audit.CoreAuditableEventType.CQL_SELECT;
import static com.datastax.bdp.db.audit.CoreAuditableEventType.CQL_UPDATE;
import static com.datastax.bdp.db.audit.CoreAuditableEventType.REQUEST_FAILURE;

public class AuditFiltersTest
{
    @Test
    public void testConfigurationValidation() throws Exception
    {
        AuditLoggingOptions options = new AuditLoggingOptions();
        options.excluded_keyspaces = "test";
        options.included_keyspaces = "test2";

        assertInvalidOptions(options, "Can't specify both included and excluded keyspaces for audit logger");

        options.excluded_keyspaces = "test";
        options.included_keyspaces = null;
        options.excluded_categories = AuditableEventCategory.DCL.toString();
        options.included_categories = AuditableEventCategory.DCL.toString();

        assertInvalidOptions(options, "Can't specify both included and excluded categories for audit logger");

        options.excluded_categories = null;
        options.included_categories = "mistake";

        assertInvalidOptions(options, "Unknown audit event category:  mistake");

        options.included_roles = "admin";
        options.excluded_roles = "admin";

        assertInvalidOptions(options, "Can't specify both included and excluded roles for audit logger");
    }

    @Test
    public void testAcceptWithNoFilters() throws Exception
    {
        AuditLoggingOptions options = new AuditLoggingOptions();

        IAuditFilter filter = AuditFilters.fromConfiguration(options);
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks1", "audited")));
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks2", "audited")));
        assertTrue(filter.accept(newEvent(CQL_UPDATE, "ks1", "audited")));
    }

    @Test
    public void testIncludedKeyspacesFilter() throws Exception
    {
        AuditLoggingOptions options = new AuditLoggingOptions();

        options.included_keyspaces = "ks2";
        IAuditFilter filter = AuditFilters.fromConfiguration(options);
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1", "audited")));
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks2", "audited")));

        options.included_keyspaces = "ks1";
        filter = AuditFilters.fromConfiguration(options);
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks111", "audited")));
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1_stuff", "audited")));
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks1", "audited")));

        options.included_keyspaces = "ks[1]*";
        filter = AuditFilters.fromConfiguration(options);
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks111", "audited")));
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1_stuff", "audited")));
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks1", "audited")));
    }

    @Test
    public void testExcludedKeyspaceFilter() throws Exception
    {
        AuditLoggingOptions options = new AuditLoggingOptions();

        options.excluded_keyspaces = "ks1, ks3";
        IAuditFilter filter = AuditFilters.fromConfiguration(options);
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1", "audited")));
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks2", "audited")));
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks3", "audited")));

        options.excluded_keyspaces = "ks1";
        filter = AuditFilters.fromConfiguration(options);
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks111", "audited")));
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks1_stuff", "audited")));
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1", "audited")));

        options.excluded_keyspaces = "ks[1]*";
        filter = AuditFilters.fromConfiguration(options);
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks111", "audited")));
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks1_stuff", "audited")));
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1", "audited")));
    }

    @Test
    public void testIncludedCategoryFilter() throws Exception
    {
        AuditLoggingOptions options = new AuditLoggingOptions();

        options.included_categories = AuditableEventCategory.DML.toString();

        IAuditFilter filter = AuditFilters.fromConfiguration(options);
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1", "audited")));
        assertTrue(filter.accept(newEvent(CQL_UPDATE, "ks1", "audited")));
    }

    @Test
    public void testExcludedCategoryFilter() throws Exception
    {
        AuditLoggingOptions options = new AuditLoggingOptions();

        options.excluded_categories = AuditableEventCategory.QUERY.toString();

        IAuditFilter filter = AuditFilters.fromConfiguration(options);
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1", "audited")));
        assertTrue(filter.accept(newEvent(CQL_UPDATE, "ks1", "audited")));
    }

    @Test
    public void testIncludedRoleFilter() throws Exception
    {
        AuditLoggingOptions options = new AuditLoggingOptions();

        options.included_roles = "audited";

        IAuditFilter filter = AuditFilters.fromConfiguration(options);
        assertTrue(filter.accept(newEvent(CQL_UPDATE, "ks1", "audited")));
        assertFalse(filter.accept(newEvent(CQL_UPDATE, "ks1", "admin")));
    }

    @Test
    public void testExcludedRolesFilter() throws Exception
    {
        AuditLoggingOptions options = new AuditLoggingOptions();

        options.excluded_roles = "audited";

        IAuditFilter filter = AuditFilters.fromConfiguration(options);
        assertFalse(filter.accept(newEvent(CQL_UPDATE, "ks1", "audited")));
        assertTrue(filter.accept(newEvent(CQL_UPDATE, "ks1", "admin")));
    }

    @Test
    public void testCompositeFilter() throws Exception
    {
        AuditLoggingOptions options = new AuditLoggingOptions();

        options.included_keyspaces = "ks1, ks2";
        options.included_categories = AuditableEventCategory.DML.toString();
        options.included_roles = "audited";

        IAuditFilter filter = AuditFilters.fromConfiguration(options);
        assertTrue(filter.accept(newEvent(CQL_UPDATE, "ks1", "audited")));
        assertFalse(filter.accept(newEvent(CQL_UPDATE, "ks1", "admin")));
        assertFalse(filter.accept(newEvent(CQL_UPDATE, "ks3", "audited")));
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks2", "audited")));

        options.included_roles = null;
        options.excluded_roles = "audited";

        filter = AuditFilters.fromConfiguration(options);
        assertFalse(filter.accept(newEvent(CQL_UPDATE, "ks1", "audited")));
        assertTrue(filter.accept(newEvent(CQL_UPDATE, "ks1", "admin")));
        assertFalse(filter.accept(newEvent(CQL_UPDATE, "ks3", "admin")));
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks2", "admin")));

        options.included_categories = null;
        options.excluded_categories = AuditableEventCategory.DML.toString();

        filter = AuditFilters.fromConfiguration(options);
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1", "audited")));
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks1", "admin")));
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks3", "admin")));
        assertFalse(filter.accept(newEvent(CQL_UPDATE, "ks2", "admin")));

        options.included_keyspaces = null;
        options.excluded_keyspaces = "ks1, ks2";

        filter = AuditFilters.fromConfiguration(options);
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks3", "audited")));
        assertTrue(filter.accept(newEvent(CQL_SELECT, "ks3", "admin")));
        assertFalse(filter.accept(newEvent(CQL_SELECT, "ks1", "admin")));
        assertFalse(filter.accept(newEvent(CQL_UPDATE, "ks3", "admin")));
    }

    /**
     * operations that don't touch a specific keyspace shouldn't NPE
     */
    @Test
    public void nullKeyspaceMatch()
    {
        AuditLoggingOptions options = new AuditLoggingOptions();
        options.included_keyspaces = "dse_system";

        IAuditFilter filter = AuditFilters.fromConfiguration(options);
        assertFalse(filter.accept(newEvent(REQUEST_FAILURE, null, "audited")));
    }

    private AuditableEvent newEvent(AuditableEventType type, String keyspace, String role)
    {
        Set<RoleResource> roles = new HashSet<>();
        roles.add(RoleResource.role(role));

        UserRolesAndPermissions user = UserRolesAndPermissions.newNormalUserRoles("user", "user", roles);

        return new AuditableEvent(user,
                                  type,
                                  "127.0.0.1",
                                  UUIDGen.getTimeUUID(),
                                  null,
                                  keyspace,
                                  "table",
                                  "..CQL statement..",
                                  null);
    }

    /**
     * Asserts that the specified options will trigger a {@code ConfigurationException} with the specified message
     * @param options the invalid options
     * @param expectedMsg the expected error message
     */
    private void assertInvalidOptions(AuditLoggingOptions options, String expectedMsg)
    {
        try
        {
            AuditFilters.fromConfiguration(options);
            fail();
        }
        catch (ConfigurationException e)
        {
            assertEquals(expectedMsg, e.getMessage());
        }
    }
}
