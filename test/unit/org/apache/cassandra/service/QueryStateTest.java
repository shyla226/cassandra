/*
 * Copyright DataStax, Inc.
 */
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;

import org.junit.*;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryStateTest
{
    private static TestRoleManager roleManager;
    private static TestAuthorizer authorizer;
    private static TestAuthenticator authenticator;

    @Before
    public void setupTest()
    {
        roleManager = new TestRoleManager();
        authorizer = new TestAuthorizer();
        authenticator = new TestAuthenticator();

        DatabaseDescriptor.setConfig(new Config());

        DatabaseDescriptor.setRoleManager(roleManager);
        DatabaseDescriptor.setAuthorizer(authorizer);
        DatabaseDescriptor.setAuthenticator(authenticator);
    }

    @Test
    public void testGrantRevokeWithoutCache()
    {
        DatabaseDescriptor.setPermissionsValidity(0);
        DatabaseDescriptor.setRolesValidity(0);
        Auth.setupCaches();

        grantRevoke();
    }

    @Test
    public void testGrantRevokeWithCache()
    {
        DatabaseDescriptor.setPermissionsValidity(100);
        DatabaseDescriptor.setRolesValidity(100);
        Auth.setupCaches();

        grantRevoke();
    }

    private void grantRevoke()
    {
        AuthenticatedUser user = new AuthenticatedUser("user");
        DataResource ks = DataResource.keyspace("ks");

        System.out.println("before queryState 1");
        QueryState queryState = QueryState.forExternalCalls(user);
        // lazy init!
        queryState.getUser();

        System.out.println("before grant EXECUTE");
        authorizer.grant(null, Collections.singleton(CorePermission.EXECUTE), ks, user.getPrimaryRole(), GrantMode.GRANT);

        System.out.println("test 1");
        assertFalse(queryState.authorize(ks, CorePermission.EXECUTE));

        System.out.println("before queryState 2");
        queryState = QueryState.forExternalCalls(user);
        System.out.println("test 1");
        assertTrue(queryState.authorize(ks, CorePermission.EXECUTE));

        //

        System.out.println("before grant SELECT");
        authorizer.grant(null, Collections.singleton(CorePermission.SELECT), ks, user.getPrimaryRole(), GrantMode.GRANT);
        System.out.println("test 1");
        assertTrue(queryState.authorize(ks, CorePermission.EXECUTE));
        System.out.println("test 2");
        assertFalse(queryState.authorize(ks, CorePermission.SELECT));

        System.out.println("before queryState 3");
        queryState = QueryState.forExternalCalls(user);
        System.out.println("test 1");
        assertTrue(queryState.authorize(ks, CorePermission.EXECUTE));
        System.out.println("test 2");
        assertTrue(queryState.authorize(ks, CorePermission.SELECT));

        //

        System.out.println("before revoke EXECUTE");
        authorizer.revoke(null, Collections.singleton(CorePermission.EXECUTE), ks, user.getPrimaryRole(), GrantMode.GRANT);
        System.out.println("test 1");
        assertTrue(queryState.authorize(ks, CorePermission.EXECUTE));
        System.out.println("test 2");
        assertTrue(queryState.authorize(ks, CorePermission.SELECT));

        System.out.println("before queryState 4");
        queryState = QueryState.forExternalCalls(user);
        System.out.println("test 1");
        assertFalse(queryState.authorize(ks, CorePermission.EXECUTE));
        System.out.println("test 2");
        assertTrue(queryState.authorize(ks, CorePermission.SELECT));
    }

    public static class TestRoleManager implements IRoleManager
    {
        final Map<RoleResource, Role> roles = new HashMap<>();

        public Set<Option> supportedOptions()
        {
            throw new UnsupportedOperationException();
        }

        public Set<Option> alterableOptions()
        {
            throw new UnsupportedOperationException();
        }

        public void createRole(AuthenticatedUser performer, RoleResource role, RoleOptions options) throws RequestValidationException, RequestExecutionException
        {
            Role r = new Role(role.getRoleName(),
                              ImmutableSet.of(),
                              options.getSuperuser().or(false),
                              options.getLogin().or(false),
                              ImmutableMap.of(),
                              options.getPassword().or(""));
            roles.put(role, r);
            Auth.invalidateRolesForPermissionsChange(role);
        }

        public void dropRole(AuthenticatedUser performer, RoleResource role) throws RequestValidationException, RequestExecutionException
        {
            roles.remove(role);
            Auth.invalidateRolesForPermissionsChange(role);
        }

        public void alterRole(AuthenticatedUser performer, RoleResource role, RoleOptions options) throws RequestValidationException, RequestExecutionException
        {
            throw new UnsupportedOperationException();
        }

        public void grantRole(AuthenticatedUser performer, RoleResource role, RoleResource grantee) throws RequestValidationException, RequestExecutionException
        {
            Role r = roles.get(grantee);
            r.memberOf.add(role);
            Auth.invalidateRolesForPermissionsChange(role, grantee);
        }

        public void revokeRole(AuthenticatedUser performer, RoleResource role, RoleResource revokee) throws RequestValidationException, RequestExecutionException
        {
            Role r = roles.get(revokee);
            r.memberOf.remove(role);
            Auth.invalidateRolesForPermissionsChange(role, revokee);
        }

        public Set<RoleResource> getRoles(RoleResource grantee, boolean includeInherited) throws RequestValidationException, RequestExecutionException
        {
            Set<RoleResource> all = new HashSet<>();
            Role r = roles.get(grantee);
            if (includeInherited)
                throw new UnsupportedOperationException();
            return all;
        }

        public Set<RoleResource> getAllRoles() throws RequestValidationException, RequestExecutionException
        {
            return roles.keySet();
        }

        public boolean isSuper(RoleResource role)
        {
            Role r = roles.get(role);
            return r != null ? r.isSuper : false;
        }

        public boolean canLogin(RoleResource role)
        {
            Role r = roles.get(role);
            return r != null ? r.canLogin : false;
        }

        public Map<String, String> getCustomOptions(RoleResource role)
        {
            return Collections.emptyMap();
        }

        public Set<RoleResource> filterExistingRoleNames(List<String> roleNames)
        {
            return roleNames.stream().map(RoleResource::role).collect(Collectors.toSet());
        }

        public boolean isExistingRole(RoleResource role)
        {
            return roles.containsKey(role);
        }

        public Role getRoleData(RoleResource role)
        {
            return roles.get(role);
        }

        public Set<? extends IResource> protectedResources()
        {
            throw new UnsupportedOperationException();
        }

        public void validateConfiguration() throws ConfigurationException
        {
        }

        public Future<?> setup()
        {
            return Futures.immediateFuture(null);
        }
    }

    public static class TestAuthorizer implements IAuthorizer
    {
        final Map<RoleResource, Map<IResource, PermissionSets>> roleResourcePermissions = new HashMap<>();

        public Map<IResource, PermissionSets> allPermissionSets(RoleResource role)
        {
            Map<IResource, PermissionSets> rolePerms = roleResourcePermissions.get(role);
            System.out.println("allPermissionSets " + role + " = " + rolePerms);
            return rolePerms;
        }

        public void grant(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource grantee, GrantMode... grantModes) throws RequestValidationException, RequestExecutionException
        {
            Map<IResource, PermissionSets> resourcePermissions = roleResourcePermissions.computeIfAbsent(grantee, (k) -> new HashMap<>());
            resourcePermissions = new HashMap<>(resourcePermissions);
            PermissionSets permissionSets = resourcePermissions.computeIfAbsent(resource, (k) -> PermissionSets.EMPTY);

            PermissionSets.Builder builder = permissionSets.unbuild();
            for (GrantMode grantMode : grantModes)
            {
                switch (grantMode)
                {
                    case GRANT:
                        builder.addGranted(permissions);
                        break;
                    case GRANTABLE:
                        builder.addGrantables(permissions);
                        break;
                    case RESTRICT:
                        builder.addRestricted(permissions);
                        break;
                }
            }
            resourcePermissions.put(resource, builder.build());
            roleResourcePermissions.put(grantee, Collections.unmodifiableMap(resourcePermissions));
            Auth.invalidateRolesForPermissionsChange(grantee);
        }

        public void revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource revokee, GrantMode... grantModes) throws RequestValidationException, RequestExecutionException
        {
            Map<IResource, PermissionSets> resourcePermissions = roleResourcePermissions.computeIfAbsent(revokee, (k) -> new HashMap<>());
            resourcePermissions = new HashMap<>(resourcePermissions);
            PermissionSets permissionSets = resourcePermissions.computeIfAbsent(resource, (k) -> PermissionSets.EMPTY);

            PermissionSets.Builder builder = permissionSets.unbuild();
            for (GrantMode grantMode : grantModes)
            {
                switch (grantMode)
                {
                    case GRANT:
                        builder.removeGranted(permissions);
                        break;
                    case GRANTABLE:
                        builder.removeGrantables(permissions);
                        break;
                    case RESTRICT:
                        builder.removeRestricted(permissions);
                        break;
                }
            }

            permissionSets = builder.build();
            if (permissionSets.granted.isEmpty()
                && permissionSets.grantables.isEmpty()
                && permissionSets.restricted.isEmpty())
                resourcePermissions.remove(resource);
            else
                resourcePermissions.put(resource, permissionSets);

            if (resourcePermissions.isEmpty())
                roleResourcePermissions.remove(revokee);
            else
                roleResourcePermissions.put(revokee, Collections.unmodifiableMap(resourcePermissions));
            Auth.invalidateRolesForPermissionsChange(revokee);
        }

        public Set<PermissionDetails> list(Set<Permission> permissions, IResource resource, RoleResource grantee) throws RequestValidationException, RequestExecutionException
        {
            throw new UnsupportedOperationException();
        }

        public void revokeAllFrom(RoleResource revokee)
        {
            throw new UnsupportedOperationException();
        }

        public void revokeAllOn(IResource droppedResource)
        {
            throw new UnsupportedOperationException();
        }

        public Set<? extends IResource> protectedResources()
        {
            throw new UnsupportedOperationException();
        }

        public void validateConfiguration() throws ConfigurationException
        {
        }

        public void setup()
        {
        }
    }

    public static class TestAuthenticator implements IAuthenticator
    {
        public boolean requireAuthentication()
        {
            return true;
        }

        public Set<? extends IResource> protectedResources()
        {
            throw new UnsupportedOperationException();
        }

        public void validateConfiguration() throws ConfigurationException
        {
        }

        public void setup()
        {
        }

        public SaslNegotiator newSaslNegotiator(InetAddress clientAddress)
        {
            throw new UnsupportedOperationException();
        }

        public AuthenticatedUser legacyAuthenticate(Map<String, String> credentials) throws AuthenticationException
        {
            throw new UnsupportedOperationException();
        }
    }
}
