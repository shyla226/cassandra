/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.auth;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.Future;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.flow.RxThreads;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

import io.reactivex.Single;

/**
 * The {@code AuthManager} provides a caching layer on top of the {@code IRoleManager} and {@code IAuthorizer}.
 * <p>If the requested data is not in the cache an the calling Thread is a TPC thread, the {@code AuthManager} will
 * defer the retrieval of data and schedule it on an IO Thread. If the calling thread is not a TPC thread or the
 * data is within the cache the call will be executed synchonously and will be blocking.</p>
 * <p>As the returned {@code Single} might have been scheduled on an IO thread, make sure the operations performed on
 * it are scheduled to be run on the Scheduler they should use.</p>
 */
public final class AuthManager
{
    /**
     * The role manager.
     */
    private final IRoleManager roleManager;

    /**
     * The role cache.
     */
    private final RolesCache rolesCache;

    /**
     * The authorizer.
     */
    private final IAuthorizer authorizer;

    /**
     * The permission cache.
     */
    private final PermissionsCache permissionsCache;

    public AuthManager(IRoleManager roleManager, IAuthorizer authorizer)
    {
        this.rolesCache = new RolesCache(roleManager);
        this.permissionsCache = new PermissionsCache(authorizer);
        this.roleManager = new RoleManagerCacheInvalidator(roleManager, authorizer);
        this.authorizer = new AuthorizerInvalidator(roleManager, authorizer);
    }

    /**
     * Returns the roles and permissions associated to the specified user.
     * <p>If the role and permission data were not cached and the calling thread is a TPC thread,
     * the returned {@code Single} might be a deferred one which is scheduled to run on an IO thread.</p>
     * @param user the user for which the roles and permissions must be retrieved.
     * @return the role and permissions associated to the specified user.
     */
    public Single<UserRolesAndPermissions> getUserRolesAndPermissions(AuthenticatedUser user)
    {
        if (user.isSystem())
            return Single.just(UserRolesAndPermissions.SYSTEM);

        if (user.isAnonymous())
            return Single.just(UserRolesAndPermissions.ANONYMOUS);

        return getUserRolesAndPermissions(user.getName(), user.getPrimaryRole());
    }

    /**
     * Returns the roles and permissions associated to the specified user.
     * <p>If the role and permission data were not cached and the calling thread is a TPC thread,
     * the returned {@code Single} might be a deferred one which is scheduled to run on an IO thread.</p>
     * @param name the user name.
     * @return the role and permissions associated to the specified user.
     */
    public Single<UserRolesAndPermissions> getUserRolesAndPermissions(String name)
    {
        return getUserRolesAndPermissions(name, RoleResource.role(name));
    }

    /**
     * Returns the roles and permissions associated to the specified user.
     * <p>If the role and permission data were not cached and the calling thread is a TPC thread,
     * the returned {@code Single} might be a deferred one which is scheduled to run on an IO thread.</p>
     * @param name the user name.
     * @param primaryRole thuser primary role
     * @return the role and permissions associated to the specified user.
     */
    public Single<UserRolesAndPermissions> getUserRolesAndPermissions(String name, RoleResource primaryRole)
    {
        // If we are not on a TPC thread we can query the cache without worrying.
        if (!TPC.isTPCThread())
            return loadRolesAndPermissionsIfNeeded(name, primaryRole);

        // If we are on the TPC thread we need to ensure that either all the needed data are in the cache or
        // that we switch to an IO thread if the data is not in the cache.
        Map<RoleResource, Role> rolePerResource = rolesCache.getRolesIfPresent(primaryRole);

        if (rolePerResource != null)
        {
            Set<RoleResource> resources = rolePerResource.keySet();

            if (isSuperUser(rolePerResource.values()))
                return Single.just(UserRolesAndPermissions.createSuperUserRolesAndPermissions(name, resources));

            if (!authorizer.requireAuthorization())
                return Single.just(UserRolesAndPermissions.newNormalUserRoles(name, resources));

            Map<RoleResource, Map<IResource, PermissionSets>> permissions = permissionsCache.getAllPresent(resources);

            if (permissions.size() == resources.size())
                return Single.just(UserRolesAndPermissions.newNormalUserRolesAndPermissions(name,
                                                                                            resources,
                                                                                            permissions));

            Single<UserRolesAndPermissions> rolesAndPermissions = Single.defer(() ->
            {
                return loadPermissionsIfNeeded(name, resources);
            });

            return RxThreads.subscribeOnIo(rolesAndPermissions, TPCTaskType.AUTHORIZATION);
        }

        Single<UserRolesAndPermissions> rolesAndPermissions = Single.defer(() ->
        {
            return loadRolesAndPermissionsIfNeeded(name, primaryRole);
        });

        return RxThreads.subscribeOnIo(rolesAndPermissions, TPCTaskType.AUTHORIZATION);
    }
    
    /**
     * Retrieves the roles and permissions from the cache allowing it to fetch them from the disk if needed.
     *
     * @param name the user name
     * @param primaryRole the user primary role
     * @return roles and permissions for the specified user
     */
    private Single<UserRolesAndPermissions> loadRolesAndPermissionsIfNeeded(String name, RoleResource primaryRole)
    {
        Map<RoleResource, Role> rolePerResource;
        rolePerResource = rolesCache.getRoles(primaryRole);

        Set<RoleResource> roleResources = rolePerResource.keySet();

        if (isSuperUser(rolePerResource.values()))
            return Single.just(UserRolesAndPermissions.createSuperUserRolesAndPermissions(name,
                                                                                          roleResources));

        if (!authorizer.requireAuthorization())
            return Single.just(UserRolesAndPermissions.newNormalUserRoles(name, roleResources));

        return Single.just(UserRolesAndPermissions.newNormalUserRolesAndPermissions(name,
                                                                                    roleResources,
                                                                                    permissionsCache.getAll(roleResources)));
    }

    /**
     * Retrieves the permissions from the cache allowing it to fetch them from the disk if needed.
     *
     * @param user the user name
     * @param roleResources the user roles
     * @return roles and permissions for the specified user
     */
    private Single<UserRolesAndPermissions> loadPermissionsIfNeeded(String user, Set<RoleResource> roleResources)
    {
        return Single.just(UserRolesAndPermissions.newNormalUserRolesAndPermissions(user,
                                                                                    roleResources,
                                                                                    permissionsCache.getAll(roleResources)));
    }

    /**
     * Returns the credentials for the supplied role.
     * <p>If the credentials data are not cached and the calling thread is a TPC thread,
     * the returned {@code Single} might be a deferred one which is scheduled to run on an IO thread.</p>
     * @param username the user name
     */
    Single<String> getCredentials(String username)
    {
        RoleResource resource = RoleResource.role(username);

        if (!TPC.isTPCThread())
            return Single.just(rolesCache.get(resource).hashedPassword);

        Role role = rolesCache.getIfPresent(resource);

        if (role != null)
            return Single.just(role.hashedPassword);

        Single<String> credential = Single.defer(() -> Single.just(rolesCache.get(resource).hashedPassword));

        return RxThreads.subscribeOnIo(credential, TPCTaskType.AUTHORIZATION);
    }


    /**
     * Get all roles granted to the supplied Role, including both directly granted
     * and inherited roles.
     * <p>If the role data are not cached and the calling thread is a TPC thread,
     * the returned {@code Single} might be a deferred one which is scheduled to run on an IO thread.</p>
     *
     * @param primaryRole the Role
     * @return set of all granted Roles for the primary Role
     */
    public Single<Set<RoleResource>> getRoles(RoleResource primaryRole)
    {
        if (!TPC.isTPCThread())
            return Single.just(unmodifiableSet(rolesCache.getRoles(primaryRole).keySet()));

        Map<RoleResource, Role> rolesPerResources = rolesCache.getRolesIfPresent(primaryRole);

        if (rolesPerResources != null)
            return Single.just(unmodifiableSet(rolesPerResources.keySet()));

        Single<Set<RoleResource>> credential = Single.defer(() -> Single.just(unmodifiableSet(rolesCache.getRoles(primaryRole).keySet())));

        return RxThreads.subscribeOnIo(credential, TPCTaskType.AUTHORIZATION);
    }

    /**
     * Retrieve all permissions on all resources for the given, particular role but
     * not any transitively assigned role.
     * <p>If the permission data are not cached and the calling thread is a TPC thread,
     * the returned {@code Single} might be a deferred one which is scheduled to run on an IO thread.</p>
     */
    public Single<Map<IResource, PermissionSets>> getPermissions(RoleResource role)
    {
        if (!TPC.isTPCThread())
            return Single.just(unmodifiableMap(permissionsCache.get(role)));

        Map<IResource, PermissionSets> permissions = permissionsCache.getIfPresent(role);

        if (permissions != null)
            return Single.just(unmodifiableMap(permissions));

        Single<Map<IResource, PermissionSets>> single = Single.defer(() -> Single.just(unmodifiableMap(permissionsCache.get(role))));

        return RxThreads.subscribeOnIo(single, TPCTaskType.AUTHORIZATION);
    }

    /**
     * Returns {@code true} if the supplied role or any other role granted to it
     * (directly or indirectly) has superuser status.
     * <p>If the role data are not cached and the calling thread is a TPC thread,
     * the returned {@code Single} might be a deferred one which is scheduled to run on an IO thread.</p>
     *
     * @param role the primary role
     * @return {@code true} if the role has superuser status, {@code false} otherwise
     */
    public Single<Boolean> hasSuperUserStatus(RoleResource role)
    {
        if (!TPC.isTPCThread())
            return Single.just(isSuperUser(rolesCache.getRoles(role).values()));

        // If we are on the TPC thread we need to ensure that either all the needed data are in the cache or
        // that we switch to an IO thread if the data is not in the cache.
        Map<RoleResource, Role> rolePerResource = rolesCache.getRolesIfPresent(role);

        if (rolePerResource != null)
            return Single.just(isSuperUser(rolePerResource.values()));

        Single<Boolean> single = Single.defer(() -> Single.just(isSuperUser(rolesCache.getRoles(role).values())));

        return RxThreads.subscribeOnIo(single, TPCTaskType.AUTHORIZATION);
    }

    /**
     * Returns {@code true} if the supplied role is allowed to login.
     * <p>If the role data are not cached and the calling thread is a TPC thread,
     * the returned {@code Single} might be a deferred one which is scheduled to run on an IO thread.</p>
     *
     * @param role the primary role
     * @return {@code true} if the role is allowed to login, {@code false} otherwise
     */
    public Single<Boolean> canLogin(RoleResource role)
    {
        if (!roleManager.transitiveRoleLogin())
        {
            if (!TPC.isTPCThread())
                return Single.just(rolesCache.get(role).canLogin);

            Role r = rolesCache.getIfPresent(role);
            if (r != null)
                return Single.just(r.canLogin);

            Single<Boolean> single = Single.defer(() -> Single.just(rolesCache.get(role).canLogin));
            return RxThreads.subscribeOnIo(single, TPCTaskType.AUTHORIZATION);
        }

        if (!TPC.isTPCThread())
            return Single.just(canLogin(rolesCache.getRoles(role).values()));

        Map<RoleResource, Role> rolePerResource = rolesCache.getRolesIfPresent(role);

        if (rolePerResource != null)
            return Single.just(canLogin(rolePerResource.values())); 

        Single<Boolean> single = Single.defer(() -> Single.just(canLogin(rolesCache.getRoles(role).values())));

        return RxThreads.subscribeOnIo(single, TPCTaskType.AUTHORIZATION);
    }

    private boolean isSuperUser(Iterable<Role> roles)
    {
       for (Role role : roles)
       {
           if (role.isSuper)
               return true;
       }
       return false;
    }

    private boolean canLogin(Iterable<Role> roles)
    {
       for (Role role : roles)
       {
           if (role.canLogin)
               return true;
       }
       return false;
    }

    /**
     * Returns a decorated {@code RoleManager} that will makes sure that cache data are properly invalidated when
     * roles are modified.
     */
    public IRoleManager getRoleManager()
    {
        return roleManager;
    }

    /**
     * Returns a decorated {@code IAuthorizer} that will makes sure that cache data are properly invalidated when
     * permissions are modified.
     */
    public IAuthorizer getAuthorizer()
    {
        return authorizer;
    }

    void handleRoleInvalidation(RoleInvalidation invalidation)
    {
        invalidate(invalidation.roles);
    }

    private void invalidateRoles(Collection<RoleResource> roles)
    {
        invalidate(roles);
        pushRoleInvalidation(roles);
    }

    private void pushRoleInvalidation(Collection<RoleResource> roles)
    {
        RoleInvalidation invalidation = new RoleInvalidation(roles);
        for (InetAddress endpoint : Gossiper.instance.getLiveMembers())
        {// only push schema to nodes with known and equal versions
            if (!endpoint.equals(FBUtilities.getBroadcastAddress()) &&
                MessagingService.instance().knowsVersion(endpoint) &&
                MessagingService.instance().getRawVersion(endpoint) == MessagingService.current_version)
            {
                MessagingService.instance().send(Verbs.AUTH.INVALIDATE.newRequest(endpoint, invalidation));
            }
        }
    }

    @VisibleForTesting
    public void invalidateCaches()
    {
        permissionsCache.invalidate();
        rolesCache.invalidate();
    }

    private void invalidate(Collection<RoleResource> roles)
    {
        if (roles.isEmpty())
        {
            permissionsCache.invalidate();
            rolesCache.invalidate();
        }
        else
        {
            for (RoleResource role : roles)
            {
                permissionsCache.invalidate(role);
                rolesCache.invalidate(role);
            }
        }
    }

    /**
     * Decorator used to invalidate cache data on role modifications
     */
    private class RoleManagerCacheInvalidator implements IRoleManager
    {
        private final IRoleManager roleManager;

        private final IAuthorizer authorizer;

        public RoleManagerCacheInvalidator(IRoleManager roleManager, IAuthorizer authorizer)
        {
            this.roleManager = roleManager;
            this.authorizer = authorizer;
        }

        @Override
        public <T extends IRoleManager> T implementation()
        {
            return (T) roleManager;
        }

        @Override
        public Set<Option> supportedOptions()
        {
            return roleManager.supportedOptions();
        }

        @Override
        public Set<Option> alterableOptions()
        {
            return roleManager.alterableOptions();
        }

        @Override
        public void createRole(AuthenticatedUser performer,
                               RoleResource role,
                               RoleOptions options)
        {
            roleManager.createRole(performer, role, options);
            invalidateRoles(Collections.singleton(role));
        }

        @Override
        public void dropRole(AuthenticatedUser performer,
                             RoleResource role)
        {
            // We do not want to load those roles within the cache so we have to fetch them directly from
            // the IRoleManager
            Set<RoleResource> roles = getRoles(role, true);
            roleManager.dropRole(performer, role);
            authorizer.revokeAllFrom(role);
            authorizer.revokeAllOn(role);
            invalidateRoles(roles);
        }

        @Override
        public void alterRole(AuthenticatedUser performer,
                              RoleResource role,
                              RoleOptions options)
        {
            // We do not want to load those roles within the cache so we have to fetch them directly from
            // the IRoleManager
            Set<RoleResource> roles = getRoles(role, true);
            roleManager.alterRole(performer, role, options);
            invalidateRoles(roles);
        }

        @Override
        public void grantRole(AuthenticatedUser performer,
                              RoleResource role,
                              RoleResource grantee)
        {
            // We do not want to load those roles within the cache so we have to fetch them directly from
            // the IRoleManager
            Set<RoleResource> roles = getRoles(role, true);
            roles.addAll(getRoles(grantee, true));
            roleManager.grantRole(performer, role, grantee);
            invalidateRoles(roles);
        }

        @Override
        public void revokeRole(AuthenticatedUser performer,
                               RoleResource role,
                               RoleResource revokee)
        {
            // We do not want to load those roles within the cache so we have to fetch them directly from
            // the IRoleManager
            Set<RoleResource> roles = getRoles(role, true);
            roles.addAll(getRoles(revokee, true));
            roleManager.revokeRole(performer, role, revokee);
            invalidateRoles(roles);
        }

        @Override
        public Set<RoleResource> getRoles(RoleResource grantee,
                                          boolean includeInherited)
        {
            return roleManager.getRoles(grantee, includeInherited);
        }

        @Override
        public Set<RoleResource> getAllRoles()
        {
            return roleManager.getAllRoles();
        }

        @Override
        public boolean isSuper(RoleResource role)
        {
            return roleManager.isSuper(role);
        }

        @Override
        public boolean canLogin(RoleResource role)
        {
            return roleManager.canLogin(role);
        }

        @Override
        public boolean transitiveRoleLogin()
        {
            return roleManager.transitiveRoleLogin();
        }

        @Override
        public Map<String, String> getCustomOptions(RoleResource role)
        {
            return roleManager.getCustomOptions(role);
        }

        @Override
        public boolean isExistingRole(RoleResource role)
        {
            return roleManager.isExistingRole(role);
        }

        @Override
        public Set<RoleResource> filterExistingRoleNames(List<String> roleNames)
        {
            return roleManager.filterExistingRoleNames(roleNames);
        }

        @Override
        public Role getRoleData(RoleResource role)
        {
            return roleManager.getRoleData(role);
        }

        @Override
        public Set<? extends IResource> protectedResources()
        {
            return roleManager.protectedResources();
        }

        @Override
        public void validateConfiguration() throws ConfigurationException
        {
            roleManager.validateConfiguration();
        }

        @Override
        public Future<?> setup()
        {
            return roleManager.setup();
        }

        @Override
        public boolean hasSuperuserStatus(RoleResource role)
        {
            return roleManager.hasSuperuserStatus(role);
        }
    }

    /**
     * Decorator used to invalidate cache data on role modifications
     */
    private class AuthorizerInvalidator implements IAuthorizer
    {
        private final IRoleManager roleManager;

        private final IAuthorizer authorizer;

        public AuthorizerInvalidator(IRoleManager roleManager, IAuthorizer authorizer)
        {
            this.roleManager = roleManager;
            this.authorizer = authorizer;
        }

        @Override
        public <T extends IAuthorizer> T implementation()
        {
            return (T) authorizer;
        }

        @Override
        public boolean requireAuthorization()
        {
            return authorizer.requireAuthorization();
        }

        @Override
        public Map<IResource, PermissionSets> allPermissionSets(RoleResource role)
        {
            return authorizer.allPermissionSets(role);
        }

        @Override
        public void grant(AuthenticatedUser performer,
                          Set<Permission> permissions,
                          IResource resource,
                          RoleResource grantee,
                          GrantMode... grantModes)
        {
            // We do not want to load those roles within the cache so we have to fetch them directly from 
            // the IRoleManager
            Set<RoleResource> roles = roleManager.getRoles(grantee, true);
            authorizer.grant(performer, permissions, resource, grantee, grantModes);
            invalidateRoles(roles);
        }

        @Override
        public void revoke(AuthenticatedUser performer,
                           Set<Permission> permissions,
                           IResource resource,
                           RoleResource revokee,
                           GrantMode... grantModes)
        {
            // We do not want to load those roles within the cache so we have to fetch them directly from
            // the IRoleManager
            Set<RoleResource> roles = roleManager.getRoles(revokee, true);
            authorizer.revoke(performer, permissions, resource, revokee, grantModes);
            invalidateRoles(roles);
        }

        @Override
        public Set<PermissionDetails> list(Set<Permission> permissions,
                                           IResource resource,
                                           RoleResource grantee)
        {
            return authorizer.list(permissions, resource, grantee);
        }

        @Override
        public void revokeAllFrom(RoleResource revokee)
        {
            // We do not want to load those roles within the cache so we have to fetch them directly from
            // the IRoleManager
            Set<RoleResource> roles = roleManager.getRoles(revokee, true);
            authorizer.revokeAllFrom(revokee);
            invalidateRoles(roles);
        }

        @Override
        public void revokeAllOn(IResource droppedResource)
        {
            authorizer.revokeAllOn(droppedResource);
        }

        @Override
        public Set<? extends IResource> protectedResources()
        {
            return authorizer.protectedResources();
        }

        @Override
        public void validateConfiguration() throws ConfigurationException
        {
            authorizer.validateConfiguration();
        }

        @Override
        public void setup()
        {
            authorizer.setup();
        }

        @Override
        public Set<Permission> applicablePermissions(IResource resource)
        {
            return authorizer.applicablePermissions(resource);
        }
    }
}
