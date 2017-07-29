/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.auth;

import java.net.InetAddress;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;

import io.reactivex.Completable;
import io.reactivex.internal.schedulers.ImmediateThinScheduler;
import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Helper class for roles, authorization and authentication.
 */
public final class Auth
{
    private static PermissionsCache permissionsCache;
    private static RolesCache rolesCache;

    private static final List<CacheInvalidator> cacheInvalidators = new ArrayList<>();

    private Auth()
    {
    }

    static
    {
        setupCaches();

        registerCacheInvalidator(new CacheInvalidator()
        {
            public void invalidate()
            {
                permissionsCache.invalidate();
                rolesCache.invalidate();
            }

            public void invalidate(RoleResource role)
            {
                permissionsCache.invalidate(role);
                rolesCache.invalidate(role);
            }
        });
    }

    /**
     * (Re)initializes the auth caches. Must NOT be used from production code!
     */
    @VisibleForTesting
    public static void setupCaches()
    {
        AuthCache old = permissionsCache;
        permissionsCache = new PermissionsCache();
        if (old != null)
            old.invalidate();

        old = rolesCache;
        rolesCache = new RolesCache(DatabaseDescriptor.getRoleManager());
        if (old != null)
            old.invalidate();
    }

    /**
     * Retrieve all permissions on all resources for the given, particular role but
     * not any transitively assigned role. This function returns a <em>shared</em>
     * instance - i.e. the returned map must <em>not</em> be modified.
     */
    public static Map<IResource, PermissionSets> getPermissions(RoleResource role)
    {
        return permissionsCache.getPermissions(role);
    }

    /**
     * Get all roles granted to the supplied Role, including both directly granted
     * and inherited roles.
     * The returned roles may be cached if {@code roles_validity_in_ms > 0}.
     * Calls from a TPC thread should be prevented.
     *
     * @param primaryRole the Role
     * @return set of all granted Roles for the primary Role
     */
    public static Set<RoleResource> getRoles(RoleResource primaryRole)
    {
        Set<RoleResource> roles = new HashSet<>();
        collectRoles(primaryRole, roles);
        return roles;
    }

    private static void collectRoles(RoleResource role, Set<RoleResource> roles)
    {
        if (role == null)
            return;

        if (!roles.add(role))
            return;

        Set<RoleResource> memberOf = rolesCache.getRoles(role);
        if (memberOf != null)
            for (RoleResource member : memberOf)
            {
                collectRoles(member, roles);
            }
    }

    /**
     * Returns true if the supplied role or any other role granted to it
     * (directly or indirectly) has superuser status.
     *
     * @param role the primary role
     * @return true if the role has superuser status, false otherwise
     * @throws org.apache.cassandra.concurrent.TPCUtils.WouldBlockException if the calling thread is a TPC thread and the fetching the result would block.
     */
    public static boolean hasSuperuserStatus(RoleResource role)
    {
        for (RoleResource r : getRoles(role))
            if (rolesCache.isSuperuser(r))
                return true;
        return false;
    }

    /**
     * Returns true if the supplied role is allowed to login.
     *
     * @param role the primary role
     * @return true if the role is allowed to login, false otherwise
     * @throws org.apache.cassandra.concurrent.TPCUtils.WouldBlockException if the calling thread is a TPC thread and the fetching the result would block.
     */
    public static boolean canLogin(RoleResource role)
    {
        if (!DatabaseDescriptor.getRoleManager().transitiveRoleLogin())
            return rolesCache.canLogin(role);

        for (RoleResource r : getRoles(role))
            if (rolesCache.canLogin(r))
                return true;
        return false;
    }

    /**
     * Returns the credentials for the supplied role.
     *
     * @throws org.apache.cassandra.concurrent.TPCUtils.WouldBlockException if the calling thread is a TPC thread and the fetching the result would block.
     */
    static String getCredentials(RoleResource role)
    {
        return rolesCache.getCredentials(role);
    }

    /**
     * Returns true if the supplied role or any other role granted to it
     * (directly or indirectly) has superuser status.
     * <p>
     * Unlike {@link #hasSuperuserStatus(RoleResource)}, this method will
     * never use any cache and always hit the role manager implementation,
     * intended for DCL statements.
     *
     * @param role the primary role
     * @return true if the role has superuser status, false otherwise
     */
    public static boolean hasSuperuserStatusUncached(RoleResource role)
    {
        IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
        for (RoleResource r : roleManager.getRoles(role, true))
            if (roleManager.isSuper(r))
                return true;
        return false;
    }

    /**
     * Invalidate the given role and all transitively assigned roles cluster wide.
     */
    public static Completable invalidateRolesForPermissionsChange(RoleResource role)
    {
        return invalidateRoles(getRoles(role));
    }

    /**
     * Invalidate the given roles and all transitively assigned roles cluster wide.
     */
    public static Completable invalidateRolesForPermissionsChange(RoleResource role1, RoleResource role2)
    {
        Set<RoleResource> roles = new HashSet<>();

        collectRoles(role1, roles);
        collectRoles(role2, roles);

        return invalidateRoles(roles);
    }

    private static Completable invalidateRoles(Collection<RoleResource> roles)
    {
        RoleInvalidation invalidation = new RoleInvalidation(roles);

        // some unit tests may rely on immediate invalidation
        handleRoleInvalidation(invalidation);

        return Completable.fromRunnable(() -> pushRoleInvalidation(invalidation))
                          .subscribeOn(TPC.isTPCThread()
                                       ? StageManager.getScheduler(Stage.AUTHZ)
                                       : ImmediateThinScheduler.INSTANCE);
    }

    private static void pushRoleInvalidation(RoleInvalidation invalidation)
    {
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

    static void handleRoleInvalidation(RoleInvalidation invalidation)
    {
        if (invalidation.roles.isEmpty())
        {
            cacheInvalidators.forEach(CacheInvalidator::invalidate);
        }
        else
        {
            for (RoleResource role : invalidation.roles)
            {
                cacheInvalidators.forEach(i -> i.invalidate(role));
            }
        }
    }

    public static void registerCacheInvalidator(CacheInvalidator cacheInvalidator)
    {
        synchronized (cacheInvalidators)
        {
            cacheInvalidators.add(cacheInvalidator);
        }
    }

    public interface CacheInvalidator
    {
        void invalidate();

        void invalidate(RoleResource role);
    }
}
