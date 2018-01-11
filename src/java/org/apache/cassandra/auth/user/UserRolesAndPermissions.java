/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.auth.user;

import java.util.*;
import java.util.function.Supplier;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.auth.IAuthorizer.TransitionalMode;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;

/**
 * The roles and permissions of an authentified user.
 */
public abstract class UserRolesAndPermissions
{
    private static final Logger logger = LoggerFactory.getLogger(UserRolesAndPermissions.class);

    /**
     * The roles and permissions of the system.
     */
    public static final UserRolesAndPermissions SYSTEM = new UserRolesAndPermissions(AuthenticatedUser.SYSTEM_USERNAME, Collections.emptySet())
    {
        @Override
        public boolean hasGrantPermission(IResource resource, Permission perm)
        {
            return false;
        }

        @Override
        public boolean hasDataPermission(DataResource resource, Permission perm)
        {
            return true;
        }

        @Override
        public boolean isSystem()
        {
            return true;
        }

        @Override
        protected void checkPermissionOnResourceChain(IResource resource, Permission perm)
        {
        }

        @Override
        protected boolean hasPermissionOnResourceChain(IResource resource, Permission perm)
        {
            return true;
        }

        @Override
        public boolean hasJMXPermission(MBeanServer mbs, ObjectName object, Permission permission)
        {
            return true;
        }

        public void additionalQueryPermission(IResource resource, PermissionSets permissionSets)
        {
        }
    };

    /**
     * The roles and permissions of an anonymous user.
     * <p>
     * Anonymous users only exists in transitional mode (when the permission are being set up).
     * Whether an anonymous user can access any table data, depends on the transitional mode being configured.
     * But anonymous users are generally blocked from performing any role or permissions operations
     * (via {@link #checkNotAnonymous()}).
     * </p>
     */
    public static final UserRolesAndPermissions ANONYMOUS = new UserRolesAndPermissions(AuthenticatedUser.ANONYMOUS_USERNAME, Collections.emptySet())
    {
        @Override
        public void checkNotAnonymous()
        {
            throw new UnauthorizedException("Anonymous users are not authorized to perform this request");
        }

        @Override
        public boolean hasGrantPermission(IResource resource, Permission perm)
        {
            return false;
        }

        @Override
        protected boolean hasPermissionOnResourceChain(IResource resource, Permission perm)
        {
            return checkPermission(perm);
        }

        @Override
        public boolean hasJMXPermission(MBeanServer mbs, ObjectName object, Permission permission)
        {
            return checkPermission(permission);
        }

        @Override
        protected void checkPermissionOnResourceChain(IResource resource, Permission perm)
        {
            if (!checkPermission(perm))
                throw new UnauthorizedException("Anonymous users are not authorized to perform this request");
        }

        private boolean checkPermission(Permission perm)
        {
            IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
            TransitionalMode mode = authorizer.getTransitionalMode();

            // Anonymous can never have AUTHORIZE permission.
            if (perm == CorePermission.AUTHORIZE || !mode.supportPermission(perm))
                return false;

            if (!authorizer.requireAuthorization())
                // Authentication is not enabled - go ahead.
                return true;

            // Transitional mode defines, that anonymous can do "everything" - go ahead.
            return !mode.enforcePermissionsAgainstAnonymous();
        }

        public void additionalQueryPermission(IResource resource, PermissionSets permissionSets)
        {
        }
    };

    /**
     * The roles and permissions of an in-inprocess user.
     * <p>
     *
     * </p>
     */
    public static final UserRolesAndPermissions INPROC = new UserRolesAndPermissions(AuthenticatedUser.INPROC_USERNAME, Collections.emptySet())
    {
        @Override
        public boolean isSuper()
        {
            return true;
        }

        @Override
        public boolean isSystem()
        {
            return true;
        }

        @Override
        public void checkNotAnonymous()
        {
            throw new UnauthorizedException("In-proc users are not authorized to perform this request");
        }

        @Override
        public boolean hasGrantPermission(IResource resource, Permission perm)
        {
            return false;
        }

        @Override
        protected boolean hasPermissionOnResourceChain(IResource resource, Permission perm)
        {
            return checkPermission(perm);
        }

        @Override
        public boolean hasJMXPermission(MBeanServer mbs, ObjectName object, Permission permission)
        {
            return checkPermission(permission);
        }

        @Override
        protected void checkPermissionOnResourceChain(IResource resource, Permission perm)
        {
            if (!checkPermission(perm))
                throw new UnauthorizedException("In-proc users are not authorized to perform this request");
        }

        private boolean checkPermission(Permission perm)
        {
            if (perm == CorePermission.AUTHORIZE)
                // In proc can never have AUTHORIZE permission.
                return false;
            return true;
        }

        public void additionalQueryPermission(IResource resource, PermissionSets permissionSets)
        {
        }
    };

    /**
     * The user name.
     */
    private final String name;

    /**
     * The name used for the authentification.
     */
    private final String authenticatedName;

    /**
     * The user roles.
     */
    private final Set<RoleResource> roles;

    private UserRolesAndPermissions(String name, String authenticatedName, Set<RoleResource> roles)
    {
        this.name = name;
        this.authenticatedName = authenticatedName;
        this.roles = roles;
    }

    private UserRolesAndPermissions(String name, Set<RoleResource> roles)
    {
        this.name = name;
        this.authenticatedName = name;
        this.roles = roles;
    }

    /**
     * Creates a new user with the specified name and roles.
     * @param name the user name
     * @param authenticatedName the name used for the authentification
     * @param roles the user roles
     * @return a new user
     */
    public static UserRolesAndPermissions newNormalUserRoles(String name, String authenticatedName, Set<RoleResource> roles)
    {
        assert !DatabaseDescriptor.getAuthorizer().requireAuthorization() : "An user without permissions can only created when the authorization are not required";
        return new NormalUserRoles(name, authenticatedName, roles);
    }

    /**
     * Creates a new user with the specified name, roles and permissions.
     * @param name the user name
     * @param authenticatedName the name used for the authentification
     * @param roles the user roles
     * @param permissions the permissions per role and resources.
     * @return a new user
     */
    public static UserRolesAndPermissions newNormalUserRolesAndPermissions(String name,
                                                                           String authenticatedName,
                                                                           Set<RoleResource> roles,
                                                                           Map<RoleResource, Map<IResource, PermissionSets>> permissions)
    {
        return new NormalUserWithPermissions(name, authenticatedName, roles, permissions);
    }

    /**
     * Creates a new super user with the specified name and roles.
     * @param name the user name
     * @param authenticatedName the name used for the authentification
     * @param roles the user roles
     * @return a new super user
     */
    public static UserRolesAndPermissions createSuperUserRolesAndPermissions(String name,
                                                                             String authenticatedName,
                                                                             Set<RoleResource> roles)
    {
        return new SuperUserRoleAndPermissions(name, authenticatedName, roles);
    }

    /**
     * Returns the user name.
     * @return the user name.
     */
    public final String getName()
    {
        return name;
    }

    /**
     * Returns the name used for the authentification.
     * @return the name used for the authentification
     */
    public final String getAuthenticatedName()
    {
        return authenticatedName;
    }

    /**
     * Checks if this user is a super user.
     * <p>Only a superuser is allowed to perform CREATE USER and DROP USER queries.
     * Im most cased, though not necessarily, a superuser will have Permission.ALL on every resource
     * (depends on IAuthorizer implementation).</p>
     */
    public boolean isSuper()
    {
        return false;
    }

    /**
     * Checks if this user is the system user (Apollo).
     * @return {@code true} if this user is the system user, {@code flase} otherwise.
     */
    public boolean isSystem()
    {
        return false;
    }

    /**
     * Validates that this user is not an anonymous one.
     */
    public void checkNotAnonymous()
    {
    }

    /**
     * Checks if the user has the specified permission on the data resource.
     * @param resource the resource
     * @param perm the permission
     * @return {@code true} if the user has the permission on the data resource,
     * {@code false} otherwise.
     */
    public boolean hasDataPermission(DataResource resource, Permission perm)
    {
        if (!DataResource.root().equals(resource))
        {
            // we only care about schema modification.
            if (isSchemaModification(perm))
            {
                String keyspace = resource.getKeyspace();

                try
                {
                    preventSystemKSSchemaModification(resource, perm);
                }
                catch (UnauthorizedException e)
                {
                    return false;
                }

                if (SchemaConstants.isReplicatedSystemKeyspace(keyspace)
                        && perm != CorePermission.ALTER
                        && !(perm == CorePermission.DROP && Resources.isDroppable(resource)))
                {
                        return false;
                }
            }

            if (perm == CorePermission.SELECT && Resources.isAlwaysReadable(resource))
                return true;

            if (Resources.isProtected(resource) && isSchemaModification(perm))
                return false;
        }

        return hasPermissionOnResourceChain(resource, perm);
    }

    /**
     * Checks if the user has the specified permission on the role resource.
     * @param resource the resource
     * @param perm the permission
     * @return {@code true} if the user has the permission on the role resource,
     * {@code false} otherwise.
     */
    public final boolean hasRolePermission(RoleResource resource, Permission perm)
    {
        return hasPermissionOnResourceChain(resource, perm);
    }

    /**
     * Checks if the user has the specified permission on the function resource.
     * @param resource the resource
     * @param perm the permission
     * @return {@code true} if the user has the permission on the function resource,
     * {@code false} otherwise.
     */
    public final boolean hasFunctionPermission(FunctionResource resource, Permission perm)
    {
        // Access to built in functions is unrestricted
        if (resource.hasParent() && isNativeFunction(resource))
            return true;

        return hasPermissionOnResourceChain(resource, perm);
    }

    /**
     * Checks if the user has the specified permission on the JMX object.
     * @param mbs The MBeanServer
     * @param object the object
     * @param permission the permission
     * @return {@code true} if the user has the permission on the function resource,
     * {@code false} otherwise.
     */
    public abstract boolean hasJMXPermission(MBeanServer mbs, ObjectName object, Permission permission);

    /**
     * Checks if the user has the specified permission on the resource.
     * @param resource the resource
     * @param perm the permission
     * @return {@code true} if the user has the permission on the resource,
     * {@code false} otherwise.
     */
    public final boolean hasPermission(IResource resource, Permission perm)
    {
        if (resource instanceof DataResource)
            return hasDataPermission((DataResource) resource, perm);

        if (resource instanceof FunctionResource)
            return hasFunctionPermission((FunctionResource) resource, perm);

         return hasPermissionOnResourceChain(resource, perm);
    }

    /**
     * Checks if the user has the right to grant the permission on the resource.
     * @param resource the resource
     * @param perm the permission
     * @return {@code true} if the user has the right to grant the permission on the resource,
     * {@code false} otherwise.
     */
    public abstract boolean hasGrantPermission(IResource resource, Permission perm);

    /**
     * Validates that the user has the permission on the data resource.
     * @param resource the resource
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on the data resource
     */
    public final void checkDataPermission(DataResource resource, Permission perm)
    {
        if (!DataResource.root().equals(resource))
        {
            preventSystemKSSchemaModification(resource, perm);

            if (perm == CorePermission.SELECT && Resources.isAlwaysReadable(resource))
                return;

            if (Resources.isProtected(resource))
                if (isSchemaModification(perm))
                    throw new UnauthorizedException(String.format("%s schema is protected", resource));
        }

        checkPermissionOnResourceChain(resource, perm);
    }

    private void preventSystemKSSchemaModification(DataResource resource, Permission perm)
    {
        String keyspace = resource.getKeyspace();
        validateKeyspace(keyspace);

        // we only care about schema modification.
        if (!isSchemaModification(perm))
            return;

        // prevent system keyspace modification (not only because this could be dangerous, but also because this just
        // wouldn't work with the way the schema of those system keyspace/table schema is hard-coded)
        if (SchemaConstants.isLocalSystemKeyspace(keyspace))
            throw new UnauthorizedException(keyspace + " keyspace is not user-modifiable.");

        // Allow users with sufficient privileges to alter options on certain distributed system keyspaces/tables.
        // We only allow ALTER, not CREATE nor DROP, outside of a few specific tables that can be dropped  because they
        // are not used anymore but we prefer leaving user the responsibility to drop them. Note that even when altering
        // is allowed, only the table options can be altered but any change to the table schema (adding/removing columns
        // typically) is forbidden by AlterTableStatement.
        if (SchemaConstants.isReplicatedSystemKeyspace(keyspace))
        {
            if (perm != CorePermission.ALTER && !(perm == CorePermission.DROP && Resources.isDroppable(resource)))
                throw new UnauthorizedException(String.format("Cannot %s %s", perm, resource));
        }
    }

    /**
     * Checks if the specified permission is for a schema modification.
     * @param perm the permission to check
     * @return {@code true} if the permission is for a schema modification, {@code false} otherwise.
     */
    private boolean isSchemaModification(Permission perm)
    {
        return (perm == CorePermission.CREATE) || (perm == CorePermission.ALTER) || (perm == CorePermission.DROP);
    }

    /**
     * Validates that the user has the permission on all the keyspaces.
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on all the keyspaces
     */
    public final void checkAllKeyspacesPermission(Permission perm)
    {
        checkDataPermission(DataResource.root(), perm);
    }

    /**
     * Validates that the user has the permission on the keyspace.
     * @param keyspace the keyspace
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on the keyspace
     */
    public final void checkKeyspacePermission(String keyspace, Permission perm)
    {
        validateKeyspace(keyspace);
        checkDataPermission(DataResource.keyspace(keyspace), perm);
    }

    /**
     * Validates that the specified keyspace is not {@code null}.
     * @param keyspace the keyspace to check.
     */
    protected static void validateKeyspace(String keyspace)
    {
        if (keyspace == null)
            throw new InvalidRequestException("You have not set a keyspace for this session");
    }

    /**
     * Validates that the user has the permission on the table.
     * @param keyspace the table keyspace
     * @param table the table
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on the table.
     */
    public final void checkTablePermission(String keyspace, String table, Permission perm)
    {
        Schema.instance.validateTable(keyspace, table);
        checkDataPermission(DataResource.table(keyspace, table), perm);
    }

    /**
     * Validates that the user has the permission on the table.
     * @param tableRef the table
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on the table.
     */
    public final void checkTablePermission(TableMetadataRef tableRef, Permission perm)
    {
        checkTablePermission(tableRef.get(), perm);
    }

    /**
     * Validates that the user has the permission on the table.
     * @param table the table metadata
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on the table.
     */
    public final void checkTablePermission(TableMetadata table, Permission perm)
    {
        checkDataPermission(table.resource, perm);
    }

    /**
     * Validates that the user has the permission on the function.
     * @param function the function
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on the function.
     */
    public final void checkFunctionPermission(Function function, Permission perm)
    {
        // built in functions are always available to all
        if (function.isNative())
            return;

        checkPermissionOnResourceChain(FunctionResource.function(function.name().keyspace,
                                                                 function.name().name,
                                                                 function.argTypes()), perm);
    }

    /**
     * Validates that the user has the permission on the function.
     * @param resource the resource
     * @param perm the function resource
     * @throws UnauthorizedException if the user does not have the permission on the function.
     */
    public final void checkFunctionPermission(FunctionResource resource, Permission perm)
    {
        // Access to built in functions is unrestricted
        if(resource.hasParent() && isNativeFunction(resource))
            return;

        checkPermissionOnResourceChain(resource, perm);
    }

    private boolean isNativeFunction(FunctionResource resource)
    {
        return resource.getKeyspace().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME);
    }

    public final void checkPermission(IResource resource, Permission perm)
    {
        if (resource instanceof DataResource)
        {
            checkDataPermission((DataResource) resource, perm);
        }
        else if (resource instanceof FunctionResource)
        {
            checkFunctionPermission((FunctionResource) resource, perm);
        }
        else
        {
            checkPermissionOnResourceChain(resource, perm);
        }
    }

    protected abstract void checkPermissionOnResourceChain(IResource resource, Permission perm);

    protected abstract boolean hasPermissionOnResourceChain(IResource resource, Permission perm);

    /**
     * Used by Row-Level-Access-Control to inject "arbitrary" permissions.
     */
    public abstract void additionalQueryPermission(IResource resource, PermissionSets permissionSets);

    /**
     * Checks that this user has the specified role.
     * @param role the role
     * @return {@code true} if the user has the specified role, {@code false} otherwise.
     */
    public final boolean hasRole(RoleResource role)
    {
        return roles.contains(role);
    }

    public final Set<RoleResource> getRoles()
    {
        return roles;
    }

    /**
     * Retrieve the aggregated permissions for the role-resources accepted by the provided filter.
     *
     * @param applicablePermissions when there is no authenticated user, the user is a superuser or authorization
     *                              is not required, a permission set with granted and grantables set to the
     *                              permissions provided by this supplier will be used
     */
    public <R> R filterPermissions(java.util.function.Function<R, R> applicablePermissions,
                                   Supplier<R> initialState,
                                   RoleResourcePermissionFilter<R> aggregate)
    {
        R state = initialState.get();
        state = applicablePermissions.apply(state);
        return state;
    }

    @FunctionalInterface
    public interface RoleResourcePermissionFilter<R>
    {
        R apply(R state, RoleResource role, IResource resource, PermissionSets permissionSets);
    }

    /**
     * A super user.
     */
    private static final class SuperUserRoleAndPermissions extends UserRolesAndPermissions
    {
        public SuperUserRoleAndPermissions(String name, String authenticatedName, Set<RoleResource> roles)
        {
            super(name, authenticatedName, roles);
        }

        @Override
        public boolean isSuper()
        {
            return true;
        }

        @Override
        public boolean hasGrantPermission(IResource resource, Permission perm)
        {
            return true;
        }

        @Override
        protected void checkPermissionOnResourceChain(IResource resource, Permission perm)
        {
        }

        @Override
        protected boolean hasPermissionOnResourceChain(IResource resource, Permission perm)
        {
            return true;
        }

        @Override
        public boolean hasJMXPermission(MBeanServer mbs, ObjectName object, Permission permission)
        {
            return true;
        }

        public void additionalQueryPermission(IResource resource, PermissionSets permissionSets)
        {
        }
    }

    /**
     * A normal user with all his roles and permissions.
     */
    private static final class NormalUserWithPermissions extends UserRolesAndPermissions
    {
        /**
         * The user permissions per role and resources.
         */
        private Map<RoleResource, Map<IResource, PermissionSets>> permissions;
        private Map<IResource, PermissionSets> additionalPermissions;

        public NormalUserWithPermissions(String name,
                                         String authenticatedName,
                                         Set<RoleResource> roles,
                                         Map<RoleResource, Map<IResource, PermissionSets>> permissions)
        {
            super(name, authenticatedName, roles);
            this.permissions = permissions;
        }

        @Override
        public boolean hasGrantPermission(IResource resource, Permission perm)
        {
            for (PermissionSets permissions : getAllPermissionSetsFor(resource))
            {
                if (permissions.grantables.contains(perm))
                    return true;
            }
            return false;
        }

        @Override
        protected void checkPermissionOnResourceChain(IResource resource, Permission perm)
        {
            TransitionalMode mode = DatabaseDescriptor.getAuthorizer().getTransitionalMode();

            if (!mode.supportPermission(perm))
                throw new UnauthorizedException(String.format("User %s has no %s permission on %s or any of its parents",
                                                              getName(),
                                                              perm,
                                                              resource));

            if (!mode.enforcePermissionsOnAuthenticatedUser())
                return;

            boolean granted = false;
            for (PermissionSets permissions : getAllPermissionSetsFor(resource))
            {
                granted |= permissions.granted.contains(perm);
                if (permissions.restricted.contains(perm))
                    throw new UnauthorizedException(String.format("Access for user %s on %s or any of its parents with %s permission is restricted",
                                                                  getName(),
                                                                  resource,
                                                                  perm));
            }

            if (!granted)
                throw new UnauthorizedException(String.format("User %s has no %s permission on %s or any of its parents",
                                                              getName(),
                                                              perm,
                                                              resource));
        }

        private List<PermissionSets> getAllPermissionSetsFor(IResource resource)
        {
            List<? extends IResource> chain = Resources.chain(resource);

            List<PermissionSets> list = new ArrayList<>(chain.size() * (super.roles.size() + 1));

            for (RoleResource roleResource : super.roles)
            {
                for (IResource res : chain)
                {
                    PermissionSets permissions = getPermissions(roleResource, res);
                    if (permissions != null)
                        list.add(permissions);
                }
            }

            if (additionalPermissions != null)
            {
                for (IResource res : chain)
                {
                    PermissionSets permissions = additionalPermissions.get(res);
                    if (permissions != null)
                        list.add(permissions);
                }
            }

            return list;
        }

        @Override
        protected boolean hasPermissionOnResourceChain(IResource resource, Permission perm)
        {
            TransitionalMode mode = DatabaseDescriptor.getAuthorizer().getTransitionalMode();

            if (!mode.supportPermission(perm))
                return false;

            if (!mode.enforcePermissionsOnAuthenticatedUser())
                return true;

            boolean granted = false;
            for (PermissionSets permissions : getAllPermissionSetsFor(resource))
            {
                granted |= permissions.granted.contains(perm);
                if (permissions.restricted.contains(perm))
                    return false;
            }
            return granted;
        }
        private PermissionSets getPermissions(RoleResource roleResource, IResource res)
        {
            Map<IResource, PermissionSets> map = this.permissions.get(roleResource);
            return map == null ? null : map.get(res);
        }

        @Override
        public boolean hasJMXPermission(MBeanServer mbs, ObjectName object, Permission permission)
        {

            TransitionalMode mode = DatabaseDescriptor.getAuthorizer().getTransitionalMode();

            if (!mode.supportPermission(permission))
                return false;

            if (!mode.enforcePermissionsOnAuthenticatedUser())
                return true;

            for (Map<IResource, PermissionSets> permissionsPerResource : permissions.values())
            {
                PermissionSets rootPerms = permissionsPerResource.get(JMXResource.root());
                if (rootPerms != null)
                {
                    if (rootPerms.restricted.contains(permission))
                    {
                        // this role is _restricted_ the required permission on the JMX root resource
                        // I.e. restricted 'permission' on _any_ JMX resource.
                        return false;
                    }
                    if (rootPerms.granted.contains(permission))
                    {
                        // This role is _granted_ (and not restricted) the required permission on the JMX resource root.
                        // I.e. granted 'permission' on _any_ JMX resource.
                        return true;
                    }
                }
            }

            // Check for individual JMX resources.
            //
            // Collecting the (permitted) JMXResource instances before checking the bean names feels
            // cheaper than vice versa - especially for 'checkPattern', which performs a call to
            // javax.management.MBeanServer.queryNames().

            Set<JMXResource> permittedResources = collectPermittedJmxResources(permission);

            if (permittedResources.isEmpty())
                return false;

            // finally, check the JMXResource from the grants to see if we have either
            // an exact match or a wildcard match for the target resource, whichever is
            // applicable
            return object.isPattern()
                    ? checkPattern(mbs, object, permittedResources)
                    : checkExact(mbs, object, permittedResources);
        }

        /**
         * Given a set of JMXResources upon which the Subject has been granted a particular permission, check whether
         * any match the pattern-type ObjectName representing the target of the method invocation. At this point, we are
         * sure that whatever the required permission, the Subject has definitely been granted it against this set of
         * JMXResources. The job of this method is only to verify that the target of the invocation is covered by the
         * members of the set.
         *
         * @param target
         * @param permittedResources
         * @return true if all registered beans which match the target can also be matched by the JMXResources the
         *         subject has been granted permissions on; false otherwise
         */
        private boolean checkPattern(MBeanServer mbs, ObjectName target, Set<JMXResource> permittedResources)
        {
            // Get the full set of beans which match the target pattern
            Set<ObjectName> targetNames = mbs.queryNames(target, null);

            // Iterate over the resources the permission has been granted on. Some of these may
            // be patterns, so query the server to retrieve the full list of matching names and
            // remove those from the target set. Once the target set is empty (i.e. all required
            // matches have been satisfied), the requirement is met.
            // If there are still unsatisfied targets after all the JMXResources have been processed,
            // there are insufficient grants to permit the operation.
            for (JMXResource resource : permittedResources)
            {
                try
                {
                    Set<ObjectName> matchingNames = mbs.queryNames(ObjectName.getInstance(resource.getObjectName()),
                                                                   null);
                    targetNames.removeAll(matchingNames);
                    if (targetNames.isEmpty())
                        return true;
                }
                catch (MalformedObjectNameException e)
                {
                    logger.warn("Permissions for JMX resource contains invalid ObjectName {}",
                                resource.getObjectName());
                }
            }

            return false;
        }

        /**
         * Given a set of JMXResources upon which the Subject has been granted a particular permission, check whether
         * any match the ObjectName representing the target of the method invocation. At this point, we are sure that
         * whatever the required permission, the Subject has definitely been granted it against this set of
         * JMXResources. The job of this method is only to verify that the target of the invocation is matched by a
         * member of the set.
         *
         * @param target
         * @param permittedResources
         * @return true if at least one of the permitted resources matches the target; false otherwise
         */
        private boolean checkExact(MBeanServer mbs, ObjectName target, Set<JMXResource> permittedResources)
        {
            for (JMXResource resource : permittedResources)
            {
                try
                {
                    if (ObjectName.getInstance(resource.getObjectName()).apply(target))
                        return true;
                }
                catch (MalformedObjectNameException e)
                {
                    logger.warn("Permissions for JMX resource contains invalid ObjectName {}",
                                resource.getObjectName());
                }
            }

            logger.trace("Subject does not have sufficient permissions on target MBean {}", target);
            return false;
        }

        private Set<JMXResource> collectPermittedJmxResources(Permission permission)
        {
            Set<JMXResource> permittedResources = new HashSet<>();
            for (Map<IResource, PermissionSets> permissionsPerResource : permissions.values())
            {
                for (Map.Entry<IResource, PermissionSets> resourcePermissionSets : permissionsPerResource.entrySet())
                {
                    if (resourcePermissionSets.getKey() instanceof JMXResource)
                    {
                        if (resourcePermissionSets.getValue().hasEffectivePermission(permission))
                            permittedResources.add((JMXResource) resourcePermissionSets.getKey());
                    }
                }
            }
            return permittedResources;
        }

        public void additionalQueryPermission(IResource resource, PermissionSets permissionSets)
        {
            if (additionalPermissions == null)
                additionalPermissions = new HashMap<>();
            PermissionSets previous = additionalPermissions.putIfAbsent(resource, permissionSets);
            assert previous == null;
        }

        @Override
        public <R> R filterPermissions(java.util.function.Function<R, R> applicablePermissions,
                                       Supplier<R> initialState,
                                       RoleResourcePermissionFilter<R> aggregate)
        {
            R state = initialState.get();
            for (RoleResource roleResource : getRoles())
            {
                Map<IResource, PermissionSets> rolePerms = permissions.get(roleResource);
                if (rolePerms == null)
                    continue;
                for (Map.Entry<IResource, PermissionSets> resourcePerms : rolePerms.entrySet())
                {
                    state = aggregate.apply(state, roleResource, resourcePerms.getKey(), resourcePerms.getValue());
                }
            }
            return state;
        }
    }

    /**
     * A normal user with his roles but without permissions.
     * <p>This user should only be used when {@link IAuthorizer#requireAuthorization()} is {@code false}.</p>
     */
    private static final class NormalUserRoles extends UserRolesAndPermissions
    {
        public NormalUserRoles(String name, String authenticatedName, Set<RoleResource> roles)
        {
            super(name, authenticatedName, roles);
        }

        @Override
        public boolean hasGrantPermission(IResource resource, Permission perm)
        {
            return true;
        }

        @Override
        protected void checkPermissionOnResourceChain(IResource resource, Permission perm)
        {
        }

        @Override
        protected boolean hasPermissionOnResourceChain(IResource resource, Permission perm)
        {
            return true;
        }

        @Override
        public boolean hasJMXPermission(MBeanServer mbs, ObjectName object, Permission permission)
        {
            return true;
        }

        public void additionalQueryPermission(IResource resource, PermissionSets permissionSets)
        {
            throw new UnsupportedOperationException();
        }
    }
}
