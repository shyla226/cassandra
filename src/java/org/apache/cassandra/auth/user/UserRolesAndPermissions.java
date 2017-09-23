/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.auth.user;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.auth.*;
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
    };

    /**
     * The roles and permissions of an anonymous user.
     * <p>Anonymous users only exists in transitional mode (when the permission are being set up).
     * An anonymous user can access any table data but is blocked from performing any role or permissions operations.
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
            return CorePermission.AUTHORIZE != perm;
        }

        @Override
        protected void checkPermissionOnResourceChain(IResource resource, Permission perm)
        {
            if (CorePermission.AUTHORIZE == perm)
                throw new UnauthorizedException("Anonymous users are not authorized to perform this request");
        }
    };

    /**
     * The user name.
     */
    private final String name;

    /**
     * The user roles.
     */
    private final Set<RoleResource> roles;

    private UserRolesAndPermissions(String name, Set<RoleResource> roles)
    {
        this.name = name;
        this.roles = roles;
    }

    /**
     * Creates a new user with the specified name and roles.
     * @param name the user name
     * @param roles the user roles
     * @return a new user
     */
    public static UserRolesAndPermissions newNormalUserRoles(String name, Set<RoleResource> roles)
    {
        assert !DatabaseDescriptor.getAuthorizer().requireAuthorization() : "An user without permissions can only created when the authorization are not required";
        return new NormalUserRoles(name, roles);
    }

    /**
     * Creates a new user with the specified name, roles and permissions.
     * @param name the user name
     * @param roles the user roles
     * @param permissions the permissions per role and resources.
     * @return a new user
     */
    public static UserRolesAndPermissions newNormalUserRolesAndPermissions(String name,
                                                                           Set<RoleResource> roles,
                                                                           Map<RoleResource, Map<IResource, PermissionSets>> permissions)
    {
        return new NormalUserWithPermissions(name, roles, permissions);
    }

    /**
     * Creates a new super user with the specified name and roles.
     * @param name the user name
     * @param roles the user roles
     * @return a new super user
     */
    public static UserRolesAndPermissions createSuperUserRolesAndPermissions(String name, Set<RoleResource> roles)
    {
        return new SuperUserRoleAndPermissions(name, roles);
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
                if (SchemaConstants.isSystemKeyspace(keyspace))
                    return false;

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
        if (SchemaConstants.isSystemKeyspace(keyspace))
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
     * Checks that this user has the specified role.
     * @param role the role
     * @return {@code true} if the user has the specified role, {@code false} otherwise.
     */
    public final boolean hasRole(RoleResource role)
    {
        return roles.contains(role);
    }

    /**
     * A super user.
     */
    private static final class SuperUserRoleAndPermissions extends UserRolesAndPermissions
    {
        public SuperUserRoleAndPermissions(String name, Set<RoleResource> roles)
        {
            super(name, roles);
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

        public NormalUserWithPermissions(String name,
                                         Set<RoleResource> roles,
                                         Map<RoleResource, Map<IResource, PermissionSets>> permissions)
        {
            super(name, roles);
            this.permissions = permissions;
        }

        @Override
        public boolean hasGrantPermission(IResource resource, Permission perm)
        {
            return resourceChainPermissions(resource).grantables.contains(perm);
        }

        @Override
        protected void checkPermissionOnResourceChain(IResource resource, Permission perm)
        {
            PermissionSets chainPermissions = resourceChainPermissions(resource);
            if (!chainPermissions.granted.contains(perm))
                throw new UnauthorizedException(String.format("User %s has no %s permission on %s or any of its parents",
                                                              getName(),
                                                              perm,
                                                              resource));

            if (chainPermissions.restricted.contains(perm))
                throw new UnauthorizedException(String.format("Access for user %s on %s or any of its parents with %s permission is restricted",
                                                              getName(),
                                                              resource,
                                                              perm));
        }

        @Override
        protected boolean hasPermissionOnResourceChain(IResource resource, Permission perm)
        {
            PermissionSets chainPermissions = resourceChainPermissions(resource);
            return chainPermissions.granted.contains(perm) && !chainPermissions.restricted.contains(perm);
        }

        /**
         * Returns a cummulated view of all granted, restricted and grantable permissions on
         * the resource <em>chain</em> of the given resource for this user.
         */
        private PermissionSets resourceChainPermissions(IResource resource)
        {
            PermissionSets.Builder permissions = PermissionSets.builder();

            List<? extends IResource> chain = Resources.chain(resource);
            if (this.permissions != null)
            {
                for (RoleResource roleResource : super.roles)
                    permissions.addChainPermissions(chain, this.permissions.get(roleResource));
            }
            return permissions.buildSingleton();
        }
    }

    /**
     * A normal user with his roles but without permissions.
     * <p>This user should only be used when {@link IAuthorizer#requireAuthorization()} is {@code false}.</p>
     */
    private static final class NormalUserRoles extends UserRolesAndPermissions
    {
        public NormalUserRoles(String name, Set<RoleResource> roles)
        {
            super(name, roles);
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
    }
}
