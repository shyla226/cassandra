/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.auth;

/**
 * Specifies whether the permission shall be {@link #GRANT granted} on a resource,
 * {@link #RESTRICT restricted} on a resource or {@link #GRANTABLE grantable} to others on a resource.
 */
public enum GrantMode
{
    /**
     * GRANT/REVOKE permission on the resource.
     */
    GRANT
    {
        @Override
        public String grantOperationName()
        {
            return "GRANT";
        }

        @Override
        public String revokeOperationName()
        {
            return "REVOKE";
        }

        @Override
        public String revokeWarningMessage(String roleName, IResource resource, String permissions)
        {
            return String.format("Role '%s' was not granted %s on %s", roleName, permissions, resource);
        }

        @Override
        public String grantWarningMessage(String roleName, IResource resource, String permissions)
        {
            return String.format("Role '%s' was already granted %s on %s", roleName, permissions, resource);
        }
    },
    /**
     * RESTRICT/UNRESTRICT permission on the resource.
     */
    RESTRICT
    {
        @Override
        public String grantOperationName()
        {
            return "RESTRICT";
        }

        @Override
        public String revokeOperationName()
        {
            return "UNRESTRICT";
        }

        @Override
        public String revokeWarningMessage(String roleName, IResource resource, String permissions)
        {
            return String.format("Role '%s' was not restricted %s on %s", roleName, permissions, resource);
        }

        @Override
        public String grantWarningMessage(String roleName, IResource resource, String permissions)
        {
            return String.format("Role '%s' was already restricted %s on %s", roleName, permissions, resource);
        }
    },
    /**
     * GRANT/REVOKE grant/authorize permission on the resource.
     */
    GRANTABLE
    {
        @Override
        public String grantOperationName()
        {
            return "GRANT AUTHORIZE FOR";
        }

        @Override
        public String revokeOperationName()
        {
            return "REVOKE AUTHORIZE FOR";
        }

        @Override
        public String revokeWarningMessage(String roleName, IResource resource, String permissions)
        {
            return String.format("Role '%s' was not granted AUTHORIZE FOR %s on %s", roleName, permissions, resource);
        }

        @Override
        public String grantWarningMessage(String roleName, IResource resource, String permissions)
        {
            return String.format("Role '%s' was already granted AUTHORIZE FOR %s on %s", roleName, permissions, resource);
        }
    };

    /**
     * Returns the name of the grant operation for this {@code GrantMode}.
     * @return the name of the grant operation for this {@code GrantMode}.
     */
    public abstract String grantOperationName();

    /**
     * Returns the name of the revoke operation for this {@code GrantMode}.
     * @return the name of the revoke operation for this {@code GrantMode}.
     */
    public abstract String revokeOperationName();

    /**
     * Returns the warning message to send to the user when a revoke or unrestrict operation had not the intended effect.
     * @param roleName the role for which the operation was performed
     * @param resource the resource
     * @param permissions the permissions that should have been revoked/unrestricted
     * @return the warning message to send to the user when a revoke or unrestrict operation had not the intended effect.
     */
    public abstract String revokeWarningMessage(String roleName, IResource resource, String permissions);

    /**
     * Returns the warning message to send to the user when a grant or restrict operation had not the intended effect.
     * @param roleName the role for which the operation was performed
     * @param resource the resource
     * @param permissions the permissions that should have been granted/restricted
     * @return the warning message to send to the user when a grant or restrict operation had not the intended effect.
     */
    public abstract String grantWarningMessage(String roleName, IResource resource, String permissions);
}
