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
    GRANT,
    /**
     * RESTRICT/UNRESTRICT permission on the resource.
     */
    RESTRICT,
    /**
     * GRANT/REVOKE grant/authorize permission on the resource.
     */
    GRANTABLE
}
