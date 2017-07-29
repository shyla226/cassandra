/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.auth;

import java.util.Map;
import java.util.Set;

/**
 * Represents all attributes of a role.
 * Never modify any of the contained collections.
 */
public final class Role
{
    public final String name;
    public final Set<RoleResource> memberOf;
    public final boolean isSuper;
    public final boolean canLogin;
    public final Map<String, String> options;
    final String hashedPassword;

    public Role withOptions(Map<String, String> options)
    {
        return new Role(name, memberOf, isSuper, canLogin, options, hashedPassword);
    }

    public Role withRoles(Set<RoleResource> memberOf)
    {
        return new Role(name, memberOf, isSuper, canLogin, options, hashedPassword);
    }

    public Role(String name, Set<RoleResource> memberOf, boolean isSuper, boolean canLogin, Map<String, String> options,
         String hashedPassword)
    {
        this.name = name;
        this.memberOf = memberOf;
        this.isSuper = isSuper;
        this.canLogin = canLogin;
        this.options = options;
        this.hashedPassword = hashedPassword;
    }
}
