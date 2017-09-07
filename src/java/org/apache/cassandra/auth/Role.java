/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.auth;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Represents all attributes of a role.
 */
public final class Role
{
    // NullObject
    public static final Role NULL_ROLE = new Role("", ImmutableSet.of(), false, false, ImmutableMap.of(), "");

    public final String name;
    public final ImmutableSet<RoleResource> memberOf;
    public final boolean isSuper;
    public final boolean canLogin;
    public final ImmutableMap<String, String> options;
    final String hashedPassword;

    public Role withOptions(ImmutableMap<String, String> options)
    {
        return new Role(name, memberOf, isSuper, canLogin, options, hashedPassword);
    }

    public Role withRoles(ImmutableSet<RoleResource> memberOf)
    {
        return new Role(name, memberOf, isSuper, canLogin, options, hashedPassword);
    }

    public Role(String name,
                ImmutableSet<RoleResource> memberOf,
                boolean isSuper,
                boolean canLogin,
                ImmutableMap<String, String> options,
                String hashedPassword)
    {
        this.name = name;
        this.memberOf = memberOf;
        this.isSuper = isSuper;
        this.canLogin = canLogin;
        this.options = ImmutableMap.copyOf(options);
        this.hashedPassword = hashedPassword;
    }
}
