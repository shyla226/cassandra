/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.auth.permission;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.enums.PartitionedEnum;
import org.apache.cassandra.auth.enums.Domains;
import org.apache.cassandra.auth.enums.PartitionedEnumSet;

/**
 * Utility methods for dealing with system defined and custom permissions.
 */
public class Permissions
{
    // always register the default permissions
    static
    {
        register(CorePermission.getDomain(), CorePermission.class);
    }

    // set of known domains of the Permission partitioned enum
    private static final Domains<Permission> domains = Domains.getDomains(Permission.class);

    /**
     * Get a previously registered Permission, given its domain and name.
     * Attempting to retrieve an unregistered Permission will result in an
     * IllegalArgumentException.
     * @param domain the domain of the Permission
     * @param name the domain-local name of the Permission
     * @return The registered Permission instance
     */
    public static Permission permission(String domain, String name)
    {
        return domains.get(domain, name);
    }

    /**
     * Get a previously registered Permission, given its fully qualified name.
     * Attempting to retrieve an unregistered Permission will result in an
     * IllegalArgumentException.
     * @param fullName the fully qualified name, in the form "domain.permission_name"
     * @return The registered Permission instance
     */
    public static Permission permission(String fullName)
    {
        int delim = fullName.indexOf('.');
        // Special case to handle legacy permissions. CassandraAuthorizer stores permissions as
        // as simple strings in the system_auth perms tables, so clusters which have upgraded
        // from older versions will still contain references to the the un-namespaced versions
        String domain = (delim == -1) ? CorePermission.getDomain() : fullName.substring(0, delim);
        String name = fullName.substring(delim + 1);
        return permission(domain, name);
    }

    /**
     * Return a mutable set of permissions, backed by PartitionedEnumSet<Permission>
     * @param permissions the initial permissions to include in the Set
     * @return mutable set of permissions
     */
    public static Set<Permission> setOf(Permission...permissions)
    {
        return PartitionedEnumSet.of(Permission.class, permissions);
    }

    /**
     * Return an immutable set of permissions, backed by PartitionedEnumSet<Permission>
     * @param permissions the permissions to include in the Set
     * @return immutable set of permissions
     */
    public static Set<Permission> immutableSetOf(Permission...permissions)
    {
        return PartitionedEnumSet.immutableSetOf(Permission.class, permissions);
    }

    /**
     * Return an immutable set of all known permissions
     * @return all permissions
     */
    public static ImmutableSet<Permission> all()
    {
        return domains.asSet();
    }

    /**
     * Register a set of Permissions. For a permission to be used, either in a DCL statement (GRANT/REVOKE)
     * or by being referenced in the access control checks of a user operation it must first be registered here.
     * It is recommended that any custom permission be registered during system initialisation.
     * @param permissions class containing the {@link Enum} defining the permissions.
     *                    Must also implement {@link Permission}.
     */
    public static <D extends Enum & Permission> void register(String domain, Class<D> permissions)
    {
        if (domain.equals(CorePermission.getDomain()) && permissions != CorePermission.class)
            throw new IllegalArgumentException(String.format("The permission domain '%s' is reserved",
                                                             CorePermission.getDomain()));

        PartitionedEnum.registerDomainForType(Permission.class, domain, permissions);
    }
}
