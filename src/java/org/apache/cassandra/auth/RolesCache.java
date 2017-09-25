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
package org.apache.cassandra.auth;

import java.util.*;
import java.util.Map.Entry;

import org.apache.cassandra.config.DatabaseDescriptor;

public class RolesCache extends AuthCache<RoleResource, Role> implements RolesCacheMBean
{
    public RolesCache(IRoleManager roleManager)
    {
        super("RolesCache",
              DatabaseDescriptor::setRolesValidity,
              DatabaseDescriptor::getRolesValidity,
              DatabaseDescriptor::setRolesUpdateInterval,
              DatabaseDescriptor::getRolesUpdateInterval,
              DatabaseDescriptor::setRolesCacheMaxEntries,
              DatabaseDescriptor::getRolesCacheMaxEntries,
              roleManager::getRoleData,
              () -> DatabaseDescriptor.getAuthenticator().requireAuthentication());
    }

    public Map<RoleResource, Role> getRolesIfPresent(RoleResource primaryRole)
    {
        Role primary = getIfPresent(primaryRole);

        if (primary == null)
            return null;

        if (primary.memberOf.isEmpty())
            return Collections.singletonMap(primaryRole, primary);

        Map<RoleResource, Role> map = new HashMap<>();
        map.put(primaryRole, primary);
        return collectRolesIfPresent(primary.memberOf, map);
    }

    public Map<RoleResource, Role> collectRolesIfPresent(Set<RoleResource> roleResources, Map<RoleResource, Role> map)
    {
        if (!roleResources.isEmpty())
        {
            Map<RoleResource, Role> roles = getAllPresent(roleResources);
            if (roles.size() != roleResources.size())
                return null;

            for (Entry<RoleResource, Role> entry : roles.entrySet())
            {
                map.put(entry.getKey(), entry.getValue());
                map = collectRolesIfPresent(entry.getValue().memberOf, map);
                if (map == null)
                    return null;
            }
        }
        return map;
    }

    public Map<RoleResource, Role> getRoles(RoleResource primaryRole)
    {
        Role primary = get(primaryRole);

        if (primary.memberOf.isEmpty())
            return Collections.singletonMap(primaryRole, primary);

        Map<RoleResource, Role> set = new HashMap<>();
        set.put(primaryRole, primary);
        return collectRoles(primary.memberOf, set);
    }

    private Map<RoleResource, Role> collectRoles(Set<RoleResource> roleResources, Map<RoleResource, Role> map)
    {
        if (!roleResources.isEmpty())
        {
            Map<RoleResource, Role> roles = getAll(roleResources);

            for (Entry<RoleResource, Role> entry : roles.entrySet())
            {
                Role role = entry.getValue();
                map.put(entry.getKey(), role);
                collectRoles(role.memberOf, map);
            }
        }
        return map;
    }

    /**
     * Retrieve the superuser status of a role.
     */
    public boolean isSuperuser(RoleResource role)
    {
        Role ret = getInternal(role);
        return ret.isSuper;
    }

    /**
     * Retrieve the superuser status of a role.
     */
    public boolean canLogin(RoleResource role)
    {
        Role ret = getInternal(role);
        return ret.canLogin;
    }

    /**
     * Retrieve the roles assigned to a role.
     */
    public Map<String, String> getCustomOptions(RoleResource role)
    {
        Role ret = getInternal(role);
        return ret.options;
    }

    private Role getInternal(RoleResource role)
    {
        return get(role);
    }
}
