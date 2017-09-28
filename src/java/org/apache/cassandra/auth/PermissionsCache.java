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

import java.util.Set;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.Pair;

public class PermissionsCache extends AuthCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> implements PermissionsCacheMBean
{
    public PermissionsCache(IAuthorizer authorizer)
    {
        super("PermissionsCache",
              DatabaseDescriptor::setPermissionsValidity,
              DatabaseDescriptor::getPermissionsValidity,
              DatabaseDescriptor::setPermissionsUpdateInterval,
              DatabaseDescriptor::getPermissionsUpdateInterval,
              DatabaseDescriptor::setPermissionsCacheMaxEntries,
              DatabaseDescriptor::getPermissionsCacheMaxEntries,
              (p) -> authorizer.authorize(p.left, p.right),
              () -> DatabaseDescriptor.getAuthorizer().requireAuthorization());
    }

    public Set<Permission> getPermissions(AuthenticatedUser user, IResource resource)
    {
        // we know CassandraAuthrorizer.authorize() will block, so we might as well
        // prevent the cache from attempting to load missing entries on the TPC threads,
        // since attempting to load only to get a WouldBlockException from the authorizer
        // would result in the cache logging errors and incrementing error statistics and
        // there also seems to be a problem somewhere in caffeine in that it will not attempt
        // to reload after an exception
        Set<Permission> ret = get(Pair.create(user, resource), !TPC.isTPCThread());
        if (ret == null)
            throw new TPCUtils.WouldBlockException(String.format("Cannot retrieve %s/%s, would block TPC thread %s",
                                                                 user, resource, Thread.currentThread().getName()));
        return ret;
    }
}
