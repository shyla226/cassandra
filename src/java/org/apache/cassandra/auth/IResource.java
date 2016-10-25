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
import java.util.function.Supplier;

/**
 * The interface at the core of Cassandra authorization.
 *
 * Represents a resource in the hierarchy.
 * Currently just one resource type is supported by Cassandra
 * @see DataResource
 */
public interface IResource
{
    /**
     * @return printable name of the resource.
     */
    String getName();

    /**
     * Gets next resource in the hierarchy. Call hasParent first to make sure there is one.
     *
     * @return Resource parent (or IllegalStateException if there is none). Never a null.
     */
    IResource getParent();

    /**
     * Indicates whether or not this resource has a parent in the hierarchy.
     *
     * Please perform this check before calling getParent() method.
     * @return Whether or not the resource has a parent.
     */
    boolean hasParent();

    /**
     * @return Whether or not this resource exists in Cassandra.
     */
    boolean exists();

    /**
     * Returns the set of Permissions that may be applied to this resource
     *
     * Certain permissions are not applicable to particular types of resources.
     * For instance, it makes no sense to talk about CREATE permission on table, or SELECT on a Role.
     * Here we filter a set of permissions depending on the specific resource they're being applied to.
     * This is necessary because the CQL syntax supports ALL as wildcard, but the set of permissions that
     * should resolve to varies by IResource.
     *
     * @return the permissions that may be granted on the specific resource
     */
    Set<Permission> applicablePermissions();


    /**
     * Some resource implementations are keyspace-aware (DataResource, FunctionResource), while others
     * are not (JMXResource, RoleResource). For some keyspace-aware resources (principally DataResource),
     * CQL syntax allows the relevant keyspace to be inferred from the ClientState, where a preceding
     * USE KEYSPACE statement has been issued. These keyspace-aware resources should implement this method
     * to return an IResource instance of the appropriate type, properly qualified with the supplied
     * keyspace (if not explicitly set already).
     * @param keyspace function to supply the name of the keyspace to set on the IResource.
     * @return An IResource instance, if the implementation is keyspace aware its keyspace will be set
     * to the result of calling the supplied function.
     */
    default IResource qualifyWithKeyspace(Supplier<String> keyspace)
    {
        return this;
    }
}
