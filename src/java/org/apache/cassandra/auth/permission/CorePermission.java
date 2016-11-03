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

import org.apache.cassandra.auth.Permission;

public enum CorePermission implements Permission
{
    @Deprecated
    READ,
    @Deprecated
    WRITE,

    CREATE,
    ALTER,
    DROP,
    SELECT,
    MODIFY,
    AUTHORIZE,
    DESCRIBE,
    EXECUTE;

    public String domain()
    {
        return getDomain();
    }

    public String getFullName()
    {
        // for backwards compatibilty with legacy clusters (which could already have stored
        // permissions without namespacing), the base C* permissions never use theirs
        return name();
    }

    public static String getDomain()
    {
        return "CORE";
    }
}
