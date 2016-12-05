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
package org.apache.cassandra.utils.versioning;

/**
 * Describe the version of something.
 * <p>
 * This is largely a marker interface to make code more explicit. This is meant to be implemented by an enum. However,
 * comes with implementing this interface the convention that declaration order of the enum matters and is that of
 * growing/consecutive versions.
 *
 * @param <V> the actual version enum type (you typically declare a version as
 *            {@code enum MyVersion implements Version<MyVersion>}).
 */
public interface Version<V extends Enum<V>>
{
    public static <V extends Enum<V> & Version<V>> V last(Class<V> versionClass)
    {
        return versionClass.getEnumConstants()[versionClass.getEnumConstants().length - 1];
    }
}
