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
package org.apache.cassandra.net;

import org.apache.cassandra.utils.Serializer;

/**
 * Serialization infos related to a particular verb for a particular messaging version.
 *
 * @param <P> the request type for the verb this is the serializer of.
 * @param <Q> the response type for the verb this is the serializer of.
 */
class VerbSerializer<P, Q>
{
    final Verb<P, Q> verb;
    final int code;
    final Serializer<P> requestSerializer;
    final Serializer<Q> responseSerializer;

    VerbSerializer(Verb<P, Q> verb,
                   int code,
                   Serializer<P> requestSerializer,
                   Serializer<Q> responseSerializer)
    {
        this.verb = verb;
        this.code = code;
        this.requestSerializer = requestSerializer;
        this.responseSerializer = responseSerializer;
    }
}
