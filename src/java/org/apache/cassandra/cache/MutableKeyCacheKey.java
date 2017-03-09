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
package org.apache.cassandra.cache;

import java.nio.ByteBuffer;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A mutable version of the KeyCacheKey to be used by read only cache checks
 *
 */
public class MutableKeyCacheKey extends KeyCacheKey
{
    public MutableKeyCacheKey(TableMetadata metadata, Descriptor desc, ByteBuffer key)
    {
        super(metadata, desc, key, false);
    }

    public void mutate(Descriptor desc, ByteBuffer key)
    {
        this.desc = desc;

        assert !key.isDirect();

        this.key = key.array();
        this.keyOffset = key.arrayOffset() + key.position();
        this.keyLength = key.remaining();
    }
}
