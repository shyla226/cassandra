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
package org.apache.cassandra.cdc;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.cassandra.cdc.quasar.Murmur3HashFunction;

public class MutationKey {
    String keyspace;
    String table;
    Object[] pkColumns;

    transient String id;

    public MutationKey(String keyspace, String table, String id) {
        this.keyspace = keyspace;
        this.table = table;
        this.pkColumns = null;
        this.id = id;
    }

    public MutationKey(String keyspace, String table, Object[] pkColumns) {
        this.keyspace = keyspace;
        this.table = table;
        this.pkColumns = pkColumns;
        this.id = (pkColumns.length == 1)
                ? pkColumns[0].toString()
                : "[" + Arrays.stream(pkColumns).map(x -> x.toString()).collect(Collectors.joining(",")) + "]";

    }

    public String id() {
        return this.id;
    }

    public int hash() {
        return Murmur3HashFunction.hash(id());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MutationKey that = (MutationKey) o;
        return Objects.equals(keyspace, that.keyspace) &&
               Objects.equals(table, that.table) &&
               Arrays.equals(pkColumns, that.pkColumns);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(keyspace, table);
        result = 31 * result + Arrays.hashCode(pkColumns);
        return result;
    }

    @Override
    public String toString()
    {
        return "MutationKey{" +
               "keyspace='" + keyspace + '\'' +
               ", table='" + table + '\'' +
               ", id='" + id + '\'' +
               '}';
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public String getTable()
    {
        return table;
    }
}
