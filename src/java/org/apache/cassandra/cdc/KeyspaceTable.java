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

import java.util.Objects;


/**
 * The KeyspaceTable uniquely identifies each table in the Cassandra cluster
 */
public class KeyspaceTable {
    public final String keyspace;
    public final String table;

    public KeyspaceTable(String keyspace, String table) {
        this.keyspace = keyspace;
        this.table = table;
    }

    public String name() {
        return keyspace + "." + table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KeyspaceTable that = (KeyspaceTable) o;
        return keyspace.equals(that.keyspace) && table.equals(that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyspace, table);
    }

    @Override
    public String toString() {
        return name();
    }

    public String identifier() {
        return keyspace + "." + table;
    }
}
