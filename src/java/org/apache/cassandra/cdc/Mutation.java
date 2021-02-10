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

import org.apache.cassandra.service.StorageService;

/**
 * An immutable data structure representing a change event, and can be converted
 * to a kafka connect Struct representing key/value of the change event.
 */

public class Mutation {
    public final long segment;
    public final int position;
    public final SourceInfo source;
    public final RowData rowData;
    public final Operation op;
    public final boolean shouldMarkOffset;
    public final long ts;
    public String jsonDocument;

    public Mutation(long segment,
                    int position,
                    SourceInfo source,
                    RowData rowData,
                    Operation op,
                    boolean shouldMarkOffset,
                    long ts,
                    String jsonDocument) {
        this.segment = segment;
        this.position = position;
        this.source = source;
        this.rowData = rowData;
        this.op = op;
        this.shouldMarkOffset = shouldMarkOffset;
        this.ts = ts;
        this.jsonDocument = jsonDocument;
    }

    public MutationKey mutationKey() {
        return new MutationKey(
        source.keyspace,
        source.table,
        rowData.primaryKeyValues());
    }

    public MutationValue mutationValue() {
        return new MutationValue(source.timestamp.toEpochMilli(), StorageService.instance.getLocalHostUUID(), op, jsonDocument);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Mutation mutation = (Mutation) o;
        return segment == mutation.segment &&
               position == mutation.position;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(segment, position);
    }

    @Override
    public String toString()
    {
        return "Mutation{" +
               "segment=" + segment +
               ", position=" + position +
               ", op=" + op +
               ", ts=" + ts +
               ", id=" + mutationKey().id() +
               '}';
    }
}
