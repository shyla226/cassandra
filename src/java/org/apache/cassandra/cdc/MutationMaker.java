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

import java.time.Instant;
import java.util.UUID;
import java.util.function.Consumer;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cdc.exceptions.CassandraConnectorTaskException;
import org.apache.cassandra.db.commitlog.CommitLogPosition;

/**
 * Responsible for generating ChangeRecord and/or TombstoneRecord for create/update/delete events, as well as EOF events.
 */
@Singleton
public class MutationMaker {
    private final boolean emitTombstoneOnDelete;

    public MutationMaker(boolean emitTombstoneOnDelete) {
        this.emitTombstoneOnDelete = emitTombstoneOnDelete;
    }

    public void insert(String cluster, UUID node, CommitLogPosition offsetPosition, String keyspace, String table, boolean snapshot,
                       Instant tsMicro, RowData data,
                       boolean markOffset, Consumer<Mutation> consumer, String jsonDocument) {
        createRecord(cluster, node, offsetPosition, keyspace, table, snapshot, tsMicro,
                data, markOffset, consumer, Operation.INSERT, jsonDocument);
    }

    public void update(String cluster, UUID node, CommitLogPosition offsetPosition, String keyspace, String table, boolean snapshot,
                       Instant tsMicro, RowData data,
                       boolean markOffset, Consumer<Mutation> consumer, String jsonDocument) {
        createRecord(cluster, node, offsetPosition, keyspace, table, snapshot, tsMicro,
                data, markOffset, consumer, Operation.UPDATE, jsonDocument);
    }

    public void delete(String cluster, UUID node, CommitLogPosition offsetPosition, String keyspace, String table, boolean snapshot,
                       Instant tsMicro, RowData data,
                       boolean markOffset, Consumer<Mutation> consumer) {
        createRecord(cluster, node, offsetPosition, keyspace, table, snapshot, tsMicro,
                data, markOffset, consumer, Operation.DELETE, null);
    }

    private void createRecord(String cluster, UUID node, CommitLogPosition offsetPosition, String keyspace, String table, boolean snapshot,
                              Instant tsMicro, RowData data,
                              boolean markOffset, Consumer<Mutation> consumer, Operation operation, String jsonDocument) {
        // TODO: filter columns
        RowData filteredData;
        switch (operation) {
            case INSERT:
            case UPDATE:
                filteredData = data;
                break;
            case DELETE:
            default:
                filteredData = data;
                break;
        }

        SourceInfo source = new SourceInfo(offsetPosition, keyspace, table, tsMicro);
        Mutation record = new Mutation(offsetPosition.segmentId, offsetPosition.position, source, filteredData, operation, markOffset, tsMicro.toEpochMilli(), jsonDocument);
        consumer.accept(record);
    }

}
