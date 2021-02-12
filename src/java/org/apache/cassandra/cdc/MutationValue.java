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
import java.util.UUID;

public class MutationValue {
    Long writetime;
    UUID nodeId;
    Operation operation;
    String document;

    public MutationValue(Long writetime,
                                       UUID nodeId,
                                       Operation operation,
                                       String document) {
        this.writetime = writetime;
        this.nodeId = nodeId;
        this.operation = operation;
        this.document = document;
    }

    public Long getWritetime()
    {
        return writetime;
    }

    public UUID getNodeId()
    {
        return nodeId;
    }

    public Operation getOperation()
    {
        return operation;
    }

    public String getDocument()
    {
        return document;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MutationValue that = (MutationValue) o;
        return Objects.equals(writetime, that.writetime) &&
               Objects.equals(nodeId, that.nodeId) &&
               operation == that.operation &&
               Objects.equals(document, that.document);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(writetime, nodeId, operation, document);
    }

    @Override
    public String toString()
    {
        return "MutationValue{" +
               "writetime=" + writetime +
               ", nodeId=" + nodeId +
               ", operation=" + operation +
               ", document='" + document + '\'' +
               '}';
    }
}
