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

import com.fasterxml.jackson.annotation.JsonProperty;



public class State {
    public static final State NO_STATE = new State(Status.UNKNOWN, -1, -1L);

    @JsonProperty("status")
    Status status;

    @JsonProperty("size")
    Integer size;

    @JsonProperty("version")
    Long version;

    public State() {}

    public State(@JsonProperty("status") Status status,
                 @JsonProperty("size") int size,
                 @JsonProperty("version") long version) {
        this.status = status;
        this.size = size;
        this.version = version;
    }

    public Status getStatus()
    {
        return status;
    }

    public Integer getSize()
    {
        return size;
    }

    public Long getVersion()
    {
        return version;
    }

    public void setStatus(Status status)
    {
        this.status = status;
    }

    public void setSize(Integer size)
    {
        this.size = size;
    }

    public void setVersion(Long version)
    {
        this.version = version;
    }

    @Override
    public String toString()
    {
        return "State{" +
               "status=" + status +
               ", size=" + size +
               ", version=" + version +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        State state = (State) o;
        return status == state.status &&
               Objects.equals(size, state.size) &&
               Objects.equals(version, state.version);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(status, size, version);
    }
}
