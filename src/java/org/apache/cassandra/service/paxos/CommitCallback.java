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

package org.apache.cassandra.service.paxos;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.Response;

public class CommitCallback extends AbstractPaxosCallback<EmptyPayload>
{
    private static final Logger logger = LoggerFactory.getLogger(CommitCallback.class);

    private final Map<InetAddress, RequestFailureReason> failureReasonByEndpoint = new ConcurrentHashMap<>();

    public CommitCallback(int targets, ConsistencyLevel consistency, long queryStartNanoTime)
    {
        super(targets, consistency, queryStartNanoTime);
    }

    public void onFailure(FailureResponse<EmptyPayload> msg)
    {
        logger.trace("Failure response to commit from {}", msg.from());

        failureReasonByEndpoint.put(msg.from(), msg.reason());

        latch.countDown();
    }

    public void onResponse(Response<EmptyPayload> msg)
    {
        logger.trace("Commit response from {}", msg.from());

        latch.countDown();
    }

    public Map<InetAddress, RequestFailureReason> getFailureReasons()
    {
        return failureReasonByEndpoint;
    }
}
