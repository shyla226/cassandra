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
package org.apache.cassandra.service;

import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;

import io.reactivex.Completable;
import io.reactivex.Single;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.utils.flow.CsFlow;

public class DigestResolver extends ResponseResolver
{
    private volatile ReadResponse dataResponse;

    DigestResolver(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistency, int maxResponseCount)
    {
        super(keyspace, command, consistency, maxResponseCount);
    }

    @Override
    public void preprocess(Response<ReadResponse> message)
    {
        super.preprocess(message);
        if (dataResponse == null && !message.payload().isDigestResponse())
            dataResponse = message.payload();
    }

    /**
     * Special case of resolve() so that CL.ONE reads never throw DigestMismatchException in the foreground
     */
    public CsFlow<FlowablePartition> getData()
    {
        assert isDataPresent();
        return FlowablePartitions.filterAndSkipEmpty(dataResponse.data(command), command.nowInSec());
    }

    /*
     * This method handles two different scenarios:
     *
     * a) we're handling the initial read of data from the closest replica + digests
     *    from the rest. In this case we check the digests against each other,
     *    throw an exception if there is a mismatch, otherwise return the data row.
     *
     * b) we're checking additional digests that arrived after the minimum to handle
     *    the requested ConsistencyLevel, i.e. asynchronous read repair check
     */
    public CsFlow<FlowablePartition> resolve() throws DigestMismatchException
    {
        if (responses.size() == 1)
            return getData();

        if (logger.isTraceEnabled())
            logger.trace("resolving {} responses", responses.size());

        return CsFlow.concat(compareResponses(),
                             FlowablePartitions.filterAndSkipEmpty(dataResponse.data(command), command.nowInSec()));
    }

    public Completable completeOnReadRepairAnswersReceived()
    {
        return Completable.complete();
    }

    public Completable compareResponses() throws DigestMismatchException
    {
        final long start = System.nanoTime();

        Completable pipeline =
                Single.concat(Iterables.transform(responses, response -> response.payload().digest(command)))
                      .reduce((prev, digest) ->
                      {
                          if (prev.equals(digest))
                              return digest;

                          // rely on the fact that only single partition queries use digests
                          throw new DigestMismatchException(((SinglePartitionReadCommand) command).partitionKey(), prev, digest);
                      })
                      .ignoreElement();

        if (logger.isTraceEnabled())
            pipeline = pipeline.doFinally(() -> logger.trace("resolve: {} ms.", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)));

        return pipeline;
    }

    public boolean isDataPresent()
    {
        return dataResponse != null;
    }
}
