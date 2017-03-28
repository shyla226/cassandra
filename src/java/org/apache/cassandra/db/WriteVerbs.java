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
package org.apache.cassandra.db;

import java.util.UUID;
import java.util.function.Function;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.net.*;
import org.apache.cassandra.net.Verb.AckedRequest;
import org.apache.cassandra.net.Verb.OneWay;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public class WriteVerbs extends VerbGroup<WriteVerbs.WriteVersion>
{
    private static final Logger logger = LoggerFactory.getLogger(ReadVerbs.class);

    public enum WriteVersion implements Version<WriteVersion>
    {
        OSS_30(EncodingVersion.OSS_30);

        public final EncodingVersion encodingVersion;

        WriteVersion(EncodingVersion encodingVersion)
        {
            this.encodingVersion = encodingVersion;
        }

        public static <T> Versioned<WriteVersion, T> versioned(Function<WriteVersion, ? extends T> function)
        {
            return new Versioned<>(WriteVersion.class, function);
        }
    }

    public final AckedRequest<Mutation> WRITE;
    public final AckedRequest<Mutation> VIEW_WRITE;
    public final AckedRequest<CounterMutation> COUNTER_FORWARDING;
    public final AckedRequest<Mutation> READ_REPAIR;
    public final AckedRequest<Batch> BATCH_STORE;
    public final OneWay<UUID> BATCH_REMOVE;

    private static final VerbHandlers.AckedRequest<Mutation> WRITE_HANDLER = (from, mutation) -> mutation.applyFuture();

    private static final VerbHandlers.AckedRequest<CounterMutation> COUNTER_FORWARDING_HANDLER = (from, mutation) ->
    {
        long queryStartNanoTime = System.nanoTime();
        logger.trace("Applying forwarded {}", mutation);

        String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
        // We should not wait for the result of the write in this thread, otherwise we could have a distributed
        // deadlock between replicas running this handler (see #4578).
        try
        {
            return StorageProxy.applyCounterMutationOnLeader(mutation, localDataCenter, queryStartNanoTime)
                               .exceptionally(t -> {
                                   if (t instanceof RequestTimeoutException)
                                   {
                                       // Getting a timeout here should mean the coordinator that forwarded that counter write has already timed out
                                       // on its own, so there isn't much benefit in answering.
                                       throw new DroppingResponseException();
                                   }
                                   else if (t instanceof RequestExecutionException)
                                   {
                                       // Other execution exceptions are more indicative of a failure (mostly WriteFailure currently but no reason not
                                       // to catch all RequestExecutionException to be on the safe side).
                                       throw new CounterForwardingException((RequestExecutionException)t);
                                   }
                                   else
                                   {
                                       // Otherwise, it's unexpected so let the normal exception handling from VerbHandlers do its job
                                       throw Throwables.propagate(t);
                                   }
                               });
        }
        catch (RequestExecutionException e)
        {
            // applyCounterMutationOnLeader can only throw OverloadedException for now, but catch RequestExecutioException
            // to future proof since we'd want to handle any request failure the same way (with the exclusion of a
            // timeout, but that's the one exception we know won't be thrown by the method as it's asynchronous)
            throw new CounterForwardingException(e);
        }
    };

    public WriteVerbs(Verbs.Group id)
    {
        super(id, false, WriteVersion.class);

        RegistrationHelper helper = helper();

        WRITE = helper.ackedRequest("WRITE", Mutation.class)
                      .stage(Stage.MUTATION)
                      .timeout(DatabaseDescriptor::getWriteRpcTimeout)
                      .withBackPressure()
                      .handler(WRITE_HANDLER);
        VIEW_WRITE = helper.ackedRequest("VIEW_WRITE", Mutation.class)
                           .stage(Stage.VIEW_MUTATION)
                           .timeout(DatabaseDescriptor::getWriteRpcTimeout)
                           .withBackPressure()
                           .handler(WRITE_HANDLER);
        COUNTER_FORWARDING = helper.ackedRequest("COUNTER_FORWARDING", CounterMutation.class)
                                   .stage(Stage.COUNTER_MUTATION)
                                   .timeout(DatabaseDescriptor::getCounterWriteRpcTimeout)
                                   .withBackPressure()
                                   .handler(COUNTER_FORWARDING_HANDLER);
        READ_REPAIR = helper.ackedRequest("VIEW_WRITE", Mutation.class)
                            .stage(Stage.MUTATION)
                            .timeout(DatabaseDescriptor::getWriteRpcTimeout)
                            .handler(WRITE_HANDLER);
        BATCH_STORE = helper.ackedRequest("BATCH_STORE", Batch.class)
                            .stage(Stage.MUTATION)
                            .timeout(DatabaseDescriptor::getWriteRpcTimeout)
                            .withBackPressure()
                            .syncHandler((from, batch) -> BatchlogManager.store(batch).blockingAwait());
        BATCH_REMOVE = helper.oneWay("BATCH_REMOVE", UUID.class)
                             .stage(Stage.MUTATION)
                             .withRequestSerializer(UUIDSerializer.serializer)
                             .handler((from, batchId) -> BatchlogManager.remove(batchId).blockingAwait());
    }
}
