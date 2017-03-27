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

import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.monitoring.Monitor;
import org.apache.cassandra.dht.BoundsVersion;
import org.apache.cassandra.net.*;
import org.apache.cassandra.net.Verb.RequestResponse;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public class ReadVerbs extends VerbGroup<ReadVerbs.ReadVersion>
{
    private static final InetAddress local = FBUtilities.getBroadcastAddress();

    public enum ReadVersion implements Version<ReadVersion>
    {
        OSS_30(EncodingVersion.OSS_30, BoundsVersion.OSS_30, DigestVersion.OSS_30),
        OSS_40(EncodingVersion.OSS_30, BoundsVersion.OSS_30, DigestVersion.OSS_30),  // Changes the meaning of isFetchAll in ColumnFilter
        DSE_60(EncodingVersion.OSS_30, BoundsVersion.OSS_30, DigestVersion.OSS_30);  // Uses the encodingVersion ordinal when writing digest versions
                                                                                     // rather than the whole messaging version.

        public final EncodingVersion encodingVersion;
        public final BoundsVersion boundsVersion;
        public final DigestVersion digestVersion;

        ReadVersion(EncodingVersion encodingVersion, BoundsVersion boundsVersion, DigestVersion digestVersion)
        {
            this.encodingVersion = encodingVersion;
            this.boundsVersion = boundsVersion;
            this.digestVersion = digestVersion;
        }

        public static <T> Versioned<ReadVersion, T> versioned(Function<ReadVersion, ? extends T> function)
        {
            return new Versioned<>(ReadVersion.class, function);
        }
    }

    public final RequestResponse<ReadCommand, ReadResponse> READ;

    public ReadVerbs(Verbs.Group id)
    {
        super(id, false, ReadVersion.class);

        RegistrationHelper helper = helper();

        READ = helper.monitoredRequestResponse("READ", ReadCommand.class, ReadResponse.class)
                     .stage(Stage.READ)
                     .timeout(command -> command instanceof SinglePartitionReadCommand
                                         ? DatabaseDescriptor.getReadRpcTimeout()
                                         : DatabaseDescriptor.getRangeRpcTimeout())
                     .handler((from, command, monitor) ->
                                  {
                                      // Note that we want to allow locally delivered reads no matter what
                                      if (StorageService.instance.isBootstrapMode() && !from.equals(local))
                                          throw new RuntimeException("Cannot service reads while bootstrapping!");

                                      // Monitoring tests want to artificially slow down their reads, but we don't want this
                                      // to impact the queries drivers do on system/schema tables
                                      if (Monitor.isTesting() && SchemaConstants.isSystemKeyspace(command.metadata().keyspace))
                                          monitor = null;

                                      CompletableFuture<ReadResponse> ret = new CompletableFuture<>();
                                      command.executeLocally(monitor).subscribe(
                                        (it) -> {
                                            ret.complete(command.createResponse(it));
                                            it.close();
                                        },
                                        ex -> {
                                            ret.completeExceptionally(ex);
                                        });
                                      return ret;
                                  });
    }
}
