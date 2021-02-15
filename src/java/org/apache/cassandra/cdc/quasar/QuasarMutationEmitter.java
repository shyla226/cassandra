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

package org.apache.cassandra.cdc.quasar;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.cdc.Mutation;
import org.apache.cassandra.cdc.MutationEmitter;
import org.apache.cassandra.cdc.MutationKey;
import org.apache.cassandra.cdc.MutationValue;
import org.apache.cassandra.cdc.exceptions.CassandraConnectorTaskException;

public class QuasarMutationEmitter implements MutationEmitter<Mutation>
{
    private static final Logger logger = LoggerFactory.getLogger(QuasarMutationEmitter.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final int NUMBER_OF_EXECUTORS = 3;
    private static final String USER_AGENT = "Cassandra CDC Replication Agent";

    String serviceUrlTemplate = System.getProperty("cassandra.cdcrep.node_url_template", "http://quasar-%d:8081");
    String serviceUrl = System.getProperty("cassandra.cdcrep.service_url", "http://quasar-0:8081");

    private final HttpClient[] httpClient = new HttpClient[NUMBER_OF_EXECUTORS];
    private final ExecutorService[] executors = new ExecutorService[NUMBER_OF_EXECUTORS];
    volatile State state = State.NO_STATE;
    volatile ConsistentHashWithVirtualNodes consistentHashWithVirtualNodes = new ConsistentHashWithVirtualNodes(0);

    public QuasarMutationEmitter()
    {
        /*
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, new TrustManager[] { new DummyTrustManager()}, new SecureRandom());
        */

        for (int i = 0; i < NUMBER_OF_EXECUTORS; i++)
        {
            executors[i] = Executors.newSingleThreadExecutor();
            httpClient[i] = HttpClient.newBuilder()
                                      .executor(executors[i])
                                      .version(HttpClient.Version.HTTP_1_1)
                                      .connectTimeout(Duration.ofSeconds(10))
                                      .followRedirects(HttpClient.Redirect.NEVER)
                                      //.sslContext(sslContext)
                                      //.sslParameters(new SSLParameters())
                                      .build();
        }
    }

    @Override
    public MutationFuture sendMutationAsync(final Mutation mutation) {
        return new MutationFuture(mutation, replicate(mutation));
    }

    CompletableFuture<State> getState()
    {
        HttpRequest request = HttpRequest.newBuilder()
                                         .GET()
                                         .uri(URI.create(serviceUrl + "/state"))
                                         .setHeader("User-Agent", USER_AGENT)
                                         .build();
        return httpClient[0].sendAsync(request, HttpResponse.BodyHandlers.ofString())
                            .thenApply(response -> {
                                try
                                {
                                    this.state = mapper.readValue(response.body(), State.class);
                                    this.consistentHashWithVirtualNodes.updateNumberOfNode(state.getSize());
                                }
                                catch (Exception e)
                                {
                                    logger.warn("error:", e);
                                }
                                logger.debug("Initial state={}", this.state);
                                return this.state;
                            });
    }

    CompletableFuture<Long> replicate(final Mutation mutation)
    {
        final MutationKey key = mutation.mutationKey();
        final MutationValue value = mutation.mutationValue();

        CompletableFuture<State> stateFuture = (!this.state.status.isRunning() || state.size == 0)
                                               ? getState()
                                               : CompletableFuture.completedFuture(this.state);

        return stateFuture.thenCompose(state -> {
            if (state.getSize() <= 0 || !state.getStatus().isRunning()) {
                logger.warn("Cannot serve request state={}");
                throw new IllegalStateException("Cannot serve request state="+state);
            }
            int hash = key.hash();
            int ordinal = consistentHashWithVirtualNodes.getOrdinal(hash);
            final String query = String.format(Locale.ROOT,
                                               String.format(Locale.ROOT, serviceUrlTemplate + "/replicate/%s/%s/%s?writetime=%d&nodeId=%s",
                                                             ordinal,
                                                             key.getKeyspace(),
                                                             key.getTable(),
                                                             key.id(),
                                                             value.getWritetime(),
                                                             value.getNodeId().toString()));

            final HttpRequest request = HttpRequest.newBuilder()
                                                   .GET()
                                                   .uri(URI.create(query))
                                                   .setHeader("User-Agent", USER_AGENT) // add request header
                                                   .build();
            logger.debug("Sending mutation={} hash={} ordinal={}", mutation, hash, ordinal);
            return httpClient[Math.abs(hash) % httpClient.length]
                   .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                   .thenApply(response -> {
                       switch (response.statusCode())
                       {
                           case 200:
                               logger.debug("Successfully replicate mutation={}", mutation);
                               return Long.parseLong(response.body());
                           case 503: // service unavailable
                           case 404: // hash not managed
                               logger.warn("error mutation={} hash={} ordinal={} query={} status={}",
                                           mutation, hash, ordinal, query, response.statusCode());
                               try
                               {
                                   String body = response.body();
                                   this.state = mapper.readValue(body, State.class);
                                   logger.debug("New state={}, retrying later", this.state);
                                   this.consistentHashWithVirtualNodes.updateNumberOfNode(this.state.getSize());
                                   if (response.statusCode() == 404)
                                   {
                                       // retry immediatelly
                                       throw new IllegalArgumentException("node["+ordinal+"] state="+state);
                                   }
                                   // retry later
                                   throw new IllegalStateException("node["+ordinal+"] state="+state);
                               }
                               catch (IOException e)
                               {
                                   logger.warn("error:", e);
                                   throw new CassandraConnectorTaskException(e);
                               }
                           default:
                               logger.warn("unexpected response code={}", response.statusCode());
                               throw new CassandraConnectorTaskException("code=" + response.statusCode());
                       }
                   });
        });
    }
}
