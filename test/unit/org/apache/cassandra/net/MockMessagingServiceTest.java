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
package org.apache.cassandra.net;

import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.net.MockMessagingService.all;
import static org.apache.cassandra.net.MockMessagingService.to;
import static org.apache.cassandra.net.MockMessagingService.verb;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MockMessagingServiceTest
{
    @BeforeClass
    public static void initCluster() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
    }

    @Before
    public void cleanup()
    {
        MockMessagingService.cleanup();
    }

    @Test
    public void testRequestResponse() throws InterruptedException, ExecutionException
    {
        Request<EmptyPayload, EmptyPayload> request = Verbs.GOSSIP.ECHO.newRequest(FBUtilities.getBroadcastAddress(), EmptyPayload.instance);
        Response<EmptyPayload> response = request.respond(EmptyPayload.instance);

        MockMessagingSpy spy = MockMessagingService
                .when(
                        all(
                        to(FBUtilities.getBroadcastAddress()),
                        verb(Verbs.GOSSIP.ECHO)
                        )
                )
                .respond(response);

        MessagingService.instance().send(request, new MessageCallback<EmptyPayload>()
        {
            public void onResponse(Response<EmptyPayload> r)
            {
                assertEquals(Verbs.GOSSIP.ECHO, r.verb());
                assertEquals(response.payload(), r.payload());
            }

            public void onFailure(FailureResponse<EmptyPayload> response)
            {
                throw new AssertionError();
            }
        });

        // we must have intercepted the outgoing message at this point
        Request<?, ?> msg = spy.captureRequests().get();
        assertEquals(1, spy.messagesIntercepted);
        assertTrue(msg == request);

        // and return a mocked response
        assertEquals(1, spy.mockedMessageResponses);
    }
}
