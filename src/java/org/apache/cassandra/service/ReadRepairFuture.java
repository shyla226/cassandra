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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.Response;

/**
 * A future waiting on read-repair results.
 * <p>
 * Read repair works by sending individual repairs (if any) on each partition and to each node as soon as it is computed.
 * We then wait on successful answers from all those repairs.  This class makes this easy: the read-repair code uses
 * a {@link #getRepairCallback()} to generate callbacks for each individual repair and once all read-repairs have been
 * sent for the query, calls {@link #onAllRepairSent()} to indicate readiness. After which this objects acts as a future
 * on (successful) responses to all those read repairs.
 * <p>
 * Please note that the {@link #onAllRepairSent()} method <b>must</b> be called or the future will never return.
 * <p>
 * Also note that any error or timeout of the individual repair is counted as a lack of response, which means in practice
 * that 1) {@link #get(long, TimeUnit)} won't throw {@link ExecutionException} (with the exception of cancellation) and
 * 2) the future will only return if all repairs are successful, so we shouldn't block on it indefinitely (in fact, the
 * {@link #get()} method is unsupported to avoid mistakes).
 */
class ReadRepairFuture extends CompletableFuture<Void>
{
    private volatile boolean ready;
    private final AtomicInteger outstandingRepairs = new AtomicInteger();

    private final MessageCallback<EmptyPayload> callback = new MessageCallback<EmptyPayload>()
    {
        public void onResponse(Response<EmptyPayload> response)
        {
            ReadRepairFuture.this.onResponse();
        }

        public void onFailure(FailureResponse<EmptyPayload> response)
        {
            // Ignore, see ReadRepairFuture class javadoc.
        }
    };

    @Override
    public Void get()
    {
        throw new UnsupportedOperationException();
    }

    private void onResponse()
    {
        if (outstandingRepairs.decrementAndGet() == 0)
        {
            // As markReady() sets ready before it's own 0-check, it's important that we check ready _after_ our 0-check
            // so than the future is always completed even if both method race. Which is why we don't merge both 'if',
            // it highlights the importance of the order.
            if (ready)
                complete(null);
        }
    }

    MessageCallback<EmptyPayload> getRepairCallback()
    {
        assert !ready : "onAllRepairSent() has already been called";
        outstandingRepairs.incrementAndGet();
        return callback;
    }

    void onAllRepairSent()
    {
        // Shouldn't be called twice, but it's really not a big deal if it is, just ignore
        if (ready)
            return;

        ready = true;
        // From that point on, any call to onResponse() that makes the count reach 0 will complete the future. It might
        // be we've reached 0 already however, and in that case we should complete the future ourselves.
        // Note that this can race with onResponse() in such a way that both method complete the future (onResponse()
        // reaches 0 but get interrupted before we set ready and read oustandingRepairs, which is now 0 and we complete;
        // then onResponse() resumes, read ready (now true) and complete as well), but this is a race we are ok with.
        if (outstandingRepairs.get() == 0)
            complete(null);
    }
}
