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

import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.google.common.collect.Iterables;
import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;

public abstract class AbstractWriteResponseHandler<T> implements IAsyncCallbackWithFailure<T>
{
    protected static final Logger logger = LoggerFactory.getLogger( AbstractWriteResponseHandler.class );

    protected final Keyspace keyspace;
    protected final Collection<InetAddress> naturalEndpoints;
    public final ConsistencyLevel consistencyLevel;
    protected final Collection<InetAddress> pendingEndpoints;
    protected final WriteType writeType;
    private static final AtomicIntegerFieldUpdater<AbstractWriteResponseHandler> failuresUpdater
        = AtomicIntegerFieldUpdater.newUpdater(AbstractWriteResponseHandler.class, "failures");
    private volatile int failures = 0;
    private final Map<InetAddress, RequestFailureReason> failureReasonByEndpoint;
    private final long queryStartNanoTime;

    final BehaviorSubject<Integer> publishSubject = BehaviorSubject.create();
    final Observable<Integer> observable;

    /**
     * @param queryStartNanoTime the time at which the query started
     */
    protected AbstractWriteResponseHandler(Keyspace keyspace,
                                           Collection<InetAddress> naturalEndpoints,
                                           Collection<InetAddress> pendingEndpoints,
                                           ConsistencyLevel consistencyLevel,
                                           WriteType writeType,
                                           long queryStartNanoTime)
    {
        this.keyspace = keyspace;
        this.pendingEndpoints = pendingEndpoints;
        this.consistencyLevel = consistencyLevel;
        this.naturalEndpoints = naturalEndpoints;
        this.writeType = writeType;
        this.failureReasonByEndpoint = new ConcurrentHashMap<>();
        this.queryStartNanoTime = queryStartNanoTime;
        observable = makeObservable();
    }


    public Observable<Integer> get()
    {
        return observable;
    }

    private Observable<Integer> makeObservable()
    {
        long requestTimeout = writeType == WriteType.COUNTER
                            ? DatabaseDescriptor.getCounterWriteRpcTimeout()
                            : DatabaseDescriptor.getWriteRpcTimeout();

        long timeout = TimeUnit.MILLISECONDS.toNanos(requestTimeout) - (System.nanoTime() - queryStartNanoTime);

        return publishSubject
            .timeout(timeout, TimeUnit.NANOSECONDS)
            .first(0).toObservable()
            .onErrorResumeNext(exc -> {
                if (exc instanceof TimeoutException)
                {
                    int blockedFor = totalBlockFor();
                    int acks = ackCount();
                    // It's pretty unlikely, but we can race between exiting await above and here, so
                    // that we could now have enough acks. In that case, we "lie" on the acks count to
                    // avoid sending confusing info to the user (see CASSANDRA-6491).
                    if (acks >= blockedFor)
                        acks = blockedFor - 1;

                    return Observable.error(new WriteTimeoutException(writeType, consistencyLevel, ackCount(), totalBlockFor()));
                }

                return Observable.error(exc);
            });
    }

    /** 
     * @return the minimum number of endpoints that must reply. 
     */
    protected int totalBlockFor()
    {
        // During bootstrap, we have to include the pending endpoints or we may fail the consistency level
        // guarantees (see #833)
        return consistencyLevel.blockFor(keyspace) + pendingEndpoints.size();
    }

    /** 
     * @return the total number of endpoints the request has been sent to. 
     */
    protected int totalEndpoints()
    {
        return naturalEndpoints.size() + pendingEndpoints.size();
    }

    /**
     * @return true if the message counts towards the totalBlockFor() threshold
     */
    protected boolean waitingFor(InetAddress from)
    {
        return true;
    }

    /**
     * @return number of responses received
     */
    protected abstract int ackCount();

    /** null message means "response from local write" */
    public abstract void response(MessageIn<T> msg);

    public void assureSufficientLiveNodes() throws UnavailableException
    {
        consistencyLevel.assureSufficientLiveNodes(keyspace, Iterables.filter(Iterables.concat(naturalEndpoints, pendingEndpoints), isAlive));
    }

    protected void signalComplete()
    {
        // emit a value of 0, because we can't emit null
        publishSubject.onNext(0);
    }

    @Override
    public void onFailure(InetAddress from, RequestFailureReason failureReason)
    {
        logger.trace("Got failure from {}", from);

        int n = waitingFor(from)
              ? failuresUpdater.incrementAndGet(this)
              : failures;

        failureReasonByEndpoint.put(from, failureReason);

        if (totalBlockFor() + n > totalEndpoints())
        {
            publishSubject.onError(
                    new WriteFailureException(consistencyLevel, ackCount(), totalBlockFor(), writeType, failureReasonByEndpoint));
        }
    }
}
