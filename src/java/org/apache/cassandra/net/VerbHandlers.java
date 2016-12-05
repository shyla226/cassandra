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

import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.schemabuilder.Drop;
import org.apache.cassandra.db.monitoring.AbortedOperationException;
import org.apache.cassandra.db.monitoring.Monitor;
import org.apache.cassandra.db.monitoring.Monitorable;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Groups sub-interfaces of {@link VerbHandler} that makes declaration of verb handlers more convenient (and enforce
 * proper handling of exceptions). The main use of thos sub-interface is through {@link VerbGroup.RegistrationHelper}.
 */
public abstract class VerbHandlers
{
    private static final Logger logger = LoggerFactory.getLogger(VerbHandlers.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 5L, TimeUnit.MINUTES);

    private VerbHandlers()
    {
    }

    private static <P, Q> FailureResponse<Q> handleFailure(Request<P, Q> request, Throwable t)
    {
        // These are the exceptions that we don't want silenced and that should (and are) dealt
        // with by callers of the definition handle() method.
        // AbortedOperation means the operation timed out and Monitoring aborted it.
        // DroppingResponse means we want to explicitly and silently not respond to a two-way request.
        Throwables.propagateIfInstanceOf(t, AbortedOperationException.class);
        Throwables.propagateIfInstanceOf(t, DroppingResponseException.class);

        RequestFailureReason reason;
        if (t instanceof InternalRequestExecutionException)
        {
            // This is an error we know can happen. Log it for operators but don't include the strack trace as it would
            // make it sound like it's a bug, which it's not.
            reason = ((InternalRequestExecutionException)t).reason;
            if (reason.shouldLogWarning())
                noSpamLogger.warn(t.getMessage());
            else
                noSpamLogger.debug(t.getMessage());
        }
        else
        {
            reason = RequestFailureReason.UNKNOWN;
            // This is unexpected and is likely a bug, so inspect for stability and log the full stack trace.
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Unexpected error during execution of request " + request, t);
        }

        return request.verb().isOneWay() ? null : request.respondWithFailure(reason);
    }

    /**
     * Sub-interface for handlers of one-way verbs.
     */
    public interface OneWay<P> extends VerbHandler<P, NoResponse>
    {
        default CompletableFuture<Response<NoResponse>> handle(Request<P, NoResponse> request)
        {
            try
            {
                handle(request.from(), request.payload());
            }
            catch (Throwable t)
            {
                handleFailure(request, t);
            }
            return null;
        }

        /**
         * Handles a one-way request.
         *
         * @param from the sender of the request.
         * @param message the payload of the request sent.
         */
        void handle(InetAddress from, P message);
    }

    /**
     * Sub-interface for (asynchronous) handlers of generic request-response verbs.
     * <p>
     * If the handler to implement is synchronous, use {@link SyncRequestResponse} instead.
     */
    public interface RequestResponse<P, Q> extends VerbHandler<P, Q>
    {
        default CompletableFuture<Response<Q>> handle(Request<P, Q> request)
        {
            try
            {
                return handleMayThrow(request).exceptionally(t -> handleFailure(request, t));
            }
            catch (Throwable t)
            {
                return CompletableFuture.completedFuture(handleFailure(request, t));
            }
        }

        default CompletableFuture<Response<Q>> handleMayThrow(Request<P, Q> request)
        {
            return handle(request.from(), request.payload()).thenApply(request::respond);
        }

        /**
         * Handles a request of a generic request-response verb.
         *
         * @param from the sender of the request.
         * @param message the payload of the request sent.
         * @return a future on the payload to send as response to {@code message}.
         */
        CompletableFuture<Q> handle(InetAddress from, P message);
    }

    /**
     * Sub-interface for synchronous handlers of generic request-response verbs.
     */
    public interface SyncRequestResponse<P, Q> extends RequestResponse<P, Q>
    {
        default CompletableFuture<Q> handle(InetAddress from, P message)
        {
            return CompletableFuture.completedFuture(handleSync(from, message));
        }

        /**
         * Handles a request of a generic request-response verb synchronously.
         *
         * @param from the sender of the request.
         * @param message the payload of the request sent.
         * @return the payload to send as response to {@code message}.
         */
        Q handleSync(InetAddress from, P message);
    }

    /**
     * Sub-interface for (asynchronous) handlers of request that are simply acked.
     * <p>
     * If the handler to implement is synchronous, use {@link SyncAckedRequest} instead.
     */
    public interface AckedRequest<P> extends RequestResponse<P, EmptyPayload>
    {
        default CompletableFuture<EmptyPayload> handle(InetAddress from, P message)
        {
            CompletableFuture<?> f = handle2(from, message);
            return f == null
                   ? CompletableFuture.completedFuture(EmptyPayload.instance)
                   : f.thenApply(x -> EmptyPayload.instance);
        }

        /**
         * Handles a request that should simply be acked when completed.
         * <p>
         * Note: the method is called "handle2" because we want it to return {@code CompletableFuture<?>} as that is
         * the type we have in all case of an asynchronous acked request, but "handle" conflicts with the method
         * inherited above. The interfaces of this class are all functional interfaces though and meant to be used
         * that way (with lambda) so the actual method doesn't matter much.
         *
         * @param from the sender of the request.
         * @param message the payload of the request sent.
         * @return a future on the payload to send as response to {@code message} (this can return {@code null}  to
         * complete immediately as a convenience).
         */
        CompletableFuture<?> handle2(InetAddress from, P message);
    }

    /**
     * Sub-interface for synchronous handlers of request that are simply acked.
     */
    public interface SyncAckedRequest<P> extends AckedRequest<P>
    {
        default CompletableFuture<?> handle2(InetAddress from, P message)
        {
            handleSync(from, message);
            return CompletableFuture.completedFuture(null);
        }

        /**
         * Handles a request that should simply be acked when completed. This method is synchronous in that returning
         * from that method means the request is complete and can to be acked.
         *
         * @param from the sender of the request.
         * @param message the payload of the request sent.
         */
        void handleSync(InetAddress from, P message);
    }

    /**
     * Sub-interface for (asynchronous) handlers of generic request-response verbs that needs to be monitored.
     * <p>
     * If the handler to implement is synchronous, use {@link SyncMonitoredRequestResponse} instead.
     */
    public interface MonitoredRequestResponse<P extends Monitorable, Q> extends RequestResponse<P, Q>
    {
        default CompletableFuture<Response<Q>> handleMayThrow(Request<P, Q> request)
        {
            Monitor monitor = Monitor.createAndStart(request.payload(),
                                                     request.operationStartMillis(),
                                                     request.timeoutMillis(),
                                                     request.isLocal());

            return handle(request.from(), request.payload(), monitor)
                   .thenApply(v -> { monitor.complete(); return request.respond(v); });
        }

        default CompletableFuture<Q> handle(InetAddress from, P message)
        {
            throw new UnsupportedOperationException();
        }

        /**
         * Handles a request of a generic request-response verb. The provided monitor is intented to be used to monitor
         * the execution of the provided request.
         * <p>
         * This method can (and should) throw or complete exceptionally with {@link AbortedOperationException} if
         * the execution exceed the time allocated to the execution by the provided monitor.
         *
         * @param from the sender of the request.
         * @param message the payload of the request sent.
         * @param monitor the monitor to use to monitor the handling of {@code message}.
         * @return a future on the response to {@code message} that can complete exceptionally with
         * {@link AbortedOperationException}.
         *
         * @throws AbortedOperationException if the execution is aborted by monitoring.
         */
        CompletableFuture<Q> handle(InetAddress from, P message, Monitor monitor);
    }

    /**
     * Sub-interface for synchronous handlers of generic request-response verbs that needs to be monitored.
     */
    public interface SyncMonitoredRequestResponse<P extends Monitorable, Q> extends MonitoredRequestResponse<P, Q>
    {
        default CompletableFuture<Q> handle(InetAddress from, P message, Monitor monitor)
        {
            return CompletableFuture.completedFuture(handleSync(from, message, monitor));
        }

        /**
         * Handles a request of a generic request-response verb. The provided monitor is intented to be used to monitor
         * the execution of the provided request.
         * <p>
         * This method can (and should) throw {@link AbortedOperationException} if the execution exceed the time
         * allocated to the execution by the provided monitor.
         *
         * @param from the sender of the request.
         * @param message the payload of the request sent.
         * @param monitor the monitor to use to monitor the handling of {@code message}.
         * @return the response to {@code message}.
         *
         * @throws AbortedOperationException if the execution is aborted by monitoring.
         */
        Q handleSync(InetAddress from, P message, Monitor monitor);
    }
}
