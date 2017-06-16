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

package org.apache.cassandra.utils.flow;

/**
 * Subscription (pull) component of the CsFlow async machinery.
 */
public interface CsSubscription extends AutoCloseable
{
    /**
     * Requests exactly one item from the subscription.
     * In response to this call, exactly one onNext/onComplete or onError call must be made.
     *
     * This call implies that all resources held by the previous item are no longer in use.
     */
    void request();

    /**
     * Stop issuing any more items and close resources. Must be called also when the flow is done (due to completion or error).
     * Stashed exceptions may be thrown.
     *
     * This call implies that all resources held by any outstanding item are no longer in use.
     */
    void close() throws Exception;

    /**
     * Propagates error to the root / source in order to collect subscriber chain during the exception creation.
     * <p>
     * This method is required for debugging purposes only. If the subscription owns a source subscription, simply
     * implement this method by calling {@link #addSubscriberChainFromSource(Throwable)} on the source subscription.
     * Otherwise, if the flow is a source, call {@link CsFlow#wrapException(Throwable, Object)} passing the error and
     * this as the second parameter (so the subscription itself is the second parameter).
     * <p>
     * {@link CsFlow#wrapException(Throwable, Object)} will add a {@link org.apache.cassandra.utils.flow.CsFlow.CsFlowException},
     * as a suppressed exception to the original error. {@link org.apache.cassandra.utils.flow.CsFlow.CsFlowException} relies
     * on calling {@link Object#toString()} on the subscription in order to create a chain of subscribers. If all subscriptions
     * in the chain call {@link CsFlow#formatTrace(String, Object, CsSubscriber)} in their toString implementations, where the second
     * parameter is a mapping operation, typically a lambda, if available, and the subscriber is the actual subscriber.
     * The second parameter (the tag) will output the line number of the associated lambda (see {@link org.apache.cassandra.utils.LineNumberInference}),
     * whilst the subscriber recursively calls toString(), which in turn recurses to its own subscribers and so on.
     */
    Throwable addSubscriberChainFromSource(Throwable throwable);
}
