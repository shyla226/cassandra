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
 * Subscription (pull) component of the Flow async machinery.
 */
public interface FlowSubscription extends AutoCloseable
{
    /**
     * Requests exactly one item from the subscription.
     * In response to this call, exactly one onNext/onComplete or onError call must be made.
     *
     * It is an error to call requestNext() or close() while another request is still active (i.e. before an onX has been
     * received; it would typically be called within/at the end of that onX call).
     *
     * This call implies that all resources held by the previous item are no longer in use.
     */
    void requestNext();

    /**
     * Stop issuing any more items and close resources. Must be called also when the flow is done (due to completion or error).
     * Stashed exceptions may be thrown.
     *
     * It is an error to call request() or close() while another request is still active (i.e. before an onX has been
     * received; it would typically be called within/at the end of that onX call). This implies that close cannot be
     * used as a means of externally cancelling a flow.
     *
     * This call implies that all resources held by any outstanding item are no longer in use.
     */
    void close() throws Exception;

    /**
     * Subscription used with some constructs that have only one value and don't need to close.
     */
    static final FlowSubscription DONE = new FlowSubscription()
    {
        public void requestNext()
        {
            throw new AssertionError("Already completed.");
        }

        public void close() throws Exception
        {
            // Nothing to do
        }
    };
}
