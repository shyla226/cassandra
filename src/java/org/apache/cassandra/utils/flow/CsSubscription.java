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
}
