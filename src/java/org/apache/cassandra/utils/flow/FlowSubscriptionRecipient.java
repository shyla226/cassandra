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

import io.reactivex.functions.BooleanSupplier;

/**
 * Recipient of a subscription. Sometimes used separately from {@link FlowSubscriber} to allow skipping over subscribers
 * (see {@link FlowTransform} and {@link Flow#takeUntil(BooleanSupplier)}).
 */
public interface FlowSubscriptionRecipient
{
    /**
     * Called after requestFirst and before onNext/onComplete to notify subscriber where to call for next requests and
     * closing.
     *
     * If different threads could be calling onSubscribe and onX, the flow must establish a happens-before relationship
     * between the two calls.
     */
    void onSubscribe(FlowSubscription source);
}
