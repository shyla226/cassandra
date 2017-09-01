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
 * Subscriber (push) component of the Flow machinery.
 * This interface is used by the subscription to push data back to the subscriber in response to a request.
 * Exactly one of the methods must be called for each Subscription.request().
 */
public interface FlowSubscriber<T> extends FlowSubscriptionRecipient
{
    /**
     * Next item in the flow. Called in response to Subscription.request().
     * Subscriber must be done with the returned item before requesting next.
     */
    void onNext(T item);

    /**
     * Indicates that the flow has generated its final item.
     * This should be treated as a combination of onNext and onComplete.
     *
     * It is the subscriber's duty to call the subscription's close method after receiving this.
     */
    void onFinal(T item);

    /**
     * Indicates that the flow has completed. Called in response to Subscription.request().
     * It is an error to call this together with onNext in response to a single request.
     *
     * It is the subscriber's duty to call the subscription's close method after receiving this.
     */
    void onComplete();

    /**
     * Indicates that there was an error evaluating the next item in the flow. Called in response to Subscription.request().
     *
     * It is the subscriber's duty to call the subscription's close method after receiving this.
     */
    void onError(Throwable t);
}
