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
package org.apache.cassandra.net.interceptors;

import org.apache.cassandra.net.interceptors.AbstractInterceptor;

/**
 * Metrics and operations exposed by those interceptors that extend {@link AbstractInterceptor}.
 */
public interface AbstractInterceptorMBean
{
    /** Enable the interceptor (starting to intercept message if it was disabled before). */
    public void enable();

    /** Disable the interceptor (no message will be intercepted until it is re-enabled). */
    public void disable();

    /** Whether the interceptor is currently enabled. */
    public boolean getEnabled();

    /**
     * The number of messages considered by the interceptor.
     * <p>
     * Note this only count messages that the interceptor is configured to intercept and only count while the
     * interceptor is enabled. In practice, the only messages that are "seen" but not "intercepted" are those
     * excluded by the "random interception ratio" (see below).
     */
    public long getSeenCount();

    /** The number of intercepted messages (those for which the interceptor did something). */
    public long getInterceptedCount();

    /** A string that indicates which messages the interceptor is configured to intercept. */
    public String getIntercepted();

    /**
     * Sets which messages the interceptor should intercept.
     * <p>
     * @param interceptedString a string representing which messages to intercept. The syntax of this string is the same
     *                          as for the "intercepted" property of {@link AbstractInterceptor} so please refer to that
     *                          class javadoc for details. This is the same format than returned by {@link #getIntercepted}.
     */
    public void setIntercepted(String interceptedString);

    /**
     * Which directions (we can intercept more than one direction) of messages is intercepted. We may intercept 'receiving',
     * or 'sending' messages.
     */
    public String getInterceptedDirections();

    /**
     * Sets which message directions the interceptor should intercept.
     * <p>
     * @param interceptedString a string representing which message directions to intercept. The syntax of this string is
     *                          the same as for the "intercepted_directions" property of {@link AbstractInterceptor} so
     *                          please refer to that class javadoc for details. This is the same format than returned by
     *                          {@link #getInterceptedDirections()}.
     */
    public void setInterceptedDirections(String interceptedString);

    /**
     * Which types ("request" or "response") of messages is intercepted. We can intercept only one type of messages or
     * both.
     */
    public String getInterceptedTypes();

    /**
     * Sets which message types the interceptor should intercept.
     * <p>
     * @param interceptedString a string representing which message types to intercept. The syntax of this string is
     *                          the same as for the "intercepted_types" property of {@link AbstractInterceptor} so
     *                          please refer to that class javadoc for details. This is the same format than returned by
     *                          {@link #getInterceptedTypes}.
     */
    public void setInterceptedTypes(String interceptedString);

    /**
     * Which localities of messages is intercepted. We can intercept either "local", "remote" or both kind of localities.
     */
    public String getInterceptedLocalities();

    /**
     * Sets which message localities the interceptor should intercept.
     * <p>
     * @param interceptedString a string representing which message localities to intercept. The syntax of this string is
     *                          the same as for the "intercepted_localities" property of {@link AbstractInterceptor} so
     *                          please refer to that class javadoc for details. This is the same format than returned by
     *                          {@link #getInterceptedLocalities()}.
     */
    public void setInterceptedLocalities(String interceptedString);

    /**
     * The current ratio for random interception.
     * <p>
     * Please see the {@link AbstractInterceptor} javadoc for what random interception is about.
     *
     * @return the current random interception ratio. This will be a negative number if random interception is disabled.
     */
    public float getInterceptionChance();

    /**
     * Sets the ratio for random interception
     *
     * @param ratio the new ratio to set. This can be a negative number to disable random interception.
     */
    public void setInterceptionChance(float ratio);
}
