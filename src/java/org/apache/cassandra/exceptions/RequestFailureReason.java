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

package org.apache.cassandra.exceptions;

import org.apache.cassandra.net.MessagingVersion;

public enum RequestFailureReason
{
    /**
     * The reason for the failure was none of the below reasons or was not recorded by the data node.
     */
    UNKNOWN                  (0x0000, true),

    /**
     * The data node read too many tombstones when attempting to execute a read query (see tombstone_failure_threshold).
     */
    READ_TOO_MANY_TOMBSTONES (0x0001, true),

    /**
     * The request queried an index but that index wasn't build on the data node.
     */
    INDEX_NOT_AVAILABLE (0x0002, true),

    /**
     * The request was writing some data on a CDC enabled table but the CDC commit log segment doesn't have space
     * anymore (slow CDC consumer).
     */
    CDC_SEGMENT_FULL (0x0003, true),

    /**
     * We executed a forwarded counter write but got a failure (any {@link RequestExecutionException} that is not a
     * timeout or an unavailable exception; typically a {@link WriteFailureException} or the like).
     */
    COUNTER_FORWARDING_FAILURE(0x0004, false);

    /** The code to be serialized as an unsigned 16 bit integer */
    private final int code;

    private final boolean shouldLogWarning;

    public static final RequestFailureReason[] VALUES = values();

    RequestFailureReason(int code, boolean shouldLogWarning)
    {
        this.code = code;
        this.shouldLogWarning = shouldLogWarning;
    }

    /**
     * Whether the replica on which the failure happened, should log it as a warning.
     * <p>
     * As such failure will be propagated back to the client if necessary, we may not always want to log it server side
     * (at least not loudly -- at WARN), but when this is likely a rare error and having it log at WARN provides useful
     * information to operators, we still do it.
     */
    public boolean shouldLogWarning()
    {
        return shouldLogWarning;
    }

    public int codeForInternodeProtocol(MessagingVersion version)
    {
        // Before v4, we only knew READ_TOO_MANY_TOMBSTONE and UNKNOWN and
        // sending anything else would fail deserialization
        if (version.compareTo(MessagingVersion.OSS_40) < 0)
            return this == READ_TOO_MANY_TOMBSTONES ? code : UNKNOWN.code;

        return code;
    }

    public int codeForNativeProtocol()
    {
        // We explicitely indicated in the protocol spec that drivers should not
        // error on unknown code so we don't have to worry about the version.
        return code;
    }

    public static RequestFailureReason fromCode(int code)
    {
        for (RequestFailureReason reasonCode : VALUES)
        {
            if (reasonCode.code == code)
                return reasonCode;
        }
        return UNKNOWN;
    }
}
