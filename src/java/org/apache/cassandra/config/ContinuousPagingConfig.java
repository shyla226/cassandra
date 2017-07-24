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

package org.apache.cassandra.config;

import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Joiner;

import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * The YAML options for continuous paging.
 */
public class ContinuousPagingConfig
{
    /** The default values currently match the values in the yaml file */
    private final static int DEFAULT_MAX_CONCURRENT_SESSIONS = 60;
    private final static int DEFAULT_MAX_SESSION_PAGES = 4;
    private final static int DEFAULT_MAX_PAGE_SIZE_MB = 8;
    private final static int DEFAULT_MAX_LOCAL_QUERY_TIME_MS = 5000;
    private final static int DEFAULT_CLIENT_TIMEOUT_SEC = 120;
    private final static int DEFAULT_CANCEL_TIMEOUT_SEC = 5;
    private final static int DEFAULT_PAUSED_CHECK_INTERVAL_MS = 1;

    /** The maximum number of concurrent sessions, any additional
        session will be rejected with an unavailable error. */
    public int max_concurrent_sessions;

    /** The maximum number of pages that can be buffered for each session */
    public int max_session_pages;

    /** The maximum size of a page, in mb */
    public int max_page_size_mb;

    /**
     * The maximum time in milliseconds for which a local continuous query should be running. Beyond this time,
     * the query will be swapped out and restarted, to ensure that the resources kept by
     * {@link ReadExecutionController} are released, such as the memtable {@link OpOrder}.
     */
    public int max_local_query_time_ms;

    /** How long the server will wait for a client to request more pages, assuming the server queue is full or
     * the client has not requested any more pages via a backpressure update request. Increase this value for
     * extremely large page sizes ({@link #max_page_size_mb}) or extremely slow networks. */
    public int client_timeout_sec;

    /** How long the server will wait for a cancel request to complete, in seconds. */
    public int cancel_timeout_sec;

    /**
     * Controls how long to wait before checking if a continuous paging sessions paused because of backpressure
     * can be resumed. If the client has not requested any more pages (DSE_V2) or if the server queue is full (DSE_V1),
     * the session is paused and an event is scheduled after {@link #paused_check_interval_ms} milliseconds to check
     * if the session can be resumed. It shouldn't be necessary to change this parameter. Obviously values that are
     * too big will give poor performance if backpressure kicks in, however the best strategy for performance is
     * not to hit backpressure in the first place by adjusting the queue size in the client and in {@link #max_session_pages}.
     */
    public int paused_check_interval_ms;

    public ContinuousPagingConfig()
    {
        this(DEFAULT_MAX_CONCURRENT_SESSIONS,
             DEFAULT_MAX_SESSION_PAGES,
             DEFAULT_MAX_PAGE_SIZE_MB,
             DEFAULT_MAX_LOCAL_QUERY_TIME_MS,
             DEFAULT_CLIENT_TIMEOUT_SEC,
             DEFAULT_CANCEL_TIMEOUT_SEC,
             DEFAULT_PAUSED_CHECK_INTERVAL_MS);
    }

    public ContinuousPagingConfig(int max_concurrent_sessions,
                                  int max_session_pages,
                                  int max_page_size_mb,
                                  int max_local_query_time_ms,
                                  int client_timeout_sec,
                                  int cancel_timeout_sec,
                                  int paused_check_interval_ms)
    {
        this.max_concurrent_sessions = max_concurrent_sessions;
        this.max_session_pages = max_session_pages;
        this.max_page_size_mb = max_page_size_mb;
        this.max_local_query_time_ms = max_local_query_time_ms;
        this.client_timeout_sec = client_timeout_sec;
        this.cancel_timeout_sec = cancel_timeout_sec;
        this.paused_check_interval_ms = paused_check_interval_ms;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;

        if (!(other instanceof ContinuousPagingConfig))
            return false;

        ContinuousPagingConfig that = (ContinuousPagingConfig) other;

        return max_concurrent_sessions == that.max_concurrent_sessions
               && max_session_pages == that.max_session_pages
               && max_page_size_mb == that.max_page_size_mb
               && max_local_query_time_ms == that.max_local_query_time_ms
               && client_timeout_sec == that.client_timeout_sec
               && cancel_timeout_sec == that.cancel_timeout_sec
               && paused_check_interval_ms == that.paused_check_interval_ms;
    }

    @Override
    public int hashCode()
    {
        return java.util.Objects.hash(max_concurrent_sessions,
                                      max_session_pages,
                                      max_page_size_mb,
                                      max_local_query_time_ms,
                                      client_timeout_sec,
                                      cancel_timeout_sec,
                                      paused_check_interval_ms);
    }

    private Map<String, String> toStringMap()
    {
        Map<String, String> m = new TreeMap<>();
        m.put("max_concurrent_sessions", Integer.toString(max_concurrent_sessions));
        m.put("max_session_pages", Integer.toString(max_session_pages));
        m.put("max_page_size_mb", Integer.toString(max_page_size_mb));
        m.put("max_local_query_time_ms", Integer.toString(max_local_query_time_ms));
        m.put("client_timeout_sec", Integer.toString(client_timeout_sec));
        m.put("cancel_timeout_sec", Integer.toString(cancel_timeout_sec));
        m.put("paused_check_interval_ms", Integer.toString(paused_check_interval_ms));
        return m;
    }

    @Override
    public String toString()
    {
        return '{' + Joiner.on(", ").withKeyValueSeparator("=").join(toStringMap()) + '}';
    }
}
