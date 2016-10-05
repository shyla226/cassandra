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

import java.util.Objects;

import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * The YAML options for continuous paging.
 */
public class ContinuousPagingConfig
{
    /** The default values currently match the values in the yaml file */
    private final static int DEFAULT_MAX_CONCURRENT_SESSIONS = 8;
    private final static int DEFAULT_MAX_SESSION_PAGES = 4;
    private final static int DEFAULT_MAX_PAGE_SIZE_MB = 16;
    private final static int DEFAULT_MAX_CLIENT_WAIT_TIME_MS = 2000;
    private final static int DEFAULT_MAX_LOCAL_QUERY_TIME_MS = 5000;

    /** The maximum number of concurrent sessions, any additional
        session will be rejected with an unavailable error. */
    public int max_concurrent_sessions;

    /** The maximum number of pages that can be buffered for each session */
    public int max_session_pages;

    /** The maximum size of a page, in mb */
    public int max_page_size_mb;

    /** The maximum time that the server will wait in case the client is not reading from the socket, if this
        timeout is exceeded, the session is aborted and the client will receive an error. */
    public int max_client_wait_time_ms;

    /**
     * The maximum time in milliseconds for which a local continuous query should be running. Beyond this time,
     * the query will be aborted and restarted, to ensure that the resources kept by
     * {@link ReadExecutionController} are released, such as the memtable {@link OpOrder}.
     */
    public int max_local_query_time_ms;

    /** The maximum number of threads dedicated to continuous paging sessions. By default this is null, which
     * means that we should use max_concurrent_sessions (one thread per session). */
    private Integer max_threads;

    public ContinuousPagingConfig()
    {
        this(DEFAULT_MAX_CONCURRENT_SESSIONS,
             DEFAULT_MAX_SESSION_PAGES,
             DEFAULT_MAX_PAGE_SIZE_MB,
             DEFAULT_MAX_CLIENT_WAIT_TIME_MS,
             DEFAULT_MAX_LOCAL_QUERY_TIME_MS);
    }

    public ContinuousPagingConfig(int max_concurrent_sessions,
                                  int max_session_pages,
                                  int max_page_size_mb,
                                  int max_client_wait_time_ms,
                                  int max_local_query_time_ms)
    {
        this.max_concurrent_sessions = max_concurrent_sessions;
        this.max_session_pages = max_session_pages;
        this.max_page_size_mb = max_page_size_mb;
        this.max_client_wait_time_ms = max_client_wait_time_ms;
        this.max_local_query_time_ms = max_local_query_time_ms;
        this.max_threads = null;
    }

    public int getNumThreads()
    {
        return max_threads == null ? max_concurrent_sessions : max_threads;
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
               && max_client_wait_time_ms == that.max_client_wait_time_ms
               && max_local_query_time_ms == that.max_local_query_time_ms
               && Objects.equals(max_threads, that.max_threads);
    }

    @Override
    public int hashCode()
    {
        return java.util.Objects.hash(max_concurrent_sessions,
                                      max_session_pages,
                                      max_page_size_mb,
                                      max_client_wait_time_ms,
                                      max_local_query_time_ms,
                                      max_threads);
    }
}
