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

package org.apache.cassandra.concurrent;

/**
 * <p>A message that defines what scheduler and executor to use for handling its related operations.</p>
 * 
 * <p>For example, for request-response messages, it's possible to execute the request on one executor, the response on
 * another executor, and to defer the execution of another operation related to the message to the specific scheduler
 * (e.g. to defer the commit log allocation for a given mutation after the commit log segment is switched).</p>
 * 
 * @see org.apache.cassandra.db.Mutation
 */
public interface SchedulableMessage
{
    /**
     * The executor to use for submitting the request runnables of the message. This will augment these runnables with
     * information about the type of the request.
     */
    TracingAwareExecutor getRequestExecutor();

    /**
     * The executor to use for submitting the response runnables of the message. By default, the response
     * executor would be the same as the request executor. For one-way messages, the response executor won't be used.
     */    
    default TracingAwareExecutor getResponseExecutor()
    {
        return getRequestExecutor();
    };
    
    /**
     * The executor to use when scheduling miscellaneous stages of the processing.
     */
    StagedScheduler getScheduler();
}
