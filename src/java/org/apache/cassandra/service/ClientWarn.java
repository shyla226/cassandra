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
package org.apache.cassandra.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.concurrent.ExecutorLocal;
import org.apache.cassandra.utils.FBUtilities;

public class ClientWarn implements ExecutorLocal<ClientWarn.State>
{
    private static final String TRUNCATED = " [truncated]";
    private static final FastThreadLocal<State> warnLocal = new FastThreadLocal<>();

    /** The client warning states mapped by message id. When a request is sent to
     * a replica, the current thread client warning state is stored in this map.
     * When the corresponding response is received, the state is removed from this
     * map and transferred back into the thread local of the executor that will process
     * the response.
     */
    private static final ConcurrentMap<Integer, State> statesByMessageId = new ConcurrentHashMap();

    public static ClientWarn instance = new ClientWarn();

    private ClientWarn()
    {
    }

    public State get()
    {
        return warnLocal.get();
    }

    public void set(State value)
    {
        warnLocal.set(value);
    }

    public void warn(String text)
    {
        State state = warnLocal.get();
        if (state != null)
            state.add(text);
    }

    public void captureWarnings()
    {
        warnLocal.set(new State());
    }

    public List<String> getWarnings()
    {
        State state = warnLocal.get();
        if (state == null || state.warnings.isEmpty())
            return null;
        return state.warnings;
    }

    public void resetWarnings()
    {
        warnLocal.remove();
    }

    /**
     * Store the current thread local client warning state for this request
     * id, if any state is present.
     * @param id - the request id.
     */
    public void storeForRequest(int id)
    {
        State state = warnLocal.get();
        if (state != null)
            statesByMessageId.put(id, state);
    }

    /**
     * Return the state for this message id or null if no state was stored.
     * @param id the id of the request
     * @return the client warning state or null
     */
    @Nullable
    public State getForMessage(int id)
    {
        return statesByMessageId.remove(id);
    }

    public static class State
    {
        private final List<String> warnings = new ArrayList<>();

        private void add(String warning)
        {
            if (warnings.size() < FBUtilities.MAX_UNSIGNED_SHORT)
                warnings.add(maybeTruncate(warning));
        }

        private static String maybeTruncate(String warning)
        {
            return warning.length() > FBUtilities.MAX_UNSIGNED_SHORT
                   ? warning.substring(0, FBUtilities.MAX_UNSIGNED_SHORT - TRUNCATED.length()) + TRUNCATED
                   : warning;
        }

    }
}
