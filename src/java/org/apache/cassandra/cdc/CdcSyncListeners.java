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

package org.apache.cassandra.cdc;

import java.util.HashSet;
import java.util.Set;

public class CdcSyncListeners
{
    public interface CdcSyncListener
    {
        public void notify(int version, long segment, int offset, boolean completed);
    }

    public static final CdcSyncListeners instance = new CdcSyncListeners();

    private final Set<CdcSyncListener> listeners = new HashSet<>();

    private CdcSyncListeners()
    {
    }

    public void register(CdcSyncListener listener)
    {
        listeners.add(listener);
    }

    public void unregister(CdcSyncListener listener)
    {
        listeners.remove(listener);
    }

    public void onSynced(int version, long segment, int position, boolean completed)
    {
        for (CdcSyncListener listener : listeners)
            listener.notify(version, segment, position, completed);
    }
}
