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

package org.apache.cassandra.db.compaction;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.CompactionParams;

/**
 * The factory for compaction strategies and their containers.
 */
public class CompactionStrategyFactory
{
    private final ColumnFamilyStore cfs;
    private final CompactionLogger compactionLogger;

    public CompactionStrategyFactory(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        this.compactionLogger = new CompactionLogger(cfs.metadata());
    }

    /**
     * Create a strategy container.
     */
    public CompactionStrategyContainer createContainer()
    {
        return createContainer(cfs.metadata().params.compaction,
                               CompactionStrategyContainer.ReloadReason.FULL);
    }

    private CompactionStrategyContainer createContainer(CompactionParams params, CompactionStrategyContainer.ReloadReason reason)
    {
        CompactionStrategyContainer ret;

        if (params.klass() == UnifiedCompactionStrategy.class)
        {
            ret = new UnifiedCompactionContainer(this);
        }
        else
        {
            ret = new CompactionStrategyManager(this);
            ret.reload(params, reason);
        }

        cfs.getTracker().subscribe(ret);
        return ret;
    }

    /**
     * Reload the existing strategy container, possibly creating a new one if required.
     *
     * @param current the current strategy container
     * @param compactionParams the new compaction parameters
     * @param reason the reason for reloading
     *
     * @return a new strategy container or the current one, but reloaded
     */
    public CompactionStrategyContainer reload(CompactionStrategyContainer current,
                                              CompactionParams compactionParams,
                                              CompactionStrategyContainer.ReloadReason reason)
    {
        if (compactionParams.klass() == UnifiedCompactionStrategy.class)
        {
            return createContainer(compactionParams, reason);
        }
        else
        {
            if (current instanceof CompactionStrategyManager)
            {
                current.reload(compactionParams, reason);
                return current;
            }
            else
            {
                return createContainer(compactionParams, reason);
            }
        }
    }

    public CompactionLogger getCompactionLogger()
    {
        return compactionLogger;
    }

    ColumnFamilyStore getCfs()
    {
        return cfs;
    }

    /**
     * Creates a compaction strategy that is managed by {@link CompactionStrategyManager} and its strategy holders.
     * These strategies must extend {@link LegacyAbstractCompactionStrategy}.
     *
     * @return an instance of the compaction strategy specified in the parameters so long as it extends {@link LegacyAbstractCompactionStrategy}
     * @throws IllegalArgumentException if the params do not contain a strategy that extends  {@link LegacyAbstractCompactionStrategy}
     */
    LegacyAbstractCompactionStrategy createLegacyStrategy(CompactionParams compactionParams)
    {
        // TODO - make it non static and pass the logger to the strategies
        try
        {
            if (!LegacyAbstractCompactionStrategy.class.isAssignableFrom(compactionParams.klass()))
                throw new IllegalArgumentException("Expected compaction params for legacy strategy: " + compactionParams);

            Constructor<? extends CompactionStrategy> constructor =
            compactionParams.klass().getConstructor(CompactionStrategyFactory.class, Map.class);
            LegacyAbstractCompactionStrategy ret = (LegacyAbstractCompactionStrategy) constructor.newInstance(this, compactionParams.options());
            compactionLogger.strategyCreated(ret);
            return ret;
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e)
        {
            throw org.apache.cassandra.utils.Throwables.cleaned(e);
        }
    }

    /**
     * Create a compaction strategy. This is only called by tiered storage so we forward to the legacy strategy.
     */
    public CompactionStrategy createStrategy(CompactionParams compactionParams)
    {
        return createLegacyStrategy(compactionParams);
    }
}