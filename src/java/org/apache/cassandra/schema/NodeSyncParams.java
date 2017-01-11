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

package org.apache.cassandra.schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQLSyntaxHelper;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Per-table parameters for NodeSync.
 */
public final class NodeSyncParams
{
    private static final Logger logger = LoggerFactory.getLogger(NodeSyncParams.class);
    private static final NoSpamLogger nospamLogger = NoSpamLogger.getLogger(logger, 5, TimeUnit.MINUTES);

    public enum Option
    {
        ENABLED,
        DEADLINE_TARGET_SEC;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    private static final boolean DEFAULT_ENABLED = false;
    private static final int MIN_DEFAULT_DEADLINE_TARGET_SEC = (int)TimeUnit.DAYS.toSeconds(4);

    public static final NodeSyncParams DEFAULT = new NodeSyncParams(null, null, Collections.emptyMap());

    @Nullable
    private final Boolean isEnabled; // null if unset
    @Nullable
    private final Integer deadlineTargetSec; // null if unset, in which case it default to min(gc_grace, 4 days)

    // When dealing with the schema, we could have unknown parameters in mixed-version clusters where never nodes have
    // new options and we don't want to throw those away.
    private final Map<String, String> unknownParameters;

    private NodeSyncParams(Boolean isEnabled,
                           Integer deadlineTargetSec,
                           Map<String, String> unknownParameters)
    {
        this.isEnabled = isEnabled;
        this.deadlineTargetSec = deadlineTargetSec;
        this.unknownParameters = unknownParameters;
    }

    /**
     * Builds the NodeSync parameters object corresponding to the provided string map.
     * <p>
     * Contrarily to {@link #fromMap}, this method is to be use on parameters directly coming from user inputs (so
     * a CREATE TABLE or ALTER TABLE statement) and will reject any invalid input, including one having unknown
     * option.
     *
     * @param map the map of user provided parameters.
     * @return the {@link NodeSyncParams} corresponding to {@code map} if it contains valid NodeSync paramters.
     *
     * @throws InvalidRequestException if {@code map} doesn't correspond to valid NodeSync per-table options.
     */
    public static NodeSyncParams fromUserProvidedParameters(Map<String, String> map)
    {
        NodeSyncParams params = fromMapInternal(map, (exc, opt) -> {
            throw (exc instanceof InvalidRequestException)
                  ? new InvalidRequestException(String.format("Invalid value for nodesync option '%s': %s", opt, exc.getMessage()))
                  : exc; // Rethrow unexpected exception as-is so it get properly considered as a server-side bug, which it is
        });

        if (!params.unknownParameters.isEmpty())
            throw new InvalidRequestException("Unknown options provided for nodesync: " + map.keySet());

        return params;
    }

    /**
     * Converts (assumed valid) NodeSync parameters in the provided string map to a proper {@link NodeSyncParams}.
     * <p>
     * This method should only be used internally (typically by schema code), when the parameters are known to be valid.
     */
    public static NodeSyncParams fromMap(String ksName, String tableName, Map<String, String> map)
    {
        return fromMapInternal(map, (exc, opt) -> nospamLogger.error("Unexpected error parsing NodeSync '{}' option for {}.{} from {}: {}",
                                                                     opt, ksName, tableName, map, exc));
    }

    private static NodeSyncParams fromMapInternal(Map<String, String> map, BiConsumer<RuntimeException, Option> errHandler)
    {
        if (map == null)
            return DEFAULT;

        Map<String, String> params = new HashMap<>(map);

        Boolean isEnabled = getOpt(params, Option.ENABLED, NodeSyncParams::parseEnabled, errHandler);
        Integer deadlineTargetSec = getOpt(params, Option.DEADLINE_TARGET_SEC, NodeSyncParams::parseDeadline, errHandler);

        return new NodeSyncParams(isEnabled, deadlineTargetSec, Collections.unmodifiableMap(params));
    }

    private static Boolean parseEnabled(String value)
    {
        // We don't rely on Boolean.parseBoolean as it coerce everything that is not equal to "true" into false, but we
        // prefer something a bit more strict
        if (value.equalsIgnoreCase("true"))
            return true;
        if (value.equalsIgnoreCase("false"))
            return false;

        throw new InvalidRequestException("expected 'true' or 'false' but got " + CQLSyntaxHelper.toCQLString(value));
    }

    private static Integer parseDeadline(String value)
    {
        try
        {
            int deadline = Integer.parseInt(value);
            if (deadline <= 0)
                throw new InvalidRequestException("expected a strictly positive integer but got " + deadline);
            return deadline;
        }
        catch (NumberFormatException e)
        {
            throw new InvalidRequestException("expect a strictly positive integer but got " + CQLSyntaxHelper.toCQLString(value));
        }
    }

    private static <T> T getOpt(Map<String, String> options,
                                Option option,
                                Function<String, T> optHandler,
                                BiConsumer<RuntimeException, Option> errHandler)
    {
        try
        {
            String val = options.remove(option.toString());
            return val == null ? null : optHandler.apply(val);
        }
        catch (RuntimeException e)
        {
            errHandler.accept(e, option);
            return null;
        }
    }

    /**
     * Returns the parameters of this object as a string map.
     */
    public Map<String, String> asMap()
    {
        Map<String, String> map = new HashMap<>(unknownParameters);
        if (isEnabled != null)
            map.put(Option.ENABLED.toString(), isEnabled.toString());
        if (deadlineTargetSec != null)
            map.put(Option.DEADLINE_TARGET_SEC.toString(), Integer.toString(deadlineTargetSec));
        return map;
    }

    /**
     * Whether NodeSync is enabled on the table of which this is the NodeSync parameters.
     */
    public boolean isEnabled(TableMetadata table)
    {
        // We force nodesync on the system distributed keyspace because it cannot be altered manually.
        return isEnabled == null
               ? (SchemaConstants.isReplicatedSystemKeyspace(table.keyspace) || DEFAULT_ENABLED)
               : isEnabled;
    }

    /**
     * The deadline target for NodeSync on the table for which this is the NodeSync parameters.
     * <p>
     * The NodeSync deadline is the maximum time we want between two validation of any given segment. If unset, this
     * default to the max of the table gc_grace value (what we generally want to avoid data resurrections) and
     * {@link #MIN_DEFAULT_DEADLINE_TARGET_SEC} (because using gc_grace=0 is not an uncommon pattern, but it's not a
     * valid deadline value, and more generally it's counterproductive to have a deadline too low, while having a
     * very low gc_grace could have its use if you know the table is not at risk of data resurrections but want to get
     * rid of expiring values ASAP).
     * <p>
     * This is call deadline "target" to emphasis the fact that NodeSync cannot guarantee that this deadline will be
     * met under any circumstances (the NodeSync rate may have been set too low for that, nodes may simply not be
     * powerful enough to achieve this deadline (maybe the deadline value has been set unrealistically low), some nodes
     * may be down during an extended period of time thus preventing proper validation of some segments, ....).
     *
     * @param table the metadata of the table of which this is the NodeSync parameters. This is used is the deadline
     *              is unset to default to the table "gc_grace" as explained above.
     * @param unit the unit in which to return the deadline.
     * @return the concrete value to use as deadline target for NodeSync on {@code table} (assuming this object _is_ the
     * NodeSync params of {@code table}).
     */
    public long deadlineTarget(TableMetadata table, TimeUnit unit)
    {
        int inSec = deadlineTargetSec == null
                    ? Math.max(MIN_DEFAULT_DEADLINE_TARGET_SEC, table.params.gcGraceSeconds)
                    : deadlineTargetSec;
        return unit.convert(inSec, TimeUnit.SECONDS);
    }

    @Override
    public String toString()
    {
        return CQLSyntaxHelper.toCQLMap(asMap());
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof NodeSyncParams))
            return false;

        NodeSyncParams that = (NodeSyncParams) o;
        return Objects.equals(this.isEnabled, that.isEnabled)
               && Objects.equals(this.deadlineTargetSec, that.deadlineTargetSec)
               && Objects.equals(this.unknownParameters, that.unknownParameters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(isEnabled, deadlineTargetSec, unknownParameters);
    }
}
