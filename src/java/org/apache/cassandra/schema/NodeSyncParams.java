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
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.apollo.nodesync.NodeSyncService;
import org.apache.cassandra.cql3.CQLSyntaxHelper;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.units.Units;

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
    private final Integer deadlineTargetSec; // null if unset, in which case it default to max(gc_grace, 4 days)

    // When dealing with the schema, we could have unknown parameters in mixed-version clusters where never nodes have
    // new options and we don't want to throw those away.
    private final Map<String, String> unknownParameters;

    @VisibleForTesting
    public NodeSyncParams(Boolean isEnabled,
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
     * @return the {@link NodeSyncParams} corresponding to {@code map} if it contains valid NodeSync parameters.
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
            throw new InvalidRequestException(String.format("Unknown options provided for nodesync: %s", map.keySet()));

        // Setting a deadline lower than the minimum time we'll enforce between validation is a sure way to make sure
        // your deadline is never meant. Doing so is also genuinely a misconfiguration since MIN_VALIDATION_INTERVAL_MS
        // is meant to be order of magnitude lower than any reasonable deadline (it is by default and it's not meant
        // to be changed from its default by user, only for testing).
        // That said, this check is pretty imperfect because while the deadline is cluster-wide and persistent thing (by
        // virtue of being part of the schema), the value of MIN_VALIDATION_INTERVAL_MS is node-based (so may not be
        // set wrong on the node on which this method is called) and could be changed on any node restart (so after
        // the deadline has been set in particular). We actually have a more consistent warning in
        // ContinuousTableValidationProposer.Proposal#computeMinTimeForNextValidation, so this one is here more because
        // it's more user friendly (directly reject the CREATE/ALTER) when it's triggered.
        if (params.deadlineTargetSec != null && TimeUnit.SECONDS.toMillis(params.deadlineTargetSec) <= NodeSyncService.MIN_VALIDATION_INTERVAL_MS)
        {
            // Somewhat random estimation of when user have toyed with the min validation interval in an un-reasonable
            // way. Only there to provide slightly more helpful message so guess-estimate is fine.
            boolean minValidationIsHigh = NodeSyncService.MIN_VALIDATION_INTERVAL_MS > TimeUnit.HOURS.toMillis(10);
            throw new InvalidRequestException(String.format("nodesync '%s' setting has been set to %s which is lower than the %s value (%s): "
                                                            + "this mean the deadline cannot be achieved, at least on this node. %s",
                                                            Option.DEADLINE_TARGET_SEC,
                                                            Units.toString(params.deadlineTargetSec, TimeUnit.SECONDS),
                                                            NodeSyncService.MIN_VALIDATION_INTERVAL_PROP_NAME,
                                                            Units.toString(NodeSyncService.MIN_VALIDATION_INTERVAL_MS, TimeUnit.MILLISECONDS),
                                                            minValidationIsHigh ? "The custom value set for " + NodeSyncService.MIN_VALIDATION_INTERVAL_PROP_NAME + " seems unwisely high"
                                                                                : "This seems like an unwisely low value for " + Option.DEADLINE_TARGET_SEC));
        }

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

    // For MVs, we default anything to it's base table and this method handles this.
    private static <T> T withViewDefaultHandling(TableMetadata table,
                                                 Option option,
                                                 T nonViewDefault,
                                                 BiFunction<NodeSyncParams, TableMetadata, T> getter)
    {
        if (!table.isView())
            return nonViewDefault;

        TableMetadataRef baseTable = View.findBaseTable(table.keyspace, table.name);
        if (baseTable == null)
        {
            // Shouldn't really happen so logging, but not a very appropriate place to break otherwise.
            nospamLogger.warn("Cannot find base table for view {} while checking NodeSync '{}' setting: "
                              + "this shouldn't happen and should be reported but defaulting to {} in the meantime",
                              table, option, nonViewDefault);
            return nonViewDefault;
        }
        TableMetadata base = baseTable.get();
        return getter.apply(base.params.nodeSync, base);
    }

    /**
     * Whether NodeSync is enabled on the table of which this is the NodeSync parameters.
     */
    public boolean isEnabled(TableMetadata table)
    {
        if (isEnabled != null)
            return isEnabled;

        // We force nodesync on the system distributed keyspace because it cannot be altered manually.
        // TODO(Sylvain): we should fix the later part, this is not ideal at all.
        if (SchemaConstants.isReplicatedSystemKeyspace(table.keyspace))
            return true;

        return withViewDefaultHandling(table, Option.ENABLED, DEFAULT_ENABLED, NodeSyncParams::isEnabled);
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
        if (deadlineTargetSec != null)
            return unit.convert(deadlineTargetSec, TimeUnit.SECONDS);

        long defaultValue = unit.convert(Math.max(MIN_DEFAULT_DEADLINE_TARGET_SEC, table.params.gcGraceSeconds), TimeUnit.SECONDS);
        return withViewDefaultHandling(table, Option.DEADLINE_TARGET_SEC, defaultValue, (p, t) -> p.deadlineTarget(t, unit));
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
