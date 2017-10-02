/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.common.base.Splitter;

import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.UUIDGen;

/**
 * Options for NodeSync tracing.
 * <p>
 * The main purpose of this class is to make it easy to convert those options to and from a simple string map for the
 * sake of JMX.
 */
class TracingOptions
{

    static final String ID = "id";
    static final String LEVEL = "level";
    static final String TIMEOUT_SEC = "timeout_sec";
    static final String TABLES = "tables";

    /** Identifier for the tracing session, which can be use to query the tracing events. */
    final UUID id;
    /** The tracing level to use. */
    final TracingLevel level;
    /** Timeout in seconds for the tracing session. A value <= 0 if no timeout is provided. */
    final long timeoutSec;
    /** A set of table to trace; any other table won't have his segments traced; if {@code null}, all tables are
     * traced. */
    @Nullable
    final Set<TableId> tables;

    TracingOptions(UUID id, TracingLevel level, long timeoutSec, Set<TableId> tables)
    {
        this.id = id;
        this.level = level;
        this.timeoutSec = timeoutSec;
        this.tables = tables;
    }

    /**
     * Parse tracing options from a string map (used to provide NodeSync tracing options through JMX).
     * <p>
     * The provided map can contain the following optional options:
     * - "id": a UUID string to use for the tracing session. The main reason to provide this is to allow saving trace
     *   events from multiple nodes in the same session. If not provided, a session id is automatically generated.
     * - "level": the tracing level to use (see {@link TracingLevel} javadoc for details). If not provided, default to "low".
     * - "timeout_sec": a timeout (in seconds) on the tracing session. If provided, the tracing session will
     *   automatically stop itself after that amount of time. If not, it will have to be disabled manually.
     * - "tables": a comma separate of fully-qualified table names. Any table not included in this list will _not_ have
     *   his segments traced. If omitted, all tables are traced.
     * <p>
     * Please note that every option name and value is case sensitive.
     *
     * @param optionMap options for tracing as a string map.
     * @return the option specified in {@code optionMap} as parsed tracing options.
     *
     * @throws IllegalArgumentException if the value of any known option is invalid.
     */
    public static TracingOptions fromMap(Map<String, String> optionMap)
    {
        Map<String, String> map = new HashMap<>(optionMap);

        String idStr = map.remove(ID);
        UUID id = idStr == null ? UUIDGen.getTimeUUID() : UUID.fromString(idStr);

        String levelStr = map.remove(LEVEL);
        TracingLevel level = levelStr == null ? TracingLevel.LOW : TracingLevel.parse(levelStr);

        String timeoutStr = map.remove(TIMEOUT_SEC);
        long timeout = -1L;
        if (timeoutStr != null)
        {
            try
            {
                timeout = Long.parseLong(timeoutStr);
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException(String.format("Invalid value '%s' for the 'timeout_sec' option, not a number", timeoutStr));
            }
            if (timeout <= 0)
                throw new IllegalArgumentException(String.format("Invalid value '%s' for the 'timeout_sec' option, must be strictly positive", timeoutStr));
        }

        String tablesStr = map.remove(TABLES);
        Set<TableId> tables = null;
        if (tablesStr != null)
        {
            tables = new HashSet<>();
            for (String s : Splitter.on(',').trimResults().omitEmptyStrings().split(tablesStr))
            {
                List<String> kt = Splitter.on('.').splitToList(s);
                if (kt.size() != 2)
                    throw new IllegalArgumentException(String.format("Invalid fully-qualified table name '%s' in 'tables' option", s));

                TableMetadata table = Schema.instance.getTableMetadata(kt.get(0), kt.get(1));
                if (table == null)
                    throw new IllegalArgumentException(String.format("Unknown table '%s.%s' in 'tables' option", kt.get(0), kt.get(1)));

                tables.add(table.id);
            }
        }

        if (!map.isEmpty())
            throw new IllegalArgumentException("Invalid options provided: " + map.keySet());

        return new TracingOptions(id, level, timeout, tables);
    }
}
