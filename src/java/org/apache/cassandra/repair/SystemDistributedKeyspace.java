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
package org.apache.cassandra.repair;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.datastax.bdp.db.nodesync.NodeSyncRecord;
import com.datastax.bdp.db.nodesync.Segment;
import com.datastax.bdp.db.nodesync.UserValidationProposer;
import com.datastax.bdp.db.nodesync.ValidationInfo;
import com.datastax.bdp.db.nodesync.ValidationMetrics;
import com.datastax.bdp.db.nodesync.ValidationOutcome;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.CreateTypeStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Throwables;
import static java.lang.String.format;

import static java.util.stream.Collectors.toSet;
import static org.apache.cassandra.schema.SchemaConstants.DISTRIBUTED_KEYSPACE_NAME;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public final class SystemDistributedKeyspace
{
    // Some piece of string we include in all NodeSync messages error messages below.
    private static final String NODESYNC_ERROR_IMPACT_MSG = "this won't prevent NodeSync but may lead to ranges being validated more often than necessary";

    private SystemDistributedKeyspace()
    {
    }

    private static final Logger logger = LoggerFactory.getLogger(SystemDistributedKeyspace.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 5L, TimeUnit.MINUTES);

    public static final String REPAIR_HISTORY = "repair_history";

    public static final String PARENT_REPAIR_HISTORY = "parent_repair_history";

    public static final String VIEW_BUILD_STATUS = "view_build_status";

    public static final String NODESYNC_VALIDATION = "nodesync_validation";
    public static final String NODESYNC_STATUS = "nodesync_status";
    public static final String NODESYNC_METRICS = "nodesync_metrics";
    public static final String NODESYNC_USER_VALIDATIONS = "nodesync_user_validations";

    private static final TableMetadata RepairHistory =
        parse(REPAIR_HISTORY,
              "Repair history",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "columnfamily_name text,"
              + "id timeuuid,"
              + "parent_id timeuuid,"
              + "range_begin text,"
              + "range_end text,"
              + "coordinator inet,"
              + "participants set<inet>,"
              + "exception_message text,"
              + "exception_stacktrace text,"
              + "status text,"
              + "started_at timestamp,"
              + "finished_at timestamp,"
              + "PRIMARY KEY ((keyspace_name, columnfamily_name), id))")
        .build();

    private static final TableMetadata ParentRepairHistory =
        parse(PARENT_REPAIR_HISTORY,
              "Repair history",
              "CREATE TABLE %s ("
              + "parent_id timeuuid,"
              + "keyspace_name text,"
              + "columnfamily_names set<text>,"
              + "started_at timestamp,"
              + "finished_at timestamp,"
              + "exception_message text,"
              + "exception_stacktrace text,"
              + "requested_ranges set<text>,"
              + "successful_ranges set<text>,"
              + "options map<text, text>,"
              + "PRIMARY KEY (parent_id))")
        .build();

    private static final TableMetadata ViewBuildStatus =
        parse(VIEW_BUILD_STATUS,
              "Materialized View build status",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "view_name text,"
              + "host_id uuid,"
              + "status text,"
              + "PRIMARY KEY ((keyspace_name, view_name), host_id))")
        .build();

    public static final UserType NodeSyncValidation =
        parseType(NODESYNC_VALIDATION,
                  "CREATE TYPE %s ("
                  + "started_at timestamp,"
                  + "outcome tinyint,"
                  + "missing_nodes set<inet>)");

    // We want to be able to query the following NodeSync table for a given sub-range, so we use the token as
    // clustering but we also should ensure we use the proper sorting, so we're extracting the type of tokens for this
    // cluster (which depends on the partitioner, but is global and immutable for the cluster lifetime).
    // Further, the number of segments for a single table could grow somewhat large so we want to spread them on
    // multiple nodes, which is why we include the "range_group" below in the partition key.
    private static final String tokenType = DatabaseDescriptor.getPartitioner().getTokenValidator().asCQL3Type().toString();
    private static final TableMetadata NodeSyncStatus =
        parse(NODESYNC_STATUS,
              "Tracks NodeSync recent validations",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "table_name text,"
              + "range_group blob,"  // first byte of start_token, used to distribute segments more evenly on the cluster.
              + "start_token " + tokenType + ','
              + "end_token " + tokenType + ','
              + "last_successful_validation frozen<" + NODESYNC_VALIDATION + ">,"
              + "last_unsuccessful_validation frozen<" + NODESYNC_VALIDATION + ">,"
              + "locked_by inet,"
              + "PRIMARY KEY ((keyspace_name, table_name, range_group), start_token, end_token))",
              Collections.singleton(NodeSyncValidation))
        .defaultTimeToLive((int)TimeUnit.DAYS.toSeconds(28))
        .build();

    public static final UserType NodeSyncMetrics =
        parseType(NODESYNC_METRICS,
                  "CREATE TYPE %s ("
                  + "data_validated bigint,"
                  + "data_repaired bigint,"
                  + "objects_validated bigint,"
                  + "objects_repaired bigint,"
                  + "repair_data_sent bigint,"
                  + "repair_objects_sent bigint,"
                  + "pages_outcomes frozen<map<text, bigint>>)");

    private static final TableMetadata NodeSyncUserValidations =
        parse(NODESYNC_USER_VALIDATIONS,
              "NodeSync user-triggered validations status",
              "CREATE TABLE %s ("
              + "id text,"
              + "keyspace_name text static,"
              + "table_name text static,"
              + "node inet,"
              + "status text,"
              + "validated_ranges frozen<set<text>>,"
              + "started_at timestamp,"
              + "ended_at timestamp,"
              + "segments_to_validate bigint,"
              + "segments_validated bigint,"
              + "outcomes frozen<map<text, bigint>>,"
              + "metrics frozen<" + NODESYNC_METRICS + ">,"
              + "PRIMARY KEY (id, node))",
              Collections.singleton(NodeSyncMetrics))
        .defaultTimeToLive((int)TimeUnit.DAYS.toSeconds(1))
        .build();

    private static TableMetadata.Builder parse(String table, String description, String cql)
    {
        return parse(table, description, cql, Collections.emptyList());
    }

    private static TableMetadata.Builder parse(String table, String description, String cql, Collection<UserType> types)
    {
        return CreateTableStatement.parse(format(cql, table), DISTRIBUTED_KEYSPACE_NAME, types)
                                   .id(TableId.forSystemTable(DISTRIBUTED_KEYSPACE_NAME, table))
                                   .dcLocalReadRepairChance(0.0)
                                   .comment(description);
    }

    private static UserType parseType(String name, String cql)
    {
        return CreateTypeStatement.parse(format(cql, name), DISTRIBUTED_KEYSPACE_NAME);
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(DISTRIBUTED_KEYSPACE_NAME,
                                       KeyspaceParams.simple(3),
                                       Tables.of(RepairHistory, ParentRepairHistory, ViewBuildStatus, NodeSyncStatus, NodeSyncUserValidations),
                                       Views.none(),
                                       types(),
                                       Functions.none());
    }

    private static Types types()
    {
        return Types.of(NodeSyncValidation, NodeSyncMetrics);
    }

    public static void startParentRepair(UUID parent_id, String keyspaceName, String[] cfnames, RepairOption options)
    {
        Collection<Range<Token>> ranges = options.getRanges();
        String query = "INSERT INTO %s.%s (parent_id, keyspace_name, columnfamily_names, requested_ranges, started_at,          options)"+
                                 " VALUES (%s,        '%s',          { '%s' },           { '%s' },          toTimestamp(now()), { %s })";
        String fmtQry = format(query,
                                      DISTRIBUTED_KEYSPACE_NAME,
                                      PARENT_REPAIR_HISTORY,
                                      parent_id.toString(),
                                      keyspaceName,
                                      Joiner.on("','").join(cfnames),
                                      Joiner.on("','").join(ranges),
                                      toCQLMap(options.asMap(), RepairOption.RANGES_KEY, RepairOption.COLUMNFAMILIES_KEY));
        processSilentBlocking(fmtQry);
    }

    private static String toCQLMap(Map<String, String> options, String ... ignore)
    {
        Set<String> toIgnore = Sets.newHashSet(ignore);
        StringBuilder map = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : options.entrySet())
        {
            if (!toIgnore.contains(entry.getKey()))
            {
                if (!first)
                    map.append(',');
                first = false;
                map.append(format("'%s': '%s'", entry.getKey(), entry.getValue()));
            }
        }
        return map.toString();
    }

    public static void failParentRepair(UUID parent_id, Throwable t)
    {
        String query = "UPDATE %s.%s SET finished_at = toTimestamp(now()), exception_message=?, exception_stacktrace=? WHERE parent_id=%s";

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        String fmtQuery = format(query, DISTRIBUTED_KEYSPACE_NAME, PARENT_REPAIR_HISTORY, parent_id.toString());
        String message = t.getMessage();
        processSilentBlocking(fmtQuery, message != null ? message : "", sw.toString());
    }

    public static void successfulParentRepair(UUID parent_id, Collection<Range<Token>> successfulRanges)
    {
        String query = "UPDATE %s.%s SET finished_at = toTimestamp(now()), successful_ranges = {%s} WHERE parent_id=%s";
        String rangesAsString = successfulRanges.isEmpty() ? "" : String.format("'%s'", Joiner.on("','").join(successfulRanges));
        String fmtQuery = format(query, DISTRIBUTED_KEYSPACE_NAME, PARENT_REPAIR_HISTORY, rangesAsString, parent_id.toString());
        processSilentBlocking(fmtQuery);
    }

    public static void startRepairs(UUID id, UUID parent_id, String keyspaceName, String[] cfnames, Collection<Range<Token>> ranges, Iterable<InetAddress> endpoints)
    {
        String coordinator = FBUtilities.getBroadcastAddress().getHostAddress();
        Set<String> participants = Sets.newHashSet(coordinator);

        for (InetAddress endpoint : endpoints)
            participants.add(endpoint.getHostAddress());

        String query =
                "INSERT INTO %s.%s (keyspace_name, columnfamily_name, id, parent_id, range_begin, range_end, coordinator, participants, status, started_at) " +
                        "VALUES (   '%s',          '%s',              %s, %s,        '%s',        '%s',      '%s',        { '%s' },     '%s',   toTimestamp(now()))";

        for (String cfname : cfnames)
        {
            for (Range<Token> range : ranges)
            {
                String fmtQry = format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, REPAIR_HISTORY,
                                              keyspaceName,
                                              cfname,
                                              id.toString(),
                                              parent_id.toString(),
                                              range.left.toString(),
                                              range.right.toString(),
                                              coordinator,
                                              Joiner.on("', '").join(participants),
                                              RepairState.STARTED.toString());
                processSilentBlocking(fmtQry);
            }
        }
    }

    public static void failRepairs(UUID id, String keyspaceName, String[] cfnames, Throwable t)
    {
        for (String cfname : cfnames)
            failedRepairJob(id, keyspaceName, cfname, t);
    }

    public static void successfulRepairJob(UUID id, String keyspaceName, String cfname)
    {
        String query = "UPDATE %s.%s SET status = '%s', finished_at = toTimestamp(now()) WHERE keyspace_name = '%s' AND columnfamily_name = '%s' AND id = %s";
        String fmtQuery = format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, REPAIR_HISTORY,
                                        RepairState.SUCCESS.toString(),
                                        keyspaceName,
                                        cfname,
                                        id.toString());
        processSilentBlocking(fmtQuery);
    }

    public static void failedRepairJob(UUID id, String keyspaceName, String cfname, Throwable t)
    {
        String query = "UPDATE %s.%s SET status = '%s', finished_at = toTimestamp(now()), exception_message=?, exception_stacktrace=? WHERE keyspace_name = '%s' AND columnfamily_name = '%s' AND id = %s";
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        String fmtQry = format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, REPAIR_HISTORY,
                                      RepairState.FAILED.toString(),
                                      keyspaceName,
                                      cfname,
                                      id.toString());
        processSilentBlocking(fmtQry, t.getMessage(), sw.toString());
    }

    public static CompletableFuture<Void> startViewBuild(String keyspace, String view, UUID hostId)
    {
        String query = "INSERT INTO %s.%s (keyspace_name, view_name, host_id, status) VALUES (?, ?, ?, ?)";
        return TPCUtils.toFutureVoid(QueryProcessor.process(format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, VIEW_BUILD_STATUS),
                                                            ConsistencyLevel.ONE,
                                                            Lists.newArrayList(bytes(keyspace),
                                                                               bytes(view),
                                                                               bytes(hostId),
                                                                               bytes(BuildStatus.STARTED.toString()))));
    }

    public static CompletableFuture<Void> successfulViewBuild(String keyspace, String view, UUID hostId)
    {
        String query = "UPDATE %s.%s SET status = ? WHERE keyspace_name = ? AND view_name = ? AND host_id = ?";
        return TPCUtils.toFutureVoid(QueryProcessor.process(format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, VIEW_BUILD_STATUS),
                                                            ConsistencyLevel.ONE,
                                                            Lists.newArrayList(bytes(BuildStatus.SUCCESS.toString()),
                                                                               bytes(keyspace),
                                                                               bytes(view),
                                                                               bytes(hostId))));
    }

    public static CompletableFuture<Map<UUID, String>> viewStatus(String keyspace, String view)
    {
        String query = format("SELECT host_id, status FROM %s.%s WHERE keyspace_name = ? AND view_name = ?", SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, VIEW_BUILD_STATUS);
        return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(query, ConsistencyLevel.ONE, keyspace, view))
                       .handle((results, error) -> {
                           if (error != null)
                               return Collections.emptyMap();

                           Map<UUID, String> status = new HashMap<>();
                           for (UntypedResultSet.Row row : results)
                           {
                               status.put(row.getUUID("host_id"), row.getString("status"));
                           }
                           return status;
                       });
    }

    public static CompletableFuture<Void> setViewRemoved(String keyspaceName, String viewName)
    {
        String buildReq = format("DELETE FROM %s.%s WHERE keyspace_name = ? AND view_name = ?", SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, VIEW_BUILD_STATUS);
        return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(buildReq, keyspaceName, viewName))
                       .thenCompose(resultSet -> forceFlush(VIEW_BUILD_STATUS));
    }

    private static void processSilentBlocking(String fmtQry, String... values)
    {
        try
        {
            List<ByteBuffer> valueList = new ArrayList<>();
            for (String v : values)
            {
                valueList.add(bytes(v));
            }
            TPCUtils.blockingGet(QueryProcessor.process(fmtQry, ConsistencyLevel.ONE, valueList));
        }
        catch (Throwable t)
        {
            logger.error("Error executing query "+fmtQry, t);
        }
    }

    private static <T> T withNodeSyncExceptionHandling(Callable<T> callable, T defaultOnError, String operationDescription)
    {
        try
        {
            return callable.call();
        }
        catch (Throwable t)
        {
            logForNodeSyncOnError(t, operationDescription);
            return defaultOnError;
        }
    }

    private static void logForNodeSyncOnError(Throwable error, String operationDescription)
    {
        Throwable cleaned = Throwables.unwrapped(error);
        if (cleaned instanceof UnavailableException)
            noSpamLogger.warn("No replica available for {} ({}): {}", operationDescription, cleaned.getMessage(), NODESYNC_ERROR_IMPACT_MSG);
        else if (cleaned instanceof RequestTimeoutException)
            noSpamLogger.warn("Timeout while {} ({}): {}", operationDescription, cleaned.getMessage(), NODESYNC_ERROR_IMPACT_MSG);
        else
            logger.error(String.format("Unexpected error while %s: %s", operationDescription, NODESYNC_ERROR_IMPACT_MSG), error);
    }

    public static List<NodeSyncRecord> nodeSyncRecords(Segment segment)
    {
        return nodeSyncRecords(segment.table, segment.range);
    }

    /**
     * Retrieves the recorded NodeSync validations that cover a specific {@code range} of a specific table.
     *
     * @param table the table for which to retrieve the records.
     * @param range the range for which to retrieve the records. This <b>must</b> be a non wrapping range.
     * @return all the NodeSync records that cover {@code range}.
     */
    // TODO(Sylvain): this actually doesn't guarantee to get all records covering a segment, because any range
    // that starts strictly before the requested segment but covers it will not be fetched. I don't think it's
    // actually possible to query efficiently all covering ranges _in general_, at least not with the current data mode
    // (and I suspect you have to complicate things quite a bit to make it work), because you really only can express
    // conditions on the start of the range and that's not enough (you can never exclude a range that would start from
    // the beginning but cover the range you want to query). It's not _that_ much of a big deal in context though
    // because segments will be well behaved and we will almost always get what we want. When we don't (after a
    // topology change typically), it's also not the end of the world as it only mean we will validate range a bit more
    // than we would otherwise, so it's only a small inefficiency.
    // All this being said, we could do a bit better than we currently do by querying from 'segment.start - <1/2 segment size>'.
    // The idea being that as we segments won't be of completely random size, we can make it so that we don't fetch
    // anything unnecessary in the "normal" case while make it much more likely to get what we should in the "bad" cases,
    // thus lowering the performance impact of this issue (to something hopefully completely negligible).
    public static List<NodeSyncRecord> nodeSyncRecords(TableMetadata table, Range<Token> range)
    {
        assert !range.isTrulyWrapAround();

        logger.trace("Requesting NodeSync records for range {} of table {}", range, table);

        Token.TokenFactory tkf = table.partitioner.getTokenFactory();

        Callable<List<NodeSyncRecord>> callable = () -> {
            UntypedResultSet rows = queryNodeSyncRecords(table, range, tkf);
            List<NodeSyncRecord> records = new ArrayList<>();
            for (UntypedResultSet.Row row : rows)
            {
                try
                {
                    Token start = tkf.fromByteArray(row.getBytes("start_token"));
                    Token end = tkf.fromByteArray(row.getBytes("end_token"));
                    ValidationInfo lastSuccessfulValidation = row.has("last_successful_validation")
                                                              ? ValidationInfo.fromBytes(row.getBytes("last_successful_validation"))
                                                              : null;
                    ValidationInfo lastUnsuccessfulValidation = row.has("last_unsuccessful_validation")
                                                                ? ValidationInfo.fromBytes(row.getBytes("last_unsuccessful_validation"))
                                                                : null;
                    // The last validation is the last successful one if either there is no unsuccessful one recorded or there is one but
                    // it is older than the last successful one (the later case shouldn't really happen since we remove the last unsuccessful
                    // on a successful one, but no harm in handling that properly if that ever change).
                    ValidationInfo lastValidation = lastUnsuccessfulValidation == null || (lastSuccessfulValidation != null && lastSuccessfulValidation.isMoreRecentThan(lastUnsuccessfulValidation))
                                                    ? lastSuccessfulValidation
                                                    : lastUnsuccessfulValidation;
                    InetAddress lockedBy = row.has("locked_by") ? row.getInetAddress("locked_by") : null;
                    records.add(new NodeSyncRecord(new Segment(table, new Range<>(start, end)), lastValidation, lastSuccessfulValidation, lockedBy));
                }
                catch (RuntimeException e)
                {
                    // Log the issue, but simply ignore the specific record otherwise
                    noSpamLogger.warn("Unexpected error (msg: {}) reading NodeSync record: {}", e.getMessage(), NODESYNC_ERROR_IMPACT_MSG);
                }
            }
            return records;
        };
        return withNodeSyncExceptionHandling(callable, Collections.emptyList(), "reading NodeSync records");
    }

    // See the table definition for why we have this
    private static int rangeGroupFor(Token token)
    {
        int val = token.asByteComparableSource().next();
        assert val >= 0 : "Got END_OF_STREAM (" + val + ") as first byte of token";
        return val;
    }

    private static List<ByteBuffer> queriedGroups(Token start, Token end)
    {
        int startGroup = rangeGroupFor(start);
        int endGroup = end.isMinimum()
                       ? 255 // Groups are the first byte of the token, so they go from 0 to 255.
                       : rangeGroupFor(end);

        assert startGroup <= endGroup : String.format("start=%s (group: %d), end=%s (group: %d)", start, startGroup, end, endGroup);

        // Common for non trivially small tables where there will be much more than 256 segments and thus one segment
        // won't span multiple groups in most case.
        if (startGroup == endGroup)
            return Collections.singletonList(ByteBufferUtil.bytes((byte)startGroup));

        List<ByteBuffer> l = new ArrayList<>(endGroup - startGroup + 1);
        for (int i = startGroup; i <= endGroup; i++)
            l.add(ByteBufferUtil.bytes((byte)i));
        return l;
    }

    private static UntypedResultSet queryNodeSyncRecords(TableMetadata table, Range<Token> range, Token.TokenFactory tkf)
    {
        Token start = range.left;
        Token end = range.right;

        String qBase = "SELECT start_token, end_token, last_successful_validation, last_unsuccessful_validation, locked_by"
                       + " FROM %s.%s"
                       + " WHERE keyspace_name = ? AND table_name = ?"
                       + " AND range_group IN ?"
                       + " AND start_token >= ?";

        // Not that even though segment ranges can't be wrapping, the "last" range will still have the min token as
        // "right" bound, which throws off a 'start_token < ?' condition against it and so we have to special case
        // (CQL doesn't currently have a way to skip a condition; would be nice to allow to do so using 'unset' values
        // but that doesn't work right now).
        if (!end.isMinimum())
            qBase += " AND start_token < ?";

        String q = String.format(qBase, DISTRIBUTED_KEYSPACE_NAME, NODESYNC_STATUS);

        List<ByteBuffer> groups = queriedGroups(start, end);
        ByteBuffer startBytes = tkf.toByteArray(start);
        ByteBuffer endBytes = end.isMinimum() ? null : tkf.toByteArray(end);

        // Not using {@link QueryProcessor#executeAsync} because executeAsync() runs subsequent operations on TPC thread.
        return end.isMinimum()
               ? QueryProcessor.execute(q, ConsistencyLevel.ONE, table.keyspace, table.name, groups, startBytes)
               : QueryProcessor.execute(q, ConsistencyLevel.ONE, table.keyspace, table.name, groups, startBytes, endBytes);
    }

    /**
     * Record that a {@code Segment} is being currently validated by NodeSync on this node (locking it temporarily).
     * <p>
     * See {@link NodeSyncRecord#lockedBy} for details on how we use the segment "lock".
     *
     * @param segment the segment that is currently being validated.
     * @param timeout the timeout to set on the record (so as to not "lock" the range indefinitely if the node dies
     *                while validating the range).
     * @param timeoutUnit the unit for timeout.
     */
    public static void lockNodeSyncSegment(Segment segment, long timeout, TimeUnit timeoutUnit)
    {
        logger.trace("Locking NodeSync segment {}", segment);

        Token.TokenFactory tkf = segment.table.partitioner.getTokenFactory();
        String q = "INSERT INTO %s.%s (keyspace_name, table_name, range_group, start_token, end_token, locked_by)"
                   + " VALUES (?, ?, ?, ?, ?, ?)"
                   + " USING TTL ?";

        String query = String.format(q, DISTRIBUTED_KEYSPACE_NAME, NODESYNC_STATUS);
        withNodeSyncExceptionHandling(() ->
                                      QueryProcessor.execute(query,
                                                             ConsistencyLevel.ONE,
                                                             segment.table.keyspace,
                                                             segment.table.name,
                                                             ByteBufferUtil.bytes((byte)rangeGroupFor(segment.range.left)),
                                                             tkf.toByteArray(segment.range.left),
                                                             tkf.toByteArray(segment.range.right),
                                                             FBUtilities.getBroadcastAddress(),
                                                             (int)timeoutUnit.toSeconds(timeout)),
                                      null,
                                      "recording ongoing NodeSync validation");
    }

    /**
     * Removes the lock set on a {@code Segment} by {@link #lockNodeSyncSegment}.
     * <p>
     * Note that 1) this is mainly used to release the lock on failure, as on normal completion {@link #recordNodeSyncValidation}
     * release the lock directly and we don't have to call this method, and 2) this doesn't perform any check that we
     * do hold the lock, so this shouldn't be called unless we know we do (but reminder that our locking is an
     * optimization in the first place so we don't have to work too hard around races either).
     *
     * @param segment the segment on which to remove the lock.
     */
    public static void forceReleaseNodeSyncSegmentLock(Segment segment)
    {
        logger.trace("Force releasing NodeSync segment {}", segment);

        Token.TokenFactory tkf = segment.table.partitioner.getTokenFactory();
        String q = "DELETE locked_by FROM %s.%s"
                   + " WHERE keyspace_name=? AND table_name=?"
                   + " AND range_group=?"
                   + " AND start_token=? AND end_token=?";
        String query = String.format(q, DISTRIBUTED_KEYSPACE_NAME, NODESYNC_STATUS);
        withNodeSyncExceptionHandling(() ->
                                      QueryProcessor.execute(query,
                                                             ConsistencyLevel.ONE,
                                                             segment.table.keyspace,
                                                             segment.table.name,
                                                             ByteBufferUtil.bytes((byte)rangeGroupFor(segment.range.left)),
                                                             tkf.toByteArray(segment.range.left),
                                                             tkf.toByteArray(segment.range.right)),
                                      null,
                                      "releasing NodeSync lock");
    }

    /**
     * Records the completion (successful or not) of the validation by NodeSync of the provided table {@code segment} on
     * this node.
     *
     * @param segment the segment that has been validated.
     * @param info the information regarding the NodeSync validation to record.
     * @param wasPreviousSuccessful whether the previous validation was successful or not. This is an optimization and
     *                              doesn't have to be exact (it's ok to pass {@code false} if this is unknown for
     *                              instance), but avoid generating too much tombstones unnecessarily.
     */
    public static void recordNodeSyncValidation(Segment segment, ValidationInfo info, boolean wasPreviousSuccessful)
    {
        logger.trace("Recording (and unlocking) NodeSync validation of segment {}: {}", segment, info);

        Token.TokenFactory tkf = segment.table.partitioner.getTokenFactory();

        // Note that we always clean the "lock" when saving a validation
        String q = "INSERT INTO %s.%s (keyspace_name, table_name, range_group, start_token, end_token, last_successful_validation, last_unsuccessful_validation, locked_by) "
                   + "VALUES (?, ?, ?, ?, ?, ?, ?, null)";

        String query = String.format(q, DISTRIBUTED_KEYSPACE_NAME, NODESYNC_STATUS);
        ByteBuffer lastSuccessfulValidation, lastUnsuccessfulValidation;
        if (info.wasSuccessful())
        {
            // If the validation is successful, we record it as such and remove the last unsuccessful one to save space
            // (we only want to record the last unsuccessful one if it's more recent than the last successful one,
            // otherwise it's no useful enough to be worth the bytes).
            lastSuccessfulValidation = info.toBytes();
            // Note that if the previous validation was already successful, we won't need to erase anything, so save
            // the tombstone, avoiding getting into tombstone threshold warnings (and generally speeding up reading).
            // Note that it's not a huge deal if the value of wasPreviousSuccessful if wrong: in the worst case we'll
            // keep around some info on an old unsuccessful validation, but it will timeout anyway.
            lastUnsuccessfulValidation = wasPreviousSuccessful ? ByteBufferUtil.UNSET_BYTE_BUFFER : null;
        }
        else
        {
            // Otherwise, it's unsuccessful so record it as such, but don't touch the last successful one
            lastSuccessfulValidation = ByteBufferUtil.UNSET_BYTE_BUFFER;
            lastUnsuccessfulValidation = info.toBytes();
        }
        withNodeSyncExceptionHandling(() ->
                                      QueryProcessor.execute(query,
                                                             ConsistencyLevel.ONE,
                                                             segment.table.keyspace,
                                                             segment.table.name,
                                                             ByteBufferUtil.bytes((byte)rangeGroupFor(segment.range.left)),
                                                             tkf.toByteArray(segment.range.left),
                                                             tkf.toByteArray(segment.range.right),
                                                             lastSuccessfulValidation,
                                                             lastUnsuccessfulValidation),
                                      null,
                                      "recording NodeSync validation"
        );
    }

    public static void recordNodeSyncUserValidation(UserValidationProposer proposer)
    {
        UserValidationProposer.Statistics statistics = proposer.statistics();

        List<Range<Token>> ranges = proposer.validatedRanges();
        Set<String> stringRanges = ranges == null ? null : ranges.stream().map(Range::toString).collect(toSet());

        ByteBuffer startTime = statistics.startTime() < 0
                               ? null
                               : ByteBufferUtil.bytes(statistics.startTime());

        UserValidationProposer.Status status = proposer.status();
        ByteBuffer endTime;
        switch (status)
        {
            case RUNNING:
                endTime = null;
                break;
            case SUCCESSFUL:
                endTime = ByteBufferUtil.bytes(statistics.endTime());
                break;
            default:
                endTime = ByteBufferUtil.bytes(System.currentTimeMillis());
        }

        ValidationMetrics metrics = statistics.metrics();

        String q = "INSERT INTO %s.%s ("
                   + "id,"
                   + "keyspace_name,"
                   + "table_name,"
                   + "node,"
                   + "status,"
                   + "validated_ranges,"
                   + "started_at,"
                   + "ended_at,"
                   + "segments_to_validate,"
                   + "segments_validated,"
                   + "outcomes, "
                   + "metrics"
                   + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        String query = String.format(q, DISTRIBUTED_KEYSPACE_NAME, NODESYNC_USER_VALIDATIONS);
        withNodeSyncExceptionHandling(() ->
                                      QueryProcessor.execute(query,
                                                             ConsistencyLevel.ONE,
                                                             proposer.id(),
                                                             proposer.table().keyspace,
                                                             proposer.table().name,
                                                             DatabaseDescriptor.getListenAddress(),
                                                             status.toString(),
                                                             stringRanges,
                                                             startTime,
                                                             endTime,
                                                             statistics.segmentsToValidate(),
                                                             statistics.segmentValidated(),
                                                             ValidationOutcome.toMap(statistics.getOutcomes()),
                                                             metrics == null ? null : metrics.toBytes()),
                                      null,
                                      "recording NodeSync user validation");
    }

    private static CompletableFuture<Void> forceFlush(String table)
    {
        if (!DatabaseDescriptor.isUnsafeSystem())
            return Keyspace.open(DISTRIBUTED_KEYSPACE_NAME)
                           .getColumnFamilyStore(table)
                           .forceFlush()
                           .thenApply(pos -> null);

        return TPCUtils.completedFuture();
    }

    private enum RepairState
    {
        STARTED, SUCCESS, FAILED
    }

    private enum BuildStatus
    {
        UNKNOWN, STARTED, SUCCESS
    }
}
