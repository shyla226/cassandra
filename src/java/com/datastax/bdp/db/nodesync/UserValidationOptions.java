/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Streams;

/**
 * Options for a user triggered validation.
 * <p>
 * The main purpose of this class is to make it easy to convert those options to and from a simple string map for the
 * sake of JMX.
 */
class UserValidationOptions
{
    private static final Splitter ON_COMMA = Splitter.on(',').omitEmptyStrings().trimResults();
    private static final Splitter ON_COLON = Splitter.on(':').omitEmptyStrings().trimResults();

    static final String ID = "id";
    static final String KEYSPACE_NAME = "keyspace";
    static final String TABLE_NAME = "table";
    static final String REQUESTED_RANGES = "ranges";

    /** Identifier for the validation. This can be anything as long as no other user validation on the node reuse that
     * same id at the time of submission. */
    final String id;
    /** The table on which the user validation should operate. */
    final TableMetadata table;
    /** The normalized list of ranges on which validation should operate. This can be {@code null} in which case the
     * user validation is on all local ranges (this cannot be empty however). */
    @Nullable
    final ImmutableList<Range<Token>> validatedRanges;

    UserValidationOptions(String id, TableMetadata table, Collection<Range<Token>> validatedRanges)
    {
        assert validatedRanges == null || !validatedRanges.isEmpty();
        this.id = id;
        this.table = table;
        this.validatedRanges = validatedRanges == null ? null : ImmutableList.copyOf(Range.normalize(validatedRanges));
    }

    /**
     * Parse user validation options from a string map (used to provide user validation options through JMX).
     * <p>
     * The provided map <b>must</b> have the following options:
     * - "id": a string identifying the validation. The only constraint on identifier is that no other user validation
     *   with the same id should be used by another validation _on the same node_ at the time of submission. This is
     *   "user" provided so as to make it easy to re-use the same identifier when submitting related validations on
     *   different nodes.
     * - "keyspace": the name of the keyspace for the table of which this is a user validation specification.
     * - "table": the name of the table of which this is a user validation specification.
     * <p>
     * Further, the following options are allowed/recognized:
     * - "ranges": the ranges to validate. If this is unset, then all local ranges (for the table specified) will be
     *   validated. If present, this should be a comma-separated list of token ranges, where each range should be of
     *   the form {@code <start token>:<end token>} (so, for instance, a value for this option could be
     *   "1231:42143,4432155:223"). Note that the ranges, if present, should all be ranges local to the node (and table)
     *   on which the validation with those option is started.
     * <p>
     * Please note that every option name and value is case sensitive.
     *
     * @param optionMap options for a user validation as a string map.
     * @return the option specified in {@code optionMap} as parsed user validation options.
     *
     * @throws IllegalArgumentException if the value of any known option is invalid (the table specified is unknown, the
     * format for the ranges is invalid, etc.), or some unknown options are provided.
     */
    public static UserValidationOptions fromMap(Map<String, String> optionMap)
    {
        String id = optionMap.get(ID);
        if (id == null)
            throw new IllegalArgumentException("Missing mandatory option " + ID);

        String ksName = optionMap.get(KEYSPACE_NAME);
        if (ksName == null)
            throw new IllegalArgumentException("Missing mandatory option " + KEYSPACE_NAME);
        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(ksName);
        if (keyspace == null)
            throw new IllegalArgumentException("Unknown keyspace " + ksName);

        String tableName = optionMap.get(TABLE_NAME);
        if (tableName == null)
            throw new IllegalArgumentException("Missing mandatory option " + TABLE_NAME);
        TableMetadata table = keyspace.getTableOrViewNullable(tableName);
        if (table == null)
            throw new IllegalArgumentException("Unknown table " + tableName);

        String rangesStr = optionMap.get(REQUESTED_RANGES);
        Collection<Range<Token>> ranges = null;
        if (rangesStr != null)
        {
            ranges = parseTokenRanges(rangesStr, table.partitioner);

            // We allow null to mean "all the ranges", but requesting validation on no ranges is nonsensical.
            if (ranges.isEmpty())
                throw new IllegalArgumentException("Invalid empty list of ranges to validate (if you want to validate "
                                                   + "all local ranges, do not specify the " + REQUESTED_RANGES + " option)");
        }
        return new UserValidationOptions(id, table, ranges);
    }

    private static Collection<Range<Token>> parseTokenRanges(String str, IPartitioner partitioner)
    {
        Token.TokenFactory tkFactory = partitioner.getTokenFactory();
        return Streams.of(ON_COMMA.split(str))
                      .map(s -> parseTokenRange(s, tkFactory))
                      .collect(Collectors.toList());
    }

    private static Range<Token> parseTokenRange(String str, Token.TokenFactory tkFactory)
    {
        List<String> l = ON_COLON.splitToList(str);
        if (l.size() != 2)
            throw new IllegalArgumentException("Invalid range definition provided: got " + str + " but expected a range of the form <start>:<end>");

        return new Range<>(parseToken(l.get(0), tkFactory), parseToken(l.get(1), tkFactory));
    }

    private static Token parseToken(String tk, Token.TokenFactory tkFactory)
    {
        try
        {
            return tkFactory.fromString(tk);
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException("Invalid token " + tk);
        }
    }
}
