/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.tools.nodesync;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TokenRange;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static com.datastax.bdp.db.nodesync.NodeSyncService.CancelledValidationException;
import static com.datastax.bdp.db.nodesync.NodeSyncService.NotFoundValidationException;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * {@link NodeSyncCommand} to submit a forced user validation.
 */
@Command(name = "submit", description = "Submit a forced user validation")
public class SubmitValidation extends NodeSyncCommand
{
    private static final Pattern RANGE_PATTERN = Pattern.compile("\\(\\s*(?<l>\\S+)\\s*,\\s*(?<r>\\S+)\\s*]");
    private static final Pattern UNSUPPORTED_RANGE_PATTERN = Pattern.compile("(\\(|\\[)\\s*(?<l>\\S+)\\s*,\\s*(?<r>\\S+)\\s*(]|\\))");

    @VisibleForTesting
    static final String UNSUPPORTED_RANGE_MESSAGE = "Invalid input range: %s: " +
                                                    "only ranges with an open start and closed end are allowed. " +
                                                    "Did you meant (%s, %s]?";

    @Arguments(usage = "<table> [<range>...]",
               description = "The qualified table name, optionally followed by token ranges of the form (x, y]. " +
                             "If no token ranges are specified, then all the tokens will be validated.")
    List<String> args = new ArrayList<>();

    @Option(type = OptionType.COMMAND,
            name = { "-r", "--rate" },
            description = "Rate to be used just for this validation, in KB per second")
    private Integer rateInKB = null;

    @Override
    @SuppressWarnings("resource")
    public final void execute(Metadata metadata, Session session, NodeProbes probes)
    {
        if (args.size() < 1)
            throw new NodeSyncException("A qualified table name should be specified");

        // Parse qualified table name
        TableMetadata tableMetadata = parseTable(metadata, null, args.get(0));
        String keyspaceName = tableMetadata.getKeyspace().getName();
        String tableName = tableMetadata.getName();

        // Parse requested ranges
        List<Range<Token>> requestedRanges = parseRanges(metadata, args.subList(1, args.size()));

        // Get replicas
        Map<Range<Token>, Set<InetAddress>> rangeReplicas = liveRangeReplicas(metadata, keyspaceName, requestedRanges);
        Set<InetAddress> allReplicas = rangeReplicas.values().stream().flatMap(Set::stream).collect(toSet());

        // Validate rate
        validateRate(rateInKB);

        String id = UUIDGen.getTimeUUID().toString();
        Set<InetAddress> successfulReplicas = new HashSet<>();

        probes.run(allReplicas, rangeReplicas::isEmpty, (address, probeSupplier) ->
        {
            // Get all the ranges for the current node, while removing the visited node from the replica lists
            Set<Range<Token>> ranges = rangeReplicas.entrySet()
                                                    .stream()
                                                    .filter(e -> e.getValue().remove(address))
                                                    .map(Map.Entry::getKey)
                                                    .collect(toCollection(TreeSet::new));

            if (ranges.isEmpty())
                return;

            NodeProbe probe = null;
            try
            {
                probe = probeSupplier.get();
                probe.startUserValidation(id, keyspaceName, tableName, format(ranges), rateInKB);

                ranges.forEach(rangeReplicas::remove);
                successfulReplicas.add(address);

                printVerbose("%s: submitted for ranges %s", address, ranges);
            }
            catch (Exception e)
            {
                // If any of the ranges has no more replicas we should propagate the error after trying
                // to cancel the validation in all the nodes where the submission has already succeded
                if (rangeReplicas.values().stream().anyMatch(Set::isEmpty))
                {
                    System.err.printf("%s: failed for ranges %s, there are no more replicas to try: %s%n",
                                      address, ranges, e.getMessage());
                    cancel(probes, successfulReplicas, id);
                    throw new NodeSyncException("Submission failed");
                }
                printVerbose("%s: failed for ranges %s, trying next replicas: %s", address, ranges, e.getMessage());
            }
            finally
            {
                NodeProbes.close(probe, address);
            }
        });

        System.out.println(id);
    }

    /**
     * Associates the specified token ranges to the RPC addresses of their live replicas in the specified cluster
     * metadata and keyspace.
     *
     * @param metadata a CQL driver cluster metadata
     * @param keyspace a keyspace name
     * @param ranges a collection of token ranges
     * @return a map associating the specified token ranges to their live replicas. The returned map keys can be
     * subranges of the specified ranges, but they will be always fully cover all the specified token ranges.
     * @throws NodeSyncException if the specified cluster metadata doesn't contain live nodes covering all the ranges
     */
    private static Map<Range<Token>, Set<InetAddress>> liveRangeReplicas(Metadata metadata,
                                                                         String keyspace,
                                                                         Collection<Range<Token>> ranges)
    {
        Token.TokenFactory tkFactory = tokenFactory(metadata);
        Map<Range<Token>, Set<InetAddress>> replicas = new HashMap<>();
        for (Host host : metadata.getAllHosts().stream().filter(Host::isUp).collect(toSet()))
        {
            // Add the intersection between the requested ranges and the host local ranges
            List<Range<Token>> localRanges = ranges(tkFactory, metadata.getTokenRanges(keyspace, host));
            localRanges.stream()
                       .flatMap(r -> ranges.stream().filter(r::intersects).map(r::intersectionWith))
                       .flatMap(Collection::stream)
                       .distinct()
                       .forEach(r -> replicas.computeIfAbsent(r, k -> Sets.newHashSet()).add(host.getAddress()));
        }

        if (ranges.stream().anyMatch(r -> !r.subtractAll(replicas.keySet()).isEmpty()))
            throw new NodeSyncException("There are not enough live replicas to cover all the requested ranges");

        return replicas;
    }

    /**
     * Tries to cancel the validation identified by the specified id in all the specified nodes.
     *
     * @param probes a JMX runner
     * @param addresses the addresses of the nodes in which the validation is going to be cancelled
     * @param id the id of the validation to be cancelled
     */
    @SuppressWarnings("resource")
    private void cancel(NodeProbes probes, Set<InetAddress> addresses, String id)
    {
        if (addresses.isEmpty())
            return;

        System.err.println("Cancelling validation in those nodes where it was already submitted: " + addresses);

        Set<InetAddress> failed = new HashSet<>();
        probes.run(addresses, () -> false, (address, probeSupplier) ->
        {
            NodeProbe probe = null;
            try
            {
                probe = probeSupplier.get();
                probe.cancelUserValidation(id);
                System.err.printf("%s: cancelled%n", address);
            }
            catch (NotFoundValidationException e)
            {
                System.err.printf("%s: already finished%n", address);
            }
            catch (CancelledValidationException e)
            {
                System.err.printf("%s: already cancelled%n", address);
            }
            catch (Exception e)
            {
                failed.add(address);
                System.err.printf("%s: cancellation failed: %s%n", address, e.getMessage());
            }
            finally
            {
                NodeProbes.close(probe, address);
            }
        });

        if (failed.isEmpty())
            System.err.printf("Validation %s has been successfully cancelled%n", id);
        else
            System.err.printf("Validation %s is still running in nodes %s%n", id, failed);
    }

    @VisibleForTesting
    static void validateRate(Integer rateInKB)
    {
        if (rateInKB != null && rateInKB <= 0)
            throw new NodeSyncException("Rate must be positive");
    }

    /**
     * Returns the token factory to be used with the specified CQL driver cluster metadata.
     *
     * @param metadata a CQL driver cluster metadata
     * @return a token factory
     */
    @VisibleForTesting
    static Token.TokenFactory tokenFactory(Metadata metadata)
    {
        return FBUtilities.newPartitioner(metadata.getPartitioner()).getTokenFactory();
    }

    /**
     * Parses the string argument with format {@code <start>:<end>[,<start>:<end>...]} as a list of {@link TokenRange}s.
     *
     * @param metadata a CQL driver cluster metadata
     * @param args the {@code String}s containing the token ranges representation to be parsed
     * @return the token ranges represented by the string argument
     */
    @VisibleForTesting
    static List<Range<Token>> parseRanges(Metadata metadata, List<String> args)
    {
        Token.TokenFactory tkFactory = tokenFactory(metadata);
        return Range.normalize(args.isEmpty()
                               ? ranges(tkFactory, metadata.getTokenRanges())
                               : args.stream().map(s -> parseRange(tkFactory, s)).collect(toList()));
    }

    /**
     * Parses the string argument with format {@code (<start>,<end>]} as a {@link TokenRange}.
     *
     * @param tokenFactory the token factory
     * @param str the {@code String} containing the token range representation to be parsed
     * @return the token range represented by the string argument
     */
    @VisibleForTesting
    static Range<Token> parseRange(Token.TokenFactory tokenFactory, String str)
    {
        String s = str.trim();
        Matcher matcher = RANGE_PATTERN.matcher(s);

        if (!matcher.matches())
        {
            // Check if it is of the form (x,y), [x,y] or [x,y) to improve the error message
            matcher = UNSUPPORTED_RANGE_PATTERN.matcher(s);
            if (matcher.matches())
            {
                String msg = String.format(UNSUPPORTED_RANGE_MESSAGE, s, matcher.group("l"), matcher.group("r"));
                throw new NodeSyncException(msg);
            }

            throw new NodeSyncException("Cannot parse range: " + s);
        }

        Token start;
        start = parseToken(tokenFactory, matcher.group("l"));
        Token end = parseToken(tokenFactory, matcher.group("r"));

        return new Range<>(start, end);
    }

    /**
     * Parses the string argument with as a {@link Token}.
     *
     * @param tokenFactory the token factory
     * @param str the {@code String} containing the token representation to be parsed
     * @return the token represented by the string argument
     */
    private static Token parseToken(Token.TokenFactory tokenFactory, String str)
    {
        try
        {
            return tokenFactory.fromString(str);
        }
        catch (Exception e)
        {
            throw new NodeSyncException("Cannot parse token: " + str);
        }
    }

    /**
     * Formats the specified token ranges as a string of the form {@code <start>:<end>[,<start>:<end>...]}.
     *
     * @param ranges the token ranges to be formatted
     * @return a string representing the ranges argument
     */
    @VisibleForTesting
    static String format(Collection<Range<Token>> ranges)
    {
        return Range.normalize(ranges).stream().map(SubmitValidation::format).collect(joining(","));
    }

    /**
     * Formats the specified token range as a string of the form {@code <start>:<end>}.
     *
     * @param range the token range to be formatted
     * @return a string representing the ranges argument
     */
    private static String format(Range<Token> range)
    {
        return range.left + ":" + range.right;
    }

    /**
     * Returns the specified CQL driver token ranges as a list of Cassandra token ranges.
     *
     * @param tokenFactory a token factory
     * @param ranges a collection of CQL driver token ranges
     * @return {@code ranges} as a list of Cassandra token ranges
     */
    private static List<Range<Token>> ranges(Token.TokenFactory tokenFactory, Collection<TokenRange> ranges)
    {
        return ranges.stream().map(s -> range(tokenFactory, s)).collect(toList());
    }

    /**
     * Returns the specified CQL driver token range as a Cassandra token range.
     *
     * @param tokenFactory a token factory
     * @param range a CQL driver token range
     * @return {@code range} as Cassandra token range
     */
    private static Range<Token> range(Token.TokenFactory tokenFactory, TokenRange range)
    {
        Token start = tokenFactory.fromString(range.getStart().toString());
        Token end = tokenFactory.fromString(range.getEnd().toString());
        return new Range<>(start, end);
    }
}