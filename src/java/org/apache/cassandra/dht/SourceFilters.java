/*
 * Copyright DataStax, Inc.
 */
package org.apache.cassandra.dht;

import java.net.InetAddress;
import java.util.*;

import org.apache.cassandra.dht.RangeStreamer.ISourceFilter;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Factory methods for {@code SourceFilters}
 */
final class SourceFilters
{
    /**
     * Creates a new composite {@code ISourceFilter}.
     * @param filters the composing sources
     * @return a new composite {@code ISourceFilter}
     */
    public static ISourceFilter composite(List<ISourceFilter> filters)
    {
        return endpoint -> filters.stream().allMatch(filter -> filter.shouldInclude(endpoint));
    }

    /**
     * Creates a new composite {@code ISourceFilter}.
     * @param filters the composing sources
     * @return a new composite {@code ISourceFilter}
     */
    public static ISourceFilter composite(ISourceFilter... filters)
    {
        return composite(Arrays.asList(filters));
    }

    /**
     * Source filter which excludes any endpoints that are not alive according to a
     * failure detector.
     */
    public static ISourceFilter failureDetectorFilter(IFailureDetector fd)
    {
        return fd::isAlive;
    }

    /**
     * Creates a new {@code ISourceFilter} which accept only the specified source.
     * @param sources the sources to accept
     * @return a new {@code ISourceFilter} which accept only the specified source
     */
    public static ISourceFilter includeSources(Collection<InetAddress> sources)
    {
        return sources::contains;
    }

    /**
     * Creates a new {@code ISourceFilter} which accept only the sources which are not part of the specified set.
     * @param sources the sources to reject
     * @return a new {@code ISourceFilter} which accept only the sources which are not part of the specified set
     */
    public static ISourceFilter excludeSources(Collection<InetAddress> sources)
    {
        return negate(includeSources(sources));
    }

    /**
     * Creates a new {@code ISourceFilter} which excludes the current node from source calculations.
     */
    public static ISourceFilter excludeLocalNode()
    {
        return excludeSources(Collections.singleton(FBUtilities.getBroadcastAddress()));
    }

    /**
     * Creates a new {@code ISourceFilter} which does not accept the sources from the specified datacenters.
     * @param filterPerDc the racks filter for each datacenters
     * @param snitch the snitch
     * @return a new {@code ISourceFilter} which does not accept the sources from the specified datacenters
     */
    public static ISourceFilter excludeDcs(Map<String, ISourceFilter> filterPerDc, IEndpointSnitch snitch)
    {
        return negate(includeDcs(filterPerDc, snitch));
    }

    /**
     * Creates a new {@code ISourceFilter} which accept only the sources from the specified datacenters.
     * @param rackFilterPerDc the racks filter for each accepted datacenters
     * @param snitch the snitch
     * @return a new {@code ISourceFilter} which accept only the sources from the specified datacenters
     */
    public static ISourceFilter includeDcs(Map<String, ISourceFilter> rackFilterPerDc, IEndpointSnitch snitch)
    {
        return endpoint ->
        {
            String dc = snitch.getDatacenter(endpoint);
            ISourceFilter filter = rackFilterPerDc.get(dc);
            return filter != null && filter.shouldInclude(endpoint);
        };
    }

    /**
     * Creates a new {@code ISourceFilter} which accept only the sources from the specified racks.
     * @param racks the racks for which the sources must be accepted
     * @param snitch the snitch
     * @return a new {@code ISourceFilter} which accept only the sources from the specified racks
     */
    public static ISourceFilter includeRacks(Collection<String> racks, IEndpointSnitch snitch)
    {
        return endpoint -> racks.contains(snitch.getRack(endpoint));
    }

    /**
     * Creates a new {@code ISourceFilter} which accept everything.
     * @return a new {@code ISourceFilter} which accept everything
     */
    public static ISourceFilter noop()
    {
        return endpoint -> true;
    }

    /**
     * Creates a new {@code ISourceFilter} which reject everything accepted by the specified filter.
     * @param filter the filter
     * @return a new {@code ISourceFilter} which reject everything accepted by the specified filter
     */
    private static ISourceFilter negate(ISourceFilter filter)
    {
        return endpoint -> !filter.shouldInclude(endpoint);
    }

    /**
     * This class should not be instantiated.
     */
    private SourceFilters()
    {
    }
}
