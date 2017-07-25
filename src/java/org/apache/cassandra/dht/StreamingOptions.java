/*
 * Copyright DataStax, Inc.
 */
package org.apache.cassandra.dht;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.google.common.collect.Multimap;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.RangeStreamer.ISourceFilter;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.Schema;

import static org.apache.cassandra.dht.SourceFilters.*;

/**
 * The options used to specify which sources and keyspaces should be involved during bootstrap or rebuild.
 *
 * <p>For bootstrap the options are specified through System properties.For rebuild the options are passed
 *  through the JMX/nodeTool arguments.</p>
 */
public final class StreamingOptions
{
    public static final String BOOTSTRAP_INCLUDE_DCS = Config.PROPERTY_PREFIX + "bootstrap.includeDCs";
    public static final String BOOTSTRAP_EXCLUDE_DCS = Config.PROPERTY_PREFIX + "bootstrap.excludeDCs";
    public static final String BOOTSTRAP_INCLUDE_SOURCES = Config.PROPERTY_PREFIX + "bootstrap.includeSources";
    public static final String BOOTSTRAP_EXCLUDE_SOURCES = Config.PROPERTY_PREFIX + "bootstrap.excludeSources";
    public static final String BOOTSTRAP_EXCLUDE_KEYSPACES = Config.PROPERTY_PREFIX + "bootstrap.excludeKeyspaces";
    public static final String ARG_INCLUDE_SOURCES = "specific_sources";
    public static final String ARG_EXCLUDE_SOURCES = "exclude_sources";
    public static final String ARG_INCLUDE_DC_NAMES = "source_dc_names";
    public static final String ARG_EXCLUDE_DC_NAMES = "exclude_dc_names";
    /**
     * The datacenters/racks that must be included in the bootstrap/rebuild.
     */
    private final Map<String, Racks> includedDcs;

    /**
     * The datacenters/racks that must be excluded from the bootstrap/rebuild.
     */
    private final Map<String, Racks> excludedDcs;

    /**
     * The source that must be included in the bootstrap/rebuild.
     */
    private final Set<InetAddress> includedSources;

    /**
     * The source that must be excluded from the bootstrap/rebuild.
     */
    private final Set<InetAddress> excludedSources;

    /**
     * The keyspaces that must be excluded from the bootstrap/rebuild.
     */
    private final Set<String> excludedKeyspaces;

    /**
     * Retrieves the {@code StreamingOptions} that need to be used for bootstrapping.
     * @return the {@code StreamingOptions} that need to be used for bootstrapping
     */
    public static StreamingOptions forBootStrap(TokenMetadata tokenMetadata)
    {
        try
        {
            Set<InetAddress> includedSources = getHostsFromProperty(BOOTSTRAP_INCLUDE_SOURCES);
            Set<InetAddress> excludedSource = getHostsFromProperty(BOOTSTRAP_EXCLUDE_SOURCES);

            Map<String, Racks> includedDCs = getDatacentersFromProperty(BOOTSTRAP_INCLUDE_DCS);
            Map<String, Racks> excludedDCs = getDatacentersFromProperty(BOOTSTRAP_EXCLUDE_DCS);

            validateSystemProperties(tokenMetadata, includedSources, excludedSource, includedDCs, excludedDCs);

            return new StreamingOptions(includedDCs,
                                        excludedDCs,
                                        includedSources,
                                        excludedSource,
                                        getKeyspacesFromProperty(BOOTSTRAP_EXCLUDE_KEYSPACES));
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(e.getMessage(), false);
        }
    }

    /**
     * Ensures that system properties specified do not conflict with each others.
     *
     * @param tokenMetadata to validate sources (endpoints), DCs and racks
     * @param includedSources the source that must be included in the bootstrap
     * @param excludedSources the source that must be excluded from the bootstrap
     * @param includedDCs the datacenters/racks that must be included in the bootstrap
     * @param excludedDCs the datacenters/racks that must be excluded from the bootstrap
     */
    private static void validateSystemProperties(TokenMetadata tokenMetadata,
                                                 Set<InetAddress> includedSources,
                                                 Set<InetAddress> excludedSources,
                                                 Map<String, Racks> includedDCs,
                                                 Map<String, Racks> excludedDCs)
    {
        if (includedSources != null && (excludedSources != null ||  includedDCs != null || excludedDCs != null))
            throw new IllegalArgumentException("The " + BOOTSTRAP_INCLUDE_SOURCES + " system property cannot be used together with the "
                                               + BOOTSTRAP_EXCLUDE_SOURCES + ", "
                                               + BOOTSTRAP_INCLUDE_DCS + " or "
                                               + BOOTSTRAP_EXCLUDE_DCS + " system properties");

        if (excludedSources != null && (includedDCs != null || excludedDCs != null))
            throw new IllegalArgumentException("The " + BOOTSTRAP_EXCLUDE_SOURCES + " system property cannot be used together with the "
                                               + BOOTSTRAP_INCLUDE_SOURCES + ", "
                                               + BOOTSTRAP_INCLUDE_DCS + " or "
                                               + BOOTSTRAP_EXCLUDE_DCS + " system properties");

        if (includedDCs != null && excludedDCs != null)
        {
            for (String dc : includedDCs.keySet())
            {
                if (excludedDCs.containsKey(dc) && excludedDCs.get(dc).isEmpty() == includedDCs.get(dc).isEmpty())
                    throw new IllegalArgumentException("The " + BOOTSTRAP_INCLUDE_DCS + " and " + BOOTSTRAP_EXCLUDE_DCS
                                                       + " system properties are conflicting for the datacenter: " + dc);
            }
        }

        validateSourcesDCsRacks(tokenMetadata, includedSources, excludedSources, includedDCs, excludedDCs);
    }

    public static StreamingOptions forRebuild(TokenMetadata tokenMetadata,
                                              List<String> srcDcNames,
                                              List<String> excludeDcNames,
                                              List<String> specifiedSources,
                                              List<String> excludeSources)
    {
        Set<InetAddress> includedSources = getHostsFromArgument(ARG_INCLUDE_SOURCES, "argument", specifiedSources);
        Set<InetAddress> excludedSource = getHostsFromArgument(ARG_EXCLUDE_SOURCES, "argument", excludeSources);

        Map<String, Racks> includedDCs = getDatacentersFromArgument(ARG_INCLUDE_DC_NAMES, "argument", srcDcNames);
        Map<String, Racks> excludedDCs = getDatacentersFromArgument(ARG_EXCLUDE_DC_NAMES, "argument", excludeDcNames);

        validateArguments(tokenMetadata, includedSources, excludedSource, includedDCs, excludedDCs);

        return new StreamingOptions(includedDCs,
                                    excludedDCs,
                                    includedSources,
                                    excludedSource,
                                    null);
    }

    public static StreamingOptions forRebuild(TokenMetadata tokenMetadata,
                                              String sourceDc,
                                              String specificSources)
    {
        Set<InetAddress> includedSources = getHostsFromArgument("specific-sources", "argument", asList(specificSources));

        Map<String, Racks> includedDCs = getDatacentersFromArgument("source-DC", "argument",
                                                                    sourceDc != null ? Collections.singletonList(sourceDc) : null);

        validateArguments(tokenMetadata, includedSources, null, includedDCs, null);

        return new StreamingOptions(includedDCs,
                                    null,
                                    includedSources,
                                    null,
                                    null);
    }

    /**
     * Ensures that arguments specified do not conflict with each others - for "rebuild".
     *
     * @param tokenMetadata to validate sources (endpoints), DCs and racks
     * @param includedSources the source that must be included in the rebuild
     * @param excludedSources the source that must be excluded from the rebuild
     * @param includedDCs the datacenters/racks that must be included in the rebuild
     * @param excludedDCs the datacenters/racks that must be excluded from the rebuild
     */
    private static void validateArguments(TokenMetadata tokenMetadata,
                                          Set<InetAddress> includedSources,
                                          Set<InetAddress> excludedSources,
                                          Map<String, Racks> includedDCs,
                                          Map<String, Racks> excludedDCs)
    {
        if (includedSources == null && excludedSources == null &&
            includedDCs == null && excludedDCs == null)
            throw new IllegalArgumentException("At least one of the "
                                               + ARG_INCLUDE_SOURCES + ", " + ARG_EXCLUDE_SOURCES + ", "
                                               + ARG_INCLUDE_DC_NAMES + ", " + ARG_EXCLUDE_DC_NAMES
                                               + " (or src-dc-name) arguments must be specified for rebuild.");

        if (includedSources != null && (excludedSources != null ||  includedDCs != null || excludedDCs != null))
            throw new IllegalArgumentException("The " + ARG_INCLUDE_SOURCES + " argument cannot be used together with"
                                               + " the " + ARG_EXCLUDE_SOURCES + ", " + ARG_INCLUDE_DC_NAMES + " or "
                                               + ARG_EXCLUDE_DC_NAMES + " arguments");

        if (excludedSources != null && (includedDCs != null || excludedDCs != null))
            throw new IllegalArgumentException("The " + ARG_EXCLUDE_SOURCES + " argument cannot be used together with"
                                               + " the " + ARG_INCLUDE_SOURCES + ", " + ARG_INCLUDE_DC_NAMES + " or "
                                               + ARG_EXCLUDE_DC_NAMES + " arguments");

        if (includedDCs != null && excludedDCs != null)
        {
            for (String dc : includedDCs.keySet())
            {
                if (excludedDCs.containsKey(dc) && excludedDCs.get(dc).isEmpty() == includedDCs.get(dc).isEmpty())
                    throw new IllegalArgumentException("The " + ARG_INCLUDE_DC_NAMES + " and " + ARG_EXCLUDE_DC_NAMES
                                                       + " arguments are conflicting for the datacenter: " + dc);
            }
        }

        validateSourcesDCsRacks(tokenMetadata, includedSources, excludedSources, includedDCs, excludedDCs);
    }

    private static void validateSourcesDCsRacks(TokenMetadata tokenMetadata,
                                                Set<InetAddress> includedSources,
                                                Set<InetAddress> excludedSources,
                                                Map<String, Racks> includedDCs,
                                                Map<String, Racks> excludedDCs)
    {
        List<String> errors = new ArrayList<>();

        validateSources(includedSources, tokenMetadata, errors);
        validateSources(excludedSources, tokenMetadata, errors);
        validateDCsRacks(includedDCs, tokenMetadata, errors);
        validateDCsRacks(excludedDCs, tokenMetadata, errors);

        if (!errors.isEmpty())
            throw new IllegalArgumentException(errors.stream().collect(Collectors.joining(", ")));
    }

    private static void validateSources(Set<InetAddress> sources, TokenMetadata tokenMetadata, List<String> errors)
    {
        if (sources == null)
            return;

        Set<InetAddress> validSources = tokenMetadata.getEndpointToHostIdMapForReading().keySet();
        sources.stream()
               .filter(source -> !validSources.contains(source))
               .forEach(source -> errors.add("Source '" + source + "' is not a known node in this cluster"));
    }

    private static void validateDCsRacks(Map<String, Racks> dcRacks, TokenMetadata tokenMetadata, List<String> errors)
    {
        if (dcRacks == null)
            return;

        Map<String, Multimap<String, InetAddress>> validDcRacks = tokenMetadata.getTopology().getDatacenterRacks();
        for (Entry<String, Racks> entry : dcRacks.entrySet())
        {
            String dc = entry.getKey();
            Multimap<String, InetAddress> validRacks = validDcRacks.get(dc);
            if (validRacks == null || validRacks.isEmpty())
            {
                errors.add("DC '" + dc + "' is not a known DC in this cluster");
                continue;
            }

            entry.getValue().racks
                            .stream()
                            .filter(rack -> !validRacks.containsKey(rack))
                            .forEach(rack -> errors.add("Rack '" + rack + "' is not a known rack in DC '" + dc + "' of this cluster"));
        }
    }

    private StreamingOptions(Map<String, Racks> includedDcs,
                             Map<String, Racks> excludedDcs,
                             Set<InetAddress> includedSources,
                             Set<InetAddress> excludedSources,
                             Set<String> excludedKeyspaces)
    {
        if ((includedDcs != null && includedDcs.isEmpty()) ||
            (excludedDcs != null && excludedDcs.isEmpty()) ||
            (includedSources != null && includedSources.isEmpty()) ||
            (excludedSources != null && excludedSources.isEmpty()))
            throw new IllegalArgumentException("Collections in this class must be ither null or not empty");

        this.includedDcs = includedDcs;
        this.excludedDcs = excludedDcs;
        this.includedSources = includedSources;
        this.excludedSources = excludedSources;
        this.excludedKeyspaces = excludedKeyspaces;
    }

    /**
     * Checks if the specified keyspace is accepted.
     * @param keyspace the keyspace name
     * @return {@code true} if the keyspace is accepted, {@code false} otherwise.
     */
    public boolean acceptKeyspace(String keyspace)
    {
        return excludedKeyspaces == null || !excludedKeyspaces.contains(keyspace);
    }

    /**
     * Creates a {@code ISourceFilter} corresponding to this {@code StreamingOptions}
     *
     * @param snitch the snitch
     * @param fd the failure detector
     * @return a {@code ISourceFilter} corresponding to this {@code StreamingOptions}
     */
    public ISourceFilter toSourceFilter(IEndpointSnitch snitch, IFailureDetector fd)
    {
        List<ISourceFilter> filters = new ArrayList<>();
        filters.add(failureDetectorFilter(fd));
        filters.add(excludeLocalNode());

        if (excludedSources != null)
            filters.add(excludeSources(excludedSources));

        if (includedSources != null)
            filters.add(includeSources(includedSources));

        if (excludedDcs != null)
            filters.add(excludeDcs(toSourceFilters(excludedDcs, snitch), snitch));

        if (includedDcs != null)
            filters.add(includeDcs(toSourceFilters(includedDcs, snitch), snitch));

        return composite(filters);
    }

    private static Map<String, ISourceFilter> toSourceFilters(Map<String, Racks> racksPerDcs, IEndpointSnitch snitch)
    {
        Map<String, ISourceFilter> filtersPerDcs = new HashMap<>(racksPerDcs.size());
        for (Entry<String, Racks> entry : racksPerDcs.entrySet())
            filtersPerDcs.put(entry.getKey(), entry.getValue().toSourceFilter(snitch));

        return filtersPerDcs;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        if (includedDcs != null)
        {
            builder.append(" included DCs: ");
            builder.append(toString(includedDcs.values()));
        }

        if (excludedDcs != null)
        {
            builder.append(" excluded DCs: ");
            builder.append(toString(excludedDcs.values()));
        }

        if (includedSources != null)
        {
            builder.append(" included sources: ")
                   .append(toString(includedSources));
        }

        if (excludedSources != null)
        {
            builder.append(" excluded sources: ")
                   .append(toString(excludedSources));
        }

        if (excludedKeyspaces != null)
        {
            builder.append(" excluded keyspaces: ")
                   .append(toString(excludedKeyspaces));
        }

        return builder.toString();
    }

    private static String toString(Collection<? extends Object> collection)
    {
        return collection.stream()
                         .map(Object::toString)
                         .collect(Collectors.joining(", "));
    }

    private static Set<String> getKeyspacesFromProperty(String property)
    {
        List<String> list = getPropertyAsList(property);

        if (list == null)
            return null;

        Set<String> keyspaces = new LinkedHashSet<>(list.size());
        Set<String> nonLocalStrategyKeyspaces = new HashSet<>(Schema.instance.getNonLocalStrategyKeyspaces());

        for (String name : list)
        {
            if (!nonLocalStrategyKeyspaces.contains(name))
                throw new IllegalArgumentException(String.format("The %s keyspace specified within the %s system property "
                                                                 + "is not an existing non local strategy keyspace", name, property));

            if (!keyspaces.add(name))
                throw new IllegalArgumentException(String.format("The %s keyspace specified within the %s system property "
                                                                 + "must be specified only once", name, property));
        }

        if (keyspaces.isEmpty())
            throw new IllegalArgumentException(String.format("The %s system property does not specify any keyspace", property));

        return keyspaces;
    }

    /**
     * Returns the set of hosts corresponding to the specified property.
     * @param property the property name
     * @return the set of hosts corresponding to the specified property
     */
    private static Set<InetAddress> getHostsFromProperty(String property)
    {
        List<String> hostNames = getPropertyAsList(property);
        return getHostsFromArgument(property, "system property", hostNames);
    }

    /**
     * Converts the specified arguments into a set of hosts.
     * @param argumentName the argument name
     * @param argumentType the argument type
     * @param argument the argument value
     * @return a set of hosts corresponding to the specified argument value
     */
    private static Set<InetAddress> getHostsFromArgument(String argumentName,
                                                         String argumentType,
                                                         List<String> argument)
    {
        if (argument == null)
            return null;

        Set<InetAddress> hosts = new HashSet<>(argument.size());

        for (String hostName : argument)
        {
            try
            {
                if (!hosts.add(InetAddress.getByName(hostName)))
                {
                    throw new IllegalArgumentException(String.format("The %s source must be specified only once in the %s %s",
                                                                     hostName, argumentName, argumentType));
                }
            }
            catch (UnknownHostException ex)
            {
                throw new IllegalArgumentException(String.format("The %s source specified within the %s %s is unknown",
                                                                 hostName, argumentName, argumentType));
            }
        }

        if (hosts.isEmpty())
            throw new IllegalArgumentException(String.format("The %s %s does not specify any source", argumentName, argumentType));

        return hosts;
    }

    /**
     * Returns the set of datacenters/racks corresponding to the specified property.
     * @param property the property name
     * @return the set of datacenters/racks corresponding to the specified property
     */
    private static Map<String, Racks> getDatacentersFromProperty(String property)
    {
        List<String> names = getPropertyAsList(property);
        return getDatacentersFromArgument(property, "system property", names);
    }

    /**
     * Converts the specified arguments into a set of datacenters/racks.
     * @param argumentName the argument name
     * @param argumentType the argument type
     * @param argument the argument value
     * @return a set of datacenters/racks corresponding to the specified argument value
     */
    private static Map<String, Racks> getDatacentersFromArgument(String argumentName,
                                                                 String argumentType,
                                                                 List<String> argument)
    {
        if (argument == null)
            return null;

        Map<String, Racks> datacenters = new LinkedHashMap<>();

        for (String name : argument)
        {
            int sep = name.indexOf(':');
            if (sep == -1)
            {
                Racks racks = datacenters.get(name);
                if (racks != null)
                {
                    if (racks.isEmpty())
                        throw new IllegalArgumentException(String.format("The %s datacenter must be specified only once "
                                                                         + "in the %s %s", name, argumentName, argumentType));

                    throw new IllegalArgumentException(String.format("The %s %s contains both a rack restriction and a "
                                                                     + "datacenter restriction for the %s datacenter",
                                                                     argumentName, argumentType, name));
                }

                datacenters.put(name, new Racks(name));
            }
            else
            {
                String dcName = name.substring(0, sep);
                String rackName = name.substring(sep + 1);
                Racks racks = datacenters.get(dcName);
                if (racks == null)
                {
                    racks = new Racks(dcName);
                    racks.addRack(rackName);
                    datacenters.put(dcName, racks);
                }
                else
                {
                    if (racks.isEmpty())
                        throw new IllegalArgumentException(String.format("The %s %s contains both a rack restriction and "
                                                                         + "a datacenter restriction for the %s datacenter",
                                                                         argumentName, argumentType, dcName));

                    if (!racks.addRack(rackName))
                        throw new IllegalArgumentException(String.format("The %s rack must be specified only once in the %s %s",
                                                                         name, argumentName, argumentType));
                }
            }
        }

        if (datacenters.isEmpty())
            throw new IllegalArgumentException(String.format("The %s %s does not specify any datacenter/rack", argumentName, argumentType));

        return datacenters;
    }

    private static List<String> getPropertyAsList(String name)
    {
        String property = System.getProperty(name);
        return asList(property);
    }

    /**
     * Converts the specified property into a {@code List}.
     * @param property the property to convert
     * @return the {@code List} corresponding to the specified property
     */
    private static List<String> asList(String property)
    {
        if (property == null)
            return null;

        if (property.trim().isEmpty())
            return Collections.emptyList();

        return Arrays.stream(property.split(","))
                     .map(String::trim)
                     .collect(Collectors.toList());
    }

    /**
     * A set of racks within a datacenter.
     */
    private static final class Racks
    {
        /**
         * The datacenter to which the racks belong.
         */
        private final String datacenter;

        /**
         * The racks.
         */
        private final Set<String> racks = new LinkedHashSet<>();

        /**
         * Creates a new rack for the specified datacenter.
         * @param datacenter the datacenter to which the rack belongs.
         */
        public Racks(String datacenter)
        {
            this.datacenter = datacenter;
        }

        public boolean isEmpty()
        {
            return racks.isEmpty();
        }

        public boolean addRack(String name)
        {
            return racks.add(name);
        }

        @Override
        public String toString()
        {
            if (racks.isEmpty())
               return datacenter;

            return racks.stream().map(e -> datacenter + ':' + e).collect(Collectors.joining(", "));
        }

        /**
         * Converts this specific {@code Racks} into a {@code SourceFilter}.
         * @param snitch the snitch to be used by the {@code SourceFilter}
         * @return a {@code SourceFilter} corresponding to this {@code Racks}.
         */
        public ISourceFilter toSourceFilter(IEndpointSnitch snitch)
        {
            if (racks.isEmpty())
                return noop();

            return includeRacks(racks, snitch);
        }
    }
}
