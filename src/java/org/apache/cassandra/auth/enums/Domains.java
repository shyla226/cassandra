/*
 * Copyright DataStax, Inc.
 */

package org.apache.cassandra.auth.enums;

import java.util.Locale;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A type safe registry of {@link Enum}s which all implement a common interface.
 *
 * Through the helper method {@link PartitionedEnum#registerDomainForType(Class, String, Class)}, individual enums which
 * implement a common interface that must be a subtype of {@link PartitionedEnum}, can be registered under a
 * unique namespace. The elements  from these enums can then be retrieved by qualified name, or by domain + ordinal.
 * Taken as a whole, the union of the individual enums which implement a named {@link PartitionedEnum} interface
 * can be considered as a single enumerated type.
 *
 * This class also holds a global type registry, essentially a mapping of {@link PartitionedEnum} subtypes to their
 * individual subdomains. This global registry is used to create {@link PartitionedEnumSet}s and to access the
 * subdomains directly.
 *
 * @param <E> the common interface implemented by enums managed by this class
 * @see PartitionedEnum javadoc for usage.
 */
public class Domains<E extends PartitionedEnum>
{
    private static final Logger logger = LoggerFactory.getLogger(Domains.class);

    // Private global registry of PartitionedEnums and the Enum classes which make up their domains
    private final static GlobalTypeRegistry TYPE_REGISTRY = new GlobalTypeRegistry();

    // register a domain for a PartitionedEnum type. Package private as callers should
    // prefer PartitionedEnum::registerDomainForType
    static <E extends PartitionedEnum, D extends Enum & PartitionedEnum> void registerDomain(Class<E> type,
                                                                                             String domainName,
                                                                                             Class<D> domain)
    {
        TYPE_REGISTRY.registerDomain(type, domainName, domain);
    }

    /**
     * Retrieve the known domains for a given PartitionedEnum type
     * @param type {@link PartitionedEnum} sublass representing the enum as a whole
     * @param <E>
     * @return Domains instance defining the classes whose  he individual enums
     * constitue the partitioned enums' domains
     */
    public static <E extends PartitionedEnum> Domains<E> getDomains(Class<E> type)
    {
        Domains<E> domains = TYPE_REGISTRY.getDomains(type);
        if (domains == null)
            throw new IllegalArgumentException(String.format("No domains registered for partitioned enum class %s",
                                                             type.getName()));
        return domains;
    }

    @VisibleForTesting
    static <E extends PartitionedEnum> void unregisterType(Class<E> type)
    {
        TYPE_REGISTRY.unregister(type);
    }

    private static final char DELIM = '.';
    private final Class<E> type;
    private final Map<String, Class<? extends Enum>> domains = Maps.newConcurrentMap();

    private Domains(Class<E> type)
    {
        this.type = type;
    }

    /**
     * Register an enum under a given domain. The enum must implement the sub-interface of
     * {@link PartitionedEnum} that the registry is parameterized with.
     * Validation is performed on inputs to ensure that:
     *     * the domain name is valid
     *     * the elements implement the correct RegisteredEnumType interface
     *     * the result of calling {@link PartitionedEnum#domain()} on each element matches the supplied domain
     *     * all element names are completely upper case
     *     * the domain is not already registered
     * Failure to satisfy any of these conditions results in a {@link IllegalArgumentException} and the enum
     * not being registered.
     *
     * Domain name is not case-sensitive and supplied values are converted to uppercase on registration.
     *
     * @param domain name of the domain
     * @param enumType the enum class to be registered
     */
    @VisibleForTesting
    <D extends Enum & PartitionedEnum> void register(String domain, Class<D> enumType)
    {
        if (domain.indexOf(DELIM) != -1)
            throw new IllegalArgumentException(String.format("Invalid domain %s, name must not include period",
                                                             domain));

        domain = domain.toUpperCase(Locale.US);
        logger.trace("Registering domain {}", domain);
        if (!type.isAssignableFrom(enumType))
            throw new IllegalArgumentException(String.format("Supplied domain class %s is not a valid " +
                                                             "domain of enumerated type %s",
                                                             enumType.getName(), type.getName()));

        for (Enum element : enumType.getEnumConstants())
        {
            PartitionedEnum domainElement = type.cast(element);
            if (!domainElement.domain().toUpperCase(Locale.US).equals(domain))
                throw new IllegalArgumentException(String.format("Invalid domain %s in enumerated type declaration %s.",
                                                                 domainElement.domain(),
                                                                 enumType.getClass().getName()));
            if (!domainElement.name().toUpperCase(Locale.US).equals(domainElement.name()))
                throw new IllegalArgumentException(String.format("Invalid name %s for %s, only upper case names" +
                                                                 " are permitted",
                                                                 domainElement.getFullName(),
                                                                 type.getName()));

        }

        Class<? extends Enum> existing = domains.putIfAbsent(domain, enumType);
        if (existing != null)
            throw new IllegalArgumentException(String.format("Domain %s was already registered for type %s",
                                                             domain, type.getName()));
    }

    /**
     * Return the high level type of the enum, which must extend {@link PartitionedEnum}
     * @return the general type of the partitioned enum this registry is for
     */
    public Class<E> getType()
    {
        return type;
    }

    /**
     * Retrieve a specific enum element given a domain and its ordinal
     * value in the corresponding enum.
     * @param domain name of the domain
     * @param ordinal ordinal value of the target element
     * @return Enum element
     */
    E get(String domain, int ordinal)
    {
        Class<? extends Enum> enumType = domains.get(domain);
        if (enumType == null)
            throw new IllegalArgumentException(String.format("Unknown domain %s", domain));

        // in the case of an AIOOBE, let it propagate
        return (E)enumType.getEnumConstants()[ordinal];
    }

    /**
     * Retrieve a specific enum element given a domain and the name of the element
     * @param domain name of the domain
     * @param name name of the target element
     * @return Enum element
     */
    @SuppressWarnings("unchecked")
    public E get(String domain, String name)
    {
        domain = domain.toUpperCase(Locale.US);
        name = name.toUpperCase(Locale.US);
        Class<? extends Enum> enumType = domains.get(domain);
        if (enumType == null)
            throw new IllegalArgumentException(String.format("Unknown domain %s", domain));

        return (E)Enum.valueOf(enumType, name);
    }

    /**
     * Retrieve a specific enum element given the fully qualified version of its name,
     * of the form "domain.name"
     * @param fullName qualified name of the target element
     * @return Enum element
     */
    public E get(String fullName)
    {
        int delim = fullName.indexOf(DELIM);
        assert delim > 0;

        String domain = fullName.substring(0, delim);
        String name = fullName.substring(delim + 1);
        return get(domain, name);
    }

    public ImmutableSet<E> asSet()
    {
        ImmutableSet.Builder<E> builder = ImmutableSet.builder();
        domains.values()
               .forEach(enumType -> builder.add((E[]) enumType.getEnumConstants()));
        return builder.build();
    }

    /**
     * Remove all registered domains
     */
    @VisibleForTesting
    public void clear()
    {
        domains.clear();
    }

    private static final class GlobalTypeRegistry
    {
        private final Map<Class<?>, Domains<?>> KNOWN_TYPES = Maps.newConcurrentMap();

        @SuppressWarnings("unchecked")
        private <E extends PartitionedEnum> Domains<E> getDomains(Class<E> type)
        {
            Domains<?> domains = KNOWN_TYPES.get(type);
            if (domains == null)
                throw new IllegalArgumentException("Unknown PartitionedEnumType " + type.getName());

            return (Domains<E>)domains;
        }

        private <E extends PartitionedEnum, D extends Enum & PartitionedEnum> void registerDomain(Class<E> type,
                                                                                                  String domainName,
                                                                                                  Class<D> domain)
        {
            Domains<E> newRegistry = new Domains<>(type);
            Domains<?> existingRegistry = KNOWN_TYPES.putIfAbsent(type, newRegistry);

            Domains<?> registry = existingRegistry != null ? existingRegistry : newRegistry;
            // register checks that D is a valid subclass of E
            registry.register(domainName, domain);
        }

        private <E extends PartitionedEnum> void unregister(Class<E> type)
        {
            KNOWN_TYPES.remove(type);
        }
    }
}
