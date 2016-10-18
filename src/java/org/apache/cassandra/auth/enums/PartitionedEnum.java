/*
 * Copyright DataStax, Inc.
 */

package org.apache.cassandra.auth.enums;

/**
 * {@link PartitionedEnum} and {@link Domains} provide a means to create enums which are partitioned by domain.
 * A primary use case for this is to help make enums more extensible and so enable things like Permissions to
 * retain the type safety and convenience of enums, but also allow them to be extended easily and non-invasively.
 *
 * To use, declare an interface which extends {@link PartitionedEnum} and one or more {@link Enum}s which then
 * implement that interface. Each specific {@link Enum} implementing a common {@link PartitionedEnum} subinterface
 * represents a  namespaced partition of the type. Registering the enums with {@link Domains} enables instances to
 * be used in a type safe manner, including instantiating from their string representations (useful if persisting
 * to tables).
 */
public interface PartitionedEnum
{
    /**
     * The namespace or domain of the partitioned enum represented by a concrete {@link Enum} element
     * @return the element's domain
     */
    String domain();

    /**
     * Simple name of the element, this will automatically be provided by {@link Enum#name()} in any
     * compliant implementation
     * @return the element's simple name
     */
    String name();

    /**
     * The ordinal value of the element within the domain, this will automatically be provided by {@link Enum#name()}
     * in any compliant implementation
     * @return the ordinal value of the element within the domain
     */
    int ordinal();

    /**
     * The fully qualified name of an element in a domain. Defaults to "domain.name"
     * @return the fully qualified name of an element in a domain
     */
    default String getFullName()
    {
        return (domain() + '.' + name());
    }

    /**
     * Helper method to register domains of a {@link PartitionedEnum}.
     * @param type The {@link PartitionedEnum} interface that the domain is part of
     * @param domainName Name of the domain
     * @param domain Concrete subdomain of the partitioned enum. This must both extend {@link Enum} and implement
     *               {@link PartitionedEnum}. Checks elsewhere, in {@link Domains} verify that
     *               D is a subclass of E.
     * @param <E> Type of the partitioned enum itself, a subclass of {@link PartitionedEnum}.
     * @param <D> Type of the {@link Enum} representing the domain elements, which must also implement E
     */
    static <E extends PartitionedEnum, D extends Enum & PartitionedEnum> void registerDomainForType(Class<E> type,
                                                                                                    String domainName,
                                                                                                    Class<D> domain)
    {
        Domains.registerDomain(type, domainName, domain);
    }
}
