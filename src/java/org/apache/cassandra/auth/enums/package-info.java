/*
 * Copyright DataStax, Inc.
 */

/**
 * Provides a means to create enums which are partitioned by domain. A primary use case for this
 * is to help make enums more extensible and so enable things like Permissions to retain the type
 * safety and convenience of enums, but also allow them to be extended easily and non-invasively.
 *
 * {@link org.apache.cassandra.auth.enums.PartitionedEnum} is an interface used to define a
 * namespaced enum type. An interface extending this should be defined to represent a particular
 * type, then enums which implement that interface make up the partitions of the type. So each
 * individual enum represents a partition or domain of the overall type.
 * Domains (the 'concrete' enums which make up the partitioned enum) must be registered before
 * use. This registration process ensures that each domain both extends {@link java.lang.Enum} and
 * implements the specific {@link org.apache.cassandra.auth.enums.PartitionedEnum} interface.
 * This contains a static helper method to do that registration, which should be called during
 * system initialization.
 *
 * {@link org.apache.cassandra.auth.enums.Domains} abstract class for tracking which
 * domains are defined for a specific enum type. A concrete subclass with the appropriate type
 * parameter should be provided, which then acts as a registry/factory for the individual
 * namespaced elements.
 *
 * {@link org.apache.cassandra.auth.enums.PartitionedEnumSet} works with the partitioned enum classes to
 * provide functionality similar to {@link java.util.EnumSet}, but for namespaced enum types.
 *
 * An example, modelling Permissions using this package:
 *
 * {@code}
 * interface Permission extends PartitionedEnum
 * {
 *    default String getDisplayName()
 *    {
 *       return String.format("<permission %s.%s>", domain().toLowerCase(), name().toLowerCase());
 *    }
 * }
 *
 * public enum SystemPermission implements Permission
 * {
 *    READ, WRITE;
 *
 *    public String domain()
 *    {
 *       return "SYSTEM";
 *    }
 * }
 *
 * public enum CustomPermission implements Permission
 * {
 *    CALL, COMMIT;
 *
 *    public String domain()
 *    {
 *       return "CUSTOM";
 *    }
 * }
 *
 * public class TestClass
 * {
 *    public TestClass()
 *    {
 *       PartitionedEnum.registerType(Permission.class, "SYSTEM", SystemPermission.class);
 *       PartitionedEnum.registerType(Permission.class, "CUSTOM", CustomPermission.class);
 *    }
 *
 *    public void doSomething()
 *    {
 *       // Intersection
 *       PartitionedEnumSet<Permission> set1 = PartitionedEnumSet.of(Permission.class,
 *                                                                   SystemPermission.READ,
 *                                                                   CustomPermission.CALL);
 *
 *       PartitionedEnumSet<Permission> set2 = PartitionedEnumSet.of(Permission.class,
 *                                                                   SystemPermission.WRITE,
 *                                                                   CustomPermission.CALL);
 *
 *       PartitionedEnumSet<Permission> set3 = PartitionedEnumSet.of(Permission.class,
 *                                                                   CustomPermission.COMMIT);
 *
 *       set3.retainAll(set1);
 *       assert set3.isEmpty();
 *
 *       set1.retainAll(set2);
 *       assert set1.size() == 1 && set1.contains(CustomPermission.CALL);
 *
 *       // Union
 *       PartitionedEnumSet<Permission> set4 = PartitionedEnumSet.of(Permission.class,
 *                                                                   SystemPermission.READ,
 *                                                                   CustomPermission.CALL);
 *       PartitionedEnumSet<Permission> set5 = PartitionedEnumSet.of(Permission.class,
 *                                                                   SystemPermission.WRITE,
 *                                                                   CustomPermission.CALL);
 *       set4.addAll(set5);
 *       assert set4.size() == 3
 *              && set4.contains(SystemPermission.READ)
 *              && set4.contains(SystemPermission.WRITE)
 *              && set4.contains(CustomPermission.CALL);
 *    }
 *
 *    public List<Permission> getPermissions()
 *    {
 *       // Deserialization
 *       String[] strings = { "SYSTEM.READ", "CUSTOM.CALL" };
 *       PartitionedEnumDomainRegistry<Permission> registry = PartitionedEnumDomainRegistry.getDomains(Permission.class);
 *       return Arrays.stream(strings)
 *                    .map(registry::get)
 *                    .collect(Collectors.toList());
 *    }
 * }
 * {@code}
 **/
package org.apache.cassandra.auth.enums;
