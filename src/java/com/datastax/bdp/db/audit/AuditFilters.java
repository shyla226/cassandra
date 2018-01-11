/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.cassandra.auth.RoleResource;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Factory methods for {@code IAuditFilter}.
 */
final class AuditFilters
{
    /**
     * Creates an {@code IAuditFilter} that accepts only the roles corresponding to
     * the specified values.
     * @param roles the accepted roles
     * @return an {@code IAuditFilter} that accepts only the roles corresponding to
     * the specified values.
     */
    public static IAuditFilter includeRoles(List<RoleResource> roles)
    {
        return (event) ->
        {
            for (RoleResource role : roles)
                if (event.userHasRole(role))
                    return true;
            return false;
        };
    }

    public static IAuditFilter excludeRoles(List<RoleResource> roles)
    {
        return not(includeRoles(roles));
    }

    public static <T> Set<T> toSet(T[] elements)
    {
        return Arrays.stream(elements).collect(Collectors.toSet());
    }

    /**
     * Creates an {@code IAuditFilter} that accepts only the keyspaces corresponding to
     * the specified regex.
     * @param patterns the regexps used to determine which keyspaces to accept
     * @return an {@code IAuditFilter} that accepts only the keyspaces corresponding to
     * the specified regex.
     */
    public static IAuditFilter includeKeyspaces(List<Pattern> patterns)
    {
        return (event) ->
        {
            String keyspace = event.getKeyspace();

            if (keyspace == null)
                return false;

            for (Pattern pattern : patterns)
                if (pattern.matcher(keyspace).matches())
                    return true;
            return false;
        };
    }

    /**
     * Creates an {@code IAuditFilter} that rejects the keyspaces corresponding to
     * the specified regex.
     * @param patterns the regexps used to determine which keyspaces to reject
     * @return an {@code IAuditFilter} that rejects the keyspaces corresponding to
     * the specified regex
     */
    public static IAuditFilter excludeKeyspaces(String... patterns)
    {
        return excludeKeyspaces(toPatterns(patterns));
    }

    /**
     * Creates an {@code IAuditFilter} that rejects the keyspaces corresponding to
     * the specified regex.
     * @param patterns the regexps used to determine which keyspaces to reject
     * @return an {@code IAuditFilter} that rejects the keyspaces corresponding to
     * the specified regex
     */
    public static IAuditFilter excludeKeyspaces(List<Pattern> patterns)
    {
        return not(includeKeyspaces(patterns));
    }

    /**
     * Creates an {@code IAuditFilter} that accepts only the specified event categories.
     * @param categories the accepted categories
     * @return an {@code IAuditFilter} that accepts only the specified event categories
     */
    public static IAuditFilter includeCategories(Set<AuditableEventCategory> categories)
    {
        return (event) -> categories.contains(event.getType().getCategory());
    }

    /**
     * Creates an {@code IAuditFilter} that rejects the specified event categories.
     * @param categories the rejected categories
     * @return an {@code IAuditFilter} that rejects the specified event categories
     */
    public static IAuditFilter excludeCategories(Set<AuditableEventCategory> categories)
    {
        return not(includeCategories(categories));
    }

    /**
     * Creates an {@code IAuditFilter} that rejects the events accepted by the specified one.
     * @param filter the filter
     * @return an {@code IAuditFilter} that rejects the events accepted by the specified one
     */
    private static IAuditFilter not(IAuditFilter filter)
    {
        return (event) -> !filter.accept(event);
    }

    /**
     * Creates an {@code IAuditFilter} that accepts eveything.
     * @return an {@code IAuditFilter} that accepts eveything.
     */
    public static IAuditFilter acceptEverything()
    {
        return (event) -> true;
    }

    /**
     * Creates an {@code IAuditFilter} that accepts the events accepted by at least one of the specified filters.
     * @param filters the filters
     * @return an {@code IAuditFilter} that accepts the events accepted by at least one of the specified filters
     */
    public static IAuditFilter composite(final List<IAuditFilter> filters)
    {
        return (event) ->
        {
            for (IAuditFilter filter : filters)
                if (!filter.accept(event))
                    return false;
            return true;
        };
    }

    /**
     * Creates a {@code IAuditFilter} corresponding to the specified configuration.
     * @param options the audit filter configuration
     * @return the {@code IAuditFilter} corresponding to the configuration
     */
    public static IAuditFilter fromConfiguration(AuditLoggingOptions options)
    {
        options.validateFilters();

        List<IAuditFilter> filters = new ArrayList<>();
        addCategoryFilters(options, filters);
        addKeyspaceFilters(options, filters);
        addRoleFilters(options, filters);

        if (filters.isEmpty())
            return AuditFilters.acceptEverything();

        if (filters.size() == 1)
            return filters.get(0);

        return composite(filters);
    }

    private static void addCategoryFilters(AuditLoggingOptions auditLoggingOptions, List<IAuditFilter> filters)
    {
        if (!isBlank(auditLoggingOptions.included_categories))
        {
            Set<AuditableEventCategory> categories = toCategories(auditLoggingOptions.included_categories);

            if (!categories.isEmpty())
                filters.add(includeCategories(categories));
        }
        else if (!isBlank(auditLoggingOptions.excluded_categories))
        {
            Set<AuditableEventCategory> categories = toCategories(auditLoggingOptions.excluded_categories);

            if (!categories.isEmpty())
                filters.add(excludeCategories(categories));
        }
    }

    private static void addRoleFilters(AuditLoggingOptions auditLoggingOptions, List<IAuditFilter> filters)
    {
        if (!isBlank(auditLoggingOptions.included_roles))
        {
            List<RoleResource> roles = toRoles(auditLoggingOptions.included_roles);

            if (!roles.isEmpty())
                filters.add(includeRoles(roles));
        }
        else if (!isBlank(auditLoggingOptions.excluded_roles))
        {
            List<RoleResource> roles = toRoles(auditLoggingOptions.excluded_roles);

            if (!roles.isEmpty())
                filters.add(excludeRoles(roles));
        }
    }

    private static void addKeyspaceFilters(AuditLoggingOptions auditLoggingOptions, List<IAuditFilter> filters)
    {
        if (!isBlank(auditLoggingOptions.included_keyspaces))
        {
            List<Pattern> patterns = toPatterns(auditLoggingOptions.included_keyspaces);

            if (!patterns.isEmpty())
                filters.add(AuditFilters.includeKeyspaces(patterns));
        }
        else if (!isBlank(auditLoggingOptions.excluded_keyspaces))
        {
            List<Pattern> patterns = toPatterns(auditLoggingOptions.excluded_keyspaces);

            if (!patterns.isEmpty())
                filters.add(AuditFilters.excludeKeyspaces(patterns));
        }
    }

    private static Set<AuditableEventCategory> toCategories(String categoriesAsString)
    {
        return Arrays.stream(categoriesAsString.split(","))
                     .map(String::trim)
                     .filter(s -> !s.isEmpty())
                     .map(AuditableEventCategory::fromString)
                     .collect(Collectors.toCollection(() -> EnumSet.noneOf(AuditableEventCategory.class)));
    }

    private static List<RoleResource> toRoles(String rolesAsString)
    {
        return Arrays.stream(rolesAsString.split(","))
                     .map(String::trim)
                     .filter(s -> !s.isEmpty())
                     .map(RoleResource::role)
                     .collect(Collectors.toList());
    }
    
    /**
     * Converts the comma separated regexps into a list of {@code Pattern}s
     * @param patternsAsString a comma separated list of regexps
     * @return a list of {@code Pattern}s
     */
    private static List<Pattern> toPatterns(String patternsAsString)
    {
        return toPatterns(patternsAsString.split(","));
    }

    /**
     * Converts the specified regexp {@code String}s into a list of {@code Pattern}s
     * @param patterns the regexp {@code String}s to convert
     * @return a list of {@code Pattern}s
     */
    private static List<Pattern> toPatterns(String[] patterns)
    {
        return Arrays.stream(patterns)
                     .map(String::trim)
                     .filter(s -> !s.isEmpty())
                     .map(Pattern::compile)
                     .collect(Collectors.toList());
    }

    private AuditFilters()
    {
    }
}
