/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.audit;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * Determines whether AuditableEvents should be logged, based on configuration
 */
public class AuditFilter
{
    private final Set<AuditableEventCategory> includedCategories;
    private final Set<AuditableEventCategory> excludedCategories;
    private final Set<Pattern> includedKeyspaces;
    private final Set<Pattern> excludedKeyspaces;

    private AuditFilter(Builder builder)
    {
        this.includedCategories = new HashSet<>(builder.includedCategories);
        this.excludedCategories = new HashSet<>(builder.excludedCategories);
        this.includedKeyspaces = new HashSet<>(builder.includedKeyspaces);
        this.excludedKeyspaces = new HashSet<>(builder.excludedKeyspaces);
    }

    static boolean matches(Set<Pattern> patterns, String str)
    {
        if (str == null)
        {
            return false;
        }

        for (Pattern pattern: patterns)
        {
            if (pattern.matcher(str).matches())
            {
                return true;
            }
        }
        return false;
    }

    private boolean shouldFilterKeyspace(AuditableEvent event)
    {
        String keyspace = event.getKeyspace();
        boolean excluded = matches(excludedKeyspaces, keyspace);
        boolean included = includedKeyspaces.size() == 0 || matches(includedKeyspaces, keyspace);
        return excluded || !included;
    }

    private boolean shouldFilterCategory(AuditableEvent event)
    {
        AuditableEventCategory category = event.getType().getCategory();
        boolean excluded = excludedCategories.contains(category);
        boolean included = includedCategories.size() == 0 || includedCategories.contains(category);
        return excluded || !included;
    }

    public boolean shouldFilter(AuditableEvent event)
    {
        return shouldFilterCategory(event) || shouldFilterKeyspace(event);
    }

    public static class Builder
    {
        private final Set<AuditableEventCategory> includedCategories = new HashSet<>();
        private final Set<AuditableEventCategory> excludedCategories = new HashSet<>();
        private final Set<Pattern> excludedKeyspaces = new HashSet<>();
        private final Set<Pattern> includedKeyspaces = new HashSet<>();

        public Builder() {}

        public Builder includeCategory(AuditableEventCategory category)
        {
            includedCategories.add(category);
            return this;
        }

        public Builder excludeCategory(AuditableEventCategory category)
        {
            excludedCategories.add(category);
            return this;
        }

        public Builder excludeKeyspace(String keyspace)
        {
            if (keyspace == null)
            {
                throw new IllegalArgumentException("excluded keyspace pattern cannot be null");
            }
            excludedKeyspaces.add(Pattern.compile(keyspace));
            return this;
        }

        public Builder includeKeyspace(String keyspace)
        {
            if (keyspace == null)
            {
                throw new IllegalArgumentException("included keyspace pattern cannot be null");
            }
            includedKeyspaces.add(Pattern.compile(keyspace));
            return this;
        }

        public Builder fromConfig()
        {
            Config configuration = DatabaseDescriptor.getRawConfig();
            for (String keyspace: configuration.getAuditIncludedKeyspaces())
                includeKeyspace(keyspace);

            for (String keyspace: configuration.getAuditExcludedKeyspaces())
                excludeKeyspace(keyspace);

            for (String category: configuration.getAuditIncludedCategories())
                includeCategory(AuditableEventCategory.valueOf(category));

            for (String category: configuration.getAuditExcludedCategories())
                excludeCategory(AuditableEventCategory.valueOf(category));

            return this;
        }

        public AuditFilter build()
        {
            if (!includedCategories.isEmpty() && !excludedCategories.isEmpty())
                throw new IllegalArgumentException("Can't specify both included and excluded categories for audit logger");
            if (!includedKeyspaces.isEmpty() && !excludedKeyspaces.isEmpty())
                throw new IllegalArgumentException("Can't specify both included and excluded keyspaces for audit logger");
            return new AuditFilter(this);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }
}
