/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import org.junit.Assert;
import org.junit.Test;

import static com.datastax.bdp.db.audit.CoreAuditableEventType.*;

public class AuditFilterTest
{
    @Test
    public void checkIncludedKeyspaceFilter() throws Exception
    {
        AuditFilter filter;

        AuditableEvent.Builder builder = new AuditableEvent.Builder("u", "127.0.0.1").type(INSERT);

        AuditableEvent excluded = builder.keyspace("ks1").build();
        AuditableEvent included = builder.keyspace("ks2").build();

        // check empty filtering
        filter = AuditFilter.builder().build();
        Assert.assertFalse(filter.shouldFilter(excluded));
        Assert.assertFalse(filter.shouldFilter(included));

        // check filtering
        filter = AuditFilter.builder().includeKeyspace("ks2").build();
        Assert.assertTrue(filter.shouldFilter(excluded));
        Assert.assertFalse(filter.shouldFilter(included));
    }

    @Test
    public void checkIncludedRegex()
    {
        AuditFilter filter;

        AuditableEvent.Builder builder = new AuditableEvent.Builder("u", "127.0.0.1").type(INSERT);

        AuditableEvent ks111 = builder.keyspace("ks111").build();
        AuditableEvent ks1_stuff = builder.keyspace("ks1_stuff").build();
        AuditableEvent ks1 = builder.keyspace("ks1").build();

        filter = AuditFilter.builder().includeKeyspace("ks1").build();
        Assert.assertTrue(filter.shouldFilter(ks111));
        Assert.assertTrue(filter.shouldFilter(ks1_stuff));
        Assert.assertFalse(filter.shouldFilter(ks1));

        filter = AuditFilter.builder().includeKeyspace("ks[1]{1,}").build();
        Assert.assertFalse(filter.shouldFilter(ks111));
        Assert.assertTrue(filter.shouldFilter(ks1_stuff));
        Assert.assertFalse(filter.shouldFilter(ks1));
    }

    @Test
    public void checkExcludedKeyspaceFilter() throws Exception
    {
        AuditFilter filter;

        AuditableEvent.Builder builder = new AuditableEvent.Builder("u", "127.0.0.1").type(INSERT);

        AuditableEvent excluded = builder.keyspace("ks1").build();
        AuditableEvent included = builder.keyspace("ks2").build();

        // check empty filtering
        filter = AuditFilter.builder().build();
        Assert.assertFalse(filter.shouldFilter(excluded));
        Assert.assertFalse(filter.shouldFilter(included));

        // check filtering
        filter = AuditFilter.builder().excludeKeyspace("ks1").build();
        Assert.assertTrue(filter.shouldFilter(excluded));
        Assert.assertFalse(filter.shouldFilter(included));
    }

    @Test
    public void checkExcludedRegex()
    {
        AuditFilter filter;

        AuditableEvent.Builder builder = new AuditableEvent.Builder("u", "127.0.0.1").type(INSERT);

        AuditableEvent ks111 = builder.keyspace("ks111").build();
        AuditableEvent ks1_stuff = builder.keyspace("ks1_stuff").build();
        AuditableEvent ks1 = builder.keyspace("ks1").build();

        filter = AuditFilter.builder().excludeKeyspace("ks1").build();
        Assert.assertFalse(filter.shouldFilter(ks111));
        Assert.assertFalse(filter.shouldFilter(ks1_stuff));
        Assert.assertTrue(filter.shouldFilter(ks1));

        filter = AuditFilter.builder().excludeKeyspace("ks[1]{1,}").build();
        Assert.assertTrue(filter.shouldFilter(ks111));
        Assert.assertFalse(filter.shouldFilter(ks1_stuff));
        Assert.assertTrue(filter.shouldFilter(ks1));
    }

    @Test
    public void checkIncludedCategoryFilter() throws Exception
    {
        AuditFilter filter;

        AuditableEvent.Builder builder = new AuditableEvent.Builder("u", "127.0.0.1");

        AuditableEvent excluded = builder.type(CQL_SELECT).build();
        AuditableEvent included = builder.type(INSERT).build();

        // check empty filtering
        filter = AuditFilter.builder().build();
        Assert.assertFalse(filter.shouldFilter(excluded));
        Assert.assertFalse(filter.shouldFilter(included));

        // check filtering
        filter = AuditFilter.builder().includeCategory(AuditableEventCategory.DML).build();
        Assert.assertTrue(filter.shouldFilter(excluded));
        Assert.assertFalse(filter.shouldFilter(included));
    }

    @Test
    public void checkExcludedCategoryFilter() throws Exception
    {
        AuditFilter filter;

        AuditableEvent.Builder builder = new AuditableEvent.Builder("u", "127.0.0.1");

        AuditableEvent excluded = builder.type(CQL_SELECT).build();
        AuditableEvent included = builder.type(INSERT).build();

        // check empty filtering
        filter = AuditFilter.builder().build();
        Assert.assertFalse(filter.shouldFilter(excluded));
        Assert.assertFalse(filter.shouldFilter(included));

        // check filtering
        filter = AuditFilter.builder().excludeCategory(AuditableEventCategory.QUERY).build();
        Assert.assertTrue(filter.shouldFilter(excluded));
        Assert.assertFalse(filter.shouldFilter(included));
    }

    @Test(expected=IllegalArgumentException.class)
    public void checkExcludeIncludeCategoryFailure() throws Exception
    {
        AuditFilter.builder().excludeCategory(AuditableEventCategory.QUERY).includeCategory(AuditableEventCategory.DML).build();
    }

    @Test(expected=IllegalArgumentException.class)
    public void checkExcludeIncludeKeyspaceFailure() throws Exception
    {
        AuditFilter.builder().excludeKeyspace("ks1").includeKeyspace("ks2").build();
    }

    @Test(expected=IllegalArgumentException.class)
    public void nullKeyspaceInclude() throws Exception
    {
        AuditFilter.builder().includeKeyspace(null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void nullKeyspaceExclude() throws Exception
    {
        AuditFilter.builder().excludeKeyspace(null);
    }

    /**
     * operations that don't touch a specific keyspace shouldn't NPE
     */
    @Test
    public void nullKeyspaceMatch()
    {
        AuditFilter filter = AuditFilter.builder().includeKeyspace("dse_system").build();
        AuditableEvent event = new AuditableEvent.Builder("u", "127.0.0.1")
                .type(LOGIN_ERROR).operation("some problem").build();
        Assert.assertTrue(filter.shouldFilter(event));
    }
}
