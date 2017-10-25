/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

public class AuditLoggerTest extends CQLTester
{
    @BeforeClass
    public static void setupClass() throws Exception {
        // Needed because this reads authentication config settings
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void checkIsEnabled() throws Exception
    {
        AuditFilter filter = AuditFilter.builder().build();

        AuditLogger logger = new AuditLogger(null, filter);

        Assert.assertFalse(logger.isEnabled());

        // shouldn't explode
        logger.recordEvent(new AuditableEvent.Builder("u", "127.0.0.1").type(AuditableEventType.GET).build());
    }

    @Test
    public void checkFilteringIsApplied() throws Exception
    {
        AuditFilter filter = AuditFilter.builder().excludeKeyspace("ks1").build();
        InProcTestAuditWriter writer = new InProcTestAuditWriter();
        InProcTestAuditWriter.reset();
        AuditLogger logger = new AuditLogger(writer, filter);

        AuditableEvent.Builder builder = new AuditableEvent.Builder("u", "127.0.0.1").type(AuditableEventType.INSERT);
        AuditableEvent excluded = builder.keyspace("ks1").build();
        AuditableEvent included = builder.keyspace("ks2").build();

        logger.recordEvent(excluded);
        Assert.assertFalse(InProcTestAuditWriter.hasEvents());

        logger.recordEvent(included);
        Assert.assertTrue(InProcTestAuditWriter.hasEvents());

        AuditableEvent lastEvent = InProcTestAuditWriter.lastEvent();

        Assert.assertEquals(included, lastEvent);
    }

}
