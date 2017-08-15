/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.audit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Completable;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;

public class AuditLogger
{
    public static final Logger logger = LoggerFactory.getLogger(AuditLogger.class);

    private static volatile boolean forceAuditLogging = false;

    @VisibleForTesting
    static void setForceAuditLogging(boolean forceAuditLogging)
    {
        AuditLogger.forceAuditLogging = forceAuditLogging;
    }

    public static boolean forceAuditLogging()
    {
        return forceAuditLogging;
    }

    private static final AuditLogger instance = new AuditLogger();

    private final IAuditWriter writer;
    private final AuditFilter filter;

    /**
     * Creates AuditLogger as configured in dse.yaml
     */
    private AuditLogger()
    {
        this(getWriterInstance(), getFilterInstance());
    }

    @VisibleForTesting
    AuditLogger(IAuditWriter writer, AuditFilter filter)
    {
        this.writer = writer;
        this.filter = filter;
        if (isEnabled())
            logger.info("Audit logging is enabled with " + writer.getClass().getName());
        else
            logger.info("Audit logging is disabled");
    }


    public static AuditLogger getInstance()
    {
        return instance;
    }

    /**
     * @return IAuditWriter configured in cassandra.yaml
     */
    private static IAuditWriter getWriterInstance()
    {
        Config config = DatabaseDescriptor.getRawConfig();
        if (!config.getAuditLoggingEnabled() && System.getProperty("cassandra.audit_writer") == null)
            return null;

        String name = System.getProperty("cassandra.audit_writer", config.getAuditLoggerName());
        if (!name.contains("."))
            name = "org.apache.cassandra.audit." + name;

        logger.info("Using logger implementation : " + name);
        try
        {
            return (IAuditWriter) Class.forName(name).newInstance();
        }
        catch (Exception e)
        {
            logger.error(String.format("Unable to load audit writer %s", name), e);
            throw new RuntimeException(e);
        }
    }

    private static AuditFilter getFilterInstance()
    {
        try
        {
            return new AuditFilter.Builder().fromConfig().build();
        }
        catch (Exception e)
        {
            {
                logger.error("Unable to create audit filter", e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Checks event against filter, and writes event if it's not excluded
     */
    public Completable recordEvent(AuditableEvent event)
    {
        if(!StorageService.instance.isAuditLoggingSetupComplete()) {
            logger.warn("Audit Logger not setup while trying to record an event.");
            return Completable.complete();
        }

        if (!filter.shouldFilter(event) && isEnabled())
            return writer.recordEvent(event);
        return Completable.complete();
    }

    public void recordEventBlocking(AuditableEvent event)
    {
        recordEvent(event).blockingAwait();
    }

    public boolean isEnabled()
    {
        return (writer != null &&
                StorageService.instance.isAuditLoggingSetupComplete());
    }

    public void setup()
    {
        if( writer instanceof CassandraAuditWriter )
        {
            CassandraAuditKeyspace.maybeConfigure();
            CassandraAuditWriter cassandraAuditWriter = (CassandraAuditWriter) this.writer;
            cassandraAuditWriter.setup();
        }
    }
}
