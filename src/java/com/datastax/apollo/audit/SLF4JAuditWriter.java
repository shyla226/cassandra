/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.audit;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import io.reactivex.Completable;
import org.apache.cassandra.utils.WrappedRunnable;

public class SLF4JAuditWriter implements IAuditWriter
{
    private static final Logger logger = LoggerFactory.getLogger(SLF4JAuditWriter.class);

    public static final String LOGGER_NAME = "SLF4JAuditWriter";
    private static final Logger AUDIT_LOG = LoggerFactory.getLogger( LOGGER_NAME );
    private final boolean loggingEnabled;

    public SLF4JAuditWriter()
    {
        Logger rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        if (rootLogger == logger)
        {
            // no explicit logger configured, so disable the plugin
            // this is a a bit paranoid, as it can't really happen, but
            // its a pretty cheap test & only done once
            loggingEnabled = false;
            logger.debug("Audit logging disabled (should not use root logger)");
            return;
        }

        if (AUDIT_LOG instanceof ch.qos.logback.classic.Logger)
        {
            LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
            final ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) AUDIT_LOG;


            if (!logbackLogger.isAdditive())
            {
                // we don't want audit log messages to propagate up the logger
                // hierarchy, so if additivity is true, disable the plugin
                loggingEnabled = false;
                logger.debug(String.format("Audit logging disabled (turn off additivity for audit logger %s)",
                                           SLF4JAuditWriter.LOGGER_NAME));
                return;
            }

            loggingEnabled = true;

            // We get optimal performance from logback with appenders which write
            // to file by turning on buffered IO. This ensures that the buffer
            // is flushed when the server is shutdown.
            Thread logFlusher = new Thread(new WrappedRunnable(){
                @Override
                protected void runMayThrow() throws Exception
                {
                    logger.info("Flushing audit logger");
                    try
                    {
                        Iterator<Appender<ILoggingEvent>> appenders = logbackLogger.iteratorForAppenders();
                        while (appenders.hasNext())
                        {
                            Appender<ILoggingEvent> appender = appenders.next();
                            // stopping a RollingFileAppender also closes it's output streams
                            appender.stop();
                        }
                    }
                    catch (Exception e)
                    {
                        logger.warn("Error flushing audit logger, some messages may be dropped", e);
                    }
                }
            });
            logFlusher.setName("Audit log flusher");
            Runtime.getRuntime().addShutdownHook(logFlusher);
        }
        else
        {
            String msg = SLF4JAuditWriter.LOGGER_NAME + " logger is not an instance of ch.qos.logback.classic.Logger." +
                    "\nNon-logback loggers are supported through slf4j, but they are not checked for additivity," +
                    "\nand the are not explicitly closed on shutdown.";
            logger.warn(msg);
            loggingEnabled = true;
        }
    }

    @Override
    public Completable recordEvent(AuditableEvent event)
    {
        AUDIT_LOG.info("{}", event);
        return Completable.complete();
    }

    @Override
    public boolean isLoggingEnabled()
    {
        return loggingEnabled;
    }
}
