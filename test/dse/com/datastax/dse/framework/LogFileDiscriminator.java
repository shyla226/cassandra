/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework;


import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Determines where to log events based on their timestamp
 */
public class LogFileDiscriminator
{

    private static final String DEFAULT_LOG_FILE = "system.log";

    public static final String TEST_LOG_FILE_KEY = "TEST_LOG_FILE";

    private static class ActiveLogFile
    {
        private final long timestamp;
        private final String fileName;

        public ActiveLogFile(long timestamp, String fileName)
        {
            this.timestamp = timestamp;
            this.fileName = fileName;
        }

        public long getTimestamp()
        {
            return timestamp;
        }

        public String getFileName()
        {
            return fileName;
        }
    }

    private static Deque<ActiveLogFile> activeLogFiles = new ConcurrentLinkedDeque<>();

    static
    {
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                                                        {
                                                            // stop the logger context on shutdown so that log files could be successfully flushed
                                                            ((LoggerContext) LoggerFactory.getILoggerFactory()).stop();
                                                        }));

        if (System.getProperty(TEST_LOG_FILE_KEY) != null)
        {
            activeLogFiles.push(new ActiveLogFile(System.currentTimeMillis(), System.getProperty(TEST_LOG_FILE_KEY)));
        }
    }

    /**
     * Start writing all log statements to the given log file
     *
     * @param fileName the file where to log statements relative to test log folder (build/test/logs)
     */
    public static void logToFile(String fileName)
    {
        activeLogFiles.push(new ActiveLogFile(System.currentTimeMillis(), fileName));
    }

    /**
     * Start writing all log statements to the default log file, as defined in the configuration (build/test/logs/system.log)
     */
    public static void logToDefaultFile()
    {
        logToFile("system.log");
    }

    public static String getDiscriminatingValue(ILoggingEvent event)
    {
        long eventTimestamp = event.getTimeStamp();
        for (ActiveLogFile alf : activeLogFiles)
        {
            if (eventTimestamp >= alf.getTimestamp())
            {
                return alf.getFileName();
            }
        }
        return DEFAULT_LOG_FILE;
    }
}