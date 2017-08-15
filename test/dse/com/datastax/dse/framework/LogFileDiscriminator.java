/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.runner.Description;

import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Determines where to log events based on their timestamp
 */
public class LogFileDiscriminator
{
    public final static String DEFAULT_LOG_FILE = "system.log";
    public final static String TEST_LOG_FILE_PROPERTY = "TEST_LOG_FILE";

    private final static String defaultLogFile = System.getProperty(TEST_LOG_FILE_PROPERTY, DEFAULT_LOG_FILE);

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

    private static String normalizeTestName(String originalTestName)
    {
        return originalTestName.replaceAll("\\s+", "_").replaceAll("/", "_");
    }

    public static String getLogFileNameForTest(Description description)
    {
        return getLogFileNameForTest(description.getClassName(), description.getMethodName());
    }

    public static String getLogFileNameForTest(String testClassName, String testMethodName)
    {
        return testClassName + "/" + normalizeTestName(testMethodName) + ".log";
    }

    public static void logToFile(Description description)
    {
        logToFile(getLogFileNameForTest(description));
    }

    public static String getLogFileNameForBefore(String testClassName)
    {
        return getLogFileNameForTest(testClassName, "before");
    }

    public static void logToBeforeFile(String testClassName)
    {
        logToFile(getLogFileNameForBefore(testClassName));
    }

    public static String getLogFileNameForAfter(String testClassName)
    {
        return getLogFileNameForTest(testClassName, "after");
    }

    public static void logToAfterFile(String testClassName)
    {
        logToFile(getLogFileNameForAfter(testClassName));
    }

    /**
     * Start writing all log statements to the given log file
     * @param fileName the file where to log statements relative to test log folder (build/test/logs)
     */
    public static void logToFile(String fileName)
    {
        // Due to millisecond accuracy of log event timestamps, we need to break here for 1 ms
        // so that the logs from subsequent tests are not confused.
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
        activeLogFiles.push(new ActiveLogFile(System.currentTimeMillis(), fileName));
    }

    /**
     * Start writing all log statements to the default log file, as defined in the configuration (build/test/logs/system.log)
     */
    public static void logToDefaultFile()
    {
        logToFile(defaultLogFile);
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
        return defaultLogFile;
    }

}
