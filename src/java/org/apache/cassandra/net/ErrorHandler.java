/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.net;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Handler that allow specific verbs to intercept some known execution errors. Note that the handler doesn't allow to
 * change the error in any way, it will be sent back to the coordinator of the request, but it allows some verbs to,
 * for instance, not log a particular exception, or trigger a particular action.
 * <p>
 * The default behavior, implemented by {@link #DEFAULT}, log the error at {@code WARN}.
 */
public interface ErrorHandler
{
    static final Logger logger = LoggerFactory.getLogger(VerbHandlers.class);
    public static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 5L, TimeUnit.MINUTES);

    public static final ErrorHandler DEFAULT = error -> {
        // This is an error we know can happen. Log it for operators but don't include the stack trace as it would
        // make it sound like it's a bug, which it's not.
        noSpamLogger.warn(error.getMessage());
    };


    void handleError(InternalRequestExecutionException error);
}
