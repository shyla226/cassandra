package org.apache.cassandra.io.util;

import io.netty.util.internal.logging.InternalLogLevel;
import org.apache.cassandra.utils.flow.Flow;

/**
 *
 */
public class AsyncReadTimeoutException extends RuntimeException implements Flow.NonWrappableException
{
    public AsyncReadTimeoutException(AsynchronousChannelProxy channel, Class caller)
    {
        super(String.format("Timed out async read from %s for file %s%s",
                            caller.getCanonicalName(), channel.filePath,
                            channel.epollChannel != null ?
                            ", more information on epoll state with " + channel.epollChannel.getEpollEventLoop().epollFd() + " in the logs." :
                            ""), null, false, false);

        if (channel.epollChannel != null)
            channel.epollChannel.getEpollEventLoop().toLogAsync(InternalLogLevel.ERROR);
    }
}
