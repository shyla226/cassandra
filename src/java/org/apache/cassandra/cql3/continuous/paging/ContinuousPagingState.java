package org.apache.cassandra.cql3.continuous.paging;

import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import io.netty.channel.Channel;
import org.apache.cassandra.config.ContinuousPagingConfig;
import org.apache.cassandra.utils.SystemTimeSource;
import org.apache.cassandra.utils.TimeSource;

/**
 * Container object for state related to the current continuous page session.
 */
public class ContinuousPagingState
{
    public final TimeSource timeSource;
    public final ContinuousPagingConfig config;
    public final ContinuousPagingExecutor executor;
    public final Supplier<Channel> channel;
    public final int averageRowSize;

    public ContinuousPagingState(ContinuousPagingConfig config, ContinuousPagingExecutor executor, Supplier<Channel> channel, int averageRowSize)
    {
        this(new SystemTimeSource(), config, executor, channel, averageRowSize);
    }

    @VisibleForTesting
    ContinuousPagingState(TimeSource timeSource, ContinuousPagingConfig config, ContinuousPagingExecutor executor, Supplier<Channel> channel, int averageRowSize)
    {
        this.timeSource = timeSource;
        this.config = config;
        this.executor = executor;
        this.channel = channel;
        this.averageRowSize = averageRowSize;
    }
}
