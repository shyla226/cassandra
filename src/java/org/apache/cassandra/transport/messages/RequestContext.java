package org.apache.cassandra.transport.messages;

import io.netty.channel.ChannelHandlerContext;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Message;

import java.util.function.Function;

public class RequestContext
{
    public final Message.Request request;
    public final QueryState queryState;
    public final long queryStartNanoTime;
    public final Message.Dispatcher dispatcher;
    public final ChannelHandlerContext channelHandlerContext;

    public RequestContext(Message.Request request, QueryState queryState, long queryStartNanoTime, Message.Dispatcher dispatcher, ChannelHandlerContext channelHandlerContext)
    {
        this.request = request;
        this.queryState = queryState;
        this.queryStartNanoTime = queryStartNanoTime;
        this.dispatcher = dispatcher;
        this.channelHandlerContext = channelHandlerContext;
    }

    // set in ExecuteMessage::executePipeline
    public QueryOptions queryOptions;
    public CQLStatement statement;

    // applied in inverse order
    public Function<Message.Response, Message.Response> messagePostProcessor;
    public Function<PartitionIterator, ResultMessage.Rows> statementPostProcessor;
    public Function<PartitionIterator, PartitionIterator> storageProxyPostProcessor;

    public PartitionIterator result;
    public Message.Response response;
}
