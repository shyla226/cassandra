/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.bdp.db.audit.cql3.AuditUtils;
import io.reactivex.Completable;

import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Logs audit events to a C* table.
 *
 * Can be run either synchronously or asychronously. When run synchronously, the event is written
 * to the table before the recordEvent method will return. When run asychronously, recordEvent pushes
 * the event into a queue, then returns. The queue is consumed by one or more EventBatcher instances,
 * which consume events from the queue until a specified number of events have been consumed, or a
 * specified amount of time has passed. The events are then written into the C* table in a batch query.
 * If a queue size is specified, recordEvent will block if the queue is full.
 *
 *
 */
public class CassandraAuditWriter implements IAuditWriter
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraAuditWriter.class);

    private static final String DROPPED_EVENT_LOGGER = "DroppedAuditEventLogger";

    private static final Logger droppedEventLogger = LoggerFactory.getLogger(DROPPED_EVENT_LOGGER);

    // number of fields in the audit table
    private static int NUM_FIELDS = 14;
    private static final ByteBuffer nodeIp = ByteBufferUtil.bytes(FBUtilities.getBroadcastAddress());

    /**
     * The writer possible states.
     */
    private enum State { NOT_STARTED, STARTING, READY };

    final int retentionPeriod;
    ModificationStatement insertStatement;

    private final ExecutorService executorService;

    private final BlockingQueue<EventBindings> eventQueue;

    private final ConsistencyLevel writeConsistency;

    private final BatchingOptions batchingOptions;

    /**
     * The writer's state.
     */
    private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);

    @Override
    public boolean isSetUpComplete()
    {
        return state.get() == State.READY;
    }

    @Override
    public void setUp()
    {
        if (state.compareAndSet(State.NOT_STARTED, State.STARTING))
        {
            CassandraAuditKeyspace.maybeConfigure();
            insertStatement = prepareInsertBlocking(retentionPeriod > 0);
            state.set(State.READY);
        }
    }

    @VisibleForTesting
    interface BatchController
    {
        /**
         * Reset the timer, called each time we enter the main loop in EventBatcher
         */
        void reset();

        /**
         * Check whether the max period has been exceeded for flushing a non-empty batch
         */
        boolean flushPeriodExceeded();

        /**
         * Return the number of milliseconds to poll the consumer queue of events.
         */
        long getNextPollPeriod();

        /**
         * Get the max size for a batch of insert statements
         */
        int getBatchSize();
    }

    @VisibleForTesting
    static class DefaultBatchController implements BatchController
    {
        final int flushPeriod;
        final int batchSize;

        private long start;
        private long lastCheckpoint;
        private final TimeSource timeSource;

        DefaultBatchController(int flushPeriod, int batchSize)
        {
            this(flushPeriod, batchSize, new SystemTimeSource());
        }

        DefaultBatchController(int flushPeriod, int batchSize, TimeSource timeSource)
        {
            this.flushPeriod = flushPeriod;
            this.batchSize = batchSize;
            this.timeSource = timeSource;
            reset();
        }

        @Override
        public void reset()
        {
            start = timeSource.currentTimeMillis();
            lastCheckpoint = start;
        }

        @Override
        public boolean flushPeriodExceeded()
        {
            lastCheckpoint = timeSource.currentTimeMillis();
            return (lastCheckpoint - start) > flushPeriod;
        }

        @Override
        public long getNextPollPeriod()
        {
            return flushPeriod - (lastCheckpoint - start);
        }

        @Override
        public int getBatchSize()
        {
            return batchSize;
        }
    }

    @VisibleForTesting
    interface BatchControllerFactory
    {
        BatchController newController();
    }

    @VisibleForTesting
    static class BatchingOptions
    {
        static BatchingOptions SYNC = new BatchingOptions();

        final BatchControllerFactory controllerFactory;
        final int queueSize;
        final int concurrentWriters;
        final boolean synchronous;

        private BatchingOptions()
        {
            queueSize = 0;
            concurrentWriters = 0;
            controllerFactory = null;
            synchronous = true;
        }

        BatchingOptions(int queueSize, int concurrentWriters, BatchControllerFactory controllerFactory)
        {
            this.queueSize = queueSize;
            this.concurrentWriters = concurrentWriters;
            this.controllerFactory = controllerFactory;
            this.synchronous = false;
        }
    }

    private static class EventPartition
    {
        private final Date date;
        private final int dayPartition;

        private EventPartition(AuditableEvent event)
        {
            DateTime eventTime = new DateTime(event.getTimestamp(), DateTimeZone.UTC);
            date = eventTime.toDateMidnight().toDate();
            dayPartition = eventTime.getHourOfDay() * 60 * 60;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            EventPartition that = (EventPartition) o;

            if (dayPartition != that.dayPartition) return false;
            return date.equals(that.date);
        }

        @Override
        public int hashCode()
        {
            int result = date.hashCode();
            result = 31 * result + dayPartition;
            return result;
        }

        @Override
        public String toString()
        {
            return "EventPartition{" +
                    "date=" + date +
                    ", dayPartition=" + dayPartition +
                    '}';
        }
    }

    private class EventBindings
    {
        private final AuditableEvent event;
        private final EventPartition partition;
        private final List<ByteBuffer> bindings;

        private EventBindings(AuditableEvent event)
        {
            assert event != null;
            this.event = event;
            partition = new EventPartition(event);

            bindings = new ArrayList<>(NUM_FIELDS + (retentionPeriod>0?1:0));
            bindings.add(TimestampType.instance.decompose(partition.date)); // date pk
            bindings.add(nodeIp);  // node inet
            bindings.add(ByteBufferUtil.bytes(partition.dayPartition));  // day partition
            bindings.add(ByteBufferUtil.bytes(event.getUid())); // event_time ck

            UUID batch_id = event.getBatchId();
            bindings.add(batch_id != null ? ByteBufferUtil.bytes(batch_id) : null);  // batch id

            bindings.add(ByteBufferUtil.bytes(event.getType().getCategory().toString()));  // category
            bindings.add(event.getKeyspace() != null ? ByteBufferUtil.bytes(event.getKeyspace()) : null);  // keyspace

            String operation = event.getOperation();
            bindings.add(operation != null ? ByteBufferUtil.bytes(operation) : null);  // operation

            String source = event.getSource();
            bindings.add(source != null ? ByteBufferUtil.bytes(source) : null);  // source

            String cf = event.getColumnFamily();
            bindings.add(cf != null ? ByteBufferUtil.bytes(cf) : null);  // table_name
            bindings.add(ByteBufferUtil.bytes(event.getType().toString()));  // type
            bindings.add(ByteBufferUtil.bytes(event.getUser()));  // user
            bindings.add(event.getAuthenticated() == null ? null : ByteBufferUtil.bytes(event.getAuthenticated()));  // authUser (proxy auth)
            bindings.add(event.getConsistencyLevel() == null ? null : ByteBufferUtil.bytes(event.getConsistencyLevel().toString()));  // consistency level
            if (retentionPeriod > 0)
                bindings.add(ByteBufferUtil.bytes(retentionPeriod));
        }

        private QueryOptions getBindings()
        {
            return QueryOptions.forInternalCalls(writeConsistency, bindings);
        }
    }

    class EventBatch
    {
        final EventPartition partition;
        final List<ModificationStatement> modifications = new LinkedList<>();  // this will always be insertStatement
        final List<List<ByteBuffer>> values = new LinkedList<>();
        final List<AuditableEvent> events = new LinkedList<>();

        public EventBatch(EventPartition partition)
        {
            this.partition = partition;
        }

        void addEvent(EventBindings event)
        {
            modifications.add(insertStatement);
            values.add(event.bindings);
            events.add(event.event);
        }

        void execute()
        {
            BatchStatement stmt = new BatchStatement(0, BatchStatement.Type.UNLOGGED, modifications, Attributes.none());

            try
            {
                AuditUtils.processBatchBlocking(stmt, writeConsistency, values);
            }
            catch (RequestExecutionException | RequestValidationException e)
            {
                logger.error("Exception writing audit events to table", e);
                for (AuditableEvent event: events)
                {
                    droppedEventLogger.warn(event.toString());
                }
            }
        }
    }

    private class EventBatcher implements Runnable
    {
        private final BatchController controller;

        private EventBatcher(BatchController controller)
        {
            this.controller = controller;
        }

        private void consume()
        {
            while (!Thread.interrupted())
            {
                // indicates that the thread should be interrupted and return
                // after saving the current set of audit events
                boolean interrupt = false;

                Map<EventPartition, EventBatch> batches = new HashMap<>();

                int numEvents = 0;
                controller.reset();
                while (true)
                {
                    if (Thread.interrupted())
                    {
                        interrupt = true;
                        break;
                    }

                    // check that we're not out of time for this batch
                    if (numEvents > 0)
                    {
                        if (controller.flushPeriodExceeded())
                            break;
                    }
                    else
                    {
                        controller.reset();
                    }

                    // get the next audit event to include in this batch
                    EventBindings event;
                    try
                    {
                        event = eventQueue.poll(controller.getNextPollPeriod(), TimeUnit.MILLISECONDS);
                        if (event == null)
                            continue;
                    }
                    catch (InterruptedException e)
                    {
                        interrupt = true;
                        break;
                    }
                    logger.trace("batching event {} into partition {}", event, event.partition);

                    EventBatch batch = batches.get(event.partition);
                    if (batch == null)
                    {
                        batch = new EventBatch(event.partition);
                        batches.put(event.partition, batch);
                    }
                    batch.addEvent(event);

                    // check if we have enough events to persist
                    numEvents++;
                    if (numEvents >= controller.getBatchSize())
                        break;
                }

                // persist the audit events
                if (numEvents > 0)
                {
                    executeBatches(batches.values());
                }

                if (interrupt)
                    break;
            }
            Thread.currentThread().interrupt();
        }

        @Override
        public void run()
        {
            // catch and log any errors generated by the queue consumption loop
            // consume() only returns if the thread has been interrupted, so this returns if consume() does
            while (true)
            {
                try
                {
                    consume();
                    return;
                }
                catch (Throwable e)
                {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    @VisibleForTesting
    void executeBatches(Collection<EventBatch> batches)
    {
        for (EventBatch batch: batches)
        {
            batch.execute();
        }
    }

    public CassandraAuditWriter()
    {
        this(DatabaseDescriptor.getAuditLoggingOptions().retention_time,
             DatabaseDescriptor.getRawConfig().getAuditCassConsistencyLevel(),
             DatabaseDescriptor.getRawConfig().getAuditLoggerCassMode().equals("sync")
                ? BatchingOptions.SYNC
                : new BatchingOptions(DatabaseDescriptor.getRawConfig().getAuditLoggerCassAsyncQueueSize(),
                                      DatabaseDescriptor.getRawConfig().getAuditLoggerNumCassLoggers(),
                                      new BatchControllerFactory()
                                      {
                                          @Override
                                          public BatchController newController()
                                          {
                                              return new DefaultBatchController(DatabaseDescriptor.getRawConfig().getAuditCassFlushTime(),
                                                                                DatabaseDescriptor.getRawConfig().getAuditCassBatchSize());
                                          }
                                      })
            );

    }

    /**
     * @param retentionPeriod the period of time audit events are kept. If null, events are kept forever
     * @param cl the consistency level with which to perform writes
     * @param batchingOptions specifies queue & batch size, concurrency factor, plus a BatchControllerFactory
     *                        to provide the controllers which decide when to batches get flushed. Use
     *                        BatchingOptions.SYNC to perform inserts synchronously
     */
    @VisibleForTesting
    CassandraAuditWriter(int retentionPeriod,
                         ConsistencyLevel cl,
                         BatchingOptions batchingOptions)
    {
        this.retentionPeriod = (int) TimeUnit.SECONDS.convert(retentionPeriod, TimeUnit.HOURS);
        this.writeConsistency = cl;
        this.batchingOptions = batchingOptions;

        if (!isSynchronous())
        {
            if (batchingOptions.queueSize > 0)
                eventQueue = new ArrayBlockingQueue<>(batchingOptions.queueSize);
            else
                eventQueue = new LinkedBlockingQueue<>();

            ThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("CassandraAuditWriter-%d").build();
            executorService = Executors.newFixedThreadPool(batchingOptions.concurrentWriters, threadFactory);

            EventBatcher eventBatcher = new EventBatcher(batchingOptions.controllerFactory.newController());
            for (int i=0; i<batchingOptions.concurrentWriters; i++)
                executorService.submit(eventBatcher);

            Runtime.getRuntime().addShutdownHook(new Thread(new WrappedRunnable()
            {
                @Override
                protected void runMayThrow() throws Exception
                {
                    logger.debug("Shutting down executor service");
                    executorService.shutdown();
                    logger.debug("Executor service shutdown complete");
                }
            }, "Audit shutdown"));
        }
        else
        {
            logger.info(String.format("%s starting in synchronous mode", CassandraAuditWriter.class.getSimpleName()));
            executorService = null;
            eventQueue = null;
        }
    }

    static ModificationStatement prepareInsertBlocking(boolean hasTTL)
    {
        String insertString = String.format(
                "INSERT INTO %s.%s (" +
                        "date, " +
                        "node, " +
                        "day_partition, " +
                        "event_time, " +
                        "batch_id, " +
                        "category, " +
                        "keyspace_name, " +
                        "operation, " +
                        "source, " +
                        "table_name, " +
                        "type, " +
                        "username," +
                        "authenticated," +
                        "consistency)" +
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                CassandraAuditKeyspace.NAME, CassandraAuditKeyspace.AUDIT_LOG);

        if (hasTTL)
        {
            insertString = insertString + " USING TTL ?";
        }

        return (ModificationStatement) prepareStatement(insertString);
    }

    private static CQLStatement prepareStatement(String cql)
    {
        try
        {
            QueryState queryState = QueryState.forInternalCalls();
            ParsedStatement.Prepared stmt = null;
            while (stmt == null)
            {
                MD5Digest stmtId = TPCUtils.blockingGet(QueryProcessor.instance.prepare(cql, queryState)).statementId;
                stmt = QueryProcessor.instance.getPrepared(stmtId);
            }
            return stmt.statement;

        } catch (RequestValidationException e)
        {
            throw new RuntimeException("Error preparing audit writer", e);
        }
    }

    public Completable recordEvent(final AuditableEvent event)
    {
        EventBindings bindings = new EventBindings(event);

        if (isSynchronous())
        {
            try
            {
                return insertStatement.execute(QueryState.forInternalCalls(),
                                               bindings.getBindings(),
                                               System.nanoTime()).toCompletable();
            }
            catch (RequestValidationException | RequestExecutionException e)
            {
                throw new RuntimeException(e);
            }
        }
        else
        {
            try
            {
                eventQueue.put(bindings);
                return Completable.complete();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException("Unable to enqueue audit event", e);
            }
        }
    }

    private boolean isSynchronous()
    {
        return this.batchingOptions.synchronous;
    }
}
