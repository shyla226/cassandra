/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.Completable;
import io.reactivex.Single;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.*;
import org.jctools.queues.MpmcArrayQueue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Logs audit events to a C* table.
 *
 * Can be run either synchronously or asychronously. When run synchronously, the event is written
 * to the table before the recordEvent method will return. When run asychronously, recordEvent pushes
 * the event into a queue, then returns. The queue is consumed by some periodic task.
 * The events are then written into the C* table in a batch query.
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
    private enum State { NOT_STARTED, STARTING, READY, STOPPING, STOPPED };

    /**
     * The startup time.
     */
    private long startupTime;

    final int retentionPeriod;

    /**
     * The statement used to perform the inserts.
     */
    private ModificationStatement insertStatement;

    /**
     * The event that need to be persisted (in async mode only).
     */
    private final Queue<EventBindings> eventQueue;

    /**
     * The consistency level at which the writes must be performed.
     */
    private final ConsistencyLevel writeConsistency;

    /**
     * The batching options.
     */
    private final BatchingOptions batchingOptions;

    /**
     * The writer's state.
     */
    private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);

    /**
     * The partition of the last batch executed.
     */
    private volatile EventPartition lastPartition;

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
            insertStatement = prepareInsertStatement(retentionPeriod > 0);
            startupTime = System.currentTimeMillis();

            if (!batchingOptions.synchronous)
            {
                scheduleNextFlush();
            }

            Runtime.getRuntime().addShutdownHook(new Thread(new WrappedRunnable()
            {
                @Override
                protected void runMayThrow() throws Exception
                {
                    logger.debug("Shutting down CassandraAuditWritter");
                    state.compareAndSet(State.READY, State.STOPPING);
                }
            }, "Audit shutdown"));

            state.set(State.READY);
        }
    }

    @VisibleForTesting
    static class BatchingOptions
    {
        static BatchingOptions SYNC = new BatchingOptions(true, 1, 0, 0);

        /**
         * {@code true} if the writes must be performed in a synchronous way, {@code false} otherwise.
         */
        public final boolean synchronous;

        /**
         * The maximum size of the batches.
         */
        public final int batchSize;

        /**
         * The interval of time between 2 flushes.
         */
        public final int flushPeriodInMillis;

        /**
         * The queue size.
         */
        public final int queueSize;

        public BatchingOptions(int batchSize, int flushPeriodInMillis, int queueSize)
        {
            this(false, batchSize, flushPeriodInMillis, queueSize);
        }

        private BatchingOptions(boolean synchronous, int batchSize, int flushPeriodInMillis, int queueSize)
        {
            this.synchronous = synchronous;
            this.batchSize = batchSize;
            this.flushPeriodInMillis = flushPeriodInMillis;
            this.queueSize = queueSize;
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

        public DecoratedKey getPartitionKey(IPartitioner partitioner)
        {
            return partitioner.decorateKey(CompositeType.build(TimestampType.instance.decompose(date),
                                                               nodeIp,
                                                               ByteBufferUtil.bytes(dayPartition)));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            EventPartition that = (EventPartition) o;

            return dayPartition == that.dayPartition && date.equals(that.date);
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
        final List<ModificationStatement> modifications = new ArrayList<>();  // this will always be insertStatement
        final List<List<ByteBuffer>> values = new ArrayList<>();
        final List<AuditableEvent> events = new ArrayList<>();

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

        int size()
        {
            return modifications.size();
        }

        void execute()
        {
            BatchStatement stmt = new BatchStatement(0, BatchStatement.Type.UNLOGGED, modifications, Attributes.none());
            lastPartition = partition;

            try
            {
                executeBatch(stmt, writeConsistency, values).doOnError(e -> logDroppedEvents(e))
                                                            .subscribe();
            }
            catch (RequestExecutionException | RequestValidationException e)
            {
                logDroppedEvents(e);
            }
        }

        private void logDroppedEvents(Throwable e)
        {
            logger.error("Exception writing audit events to table", e);
            for (AuditableEvent event: events)
            {
                droppedEventLogger.warn(event.toString());
            }
        }

        /**
         * Execute the batch statement.
         *
         * @param statement - the prepared statement to process
         * @param cl - the consistency level
         * @param values - the list of values to bind to the prepared statement
         */
        private Single<ResultMessage> executeBatch(BatchStatement statement, ConsistencyLevel cl, List<List<ByteBuffer>> values)
        {
            BatchQueryOptions options =
            BatchQueryOptions.withPerStatementVariables(QueryOptions.forInternalCalls(cl, Collections.<ByteBuffer>emptyList()),
                                                        values,
                                                        Collections.emptyList());

            return QueryProcessor.instance.processBatch(statement, QueryState.forInternalCalls(), options, System.nanoTime());
        }
    }

    /**
     * Schedules the next flush.
     */
    private void scheduleNextFlush()
    {
        // Check if we are stopping
        if (!state.compareAndSet(State.STOPPING, State.STOPPED))
        {
            // To reschedule the task, we need to compute the delay before the next expected flush.
            long currentTimeMillis = System.currentTimeMillis();
            long nextFlushTime = nextFlushAfter(currentTimeMillis);
            long delay = nextFlushTime - currentTimeMillis;

            flushTask().doOnError(e -> logger.error("Audit logging batching task failed.", e))
                       .doFinally(() -> scheduleNextFlush())
                       .delaySubscription(delay, TimeUnit.MILLISECONDS, getTpcSchedulerForNextFlush())
                       .subscribe();
        }
    }

    /**
     * Returns the scheduler to use for the next flush.
     * <p>We want to try to schedule the next flush on the same scheduler as the batches. By doing so the next flush
     * should only be triggered when all the batches have been executed. If the system get overloaded the queue
     * will get full and some events will be dropped releasing some pressure on the system.</p>
     * @return the scheduler to use for the next flush
     */
    private TPCScheduler getTpcSchedulerForNextFlush()
    {
        return lastPartition == null ? TPC.bestTPCScheduler() 
                                     : TPC.getForKey(CassandraAuditKeyspace.getKeyspace(),
                                                     lastPartition.getPartitionKey(CassandraAuditKeyspace.getAuditLogPartitioner()));
    }

    /**
     * Computes the timestamp of the next flush after the specified time.
     * @param currentTimeMillis the curent time in milliseconds
     * @return the timestamp of the next flush after the specified time
     */
    private long nextFlushAfter(long currentTimeMillis)
    {
        long floor = floor(currentTimeMillis, startupTime, batchingOptions.flushPeriodInMillis);
        return floor + batchingOptions.flushPeriodInMillis;
    }

    /**
     * Computes the largest timestamp which is less or equals to the current timestamp an a multiple of the period.
     * @param currentTime the current time in ms
     * @param startupTime the time representing the start of the process in ms
     * @param period the time period in ms
     * @return the largest timestamp which is less or equals to the current timestamp an a multiple of the period.
     */
    private static long floor(long currentTime, long startupTime, int period)
    {
        long delta = (currentTime - startupTime) % period;
        return currentTime - delta;
    }

    /**
     * Creates a task that will flush the events that are waiting in the queue.
     * Once it is terminated the task will reschedule itself.
     * @return the flush task.
     */
    private Single<Boolean> flushTask()
    {
        return Single.fromCallable(new Callable<Boolean>()
        {
            @Override
            public Boolean call() throws Exception
            {
                Map<EventPartition, EventBatch> batches = new HashMap<>();
                int numberOfEvents = 0;
                do
                {
                    EventBindings event = eventQueue.poll();

                    if (event == null)
                        break;

                    numberOfEvents++;

                    EventBatch batch = batches.get(event.partition);
                    if (batch == null)
                    {
                        batch = new EventBatch(event.partition);
                        batches.put(event.partition, batch);
                    }

                    batch.addEvent(event);

                    if (batch.size() >= batchingOptions.batchSize)
                    {
                        executeBatches(Collections.singletonList(batch));
                        batches.remove(event.partition);
                    }
                }
                while (numberOfEvents < batchingOptions.queueSize); // We do not want to cause starvation

                executeBatches(batches.values());

                return Boolean.TRUE;
            }
        });
    }

    @VisibleForTesting
    protected void executeBatches(Collection<EventBatch> batches)
    {
        for (EventBatch batch: batches)
            batch.execute();
    }

    public CassandraAuditWriter()
    {
        this(DatabaseDescriptor.getAuditLoggingOptions().retention_time,
             DatabaseDescriptor.getRawConfig().getAuditCassConsistencyLevel(),
             DatabaseDescriptor.getRawConfig().getAuditLoggerCassMode().equals("sync")
                ? BatchingOptions.SYNC
                : new BatchingOptions(DatabaseDescriptor.getRawConfig().getAuditCassBatchSize(),
                                      DatabaseDescriptor.getRawConfig().getAuditCassFlushTime(),
                                      DatabaseDescriptor.getRawConfig().getAuditLoggerCassAsyncQueueSize())
            );

    }

    /**
     * @param retentionPeriod the period of time audit events are kept. If null, events are kept forever
     * @param cl the consistency level with which to perform writes
     * @param batchingOptions specifies batching options (e.g. batch size, flush period). Use
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
        this.eventQueue = batchingOptions.synchronous ? null 
                                                      : batchingOptions.queueSize > 0 ? new MpmcArrayQueue<>(batchingOptions.queueSize)
                                                                                      : new ConcurrentLinkedQueue<>();
    }

    /**
     * Prepares the {@code CQLStatement} used to perform the inserts.
     * @param hasTTL {@code true} if the data has a limited retention period
     * @return the {@code CQLStatement} used to perform the inserts
     */
    private static ModificationStatement prepareInsertStatement(boolean hasTTL)
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

    /**
     * Gets the CQL statement corresponding to the specified CQL.
     * @param cql the CQL query
     * @return the {@code CQLStatement} corresponding to the specified CQL
     */
    private static CQLStatement prepareStatement(String cql)
    {
        try
        {
            QueryState queryState = QueryState.forInternalCalls();
            return QueryProcessor.getStatement(cql, queryState).statement;
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Error preparing audit writer", e);
        }
    }

    @Override
    public Completable recordEvent(final AuditableEvent event)
    {
        EventBindings bindings = new EventBindings(event);

        if (isSynchronous())
            return insertStatement.execute(QueryState.forInternalCalls(),
                                           bindings.getBindings(),
                                           System.nanoTime())
                                  .toCompletable();

        if (!eventQueue.add(bindings))
        {
            // queue is full
            droppedEventLogger.warn(event.toString());
        }
        return Completable.complete();
    }

    private boolean isSynchronous()
    {
        return this.batchingOptions.synchronous;
    }
}
