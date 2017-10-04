package org.apache.cassandra.cql3.continuous.paging;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.reactivex.Scheduler;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.ContinuousPagingConfig;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.selection.ResultBuilder;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TestTimeSource;
import org.apache.cassandra.utils.TimeSource;

import static org.junit.Assert.*;

public class ContinuousPagingServiceTest
{
    private static final Selection SELECTION = Selection.wildcard(TableMetadata.minimal("ks", "cf"), false);
    private TestSpecs test;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void removeSession()
    {
        // Always set the direct event loop, so that the cancellation can be processed:
        test.channel.setEventLoop(new ContinuousPagingTestStubs.DirectEventLoop());
        TPCUtils.blockingGet(ContinuousPagingService.cancel(test.queryState, test.streamId));
    }

    @Test(expected = ContinuousBackPressureException.class)
    public void testBackpressureIsTriggeredOnNextPages()
    {
        test = new TestSpecs()
               .numNextPages(1);

        ResultBuilder builder = test.build();
        builder.onRowCompleted(Arrays.asList(ByteBufferUtil.bytes("testColumn")), true);
    }

    @Test(expected = ContinuousBackPressureException.class)
    public void testBackpressureIsTriggeredOnMaxPages()
    {
        test = new TestSpecs()
               .channelEventLoop(new ContinuousPagingTestStubs.BlackholeEventLoop())
               .maxPagesPerSession(1);

        ResultBuilder builder = test.build();

        // This raises ContinuousBackPressureException because the blackhole event loop doesn't consume the page
        builder.onRowCompleted(Arrays.asList(ByteBufferUtil.bytes("testColumn")), true);
    }

    @Test(expected = ContinuousBackPressureException.class)
    public void testBackpressureIsTriggeredOnMaxLocalRunningTimeForLocalQueries()
    {
        test = new TestSpecs()
               .isLocalQuery(true)
               .maxLocalQueryTimeMs(50);

        ResultBuilder builder = test.build();
        builder.onRowCompleted(Arrays.asList(ByteBufferUtil.bytes("testColumn")), true);

        test.timeSource.sleepUninterruptibly(test.maxLocalQueryTimeMs, TimeUnit.MILLISECONDS);
        builder.onRowCompleted(Arrays.asList(ByteBufferUtil.bytes("testColumn")), true); // should throw
    }

    @Test
    public void testBackpressureIsNotTriggeredOnMaxLocalRunningTimeForNonLocalQueries()
    {
        test = new TestSpecs()
               .isLocalQuery(false)
               .maxLocalQueryTimeMs(50);

        ResultBuilder builder = test.build();
        builder.onRowCompleted(Arrays.asList(ByteBufferUtil.bytes("testColumn")), true);

        test.timeSource.sleepUninterruptibly(test.maxLocalQueryTimeMs, TimeUnit.MILLISECONDS);
        builder.onRowCompleted(Arrays.asList(ByteBufferUtil.bytes("testColumn")), true); // should not throw
    }

    @Test
    public void testUpdateBackpressureSessionNotFound()
    {
        test = new TestSpecs();

        test.build();

        boolean ret = ContinuousPagingService.updateBackpressure(test.queryState, test.streamId, 2);
        assertTrue(ret); // correct stream id

        ret = ContinuousPagingService.updateBackpressure(test.queryState, test.streamId + 1, 2);
        assertFalse(ret); // wrong stream id
    }

    @Test(expected = InvalidRequestException.class)
    public void testUpdateBackpressureWithZero()
    {
        test = new TestSpecs();

        test.build();

        ContinuousPagingService.updateBackpressure(test.queryState,
                                                   test.streamId,
                                                   0); // numPagesReceived should be positive
    }

    @Test(expected = InvalidRequestException.class)
    public void testUpdateBackpressureWithNeg()
    {
        test = new TestSpecs();

        test.build();

        ContinuousPagingService.updateBackpressure(test.queryState,
                                                   test.streamId,
                                                   -1); // numPagesReceived should be positive
    }

    @Test
    public void testUpdateBackpressureWithMax()
    {
        test = new TestSpecs();

        test.build();

        boolean ret = ContinuousPagingService.updateBackpressure(test.queryState,
                                                                 test.streamId,
                                                                 Integer.MAX_VALUE);

        assertFalse("Increasing with max should have resulted in overflow", ret);
    }

    @Test
    public void testUpdateBackpressureFromMax()
    {
        test = new TestSpecs().numNextPages(Integer.MAX_VALUE);

        test.build();

        boolean ret = ContinuousPagingService.updateBackpressure(test.queryState,
                                                                 test.streamId,
                                                                 1);

        assertFalse("Increasing from max should have resulted in overflow", ret);
    }

    @Test
    public void testUpdateBackpressureFromZero()
    {
        test = new TestSpecs().numNextPages(0);

        test.build();

        boolean ret = ContinuousPagingService.updateBackpressure(test.queryState,
                                                                 test.streamId,
                                                                 1);

        assertFalse("Increasing from zero should have ignored request", ret);
    }

    @Test
    public void testBackpressureIsResumedAfterPageIsConsumed()
    {
        test = new TestSpecs()
               .channelEventLoop(new ContinuousPagingTestStubs.RecordingEventLoop())
               .maxPagesPerSession(2);

        ResultBuilder builder = test.build();

        // First page is fine:
        builder.onRowCompleted(Arrays.asList(ByteBufferUtil.bytes("testColumn")), true);

        // Second page raises ContinuousBackPressureException because the recording event loop doesn't consume pages:
        try
        {
            builder.onRowCompleted(Arrays.asList(ByteBufferUtil.bytes("testColumn")), true);
            fail("No ContinuousBackPressureException");
        }
        catch (ContinuousBackPressureException ex)
        {
            // expected
        }

        // Now manually consume pages:
        ((ContinuousPagingTestStubs.RecordingEventLoop) test.channelEventLoop).runAll();

        // Third page is fine again:
        builder.onRowCompleted(Arrays.asList(ByteBufferUtil.bytes("testColumn")), true);
    }

    @Test
    public void testBackpressureIsUpdated()
    {
        test = new TestSpecs()
               .rowsPerPage(1)
               .numNextPages(1)
               .maxPagesPerSession(10);

        AtomicReference<ResultBuilder> builder = new AtomicReference<>(test.build());
        ResultBuilder initialBuilder = builder.get();

        try
        {
            builder.get().onRowCompleted(Arrays.asList(ByteBufferUtil.bytes("testColumn")), true); // first page
            fail("No ContinuousBackPressureException");
        }
        catch (ContinuousBackPressureException ex)
        {
            // expected
        }

        Assert.assertEquals(1, test.channel.writeCalls);
        Assert.assertEquals(1, test.channel.flushCalls);

        test.executor.onSchedule = (state, bldr) -> builder.set(bldr);

        ContinuousPagingService.updateBackpressure(test.queryState,
                                                   test.streamId,
                                                   3);

        // at this point updateBackpressure should have scheduled maybePause, which in turn will call executor schedule and
        // therefore onSchedule set above
        test.scheduler.runAll();

        Assert.assertFalse("Builder should have been updated when resuming after backpressure",
                           initialBuilder.equals(builder.get()));

        builder.get().onRowCompleted(Arrays.asList(ByteBufferUtil.bytes("testColumn")), true);

        Assert.assertEquals(2, test.channel.writeCalls);
        Assert.assertEquals(2, test.channel.flushCalls);

        builder.get().onRowCompleted(Arrays.asList(ByteBufferUtil.bytes("testColumn")), true); // third page

        Assert.assertEquals(3, test.channel.writeCalls);
        Assert.assertEquals(3, test.channel.flushCalls);

        builder.get().complete(); // final empty page

        Assert.assertEquals(4, test.channel.writeCalls);
        Assert.assertEquals(4, test.channel.flushCalls);
    }

    @Test
    public void testErrorIsSentAfterMaxClientWait()
    {
        test = new TestSpecs()
               .rowsPerPage(1)
               .numNextPages(10)
               .maxPagesPerSession(10)
               .maxClientWait(120);

        ResultBuilder builder = test.build();

        // Write numPagesRequestedNext - 1 pages (with one row per page):
        for (int i = 0; i < test.numNextPages - 1; i++)
        {
            builder.onRowCompleted(Arrays.asList(ByteBufferUtil.bytes("testColumn")), true);
        }
        Assert.assertEquals(0, ContinuousPagingService.pendingPages());
        Assert.assertEquals(test.numNextPages - 1, test.channel.writeCalls);
        Assert.assertEquals(test.numNextPages - 1, test.channel.flushCalls);

        // The last page will cause backpressure:
        try
        {
            builder.onRowCompleted(Arrays.asList(ByteBufferUtil.bytes("testColumn")), true);
            fail("Backpressure expected!");
        }
        catch(ContinuousBackPressureException ex)
        {
            test.channel.reset();
        }

        // Simulate a slow client by sleeping for maxClientWait:
        test.timeSource.sleepUninterruptibly(test.maxClientWait, TimeUnit.SECONDS);

        // At this point, the client is still consuming and backpressure hasn't been updated with a new value
        // for numPagesRequestedNext, so the maybeResume call recorded on the scheduler will send an error:
        test.scheduler.runAll();
        Assert.assertEquals(1, test.channel.writeCalls);
        Assert.assertEquals(1, test.channel.flushCalls);
        Assert.assertEquals(Message.Type.ERROR, ((Frame) test.channel.writeObjects.get(0)).header.type);
    }

    @Test
    public void testErrorIsSentAfterOverflowOfNumPagesSent()
    {
        test = new TestSpecs()
               .rowsPerPage(1)
               .numNextPages(Integer.MAX_VALUE);

        ResultBuilder builder = test.build();

        // set numPagesSent to MAX - 1
        ((ContinuousPagingService.ContinuousPagingSession.Builder)builder).session.numPagesSent = Integer.MAX_VALUE - 1;

        // sending one more page should result in an error
        builder.onRowCompleted(Arrays.asList(ByteBufferUtil.bytes("testColumn")), true);
        test.scheduler.runAll();

        Assert.assertEquals(2, test.channel.writeCalls);
        Assert.assertEquals(2, test.channel.flushCalls);
        Assert.assertEquals(Message.Type.ERROR, ((Frame) test.channel.writeObjects.get(1)).header.type);
    }

    private static class TestSpecs
    {
        TimeSource timeSource;
        QueryState queryState;
        QueryOptions queryOptions;
        ContinuousPagingTestStubs.RecordingChannel channel;
        ContinuousPagingConfig config;
        ContinuousPagingTestStubs.RecordingScheduler scheduler;
        TestPagingExecutor executor;
        ContinuousPagingState state;

        int streamId = 1;
        int rowsPerPage = 1;
        int numNextPages = 10;
        int maxPagesPerSession = 10;
        int maxPageSizeMb = 1;
        int maxLocalQueryTimeMs = 1000;
        int maxClientWait = 120;
        int maxCancelWait = 5;
        int checkInterval = 1000;
        boolean isLocalQuery = false;

        ContinuousPagingTestStubs.TestEventLoop channelEventLoop = new ContinuousPagingTestStubs.DirectEventLoop();

        TestSpecs streamId(int streamId)
        {
            this.streamId = streamId;
            return this;
        }

        TestSpecs rowsPerPage(int rowsPerPage)
        {
            this.rowsPerPage = rowsPerPage;
            return this;
        }

        TestSpecs numNextPages(int numNextPages)
        {
            this.numNextPages = numNextPages;
            return this;
        }

        TestSpecs maxPagesPerSession(int maxPagesPerSession)
        {
            this.maxPagesPerSession = maxPagesPerSession;
            return this;
        }

        TestSpecs maxPageSizeMb(int maxPageSizeMb)
        {
            this.maxPageSizeMb = maxPageSizeMb;
            return this;
        }

        TestSpecs maxLocalQueryTimeMs(int maxLocalQueryTimeMs)
        {
            this.maxLocalQueryTimeMs = maxLocalQueryTimeMs;
            return this;
        }

        TestSpecs maxClientWait(int maxClientWait)
        {
            this.maxClientWait = maxClientWait;
            return this;
        }

        TestSpecs maxCancelWait(int maxCancelWait)
        {
            this.maxCancelWait = maxCancelWait;
            return this;
        }

        TestSpecs checkInterval(int checkInterval)
        {
            this.checkInterval = checkInterval;
            return this;
        }

        TestSpecs isLocalQuery(boolean isLocalQuery)
        {
            this.isLocalQuery = isLocalQuery;
            return this;
        }

        TestSpecs channelEventLoop(ContinuousPagingTestStubs.TestEventLoop channelEventLoop)
        {
            this.channelEventLoop = channelEventLoop;
            return this;
        }

        ResultBuilder build()
        {
            this.timeSource = new TestTimeSource();
            this.queryState = new QueryState(ClientState.forInternalCalls(), streamId);
            this.queryOptions = QueryOptions.create(ConsistencyLevel.ONE,
                                                    Arrays.asList(ByteBufferUtil.bytes("testColumn")),
                                                    false,
                                                    new QueryOptions.PagingOptions(new PageSize(rowsPerPage,
                                                                                                PageSize.PageUnit.ROWS),
                                                                                   QueryOptions.PagingOptions.Mechanism.CONTINUOUS,
                                                                                   null,
                                                                                   0,
                                                                                   0,
                                                                                   numNextPages),
                                                    ConsistencyLevel.SERIAL,
                                                    ProtocolVersion.DSE_V2,
                                                    null);
            this.channel = new ContinuousPagingTestStubs.RecordingChannel(channelEventLoop);
            this.config = new ContinuousPagingConfig(Integer.MAX_VALUE, maxPagesPerSession, maxPageSizeMb, maxLocalQueryTimeMs, maxClientWait, maxCancelWait, checkInterval);
            this.scheduler = new ContinuousPagingTestStubs.RecordingScheduler();
            this.executor = new TestPagingExecutor(scheduler, timeSource, isLocalQuery);
            this.state = new ContinuousPagingState(timeSource,
                                                   config,
                                                   executor,
                                                   () -> channel,
                                                   100);

            return ContinuousPagingService.createSession(SELECTION.newSelectors(queryOptions),
                                                         null,
                                                         ResultSet.ResultMetadata.EMPTY,
                                                         state,
                                                         queryState,
                                                         queryOptions);
        }
    }

    private static class TestPagingExecutor implements ContinuousPagingExecutor
    {
        private final Scheduler scheduler;
        private final TimeSource timeSource;
        public BiConsumer<PagingState, ResultBuilder> onSchedule;
        public long queryStartTimeInNanos;
        public long queryStartTimeMillis;
        public boolean isLocal;
        public PagingState state;

        public TestPagingExecutor(Scheduler scheduler, TimeSource timeSource, boolean isLocal)
        {
            this.scheduler = scheduler;
            this.timeSource = timeSource;
            this.queryStartTimeInNanos = timeSource.nanoTime(); // this is set once when the request is received
            this.queryStartTimeMillis = timeSource.currentTimeMillis(); // this is set each time a task is scheduled
            this.isLocal = isLocal;
        }

        @Override
        public Scheduler getScheduler()
        {
            return scheduler;
        }

        @Override
        public void schedule(PagingState pagingState, ResultBuilder builder)
        {
            this.queryStartTimeMillis = timeSource.currentTimeMillis();
            if (onSchedule != null)
                onSchedule.accept(pagingState, builder);
        }

        @Override
        public long queryStartTimeInNanos()
        {
            return queryStartTimeInNanos;
        }

        @Override
        public PagingState state(boolean inclusive)
        {
            return state;
        }

        @Override
        public boolean isLocalQuery()
        {
            return isLocal;
        }

        @Override
        public long localStartTimeInMillis()
        {
            return isLocal ? queryStartTimeMillis : -1;
        }
    }
}
