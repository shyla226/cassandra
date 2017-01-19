/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3.continuous.paging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Ignore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.RowIterator;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.shaded.netty.channel.EventLoopGroup;
import com.datastax.shaded.netty.channel.nio.NioEventLoopGroup;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.apache.cassandra.cql3.CQLTester.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Ignore
class ContinuousPagingTestUtils
{
    private static final Logger logger = LoggerFactory.getLogger(ContinuousPagingFeaturesTest.class);
    private static final Random randomGenerator = new Random();

    public static void startup()
    {
        CQLTester.requireNetwork(false);

        long seed = System.nanoTime();
        logger.info("Using seed {}", seed);
        randomGenerator.setSeed(seed);
    }

    interface TestSchema
    {
        /** Creates the schema (typically a table) and inserts any data */
        public void create(CQLTester tester) throws Throwable;

        /** Return the query to run */
        public String getQuery();

        /** Return an array of rows that the query should return. */
        public Object[][] expectedRows();
    }

    /**
     * A schema with one partition key, one clustering key two text values of
     * identical size and a static column.
     */
    static class FixedSizeSchema implements TestSchema
    {
        final int numPartitions;
        final int numClusterings;
        final int partitionSize;
        final Object[][] rows;
        final boolean compression;

        FixedSizeSchema(int numPartitions, int numClusterings, int partitionSize)
        {
            this(numPartitions, numClusterings, partitionSize, false);
        }

        FixedSizeSchema(int numPartitions, int numClusterings, int partitionSize, boolean compression)
        {
            this.numPartitions = numPartitions;
            this.numClusterings = numClusterings;
            this.partitionSize = partitionSize;
            this.rows = new Object[numPartitions * numClusterings][];
            this.compression = compression;
        }

        static String generateText(int size)
        {
            Random rnd = new Random();
            char[] chars = new char[size];

            for (int i = 0; i < size; )
                for (long v = rnd.nextLong(),
                     n = Math.min(size - i, Long.SIZE / Byte.SIZE);
                     n-- > 0; v >>= Byte.SIZE)
                    chars[i++] = (char) (((v & 127) + 32) & 127);
            return new String(chars, 0, size);
        }

        /** Create the rows that will be used in create() to insert data. */
        protected void createRows()
        {
            // These are CQL sizes and at the moment we duplicate partition and static values in each CQL row
            // 12 is the size of the 3 integers (pk, ck, static val)
            int rowSize = partitionSize / numClusterings;
            int textSize = Math.max(1, (rowSize - 12) / 2);
            for (int i = 0; i < numPartitions; i++)
            {
                for (int j = 0; j < numClusterings; j++)
                {
                    String text1 = generateText(textSize);
                    String text2 = generateText(textSize);
                    rows[i * numClusterings + j] = row(i, j, text1, text2, i);
                }
            }
        }

        public void create(CQLTester tester) throws Throwable
        {
            createRows();

            tester.createTable(createStatement());

            for (Object[] row : insertRows())
                tester.execute(insertStatement(), row);
        }

        protected String createStatement()
        {
            String ret = "CREATE TABLE %s (k INT, c INT, val1 TEXT, val2 TEXT, s INT STATIC, PRIMARY KEY(k, c))";
            if (!compression)
                ret += " WITH compression = {'sstable_compression' : ''}";
            return ret;
        }

        protected String insertStatement()
        {
            return "INSERT INTO %s (k, c, val1, val2, s) VALUES (?, ?, ?, ?, ?)";
        }

        protected Object[][] insertRows()
        {
            return rows;
        }

        public String getQuery()
        {
            return "SELECT k, c, val1, val2, s FROM %s";
        }

        public Object[][] expectedRows()
        {
            return rows;
        }
    }

    /**
     * This schema is the same as the fixed size schema except that each text value
     * has a random size, making each row of size different size.
     */
    static class VariableSizeSchema extends FixedSizeSchema
    {
        VariableSizeSchema(int numPartitions, int numClusterings, int partitionSize)
        {
            super(numPartitions, numClusterings, partitionSize);
        }

        @Override
        public void createRows()
        {
            int rowSize = partitionSize / numClusterings;
            int textSize = Math.max(1, (rowSize - 12) / 2);
            for (int i = 0; i < numPartitions; i++)
            {
                for (int j = 0; j < numClusterings; j++)
                {
                    String text1 = generateText(1 + randomGenerator.nextInt(textSize));
                    String text2 = generateText(1 + randomGenerator.nextInt(textSize));
                    rows[i * numClusterings + j] = row(i, j, text1, text2, i);
                }
            }
        }
    }

    /**
     * This schema is the same as the fixed size schema except that it introduces rows
     * that are abnormally large.
     */
    static class AbonormallyLargeRowsSchema extends FixedSizeSchema
    {
        /** The percentage of large rows */
        private final double percentageLargeRows;

        AbonormallyLargeRowsSchema(int numPartitions, int numClusterings, int partitionSize, double percentageLargeRows)
        {
            super(numPartitions, numClusterings, partitionSize);
            this.percentageLargeRows = percentageLargeRows;
        }

        @Override
        public void createRows()
        {
            // These are CQL sizes and at the moment we duplicate partition and static values in each CQL row
            // 12 is the size of the 3 integers (pk, ck, static val)
            int rowSize = partitionSize / numClusterings;
            int textSize = Math.max(1, (rowSize - 12) / 2);

            int totRows = numPartitions * numClusterings;
            int largeRows = (int)(totRows * percentageLargeRows);
            int[] largeIndexes = randomGenerator.ints(0, totRows).distinct().limit(largeRows).toArray();
            Arrays.sort(largeIndexes);
            int currentRow = 0;
            int nextLargeRow = 0;

            for (int i = 0; i < numPartitions; i++)
            {
                for (int j = 0; j < numClusterings; j++)
                {
                    int size;
                    if (nextLargeRow < largeIndexes.length && currentRow == largeIndexes[nextLargeRow])
                    {   // make this row up to 50 times bigger (2 text values up to 25 times bigger)
                        size = textSize * (1 + randomGenerator.nextInt(25));
                        nextLargeRow++;
                    }
                    else
                    {
                        size = textSize;
                    }
                    String text1 = generateText(size);
                    String text2 = generateText(size);
                    rows[i * numClusterings + j] = row(i, j, text1, text2, i);
                    currentRow++;
                }
            }
        }
    }

    /**
     * This schema is the same as the fixed size schema except that the select
     * statement will only select the rows in the first N partitions.
     */
    static class SelectInitialPartitionsSchema extends FixedSizeSchema
    {
        private final int numSelectPartitions;

        SelectInitialPartitionsSchema(int numPartitions, int numClusterings, int partitionSize, int numSelectPartitions)
        {
            super(numPartitions, numClusterings, partitionSize);
            this.numSelectPartitions = numSelectPartitions;
        }

        public String getQuery()
        {
            if (numSelectPartitions == 1)
                return "SELECT k, c, val1, val2, s FROM %s WHERE k = 0";

            return "SELECT k, c, val1, val2, s FROM %s WHERE k in ("
                   + IntStream.range(0, numSelectPartitions).mapToObj(Integer::toString).collect(Collectors.joining(", "))
                   + ')';
        }

        public Object[][] expectedRows()
        {
            return Arrays.copyOfRange(rows, 0, numSelectPartitions * numClusterings);
        }
    }

    /**
     * A schema for testing group by queries, because at the moment they have a dedicated pager.
     */
    static class GroupBySchema implements TestSchema
    {
        final boolean compression;
        final String query;
        final Object[][] expectedRows;

        GroupBySchema(String query, Object[][] expectedRows)
        {
            this(query, expectedRows, false);
        }

        GroupBySchema(String query, Object[][] expectedRows, boolean compression)
        {
            this.query = query;
            this.expectedRows = expectedRows;
            this.compression = compression;
        }

        public void create(CQLTester tester) throws Throwable
        {
            String createTable = "CREATE TABLE %s (a int, b int, c int, d int, e int, primary key (a, b, c, d))";
            if (!compression)
                createTable += " WITH compression = {'sstable_compression' : ''}";

            tester.createTable(createTable);

            tester.execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 3, 6)");
            tester.execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 6, 12)");
            tester.execute("INSERT INTO %s (a, b, c, d) VALUES (1, 3, 2, 12)");
            tester.execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, 2, 12, 24)");
            tester.execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, 2, 6, 12)");
            tester.execute("INSERT INTO %s (a, b, c, d, e) VALUES (2, 2, 3, 3, 6)");
            tester.execute("INSERT INTO %s (a, b, c, d, e) VALUES (2, 4, 3, 6, 12)");
            tester.execute("INSERT INTO %s (a, b, c, d, e) VALUES (4, 8, 2, 12, 24)");
            tester.execute("INSERT INTO %s (a, b, c, d) VALUES (5, 8, 2, 12)");

            tester.execute("DELETE FROM %s WHERE a = 1 AND b = 3 AND c = 2");
            tester.execute("DELETE FROM %s WHERE a = 5");
        }

        public String getQuery()
        {
            return query;
        }

        public Object[][] expectedRows()
        {
            return expectedRows;
        }
    }

    static class SchemaBuilder
    {
        final CQLTester tester;
        final ProtocolVersion protocolVersion = ProtocolVersion.CURRENT;
        Function<SchemaBuilder, TestSchema> schemaSupplier;
        int numPartitions;
        int numClusterings;
        int partitionSize;

        SchemaBuilder(CQLTester tester)
        {
            this.tester = tester;
        }

        SchemaBuilder schemaSupplier(Function<SchemaBuilder, TestSchema> schemaSupplier)
        {
            this.schemaSupplier = schemaSupplier;
            return this;
        }

        SchemaBuilder numPartitions(int numPartitions)
        {
            this.numPartitions = numPartitions;
            return this;
        }

        SchemaBuilder numClusterings(int numClusterings)
        {
            this.numClusterings = numClusterings;
            return this;
        }

        SchemaBuilder partitionSize(int partitionSize)
        {
            this.partitionSize = partitionSize;
            return this;
        }

        TestSchema build() throws Throwable
        {
            if (this.schemaSupplier == null)
                this.schemaSupplier = (b) -> new FixedSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize);

            TestSchema ret = schemaSupplier.apply(this);
            createData(ret);
            return ret;
        }

        private void createData(TestSchema schema) throws Throwable
        {
            schema.create(tester);
            logger.info("Finished creating schema, including test data.");

            tester.flush();
            logger.info("Finished flushing.");

            tester.compact();
            logger.info("Finished compacting.");
        }
    }

    static class TestBuilder
    {
        final CQLTester tester;
        SchemaBuilder schemaBuilder;
        TestSchema schema;

        int numClientThreads;
        int clientPauseMillis;
        boolean checkRows;
        boolean checkNumberOfRowsInPage = true;
        int maxRows;
        int maxPages; // we ask the server to send at most max pages
        int maxPagesPerSecond; // we ask the server to send at most max pages per second
        int cancelAfter; // send a cancel after this number of pages
        int failAfter = -1; // we expect a failure after this number of pages

        TestBuilder(CQLTester tester)
        {
            this(tester, new SchemaBuilder(tester), null);
        }

        TestBuilder(CQLTester tester, TestSchema schema)
        {
            this(tester, null, schema);
        }

        private TestBuilder(CQLTester tester, SchemaBuilder schemaBuilder, TestSchema schema)
        {
            this.tester = tester;
            this.schemaBuilder = schemaBuilder;
            this.schema = schema;
        }

        TestBuilder schemaSupplier(Function<SchemaBuilder, TestSchema> schemaSupplier)
        {
            assert schemaBuilder != null && schema == null;
            schemaBuilder = schemaBuilder.schemaSupplier(schemaSupplier);
            return this;
        }

        TestBuilder numPartitions(int numPartitions)
        {
            assert schemaBuilder != null && schema == null;
            schemaBuilder = schemaBuilder.numPartitions(numPartitions);
            return this;
        }

        TestBuilder numClusterings(int numClusterings)
        {
            assert schemaBuilder != null && schema == null;
            schemaBuilder = schemaBuilder.numClusterings(numClusterings);
            return this;
        }

        TestBuilder partitionSize(int partitionSize)
        {
            assert schemaBuilder != null && schema == null;
            schemaBuilder = schemaBuilder.partitionSize(partitionSize);
            return this;
        }

        TestBuilder numClientThreads(int numClientThreads)
        {
            this.numClientThreads = numClientThreads;
            return this;
        }

        TestBuilder clientPauseMillis(int clientPauseMillis)
        {
            this.clientPauseMillis = clientPauseMillis;
            return this;
        }

        TestBuilder checkRows(boolean checkRows)
        {
            this.checkRows = checkRows;
            return this;
        }

        TestBuilder checkNumberOfRowsInPage(boolean checkNumberOfRowsInPage)
        {
            this.checkNumberOfRowsInPage = checkNumberOfRowsInPage;
            return this;
        }

        TestBuilder maxRows(int maxRows)
        {
            this.maxRows = maxRows;
            return this;
        }

        TestBuilder maxPages(int maxPages)
        {
            this.maxPages = maxPages;
            return this;
        }

        TestBuilder maxPagesPerSecond(int maxPagesPerSecond)
        {
            this.maxPagesPerSecond = maxPagesPerSecond;
            return this;
        }

        TestBuilder cancelAfter(int cancelAfter)
        {
            this.cancelAfter = cancelAfter;
            return this;
        }

        TestBuilder failAfter(int failAfter)
        {
            this.failAfter = failAfter;
            return this;
        }

        TestSchema buildSchema() throws Throwable
        {
            if (schema != null)
                return schema;

            return schemaBuilder.build();
        }

        TestHelper build() throws Throwable
        {
            return new TestHelper(this);
        }
    }

    static class TestHelper implements AutoCloseable
    {
        private static AtomicInteger clusterNo = new AtomicInteger(0);

        private final CQLTester tester;
        private final TestSchema schema;
        private final ProtocolVersion protocolVersion;
        private final int numClientThreads;
        private final int clientPauseMillis;
        private final boolean checkRows;
        private final boolean checkNumberOfRowsInPage;
        private final int maxRows;
        private final int maxPages;
        private final int maxPagesPerSecond;
        private final int cancelAfter;
        private final int failAfter;
        private final Cluster cluster;
        private final Session session;

        TestHelper(TestBuilder builder) throws Throwable
        {
            this.tester = builder.tester;
            this.schema = builder.buildSchema();
            this.protocolVersion = ProtocolVersion.DSE_V1;
            this.numClientThreads = builder.numClientThreads;
            this.clientPauseMillis = builder.clientPauseMillis;
            this.checkRows = builder.checkRows;
            this.checkNumberOfRowsInPage = builder.checkNumberOfRowsInPage;
            this.maxRows = builder.maxRows;
            this.maxPages = builder.maxPages;
            this.maxPagesPerSecond = builder.maxPagesPerSecond;
            this.cancelAfter = builder.cancelAfter;
            this.failAfter = builder.failAfter;
            this.cluster = CQLTester.createClientCluster(protocolVersion,
                                                         String.format("Test cluster %d", clusterNo.incrementAndGet()),
                                                         new CustomNettyOptions(numClientThreads));
            this.session = cluster.connect();
        }

        long testLegacyPaging(int numTrials, int pageSizeRows) throws Throwable
        {
            PreparedStatement prepared = session.prepare(tester.formatQuery(schema.getQuery()));
            BoundStatement statement = prepared.bind();
            statement.setFetchSize(pageSizeRows);

            long start = System.nanoTime();
            for (int i = 0; i < numTrials; i++)
            {
                ListenableFuture<ResultSet> resultFuture = session.executeAsync(statement);
                final CheckResultSet checker = new CheckResultSet(pageSizeRows, ContinuousPagingOptions.PageUnit.ROWS);
                while (resultFuture != null)
                {
                    final ResultSet resultSet = resultFuture.get();
                    checker.checkPage(resultSet); // must check before fetching or we may receive too many rows in current page
                    if (!resultSet.isFullyFetched()) // the best we can do here is start fetching before processing
                        resultFuture = resultSet.fetchMoreResults(); // this batch of results
                    else
                        resultFuture = null;

                    maybePauseClient();
                }

                checker.checkAll();
            }

            return (System.nanoTime() - start) / (1000000 * numTrials);

        }

        long testContinuousPaging(int numTrials, int pageSize, ContinuousPagingOptions.PageUnit pageUnit) throws Throwable
        {
            long start = System.nanoTime();

            String query = schema.getQuery();
            if (maxRows > 0)
                query += String.format(" LIMIT %d", maxRows);

            for (int i = 0; i < numTrials; i++)
            {
                Statement statement = new SimpleStatement(tester.formatQuery(query));

                final CheckResultSet checker = new CheckResultSet(pageSize, pageUnit);

                ContinuousPagingOptions pagingOptions = ContinuousPagingOptions.create(pageSize, pageUnit, maxPages, maxPagesPerSecond);
                try(RowIterator it = session.execute(statement, pagingOptions))
                {
                    int currentPage = it.pageNo();
                    assertEquals(0, currentPage);

                    List<Row> rows = new ArrayList<>(pageUnit == ContinuousPagingOptions.PageUnit.ROWS ? pageSize : 1000);
                    while (it.hasNext())
                    {
                        if (currentPage != it.pageNo())
                        {
                            assertEquals(currentPage + 1, it.pageNo());

                            if (rows.size() > 0)
                            {
                                checker.checkPage(rows, it.getColumnDefinitions());

                                if (cancelAfter > 0 && currentPage >= cancelAfter)
                                    break;

                                maybePauseClient();
                            }
                            else
                            {
                                // zero means no page, this is the only case where we are OK with no rows
                                assertEquals(0, currentPage);
                            }


                            currentPage = it.pageNo();
                        }

                        rows.add(it.next());
                    }

                    if (rows.size() > 0)
                        checker.checkPage(rows, it.getColumnDefinitions());
                }

                checker.checkAll();
            }

            return (System.nanoTime() - start) / (1000000 * numTrials);
        }

        /**
         * Read the entire table starting with continuous paging, interrupting and resuming again.
         *
         * @param pageSize - the page size in the page unit specified
         * @param pageUnit  - the page unit, bytes or rows
         * @param interruptions - the row index where we should interrupt
         * @return the time it took in milliseconds
         */
        long testResumeWithContinuousPaging(int pageSize, ContinuousPagingOptions.PageUnit pageUnit, int[] interruptions) throws Throwable
        {
            long start = System.nanoTime();

            String query = schema.getQuery();
            if (maxRows > 0)
                query += String.format(" LIMIT %d", maxRows);

            Statement statement = new SimpleStatement(tester.formatQuery(query));
            statement.setFetchSize(pageSize);

            final CheckResultSet checker = new CheckResultSet(pageSize, pageUnit);

            ContinuousPagingOptions pagingOptions = ContinuousPagingOptions.create(pageSize, pageUnit, maxPages, maxPagesPerSecond);

            RowIterator.State state = null;
            int num = 0;
            List<Row> rows = new ArrayList<>(pageUnit == ContinuousPagingOptions.PageUnit.ROWS ? pageSize : 1000);

            for (int interruptAt : interruptions)
            {
                try(RowIterator it = state == null ? session.execute(statement, pagingOptions) : state.resume())
                {
                    int currentPage = it.pageNo();
                    logger.debug("Current page {}, rows {}, interrupting at {}", currentPage, rows.size(), interruptAt);

                    if (state == null)
                        assertEquals(0, currentPage);
                    else
                        assertTrue(String.format("Current page: %d, rows %d", currentPage, rows.size()),
                                   currentPage == 0 || currentPage == 1);

                    while (it.hasNext() && num < interruptAt)
                    {
                        if (currentPage != it.pageNo())
                        {
                            assertEquals(currentPage + 1, it.pageNo());
                            if (rows.size() > 0)
                            {
                                checker.checkPage(rows, it.getColumnDefinitions());
                            }
                            else
                            {
                                assertEquals(0, currentPage);
                            }
                            currentPage = it.pageNo();
                        }

                        rows.add(it.next());
                        num++;
                    }

                    if ((rows.size() > 0 && pageUnit == ContinuousPagingOptions.PageUnit.BYTES) || rows.size() == pageSize)
                        checker.checkPage(rows, it.getColumnDefinitions());

                    state = it.state();
                }
            }

            if (state != null)
            {
                try(RowIterator it = state.resume())
                {
                    int currentPage = it.pageNo();
                    logger.debug("Current page {}, rows {}, final iteration", currentPage, rows.size());

                    assertTrue(String.format("Current page: %d, rows %d", currentPage, rows.size()),
                               currentPage == 0 || currentPage == 1);

                    while (it.hasNext())
                    {
                        if (currentPage != it.pageNo())
                        {
                            assertEquals(currentPage + 1, it.pageNo());
                            if (rows.size() > 0)
                                checker.checkPage(rows, it.getColumnDefinitions());
                            else
                                assertEquals(0, currentPage);

                            currentPage = it.pageNo();
                        }

                        rows.add(it.next());
                    }

                    if (rows.size() > 0)
                        checker.checkPage(rows, it.getColumnDefinitions());
                }
            }

            checker.checkAll();

            return (System.nanoTime() - start) / 1000000;
        }

        private void maybePauseClient() throws Throwable
        {
            if (clientPauseMillis > 0)
                Thread.sleep(clientPauseMillis);
        }

        //simulates performing some processing with the results
        private class CheckResultSet
        {
            private final int pageSize;
            private final ContinuousPagingOptions.PageUnit pageUnit;
            private final Object[][] rows;
            private final List<Object[]> rowsReceived;
            private int numRowsReceived ;
            private int numPagesReceived;

            CheckResultSet(int pageSize, ContinuousPagingOptions.PageUnit pageUnit)
            {
                this.pageSize = pageSize;
                this.pageUnit = pageUnit;
                this.rows = schema.expectedRows();
                this.rowsReceived = new ArrayList<>(rows.length);
            }

            Comparator<Object[]> RowComparator = (Comparator<Object[]>) (row1, row2) -> {
                int ret = Integer.compare(row1.length, row2.length);
                if (ret != 0)
                    return ret;

                for (int i = 0; i < row1.length; i++)
                {
                    if (row1[i] instanceof Integer && row2[i] instanceof Integer)
                        ret = Integer.compare((int)row1[i], (int)row2[i]);
                    else if (row1[i] instanceof String && row2[i] instanceof String)
                        ret = ((String)row1[i]).compareTo((String)row2[i]);
                    else
                        ret = Integer.compare(row1[1].hashCode(), row2[1].hashCode());
                    if (ret != 0)
                        return ret;
                }

                return 0;
            };

            private synchronized void checkPage(ResultSet resultSet)
            {
                int numRows = resultSet.getAvailableWithoutFetching();
                List<Row> pageRows = new ArrayList<>(numRows);
                for (Row row : resultSet)
                {
                    pageRows.add(row);

                    if (--numRows == 0)
                        break;
                }
                assertEquals(0, numRows);

                checkPage(pageRows, resultSet.getColumnDefinitions());
            }

            private void checkPage(List<Row> pageRows, ColumnDefinitions meta)
            {
                int numRows = pageRows.size();
                if (logger.isTraceEnabled())
                    logger.trace("{} - Received page with {} rows for page size {} and meta {}", hashCode(), numRows, pageSize, meta);

                assertNotNull(meta);

                if (checkNumberOfRowsInPage && pageUnit == ContinuousPagingOptions.PageUnit.ROWS && numRows > 0)
                {
                    int totRows = maxRows > 0 ? maxRows : rows.length;
                    assertEquals(String.format("Unexpected number of rows in current page: page size %d, total expected rows %d, received pages so far %d",
                                               pageSize, totRows, numPagesReceived),
                                 Math.min(pageSize, totRows - numRowsReceived), numRows);
                }

                if (checkRows)
                {
                    Object[][] rows = tester.getRowsNet(cluster, meta, pageRows);
                    for (int i = 0; i < rows.length; i++)
                    {
                        for (int j = 0; j < rows[i].length; j++)
                            assertNotNull(String.format("Row %d has a null field: %s", i, Arrays.toString(rows[i])),
                                          rows[i][j]);
                    }
                    rowsReceived.addAll(Arrays.asList(rows));
                }
                numRowsReceived += numRows;
                numPagesReceived += 1;
                pageRows.clear();
            }

            private void checkAll()
            {
                if (failAfter >= 0)
                {   // check that if the server was supposed to fail after failAfterPages, we haven't received any
                    // extra pages
                    assertEquals(failAfter, numPagesReceived);
                    return;
                }

                if (maxPages > 0)
                { // check that we've received exactly the number of pages requested
                    assertFalse("Cannot check rows if receiving fewer pages", checkRows);
                    assertEquals(maxPages, numPagesReceived);
                    return;
                }

                if (maxRows > 0)
                {   // check that we've received exactly the number of rows requested
                    assertFalse("Cannot check rows if receiving fewer pages", checkRows);
                    assertEquals(maxRows, numRowsReceived);
                    return;
                }

                if (cancelAfter > 0)
                {   // check that we haven't received too few pages and that the last page
                    // still has more too fetch (this could become flacky if client is too fast)
                    assertFalse("Cannot check rows if receiving fewer pages", checkRows);
                    logger.info("Received {} pages when cancelling after {} pages", numPagesReceived, cancelAfter);
                    assertTrue(String.format("%d < %d", numPagesReceived, cancelAfter), numPagesReceived >= cancelAfter);
                    return;
                }

                //otherwise check we've received all table rows
                assertEquals(rows.length, numRowsReceived);

                //check every single row matches if so requested, requires sorting rows
                if (checkRows)
                {
                    assertEquals("Received different number of rows", rowsReceived.size(), rows.length);
                    Collections.sort(rowsReceived, RowComparator);
                    for (int i = 0; i < rows.length; i++)
                    {
                        if (rows[i].length != rowsReceived.get(i).length)
                        {
                            Assert.fail(String.format("Row %d has a different number of values:\n%s\n%s",
                                                      i,
                                                      printRows(Arrays.asList(rows)),
                                                      printRows(rowsReceived)));
                        }
                        for (int j = 0; j < rows[i].length; j++)
                        {
                            if (!rows[i][j].equals(rowsReceived.get(i)[j]))
                                Assert.fail(String.format("Row %d column %d has a different value\n%s\n%s",
                                                          i,
                                                          j,
                                                          printRows(Arrays.asList(rows)),
                                                          printRows(rowsReceived)));
                        }
                    }
                }
            }
        }

        private String printRows(List<Object[]> rows)
        {
            StringBuilder ret = new StringBuilder();
            for (Object[] row : rows)
                ret.append(Arrays.toString(row));

            return ret.toString();
        }

        public void close() throws Exception
        {
            session.close();
            CQLTester.closeClientCluster(cluster);
        }
    }

    private static class CustomNettyOptions extends NettyOptions
    {
        private final int numThreads; // zero means use the netty default value

        CustomNettyOptions(int numThreads)
        {
            this.numThreads = numThreads;
        }

        public EventLoopGroup eventLoopGroup(ThreadFactory threadFactory)
        {
            // the driver should use NIO anyway when Netty is shaded
            return new NioEventLoopGroup(numThreads, threadFactory);
        }
    }
}
