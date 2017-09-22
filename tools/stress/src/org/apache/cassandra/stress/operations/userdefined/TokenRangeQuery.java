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

package org.apache.cassandra.stress.operations.userdefined;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.datastax.driver.core.AsyncContinuousPagingResult;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.StressYaml;
import org.apache.cassandra.stress.WorkManager;
import org.apache.cassandra.stress.generate.TokenRangeIterator;
import org.apache.cassandra.stress.report.Timer;
import org.apache.cassandra.stress.settings.SettingsTokenRange;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JavaDriverClient;

public class TokenRangeQuery extends Operation
{
    private final FastThreadLocal<State> currentState = new FastThreadLocal<>();

    private final TableMetadata tableMetadata;
    private final TokenRangeIterator tokenRangeIterator;
    private final String columns;
    private final int pageSize;
    private final int queueSize;
    private final boolean isWarmup;
    private final PrintWriter resultsWriter;
    private final int timeoutSeconds;

    public TokenRangeQuery(Timer timer,
                           StressSettings settings,
                           TableMetadata tableMetadata,
                           TokenRangeIterator tokenRangeIterator,
                           StressYaml.TokenRangeQueryDef def,
                           boolean isWarmup)
    {
        super(timer, settings);
        this.tableMetadata = tableMetadata;
        this.tokenRangeIterator = tokenRangeIterator;
        this.columns = sanitizeColumns(def.columns, tableMetadata);
        this.pageSize = isWarmup ? Math.min(100, def.page_size) : def.page_size;
        this.queueSize = def.queue_size;
        this.isWarmup = isWarmup;
        this.resultsWriter = maybeCreateResultsWriter(settings.tokenRange);
        this.timeoutSeconds = def.timeout_sec;
    }

    private static PrintWriter maybeCreateResultsWriter(SettingsTokenRange settings)
    {
        try
        {
            return settings.saveData ? new PrintWriter(settings.dataFileName, "UTF-8") : null;
        }
        catch (IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * We need to specify the columns by name because we need to add token(partition_keys) in order to count
     * partitions. So if the user specifies '*' then replace it with a list of all columns.
     */
    private static String sanitizeColumns(String columns, TableMetadata tableMetadata)
    {
        if (!columns.equals("*"))
            return columns;

        return String.join(", ", tableMetadata.getColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toList()));
    }

    /**
     * The state of a token range currently being retrieved.
     * Here we store the row iterator to retrieve more pages
     * and we keep track of which partitions have already been retrieved,
     */
    private final static class State
    {
        public final TokenRange tokenRange;
        public final String query;
        public AsyncContinuousPagingResult result;
        public Set<Token> partitions = new HashSet<>();

        public State(TokenRange tokenRange, String query)
        {
            this.tokenRange = tokenRange;
            this.query = query;
        }

        @Override
        public String toString()
        {
            return String.format("[%s, %s]", tokenRange.getStart(), tokenRange.getEnd());
        }
    }

    abstract static class Runner implements RunOp
    {
        int partitionCount;
        int rowCount;

        @Override
        public int partitionCount()
        {
            return partitionCount;
        }

        @Override
        public int rowCount()
        {
            return rowCount;
        }
    }

    private class JavaDriverRun extends Runner
    {
        final JavaDriverClient client;

        private JavaDriverRun(JavaDriverClient client)
        {
            this.client = client;
        }

        public boolean run() throws Exception
        {
            State state = currentState.get();
            if (state == null)
            { // start processing a new token range
                TokenRange range = tokenRangeIterator.next();
                if (range == null)
                    return true; // no more token ranges to process

                state = new State(range, buildQuery(range));
                currentState.set(state);
            }

            if (state.result == null || state.result.isLast())
            {
                SimpleStatement statement = new SimpleStatement(state.query);
                statement.setReadTimeoutMillis((int)TimeUnit.SECONDS.toMillis(timeoutSeconds));
                statement.setRoutingToken(state.tokenRange.getEnd());
                statement.setConsistencyLevel(JavaDriverClient.from(settings.command.consistencyLevel));
                state.result = client.execute(statement, ContinuousPagingOptions.builder()
                                                                            .withMaxEnqueuedPages(queueSize)
                                                                            .withPageSize(pageSize, ContinuousPagingOptions.PageUnit.ROWS)
                                                                            .build()).get(timeoutSeconds, TimeUnit.SECONDS);
            }
            else
            {
                state.result = state.result.nextPage().get(timeoutSeconds, TimeUnit.SECONDS);
            }

            for (Row row : state.result.currentPage())
            {
                rowCount++;

                // this call will only succeed if we've added token(partition keys) to the query
                Token partition = row.getPartitionKeyToken();
                if (!state.partitions.contains(partition))
                {
                    partitionCount += 1;
                    state.partitions.add(partition);
                }

                if (shouldSaveResults())
                    saveRow(row);
            }

            if (shouldSaveResults())
                flushResults();

            if (isWarmup || state.result.isLast())
            { // no more pages to fetch or just warming up, ready to move on to another token range
                if (isWarmup)
                    state.result.cancel();

                currentState.set(null);
            }

            return true;
        }

        private boolean shouldSaveResults()
        {
            return resultsWriter != null && !isWarmup;
        }

        private void flushResults()
        {
            synchronized (resultsWriter)
            {
                resultsWriter.flush();
            }
        }

        private void saveRow(Row row)
        {
            assert resultsWriter != null;
            synchronized (resultsWriter)
            {
                for (ColumnDefinitions.Definition cd : row.getColumnDefinitions())
                {
                    Object value = row.getObject(cd.getName());

                    if (value instanceof String)
                        resultsWriter.print(((String)value).replaceAll("\\p{C}", "?"));
                    else
                        resultsWriter.print(value.toString());

                    resultsWriter.print(',');
                }
                resultsWriter.print(System.lineSeparator());
            }
        }
    }


    private String buildQuery(TokenRange tokenRange)
    {
        Token start = tokenRange.getStart();
        Token end = tokenRange.getEnd();
        List<String> pkColumns = tableMetadata.getPartitionKey().stream().map(ColumnMetadata::getName).collect(Collectors.toList());
        String tokenStatement = String.format("token(%s)", String.join(", ", pkColumns));

        StringBuilder ret = new StringBuilder();
        ret.append("SELECT ");
        ret.append(tokenStatement); // add the token(pk) statement so that we can count partitions
        ret.append(", ");
        ret.append(columns);
        ret.append(" FROM ");
        ret.append(tableMetadata.getKeyspace().getName());
        ret.append(".");
        ret.append(tableMetadata.getName());
        if (start != null || end != null)
            ret.append(" WHERE ");
        if (start != null)
        {
            ret.append(tokenStatement);
            ret.append(" > ");
            ret.append(start.toString());
        }

        if (start != null && end != null)
            ret.append(" AND ");

        if (end != null)
        {
            ret.append(tokenStatement);
            ret.append(" <= ");
            ret.append(end.toString());
        }

        return ret.toString();
    }

    @Override
    public void run(JavaDriverClient client) throws IOException
    {
        timeWithRetry(new JavaDriverRun(client));
    }

    public int ready(WorkManager workManager)
    {
        tokenRangeIterator.update();

        if (tokenRangeIterator.exhausted() && currentState.get() == null)
            return 0;

        int numLeft = workManager.takePermits(1);
        int ret = numLeft > 0 ? 1 : 0;

        if (ret == 0)
        {
            State state = currentState.get();
            if (state != null && state.result != null)
                state.result.cancel(); // be nice to the driver
        }

        return ret;
    }

    public String key()
    {
        State state = currentState.get();
        return state == null ? "-" : state.toString();
    }
}
