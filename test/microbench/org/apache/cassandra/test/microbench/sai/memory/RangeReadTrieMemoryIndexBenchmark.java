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

package org.apache.cassandra.test.microbench.sai.memory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.memory.TrieMemoryIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@Fork(1)
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 5, time = 3)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@State(Scope.Thread)
public class RangeReadTrieMemoryIndexBenchmark extends AbstractTrieMemoryIndexBenchmark
{
    private static final int NUMBER_OF_SEARCHES = 1000;
    private static final AbstractBounds<PartitionPosition> ALL_DATA_RANGE = DataRange.allData(Murmur3Partitioner.instance).keyRange();

    @Param({ "1000000" })
    protected int numberOfTerms;

    @Param({ "10", "100", "1000", "10000", "100000"})
    protected  int rangeSize;

    private Random random;
    private Expression[] integerRangeExpressions;

    @Setup(Level.Iteration)
    public void initialiseIndexes()
    {
        initialiseColumnData(numberOfTerms, 1);
        integerIndex = new TrieMemoryIndex(integerContext);

        for (int i = 0; i < numberOfTerms; i++)
        {
            integerIndex.add(partitionKeys[i], Clustering.EMPTY, integerTerms[i]);
        }
        random = new Random(randomSeed);

        integerRangeExpressions = new Expression[NUMBER_OF_SEARCHES];

        for (int i = 0; i < NUMBER_OF_SEARCHES; i++)
        {
            int lowerValue = random.nextInt(numberOfTerms - rangeSize);

            integerRangeExpressions[i] = new Expression(integerContext)
            {{
                operation = Expression.Op.RANGE;
                lower = new Expression.Bound(Int32Type.instance.decompose(lowerValue), Int32Type.instance, true);
                upper = new Expression.Bound(Int32Type.instance.decompose(lowerValue + rangeSize), Int32Type.instance, true);
            }};
        }
    }

    @Benchmark
    public long integerRangeBenchmark()
    {
        long size = 0;
        for (int i = 0; i < NUMBER_OF_SEARCHES; i++)
        {
            integerIndex.search(integerRangeExpressions[i], ALL_DATA_RANGE);
        }
        return size;
    }
}
