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
package org.apache.cassandra.metrics;

import java.util.concurrent.TimeUnit;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics about latencies
 */
public class LatencyMetrics
{
    /** Latency */
    public final Timer latency;
    /** Total latency in micro sec */
    public final Counter totalLatency;

    protected final MetricNameFactory factory;
    protected final MetricNameFactory aliasFactory;
    protected final String namePrefix;
    private final boolean isComposite;

    /**
     * Create a non-composite LatencyMetrics with given type, and scope. Name prefix for each metric will be empty.
     *
     * @param type Type name
     * @param scope Scope
     */
    public LatencyMetrics(String type, String scope)
    {
        this(type, scope, false);
    }

    /**
     * Create LatencyMetrics with given type, scope and isComposite. Name prefix for each metric will be empty.
     *
     * @param type Type name
     * @param scope Scope
     * @param isComposite whether this metrics are composite, see {@link Composable.Type#COMPOSITE}
     */
    public LatencyMetrics(String type, String scope, boolean isComposite)
    {
        this(type, "", scope, isComposite);
    }

    /**
     * Create LatencyMetrics with given group, type, prefix to append to each metric name, and scope.
     *
     * @param type Type name
     * @param namePrefix Prefix to append to each metric name
     * @param scope Scope of metrics
     * @param isComposite whether these latency metrics are composite, see {@link Composable.Type#COMPOSITE}
     */
    public LatencyMetrics(String type, String namePrefix, String scope, boolean isComposite)
    {
        this(new DefaultNameFactory(type, scope), namePrefix, isComposite);
    }

    /**
     * Create LatencyMetrics with given group, type, prefix to append to each metric name, and scope.
     *
     * @param factory MetricName factory to use
     * @param namePrefix Prefix to append to each metric name
     * @param isComposite whether these latency metrics are composite, see {@link Composable.Type#COMPOSITE}
     */
    public LatencyMetrics(MetricNameFactory factory, String namePrefix, boolean isComposite)
    {
        this(factory, null, namePrefix, isComposite);
    }

    public LatencyMetrics(MetricNameFactory factory, MetricNameFactory aliasFactory, String namePrefix, boolean isComposite)
    {
        this.factory = factory;
        this.aliasFactory = aliasFactory;
        this.namePrefix = namePrefix;
        this.isComposite = isComposite;

        if (aliasFactory == null)
        {
            latency = Metrics.timer(factory.createMetricName(namePrefix + "Latency"), isComposite);
            totalLatency = Metrics.counter(factory.createMetricName(namePrefix + "TotalLatency"), isComposite);
        }
        else
        {
            latency = Metrics.timer(factory.createMetricName(namePrefix + "Latency"), aliasFactory.createMetricName(namePrefix + "Latency"), isComposite);
            totalLatency = Metrics.counter(factory.createMetricName(namePrefix + "TotalLatency"), aliasFactory.createMetricName(namePrefix + "TotalLatency"), isComposite);
        }
    }
    
    /**
     * Create non-composite LatencyMetrics with given group, type, prefix to append to each metric name, and scope.
     * Compose the new latency metrics into the parent metrics.
     *
     * @param factory MetricName factory to use
     * @param namePrefix Prefix to append to each metric name
     * @param parents any amount of parents to replicate updates to
     */
    public LatencyMetrics(MetricNameFactory factory, MetricNameFactory aliasFactory, String namePrefix, LatencyMetrics ... parents)
    {
        this(factory, aliasFactory, namePrefix, false);

        for (LatencyMetrics parent : parents)
            parent.compose(this);
    }

    private void compose(LatencyMetrics child)
    {
        if (!isComposite)
            throw new UnsupportedOperationException("Non composite latency metrics cannot be composed with another metric");

        latency.compose(child.latency);
        totalLatency.compose(child.totalLatency);
    }

    /** takes nanoseconds **/
    public void addNano(long nanos)
    {
        // convert to microseconds. 1 millionth
        latency.update(nanos, TimeUnit.NANOSECONDS);
        totalLatency.inc(nanos / 1000);
    }

    public void release()
    {
        if (aliasFactory == null)
        {
            Metrics.remove(factory.createMetricName(namePrefix + "Latency"));
            Metrics.remove(factory.createMetricName(namePrefix + "TotalLatency"));
        }
        else
        {
            Metrics.remove(factory.createMetricName(namePrefix + "Latency"), aliasFactory.createMetricName(namePrefix + "Latency"));
            Metrics.remove(factory.createMetricName(namePrefix + "TotalLatency"), aliasFactory.createMetricName(namePrefix + "TotalLatency"));
        }
    }
}
