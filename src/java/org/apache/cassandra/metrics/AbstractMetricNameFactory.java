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

import javax.annotation.Nullable;

public class AbstractMetricNameFactory implements MetricNameFactory
{
    private final String group;
    private final String type;
    @Nullable
    private final String keyspace;
    @Nullable
    private final String path;
    @Nullable
    private final String scope;

    public AbstractMetricNameFactory(String group, String type)
    {
        this(group, type, null);
    }

    public AbstractMetricNameFactory(String group, String type, String scope)
    {
        this(group, type, null, null, scope);
    }

    public AbstractMetricNameFactory(String group, String type, String keyspace, String path, String scope)
    {
        assert path == null || scope != null : "Can't have a path without a scope";
        assert path == null || keyspace == null : "Can't use both path and keyspace";
        this.group = group;
        this.type = type;
        this.keyspace = keyspace;
        this.path = path;
        this.scope = scope;
    }

    public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
    {
        return createMetricName(group, type, keyspace, path, scope, metricName);
    }

    public static CassandraMetricsRegistry.MetricName createMetricName(String group,
                                                                       String type,
                                                                       String keyspace,
                                                                       String path,
                                                                       String scope,
                                                                       String metricName)
    {
        String metricScope = keyspace == null
                             ? (path == null ? scope : path + '.' + scope)
                             : (scope == null ? keyspace : keyspace +  '.' + scope);
        return new CassandraMetricsRegistry.MetricName(group,
                                                       type,
                                                       metricName,
                                                       metricScope,
                                                       createMBeanName(group, type, keyspace, path, scope, metricName));
    }

    private static String createMBeanName(String group, String type, String keyspace, String path, String scope, String name)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(group);
        builder.append(":type=").append(type);
        if (keyspace != null)
            builder.append(",keyspace=").append(keyspace);
        if (path != null)
            builder.append(",path=").append(path);
        if (scope != null)
            builder.append(",scope=").append(scope);
        if (name.length() > 0)
            builder.append(",name=").append(name);
        return builder.toString();
    }
}
