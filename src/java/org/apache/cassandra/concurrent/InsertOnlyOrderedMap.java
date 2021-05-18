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
package org.apache.cassandra.concurrent;

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

public interface InsertOnlyOrderedMap<K extends Comparable<? super K>, V>
{

    public V get(K key);
    public V putIfAbsent(K key, V value);
    public int size();
    public Iterable<Map.Entry<K, V>> range(K lb, K ub);
    public Iterable<Map.Entry<K, V>> range(K lb, boolean lbInclusive, K ub, boolean ubInclusive);

    public static final class Adapter<K extends Comparable<? super K>, V> implements InsertOnlyOrderedMap<K, V>
    {
        final ConcurrentNavigableMap<K, V> wrapped;
        public Adapter(ConcurrentNavigableMap<K, V> wrapped)
        {
            this.wrapped = wrapped;
        }

        public Iterable<Map.Entry<K, V>> range(K lb, K ub) {
            return range(lb, true, ub, true);
        }

        public Iterable<Map.Entry<K, V>> range(K lb, boolean lbInclusive, K ub, boolean ubInclusive)
        {
            if (lb == null || ub == null)
            {
                if (lb == null && ub == null)
                    return wrapped.entrySet();
                else if (lb == null)
                    return wrapped.headMap(ub, ubInclusive).entrySet();
                else
                    return wrapped.tailMap(lb, lbInclusive).entrySet();
            }
            return wrapped.subMap(lb, lbInclusive, ub, ubInclusive).entrySet();
        }

        public int size()
        {
            return wrapped.size();
        }

        public V putIfAbsent(K key, V value)
        {
            return wrapped.putIfAbsent(key, value);
        }

        public V get(K key)
        {
            return wrapped.get(key);
        }
    }

}
