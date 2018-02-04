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
package org.apache.cassandra.utils;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTimeoutTask;
import org.apache.cassandra.concurrent.TPCTimer;

public class ExpiringMap<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(ExpiringMap.class);
    private volatile boolean shutdown;

    public static class ExpiringObject<K, V>
    {
        private final TPCTimeoutTask<Pair<K, V>> task;
        private final long timeout;

        private ExpiringObject(TPCTimer timer, K key, V value, long timeout)
        {
            this.task = new TPCTimeoutTask<>(timer, Pair.create(key, value));
            this.timeout = timeout;
        }

        /**
         * The key stored, or null if expired.
         */
        public K getKey()
        {
            Pair<K, V> kv = task.getValue();
            return kv != null ? kv.left : null;
        }

        /**
         * The value stored, or null if expired.
         */
        public V getValue()
        {
            Pair<K, V> kv = task.getValue();
            return kv != null ? kv.right : null;
        }

        /**
         * The timeout when this will expire.
         */
        public long timeoutMillis()
        {
            return timeout;
        }

        void submit(Consumer<Pair<K, V>> action)
        {
            task.submit(action, timeout, TimeUnit.MILLISECONDS);
        }

        void cancel()
        {
            task.dispose();
        }
    }

    private final ConcurrentMap<K, ExpiringObject<K, V>> cache = new ConcurrentHashMap<>();
    private final long defaultExpiration;
    private final Consumer<ExpiringObject<K, V>> postExpireHook;

    /**
     * @param defaultExpiration The default TTL for objects in the cache in milliseconds
     * @param postExpireHook The task to execute at expiration.
     */
    public ExpiringMap(long defaultExpiration, Consumer<ExpiringObject<K, V>> postExpireHook)
    {
        if (defaultExpiration <= 0)
            throw new IllegalArgumentException("Argument specified must be a positive number");

        this.defaultExpiration = defaultExpiration;
        this.postExpireHook = postExpireHook;
    }

    public void reset()
    {
        shutdown = false;
        cache.forEach((k, v) -> v.cancel());
        cache.clear();
    }

    public boolean shutdownBlocking(long timeout)
    {
        shutdown = true;
        Uninterruptibles.sleepUninterruptibly(timeout, TimeUnit.MILLISECONDS);
        return cache.isEmpty();
    }

    public V put(K key, V value)
    {
        return put(key, value, this.defaultExpiration, TPC.bestTPCTimer());
    }

    public V put(K key, V value, long timeout)
    {
        return put(key, value, timeout, TPC.bestTPCTimer());
    }

    public V put(K key, V value, long timeout, TPCTimer timer)
    {
        if (shutdown)
        {
            // StorageProxy isn't equipped to deal with "I'm nominally alive, but I can't send any messages out."
            // So we'll just sit on this thread until the rest of the server shutdown completes.
            //
            // See comments in CustomTThreadPoolServer.serve, CASSANDRA-3335, and CASSANDRA-3727.
            // TPC: If we are on a TPC thread we cannot wait and so we just ignore the request
            if (!TPC.isTPCThread())
            {
                Uninterruptibles.sleepUninterruptibly(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            }
            else
            {
                logger.debug("Received request after messaging service shutdown, ignoring it");
                return null;
            }
        }

        ExpiringObject<K, V> current = new ExpiringObject<>(timer, key, value, timeout);
        ExpiringObject<K, V> previous = cache.put(key, current);
        V previousValue = null;
        if (previous != null)
        {
            previousValue = previous.getValue();
            previous.cancel();
        }

        current.submit(kv ->
        {
            ExpiringObject<K, V> removed = cache.remove(kv.left);
            if (removed != null)
                postExpireHook.accept(removed);
        });

        return (previousValue == null) ? null : previousValue;
    }

    public V get(K key)
    {
        ExpiringObject<K, V> expiring = cache.get(key);
        return expiring != null ? expiring.getValue() : null;
    }

    public V remove(K key)
    {
        ExpiringObject<K, V> removed = cache.remove(key);
        V value = null;
        if (removed != null)
        {
            value = removed.getValue();
            removed.cancel();
        }

        return value;
    }

    public int size()
    {
        return cache.size();
    }

    public boolean containsKey(K key)
    {
        return cache.containsKey(key);
    }

    public boolean isEmpty()
    {
        return cache.isEmpty();
    }

    public Set<K> keySet()
    {
        return cache.keySet();
    }
}
