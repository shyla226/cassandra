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

import io.reactivex.disposables.Disposable;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTimer;

public class ExpiringMap<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(ExpiringMap.class);
    private volatile boolean shutdown;

    public static class ExpiringObject<T>
    {
        private final T value;
        private final long timeout;
        private volatile Disposable disposable;

        private ExpiringObject(T value, long timeout)
        {
            assert value != null;
            this.value = value;
            this.timeout = timeout;
        }

        /**
         * The value stored.
         */
        public T get()
        {
            return value;
        }

        /**
         * The timeout when this will expire.
         */
        public long timeoutMillis()
        {
            return timeout;
        }

        void cancel()
        {
            if (disposable != null)
                disposable.dispose();
        }

        void makeCancellable(Disposable disposable)
        {
            this.disposable = disposable;
        }
    }

    private final ConcurrentMap<K, ExpiringObject<V>> cache = new ConcurrentHashMap<>();
    private final long defaultExpiration;
    private final Consumer<Pair<K, ExpiringObject<V>>> postExpireHook;

    /**
     * @param defaultExpiration The default TTL for objects in the cache in milliseconds
     * @param postExpireHook The task to execute at expiration.
     */
    public ExpiringMap(long defaultExpiration, Consumer<Pair<K, ExpiringObject<V>>> postExpireHook)
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

        ExpiringObject<V> current = new ExpiringObject<>(value, timeout);
        ExpiringObject<V> previous = cache.put(key, current);
        if (previous != null)
            previous.cancel();

        current.makeCancellable(timer.onTimeout(() ->
        {
            ExpiringObject<V> removed = cache.remove(key);
            if (removed != null)
                postExpireHook.accept(Pair.create(key, removed));
        }, timeout, TimeUnit.MILLISECONDS));

        return (previous == null) ? null : previous.value;
    }

    public ExpiringObject<V> get(K key)
    {
        return cache.get(key);
    }

    public ExpiringObject<V> remove(K key)
    {
        ExpiringObject<V> removed = cache.remove(key);
        if (removed != null)
            removed.cancel();

        return removed;
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
