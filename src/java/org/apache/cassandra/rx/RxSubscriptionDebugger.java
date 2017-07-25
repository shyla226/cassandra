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

package org.apache.cassandra.rx;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.plugins.RxJavaPlugins;
import org.apache.cassandra.utils.Pair;

/**
 * Identifies observables that were not subscribed to after 5 seconds
 */
public class RxSubscriptionDebugger
{
    static final Logger logger = LoggerFactory.getLogger(RxSubscriptionDebugger.class);
    static final AtomicBoolean enabled = new AtomicBoolean();
    static final long timeoutInSeconds = 5;
    static final long timeoutInNanos = TimeUnit.SECONDS.toNanos(5);

    private static final ConcurrentMap<Integer, Pair<Long, StackTraceElement[]>> observables = Maps.newConcurrentMap();

    public static void enable()
    {
        if (enabled.compareAndSet(false, true))
        {
            RxJavaPlugins.setOnObservableAssembly(RxSubscriptionDebugger::onCreate);
            RxJavaPlugins.setOnObservableSubscribe(RxSubscriptionDebugger::onSubscribe);

            RxJavaPlugins.setOnCompletableAssembly(RxSubscriptionDebugger::onCreate);
            RxJavaPlugins.setOnCompletableSubscribe(RxSubscriptionDebugger::onSubscribe);

            RxJavaPlugins.setOnSingleAssembly(RxSubscriptionDebugger::onCreate);
            RxJavaPlugins.setOnSingleSubscribe(RxSubscriptionDebugger::onSubscribe);

            RxJavaPlugins.setOnMaybeAssembly(RxSubscriptionDebugger::onCreate);
            RxJavaPlugins.setOnMaybeSubscribe(RxSubscriptionDebugger::onSubscribe);

            RxJavaPlugins.setOnFlowableAssembly(RxSubscriptionDebugger::onCreate);
            RxJavaPlugins.setOnFlowableSubscribe(RxSubscriptionDebugger::onSubscribe);

            startWatcher();
        }
    }

    public static void disable()
    {
        if (enabled.compareAndSet(true, false))
        {
            RxJavaPlugins.setOnCompletableAssembly(null);
            RxJavaPlugins.setOnSingleAssembly(null);
            RxJavaPlugins.setOnMaybeAssembly(null);
            RxJavaPlugins.setOnObservableAssembly(null);
            RxJavaPlugins.setOnFlowableAssembly(null);
            RxJavaPlugins.setOnCompletableSubscribe(null);
            RxJavaPlugins.setOnSingleSubscribe(null);
            RxJavaPlugins.setOnMaybeSubscribe(null);
            RxJavaPlugins.setOnObservableSubscribe(null);
            RxJavaPlugins.setOnFlowableSubscribe(null);
        }
    }

    static <T> T onCreate(T observable)
    {
        observables.putIfAbsent(System.identityHashCode(observable), Pair.create(System.nanoTime(), Thread.currentThread().getStackTrace()));
        return observable;
    }

    static <T,O> O onSubscribe(T observable, O observer)
    {
        observables.remove(System.identityHashCode(observable));
        return observer;
    }

    static void startWatcher()
    {
        Thread watcher = new Thread( () -> {
            while (enabled.get())
            {
                long now = System.nanoTime();
                for (Map.Entry<Integer, Pair<Long, StackTraceElement[]>> entry : observables.entrySet())
                {
                    if (now - entry.getValue().left > timeoutInNanos)
                    {
                        logger.error("No Subscription after {} seconds: {}", timeoutInSeconds, printStackTrace(entry.getValue().right));
                        observables.remove(entry.getKey());
                    }
                }

                Uninterruptibles.sleepUninterruptibly(timeoutInSeconds, TimeUnit.SECONDS);
            }
        });

        watcher.setName("rx-debug-subscription-watcher");
        watcher.start();
    }

    static String printStackTrace(StackTraceElement[] elements)
    {
        StringBuilder sb = new StringBuilder();

        for (int i = 4; i < elements.length; i++)
        {
            StackTraceElement s = elements[i];
            sb.append("\n\tat " + s.getClassName() + "." + s.getMethodName() + "(" + s.getFileName() + ":" + s.getLineNumber() + ")");
        }

        return sb.toString();
    }
}
