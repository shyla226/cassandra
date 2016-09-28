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

import java.util.concurrent.TimeUnit;

import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.disposables.EmptyDisposable;


public class ImmediateRxScheduler extends Scheduler
{
    public Worker createWorker()
    {
        return new Worker()
        {
            volatile boolean disposed = false;

            public Disposable schedule(Runnable runnable, long l, TimeUnit timeUnit)
            {
                if (l > 0)
                    throw new AssertionError("Immedite scheduler can only run taks immediately");


                runnable.run();

                return EmptyDisposable.INSTANCE;
            }

            public void dispose()
            {
                disposed = true;
            }

            public boolean isDisposed()
            {
                return disposed;
            }
        };
    }
}
