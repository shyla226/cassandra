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

package org.apache.cassandra.cache;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

import org.apache.cassandra.io.util.AsynchronousChannelProxy;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;

/**
 * Mock classes to inject {@link org.apache.cassandra.io.util.Rebufferer.NotInCacheException}s into the cache
 * rebufferers.
 */
public class ChunkCacheMocks
{
    public static void interceptCache(Random rand)
    {
        ChunkCache.instance.intercept(rf -> new TestRebuffererFactory(rf, rand));
    }

    public static void clearIntercept()
    {
        ChunkCache.instance.enable(true);
    }

    private static final class TestRebuffererFactory implements RebuffererFactory
    {
        final RebuffererFactory wrapped;
        final Random rand;

        TestRebuffererFactory(RebuffererFactory wrapped, Random rand)
        {
            this.wrapped = wrapped;
            this.rand = rand;
        }

        public void close()
        {
            wrapped.close();
        }

        public AsynchronousChannelProxy channel()
        {
            return wrapped.channel();
        }

        public long fileLength()
        {
            return wrapped.fileLength();
        }

        public double getCrcCheckChance()
        {
            return wrapped.getCrcCheckChance();
        }

        public Rebufferer instantiateRebufferer()
        {
            return new TestRebufferer(wrapped.instantiateRebufferer(), rand);
        }
    }

    private static final class TestRebufferer implements Rebufferer
    {
        final Rebufferer wrapped;
        final Random rand;

        TestRebufferer(Rebufferer wrapped, Random rand)
        {
            this.wrapped = wrapped;
            this.rand = rand;
        }

        public void close()
        {
            wrapped.close();
        }

        public AsynchronousChannelProxy channel()
        {
            return wrapped.channel();
        }

        public long fileLength()
        {
            return wrapped.fileLength();
        }

        public double getCrcCheckChance()
        {
            return wrapped.getCrcCheckChance();
        }

        public BufferHolder rebuffer(long position)
        {
            return wrapped.rebuffer(position);
        }

        public void closeReader()
        {
            wrapped.closeReader();
        }

        public BufferHolder rebuffer(long position, ReaderConstraint constraint)
        {
            if (constraint == ReaderConstraint.IN_CACHE_ONLY && rand.nextDouble() < 0.25)
            {
                CompletableFuture<ChunkCache.Buffer> buf = new CompletableFuture<>();

                if (rand.nextDouble() < 0.5)
                    buf.complete(null); // mark ready, so that reload starts immediately
                else
                    ForkJoinPool.commonPool().submit(() -> buf.complete(null)); // switch thread from time to time
                // to avoid stack overflow

                throw new NotInCacheException(buf, wrapped.channel().filePath(), position);
            }
            return wrapped.rebuffer(position, constraint);
        }
    }
}
