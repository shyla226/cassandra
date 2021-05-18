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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.utils.HashComparable;

public class LongNonBlockingHashOrderedMapTest
{

    // aim to use ~50% of memory available to the test; run the test with a lot of memory for best results
    private static final ExecutorService EXEC = Executors.newCachedThreadPool();
    private static final int TOTAL_KEYS = (int) (Runtime.getRuntime().maxMemory() / 160);

    @Test
    public void testAllOverlappingHashes() throws InterruptedException
    {
        doTest(1f, 1);
        doTest(1f, 10);
    }

    @Test
    public void testSomeOverlappingHashes() throws InterruptedException
    {
        doTest(0.5f, 1);
        doTest(0.5f, 10);
    }

    @Test
    public void testNonOverlappingInserts() throws InterruptedException
    {
        doTest(0f, 1);
        doTest(0f, 10);
    }

    void doTest(float overlap, int collisions) throws InterruptedException
    {
        int threads = Math.max(4, Runtime.getRuntime().availableProcessors() / 2);
        for (int shift = 0 ; shift <= 2 ; shift++)
        {
            doTest(overlap, collisions, threads, 32768, 2);
            doTest(overlap, collisions, threads, 4096, 2);
            doTest(overlap, collisions, threads, 512, 2);
            doTest(overlap, collisions, threads, 64, 2);
            doTest(overlap, collisions, threads, 4, 2);
            threads *= 2;
        }
    }

    void doTest(float overlap, int collisions, int threads, int batches, int repeats) throws InterruptedException
    {
        int writers = threads / 2;
        int readers = threads / 2;
        for (int repeat = 0 ; repeat < repeats ; repeat += 1)
        {
            final AtomicBoolean failure = new AtomicBoolean();
            final List<WriterKeyset> results = new ArrayList<>();
            final NonBlockingHashOrderedMap<Key, Integer> map = new NonBlockingHashOrderedMap<>(Collections.emptyList());
            final Keys keys = new Keys(overlap, writers, collisions, batches);
            final ReadySync sync = new ReadySync(threads);
            System.out.printf("Running (hashes=%d,overlap=%.2f,collisions=%d,threads=%d,slices=%d); %d of %d\n",
                              keys.hashesPerWorker, overlap, collisions, threads, batches, repeat + 1, repeats);
            {
                for (int i = 0 ; i < writers ; i++)
                    results.add(new Writer(map, keys, sync, failure).keys);
                for (int i = 0 ; i < readers ; i++)
                    new Reader(map, sync, keys, failure);
            }
            keys.advance();
            sync.start();
            do
            {
                sync.doRound();
                Assert.assertTrue(map.valid());
                keys.advance();
            } while (!failure.get() && !keys.done());
            sync.endCoordinator();
            Assert.assertFalse(failure.get());
            Iterator<Insert> src = WriterKeyset.merge(results).iterator();
            Iterator<Map.Entry<Key, Integer>> trg = map.range(null, null).iterator();
            while (src.hasNext() && trg.hasNext())
            {
                Insert srcv = src.next();
                Map.Entry<Key, Integer> trgv = trg.next();
                Assert.assertEquals(srcv.key.key, trgv.getKey().key);
                Assert.assertEquals(srcv.writer, trgv.getValue());
            }
            Assert.assertFalse(src.hasNext());
            Assert.assertFalse(trg.hasNext());
        }
    }

    private static final class Writer implements Runnable
    {
        final NonBlockingHashOrderedMap<Key, Integer> map;
        final Integer id;
        final ReadySync ready;
        final WriterKeyset keys;
        final AtomicBoolean failure;

        private Writer(NonBlockingHashOrderedMap<Key, Integer> map, Keys keys, ReadySync ready, AtomicBoolean failure)
        {
            this.map = map;
            this.keys = keys.nextWriter();
            this.ready = ready;
            this.id = this.keys.id;
            this.failure = failure;
            EXEC.execute(this);
        }

        public void run()
        {
            while (!failure.get() && !keys.done())
            {
                ready.waitForNextRoundStart();
                int c = 0;
                while (!keys.doneRound())
                {
                    Key key = keys.next();
                    if (map.putIfAbsent(key, id) == null)
                    {
                        keys.inserted(key);
                        if ((++c & 16) == 0)
                        {
                            // periodically check the data we contain to ensure it is correct
                            boolean found = false;
                            for (Map.Entry<Key, Integer> e : map.range(key, key))
                            {
                                if (e.getKey() != key || e.getValue() != id)
                                {
                                    System.err.println("Range query returned more than expected key");
                                    failure.set(true);
                                }
                                else
                                {
                                    found = true;
                                }
                            }
                            found &= map.get(key) == id;

                            if (!found)
                            {
                                System.err.println("failed to query just inserted key");
                                failure.set(true);
                            }
                        }
                    }
                }
            }
            ready.endWorker();
        }
    }

    private static final class Reader implements Runnable
    {
        final NonBlockingHashOrderedMap<Key, Integer> map;
        final ReadySync ready;
        final Keys keys;
        final Random random = new Random();
        final AtomicBoolean failure;

        private Reader(NonBlockingHashOrderedMap<Key, Integer> map, ReadySync ready, Keys keys, AtomicBoolean failure)
        {
            this.map = map;
            this.ready = ready;
            this.keys = keys;
            this.failure = failure;
            EXEC.execute(this);
        }

        public void run()
        {
            while (!failure.get() && !keys.done())
            {
                ready.waitForNextRoundStart();
                if (keys.minRound < 0)
                    continue;
                while (!keys.done() && !failure.get() && ready.majorityRunning())
                {
                    // decide on the scale of the size of range to query
                    int lengthBits = random.nextInt(30);
                    // translate this into an actual size of range
                    long length = ((long) random.nextInt(1 << lengthBits)) << 32;
                    // pick a start for the range, and an end that is length from start
                    long start = (long) random.nextInt() << 32;
                    long end = start + length;
                    if (end < start)
                        end = Long.MAX_VALUE;

                    KeyRangeValidator validator = new KeyRangeValidator(start, end, keys);
                    for (Map.Entry<Key, Integer> e : map.range(new Key(start), new Key(end)))
                    {
                        if (!validator.accept(e.getKey()))
                        {
                            failure.set(true);
                            break;
                        }
                    }
                }
            }
            ready.endWorker();
        }
    }

    // a key
    private static final class Key implements HashComparable<Key>
    {
        final long key;
        private Key(long key)
        {
            this.key = key;
        }

        // we only use top 32 bits so we can introduce collisions
        public long comparableHashCode()
        {
            return key & (-1L << 32);
        }

        public int compareTo(Key that)
        {
            return Long.compare(this.key, that.key);
        }
        public String toString()
        {
            return Long.toString(key) + "@" + System.identityHashCode(this);
        }

        public boolean equals(Object that)
        {
            return that instanceof Key && ((Key) that).key == key;
        }
    }

    // manages the keys over which each writer operates; each round we advance the key space by a portion
    // and each writer operates over a space that overlaps other writers by some amount
    private static final class Keys
    {
        final int writers;
        final int collisions;
        final int roundCount;
        final int hashes;
        final int hashesPerWorker;
        final List<WriterKeyset> generators = new ArrayList<>();
        final int hashInterval;
        final float overlap;

        int nextId;
        int nextStart;
        int nextRound = 0, minRound = -1, maxRound = -1;

        private Keys(float overlap, int writers, int collisions, int roundCount)
        {
            this.writers = writers;
            this.collisions = collisions;
            this.hashes = TOTAL_KEYS / collisions;
            this.hashesPerWorker = (int) (hashes / (Math.max(1f, (1f - overlap) * writers)));
            this.roundCount = roundCount;
            this.hashInterval = 2 * (Integer.MAX_VALUE / hashes);
            this.overlap = overlap;
        }

        // called once by each writer to construct its state generator, i.e. called same number as "writers" value
        WriterKeyset nextWriter()
        {
            int start = nextStart;
            int end = start + hashesPerWorker;
            nextStart = (int) (end - (hashesPerWorker * overlap));
            WriterKeyset generator = new WriterKeyset(nextId++, start, end, this);
            generators.add(generator);
            return generator;
        }

        void advance()
        {
            advance((int) Math.sqrt(roundCount));
        }

        void advance(int rounds)
        {
            minRound = maxRound;
            maxRound = Math.min(roundCount - 1, maxRound + rounds);
            final List<Key> keys = new ArrayList<>();
            while (nextRound <= maxRound)
            {
                for (int i = nextRound++ ; i < hashes; i += roundCount)
                {
                    for (int j = 0 ; j < collisions ; j++)
                    {
                        Key key = new Key(key(i, j));
                        keys.add(key);
                        assert index(key.key) == i;
                    }
                }
            }
            for (WriterKeyset generator : generators)
                generator.populate(keys);
        }

        boolean done()
        {
            return minRound == roundCount - 1;
        }

        private int hash(int index)
        {
            return Integer.MIN_VALUE + (index * hashInterval);
        }

        private long key(int index, int value)
        {
            if (index > hashes)
                return Long.MAX_VALUE;
            long hash = hash(index);
            return (hash << 32) | value;
        }

        private int index(long key)
        {
            return (int) ((((key >>> 32) - Integer.MIN_VALUE) & 0xFFFFFFFFL) / hashInterval);
        }

        private int collision(long key)
        {
            return ((int) key) & Integer.MAX_VALUE;
        }

        // the value must be present
        private boolean inRequiredRange(int index)
        {
            return index % roundCount <= minRound;
        }

        // the value may be present, or may not be present
        private boolean inOptionalRange(int index)
        {
            int round = index % roundCount;
            return round > minRound && round <= maxRound;
        }

        private int priorRequiredIndex(int index)
        {
            int round = index % roundCount;
            return index - (round - minRound);
        }

        private int nextRequiredIndex(int index)
        {
            int round = index % roundCount;
            return index + (roundCount - round);
        }
    }

    private static final class KeyRangeValidator
    {
        final long end;
        final Keys keys;
        int index, collision;
        long prev = Long.MIN_VALUE;

        private KeyRangeValidator(long start, long end, Keys keys)
        {
            this.end = end;
            this.keys = keys;
            this.index = keys.index(start);
            if (!keys.inRequiredRange(index))
                index = keys.priorRequiredIndex(index);
            while (index < keys.hashes && keys.key(index, collision) < start)
                advance();
        }

        void advance()
        {
            if (++collision == keys.collisions)
            {
                collision = 0;
                index++;
                if (!keys.inRequiredRange(index))
                    index = keys.nextRequiredIndex(index);
            }
        }

        public boolean accept(Key key)
        {
            if (key.key <= prev)
            {
                System.out.println("Out of order key");
                return false;
            }
            if (key.key > end)
            {
                System.out.println("Out of range key");
                return false;
            }
            prev = key.key;
            if (key.key == keys.key(index, collision))
            {
                advance();
                return true;
            }
            if (key.key < keys.key(index, collision) && keys.inOptionalRange(keys.index(key.key)))
                return true;
            System.out.println("Incorrect key order");
            return false;
        }
    }

    // a set of keys to be inserted by a writer.
    // defines its total permitted range, and its currently allocated portion thereof
    private static final class WriterKeyset
    {
        final Integer id;
        final long start, end;
        final Keys parent;

        Iterator<Key> insert;
        final BitSet inserted = new BitSet(TOTAL_KEYS);

        private WriterKeyset(Integer id, long start, long end, Keys parent)
        {
            this.id = id;
            this.start = start;
            this.end = end;
            this.parent = parent;
        }

        Key next()
        {
            return insert.next();
        }

        void inserted(Key key)
        {
            int index = parent.index(key.key);
            int collision = parent.collision(key.key);
            inserted.set(index * parent.collisions + collision);
        }

        void populate(List<Key> candidates)
        {
            List<Key> keys = new ArrayList<>();
            for (Key key : candidates)
                if (contains(key))
                    keys.add(key);
            shuffle(keys);
            this.insert = keys.iterator();
        }

        boolean done()
        {
            return parent.done();
        }

        boolean doneRound()
        {
            return !insert.hasNext();
        }

        private Iterator<Key> inserted()
        {
            return new Iterator<Key>()
            {
                int next = -1;
                {
                    advance();
                }

                private void advance()
                {
                    next++;
                    while (next < inserted.size() && !inserted.get(next))
                        next++;
                }

                public boolean hasNext()
                {
                    return next < inserted.size();
                }

                public Key next()
                {
                    Key key = new Key(parent.key(next / parent.collisions, next % parent.collisions));
                    advance();
                    return key;
                }

                public void remove()
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        class Merge implements Comparable<Merge>
        {
            final Integer id = WriterKeyset.this.id;
            Key current;
            Iterator<Key> keys = WriterKeyset.this.inserted();

            public boolean next()
            {
                if (!keys.hasNext())
                    return false;
                current = keys.next();
                return true;
            }

            public int compareTo(Merge that)
            {
                return this.current.compareTo(that.current);
            }
        }

        private Merge merge()
        {
            return new Merge();
        }

        private static Iterable<Insert> merge(final List<WriterKeyset> results)
        {
            return new Iterable<Insert>()
            {
                public Iterator<Insert> iterator()
                {
                    return new Iterator<Insert>()
                    {
                        final PriorityQueue<Merge> next = new PriorityQueue<>();
                        {
                            for (WriterKeyset result : results)
                            {
                                Merge merge = result.merge();
                                if (merge.next())
                                    next.add(merge);
                            }
                        }

                        public boolean hasNext()
                        {
                            return !next.isEmpty();
                        }

                        public Insert next()
                        {
                            Merge merge = next.poll();
                            Insert cur = new Insert(merge.current, merge.id);
                            if (merge.next())
                                next.add(merge);
                            return cur;
                        }
                        public void remove()
                        {
                        }
                    };
                }
            };
        }

        private boolean contains(Key key)
        {
            int idx = parent.index(key.key);
            return idx >= start && idx < end;
        }

        private static void shuffle(List<Key> keys)
        {
            // not a perfect shuffle, but good enough
            Random random = ThreadLocalRandom.current();
            for (int i = 0 ; i < keys.size() ; i++)
            {
                int offset = random.nextInt(keys.size() - i);
                Key tmp = keys.get(i + offset);
                keys.set(i + offset, keys.get(i));
                keys.set(i, tmp);
            }
        }
    }

    // a key with associated writer that succeeded in inserting it
    private static final class Insert implements Comparable<Insert>
    {
        final Key key;
        final Integer writer;

        private Insert(Key key, Integer writer)
        {
            this.key = key;
            this.writer = writer;
        }

        public int compareTo(Insert that)
        {
            int c = this.key.compareTo(that.key);
            if (c != 0)
                return c;
            return this.writer.compareTo(that.writer);
        }

        public String toString()
        {
            return "(" + key.key + "," + writer + ")";
        }
    }

    // keeps the threads busy spinning until all are ready to rock
    private static final class ReadySync
    {
        final int workers;
        final Random random = new Random();
        final CyclicBarrier barrier;
        final AtomicInteger readyCounter = new AtomicInteger();
        final CountDownLatch done;

        private ReadySync(int workers)
        {
            this.barrier = new CyclicBarrier(workers + 1);
            this.workers = workers;
            this.done = new CountDownLatch(workers);
        }

        void waitForNextRoundStart()
        {
            await();
            int prev = readyCounter.get();
            await();
            readyCounter.incrementAndGet();
            while (readyCounter.get() - prev != workers)
                random.nextInt();
        }

        void start()
        {
            await();
        }

        void endCoordinator()
        {
            await();
            Uninterruptibles.awaitUninterruptibly(done);
        }

        void endWorker()
        {
            done.countDown();
        }

        void doRound()
        {
            await();
            await();
        }

        boolean majorityRunning()
        {
            return barrier.getNumberWaiting() < workers / 2;
        }

        private void await()
        {
            try
            {
                barrier.await();
            }
            catch (InterruptedException | BrokenBarrierException ie)
            {
                throw new IllegalStateException();
            }
        }
    }
}
