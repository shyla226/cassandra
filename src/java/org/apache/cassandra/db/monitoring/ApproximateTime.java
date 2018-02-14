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

package org.apache.cassandra.db.monitoring;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.ParkedThreadsMonitor;

import static org.apache.cassandra.db.monitoring.ApproximateTime.NETTY_COMPATIBLE_START_TIME_NS;
import static org.apache.cassandra.db.monitoring.ApproximateTime.START_TIME_MS;

abstract class ApproximateTimePad0 {long l0,l1,l2,l3,l4,l5,l6,l7,l10,l11,l12,l13,l14,l15,l16;}
abstract class ApproximateTimeNanos extends ApproximateTimePad0
{
    // padded to maximize caching oppurtunity
    // must be volatile as it is updated by one thread and read by another, in JDK9 can opt for an opaque read
    volatile long nanotime = NETTY_COMPATIBLE_START_TIME_NS;
}
abstract class ApproximateTimePad1 extends ApproximateTimeNanos {long l0,l1,l2,l3,l4,l5,l6,l7,l10,l11,l12,l13,l14,l15,l16;}
abstract class ApproximateTimeMillis extends ApproximateTimePad1
{
    // padded to maximize caching oppurtunity
    // must be volatile as it is updated by one thread and read by another, in JDK9 can opt for an opaque read
    volatile long millis = START_TIME_MS;
    volatile long currentTimeMillis = START_TIME_MS;
    long lastUpdateNs;
}
abstract class ApproximateTimePad2 extends ApproximateTimeMillis {long l0,l1,l2,l3,l4,l5,l6,l7,l10,l11,l12,l13,l14,l15,l16;}

/**
 * This is an approximation of System.currentTimeInMillis() and System.nanoTime(), to be used as a faster alternative
 * when we can sacrifice accuracy.
 * <p>
 * The current nanoTime is updated by the Watcher thread of {@link ParkedThreadsMonitor}, which should be
 * of approximately 50 to 100 microseconds. To be on the safe side, we assume an accuracy of roughly 200 microseconds.
 */
public class ApproximateTime extends ApproximateTimePad2
{
    static final long START_TIME_MS;
    static final long START_TIME_NS;
    static final long NETTY_COMPATIBLE_START_TIME_NS;
    static final long NANOS_PER_MS;
    private static final ApproximateTime INSTANCE;

    // HACK ATTACK! we should make that value available more directly in out Netty fork
    static long getStartTime() throws RuntimeException
    {
        try
        {
            Field field = Class.forName("io.netty.util.concurrent.ScheduledFutureTask", true, ClassLoader.getSystemClassLoader())
                               .getDeclaredField("START_TIME");
            field.setAccessible(true);
            return field.getLong(null);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * register class with the {@link ParkedThreadsMonitor}
     */
    static
    {
        START_TIME_MS = System.currentTimeMillis();
        START_TIME_NS = System.nanoTime();
        NETTY_COMPATIBLE_START_TIME_NS = getStartTime();
        NANOS_PER_MS = TimeUnit.MILLISECONDS.toNanos(1);
        INSTANCE = new ApproximateTime();

        ParkedThreadsMonitor.instance.get().addAction(ApproximateTime::tick);
    }

    /**
     * The accuracy when called by the {@link ParkedThreadsMonitor}
     */
    private static final long ACCURACY_MICROS = 200; // see comment in class description

    private ApproximateTime()
    {
        tick(this);
    }
    /**
     * Update the current time. This must be called by the same thread
     */
    public static void tick()
    {
        ApproximateTime instance = INSTANCE;
        tick(instance);
    }

    private static void tick(ApproximateTime instance)
    {
        long now = System.nanoTime();
        instance.nanotime = now;
        if ((now - instance.lastUpdateNs) > NANOS_PER_MS)
        {
            instance.lastUpdateNs = now;
            instance.millis = START_TIME_MS + ((now - START_TIME_NS) / NANOS_PER_MS);
            instance.currentTimeMillis = System.currentTimeMillis();
        }
    }

    /**
     * This approximation reflects monotonic timestamp in millis, which should track System.currentTimeMillis when it is
     * not subject to any wall clock adjustments.
     *
     * @return an approximate time in millis with the accuracy returned by {@link ApproximateTime#accuracy()}.
     */
    public static long millisTime()
    {
        return INSTANCE.millis;
    }

    /**
     * This approximation reflects currentTimeMillis system clock value, which can be affected by human or automated
     * external adjustment (e.g. NTP).
     *
     * @return an approximate time in millis with the accuracy returned by {@link ApproximateTime#accuracy()}.
     */
    public static long currentTimeMillis()
    {
        return INSTANCE.currentTimeMillis;
    }

    /**
     * Note this computes similar to "millisTime() - millisAtStartup". NOT subject to System clock changes.
     *
     * @return an approximate time since startup with the accuracy returned by {@link ApproximateTime#accuracy()}.
     */
    public static long millisSinceStartup()
    {
        return INSTANCE.millis - START_TIME_MS;
    }

    /**
     * @return the approximate time in nanoseconds, to be compared with other values returned by this method
     */
    public static long nanoTime()
    {
        return INSTANCE.nanotime;
    }

    /**
     * @return the approximate time since startup in nanoseconds, to be compared with other values returned by this method
     */
    public static long nanoSinceStartup()
    {
        return INSTANCE.nanotime - NETTY_COMPATIBLE_START_TIME_NS;
    }

    /**
     * NOTE: From https://en.wikipedia.org/wiki/Accuracy_and_precision :
     * "the accuracy of a measurement system is the degree of closeness of measurements of a quantity to that
     * quantity's true value.The precision of a measurement system [...] is the degree to which repeated measurements
     * under unchanged conditions show the same results."
     * "In numerical analysis, accuracy is also the nearness of a calculation to the true value; while precision is
     * the resolution of the representation, typically defined by the number of decimal or binary digits."
     * <p>
     * While the precision is in the respective unit for the method, the accuracy of this caching approach is limited to
     * what the tick frequency and the JVM allows. In particular in the face of STW pauses the cached value can be
     * observed to have an inaccuracy similar to the scale of the JVM pauses. Given that this is not a risk that normal
     * time observation in Java can reliably avert it is not a real issue (we believe).
     *
     * @return the accuracy of the approximate time, see javadoc above.
     */
    public static long accuracy()
    {
        return TimeUnit.MILLISECONDS.convert(ACCURACY_MICROS, TimeUnit.MICROSECONDS);
    }

}
