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
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public abstract class TimeFcts
{
    public static Logger logger = LoggerFactory.getLogger(TimeFcts.class);

    public static Collection<Function> all()
    {
        return ImmutableList.of(now("now", TimeUUIDType.instance),
                                now("currenttimeuuid", TimeUUIDType.instance),
                                now("currenttimestamp", TimestampType.instance),
                                now("currentdate", SimpleDateType.instance),
                                now("currenttime", TimeType.instance),
                                minTimeuuidFct,
                                maxTimeuuidFct,
                                dateOfFct,
                                unixTimestampOfFct,
                                toDate(TimeUUIDType.instance),
                                toDate(TimestampType.instance),
                                toTimestamp(TimeUUIDType.instance),
                                toUnixTimestamp(TimeUUIDType.instance),
                                toUnixTimestamp(TimestampType.instance),
                                toDate(SimpleDateType.instance),
                                toUnixTimestamp(SimpleDateType.instance),
                                toTimestamp(SimpleDateType.instance),
                                FloorTimestampFunction.newInstance(),
                                FloorTimestampFunction.newInstanceWithStartTimeArgument(),
                                FloorTimeUuidFunction.newInstance(),
                                FloorTimeUuidFunction.newInstanceWithStartTimeArgument(),
                                FloorDateFunction.newInstance(),
                                FloorDateFunction.newInstanceWithStartTimeArgument(),
                                floorTime);
    }

    public static final Function now(final String name, final TemporalType<?> type)
    {
        return new NativeScalarFunction(name, type)
        {
            @Override
            public ByteBuffer execute(Arguments arguments)
            {
                return type.now();
            }

            @Override
            public boolean isDeterministic()
            {
                return false;
            }
        };
    };

    public static abstract class TemporalConversionFunction extends NativeScalarFunction
    {
        protected TemporalConversionFunction(String name, AbstractType<?> returnType, AbstractType<?>... argsType)
        {
            super(name, returnType, argsType);
        }

        public ByteBuffer execute(Arguments arguments)
        {
            beforeExecution();

            if (arguments.containsNulls())
                return null;

            return convertArgument(arguments.getAsLong(0));
        }

        protected void beforeExecution()
        {
        }

        protected abstract ByteBuffer convertArgument(long timeInMillis);
    };

    public static final Function minTimeuuidFct = new TemporalConversionFunction("mintimeuuid", TimeUUIDType.instance, TimestampType.instance)
    {
        @Override
        protected ByteBuffer convertArgument(long timeInMillis)
        {
            return UUIDGen.toByteBuffer(UUIDGen.minTimeUUID(timeInMillis));
        }
    };

    public static final Function maxTimeuuidFct = new TemporalConversionFunction("maxtimeuuid", TimeUUIDType.instance, TimestampType.instance)
    {
        @Override
        protected ByteBuffer convertArgument(long timeInMillis)
        {
            return UUIDGen.toByteBuffer(UUIDGen.maxTimeUUID(timeInMillis));
        }
    };

    /**
     * Function that convert a value of <code>TIMEUUID</code> into a value of type <code>TIMESTAMP</code>.
     * @deprecated Replaced by the {@link #timeUuidToTimestamp} function
     */
    public static final NativeScalarFunction dateOfFct = new TemporalConversionFunction("dateof", TimestampType.instance, TimeUUIDType.instance)
    {
        private volatile boolean hasLoggedDeprecationWarning;

        public void beforeExecution()
        {
            if (!hasLoggedDeprecationWarning)
            {
                hasLoggedDeprecationWarning = true;
                logger.warn("The function 'dateof' is deprecated." +
                        " Use the function 'toTimestamp' instead.");
            }
        }

        @Override
        protected ByteBuffer convertArgument(long timeInMillis)
        {
            return ByteBufferUtil.bytes(timeInMillis);
        }
    };

    /**
     * Function that convert a value of type <code>TIMEUUID</code> into an UNIX timestamp.
     * @deprecated Replaced by the {@link #timeUuidToUnixTimestamp} function
     */
    public static final NativeScalarFunction unixTimestampOfFct = new TemporalConversionFunction("unixtimestampof", LongType.instance, TimeUUIDType.instance)
    {
        private volatile boolean hasLoggedDeprecationWarning;

        public void beforeExecution()
        {
            if (!hasLoggedDeprecationWarning)
            {
                hasLoggedDeprecationWarning = true;
                logger.warn("The function 'unixtimestampof' is deprecated." +
                            " Use the function 'toUnixTimestamp' instead.");
            }
        }

        @Override
        protected ByteBuffer convertArgument(long timeInMillis)
        {
            return ByteBufferUtil.bytes(timeInMillis);
        }
    };

   /**
    * Creates a function that convert a value of the specified type into a <code>DATE</code>.
    * @param type the temporal type
    * @return a function that convert a value of the specified type into a <code>DATE</code>.
    */
   public static final NativeScalarFunction toDate(final TemporalType<?> type)
   {
        return new TemporalConversionFunction("todate", SimpleDateType.instance, type)
        {
            @Override
            protected ByteBuffer convertArgument(long timeInMillis)
            {
                return SimpleDateType.instance.fromTimeInMillis(timeInMillis);
            }

            @Override
            public boolean isMonotonic()
            {
                return true;
            }
        };
   }

   /**
    * Creates a function that convert a value of the specified type into a <code>TIMESTAMP</code>.
    * @param type the temporal type
    * @return a function that convert a value of the specified type into a <code>TIMESTAMP</code>.
    */
   public static final NativeScalarFunction toTimestamp(final TemporalType<?> type)
   {
       return new TemporalConversionFunction("totimestamp", TimestampType.instance, type)
       {
           @Override
           protected ByteBuffer convertArgument(long timeInMillis)
           {
               return TimestampType.instance.fromTimeInMillis(timeInMillis);
           }

           @Override
           public boolean isMonotonic()
           {
               return true;
           }
       };
   }

    /**
     * Creates a function that convert a value of the specified type into an UNIX timestamp.
     * @param type the temporal type
     * @return a function that convert a value of the specified type into an UNIX timestamp.
     */
    public static final NativeScalarFunction toUnixTimestamp(final TemporalType<?> type)
    {
        return new TemporalConversionFunction("tounixtimestamp", LongType.instance, type)
        {
            @Override
            protected ByteBuffer convertArgument(long timeInMillis)
            {
                return ByteBufferUtil.bytes(timeInMillis);
            }

            @Override
            public boolean isMonotonic()
            {
                return true;
            }
        };
    }

   /**
    * Function that rounds a timestamp down to the closest multiple of a duration.
    */
    private static abstract class FloorFunction extends NativeScalarFunction
    {
        protected FloorFunction(AbstractType<?> returnType,
                                AbstractType<?>... argsType)
        {
            super("floor", returnType, argsType);
        }

        @Override
        public boolean isPartialApplicationMonotonic(List<ByteBuffer> partialArguments)
        {
            return partialArguments.get(0) == UNRESOLVED
                    && partialArguments.get(1) != UNRESOLVED
                    && (partialArguments.size() == 2 || partialArguments.get(2) != UNRESOLVED);
        }

        public final ByteBuffer execute(Arguments arguments)
        {
            if (arguments.containsNulls())
                return null;

            long time = arguments.getAsLong(0);
            Duration duration = arguments.get(1);
            long startingTime = getStartingTime(arguments);
            validateDuration(duration);

            long floor = Duration.floorTimestamp(time, duration, startingTime);

            return fromTimeInMillis(floor);
        }

        /**
         * Returns the time to use as the starting time.
         *
         * @param arguments the function arguments
         * @return the time to use as the starting time
         */
        private long getStartingTime(Arguments arguments)
        {
            if (arguments.size() == 3)
                return arguments.getAsLong(2);

            return 0L;
        }

        /**
         * Validates that the duration has the correct precision.
         * @param duration the duration to validate.
         */
        protected void validateDuration(Duration duration)
        {
            // Checks that the duration has no data bellow milliseconds. We can do that by checking that the last
            // 6 bits of the number of nanoseconds are all zeros. The compiler will replace the call to
            // numberOfTrailingZeros by a TZCNT instruction.
            if (Long.numberOfTrailingZeros(duration.getNanoseconds()) < 6)
                throw invalidRequest("The floor cannot be computed for the %s duration as precision is below 1 millisecond", duration);
        }

        /**
         * Serializes the specified time.
         *
         * @param timeInMillis the time in milliseconds
         * @return the serialized time
         */
        protected abstract ByteBuffer fromTimeInMillis(long timeInMillis);
    };

   /**
    * Function that rounds a timestamp down to the closest multiple of a duration.
    */
    public static final class FloorTimestampFunction extends FloorFunction
    {
        public static FloorTimestampFunction newInstance()
        {
            return new FloorTimestampFunction(TimestampType.instance,
                                              TimestampType.instance,
                                              DurationType.instance);
        }

        public static FloorTimestampFunction newInstanceWithStartTimeArgument()
        {
            return new FloorTimestampFunction(TimestampType.instance,
                                              TimestampType.instance,
                                              DurationType.instance,
                                              TimestampType.instance);
        }

        private FloorTimestampFunction(AbstractType<?> returnType,
                                       AbstractType<?>... argTypes)
        {
            super(returnType, argTypes);
        }

        protected ByteBuffer fromTimeInMillis(long timeInMillis)
        {
            return TimestampType.instance.fromTimeInMillis(timeInMillis);
        }
    };

    /**
     * Function that rounds a timeUUID down to the closest multiple of a duration.
     */
    public static final class FloorTimeUuidFunction extends FloorFunction
    {
        public static FloorTimeUuidFunction newInstance()
        {
            return new FloorTimeUuidFunction(TimestampType.instance,
                                             TimeUUIDType.instance,
                                             DurationType.instance);
        }

        public static FloorTimeUuidFunction newInstanceWithStartTimeArgument()
        {
            return new FloorTimeUuidFunction(TimestampType.instance,
                                             TimeUUIDType.instance,
                                             DurationType.instance,
                                             TimestampType.instance);
        }

        private FloorTimeUuidFunction(AbstractType<?> returnType,
                                      AbstractType<?>... argTypes)
        {
            super(returnType, argTypes);
        }

        protected ByteBuffer fromTimeInMillis(long timeInMillis)
        {
            return TimestampType.instance.fromTimeInMillis(timeInMillis);
        }
    };

    /**
     * Function that rounds a date down to the closest multiple of a duration.
     */
    public static final class FloorDateFunction extends FloorFunction
    {
        public static FloorDateFunction newInstance()
        {
            return new FloorDateFunction(SimpleDateType.instance,
                                         SimpleDateType.instance,
                                         DurationType.instance);
        }

        public static FloorDateFunction newInstanceWithStartTimeArgument()
        {
            return new FloorDateFunction(SimpleDateType.instance,
                                         SimpleDateType.instance,
                                         DurationType.instance,
                                         SimpleDateType.instance);
        }

        private FloorDateFunction(AbstractType<?> returnType,
                                  AbstractType<?>... argTypes)
        {
            super(returnType, argTypes);
        }

        protected ByteBuffer fromTimeInMillis(long timeInMillis)
        {
            return SimpleDateType.instance.fromTimeInMillis(timeInMillis);
        }

        @Override
        protected void validateDuration(Duration duration)
        {
            // Checks that the duration has no data below days.
            if (duration.getNanoseconds() != 0)
                throw invalidRequest("The floor on %s values cannot be computed for the %s duration as precision is below 1 day",
                                     SimpleDateType.instance.asCQL3Type(), duration);
        }
    };

    /**
     * Function that rounds a time down to the closest multiple of a duration.
     */
    public static final NativeScalarFunction floorTime = new NativeScalarFunction("floor", TimeType.instance, TimeType.instance, DurationType.instance)
    {
        @Override
        public boolean isPartialApplicationMonotonic(List<ByteBuffer> partialParameters)
        {
            return partialParameters.get(0) == UNRESOLVED && partialParameters.get(1) != UNRESOLVED;
        }

        public final ByteBuffer execute(Arguments arguments)
        {
            if (arguments.containsNulls())
                return null;

            long time = arguments.getAsLong(0);
            Duration duration = arguments.get(1);

            long floor = Duration.floorTime(time, duration);

            return TimeType.instance.decompose(floor);
        }
    };
}
