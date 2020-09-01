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
package org.apache.cassandra.index.sai.cql.types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.serializers.TimeSerializer;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.index.sai.cql.types.IndexingTypeSupport.NUMBER_OF_VALUES;

public abstract class DataSet<T>
{
    public T[] values;

    public abstract QuerySet querySet();

    public String decorateIndexColumn(String column)
    {
        return column;
    }

    public static abstract class NumericDataSet<T extends Number> extends DataSet<T>
    {
        NumericDataSet(Random random)
        {
            values = emptyValues();
            List<T> list = Arrays.asList(values);
            for (int index = 0; index < values.length; index += 2)
            {
                T value1, value2;
                while (true)
                {
                    value1 = nextValue(random);
                    value1 = random.nextBoolean() ? negate(value1) : abs(value1);
                    value2 = increment(value1);
                    if (!list.contains(value1) && !list.contains(value2))
                        break;
                }
                values[index] = value1;
                values[index + 1] = value2;
            }
            Arrays.sort(values);
        }

        abstract T[] emptyValues();

        abstract T nextValue(Random random);

        abstract T negate(T value);

        abstract T abs(T value);

        abstract T increment(T value);

        public QuerySet querySet()
        {
            return new QuerySet.NumericQuerySet(this);
        }
    }

    public static class IntDataSet extends NumericDataSet<Integer>
    {
        public IntDataSet(Random random)
        {
            super(random);
        }

        @Override
        Integer[] emptyValues()
        {
            return new Integer[NUMBER_OF_VALUES];
        }

        @Override
        Integer nextValue(Random random)
        {
            return random.nextInt();
        }

        @Override
        Integer negate(Integer value)
        {
            return value > 0 ? -value : value;
        }

        @Override
        Integer abs(Integer value)
        {
            return value < 0 ? Math.abs(value) : value;
        }

        @Override
        Integer increment(Integer value)
        {
            return ++value;
        }

        public String toString()
        {
            return "int";
        }
    }

    public static class BigintDataSet extends NumericDataSet<Long>
    {
        public BigintDataSet(Random random)
        {
            super(random);
        }

        @Override
        Long[] emptyValues()
        {
            return new Long[NUMBER_OF_VALUES];
        }

        @Override
        Long nextValue(Random random)
        {
            return random.nextLong();
        }

        @Override
        Long negate(Long value)
        {
            return value > 0 ? -value : value;
        }

        @Override
        Long abs(Long value)
        {
            return value < 0 ? Math.abs(value) : value;
        }

        @Override
        Long increment(Long value)
        {
            return ++value;
        }

        public String toString()
        {
            return "bigint";
        }
    }

    public static class SmallintDataSet extends NumericDataSet<Short>
    {
        public SmallintDataSet(Random random)
        {
            super(random);
        }

        @Override
        Short[] emptyValues()
        {
            return new Short[NUMBER_OF_VALUES];
        }

        @Override
        Short nextValue(Random random)
        {
            return (short)random.nextInt(Short.MAX_VALUE + 1);
        }

        @Override
        Short negate(Short value)
        {
            return value > 0 ? (short)-value : value;
        }

        @Override
        Short abs(Short value)
        {
            return value < 0 ? (short)Math.abs(value) : value;
        }

        @Override
        Short increment(Short value)
        {
            return ++value;
        }

        public String toString()
        {
            return "smallint";
        }
    }

    public static class TinyintDataSet extends NumericDataSet<Byte>
    {
        public TinyintDataSet(Random random)
        {
            super(random);
        }

        @Override
        Byte[] emptyValues()
        {
            return new Byte[NUMBER_OF_VALUES];
        }

        @Override
        Byte nextValue(Random random)
        {
            return (byte)random.nextInt(Byte.MAX_VALUE + 1);
        }

        @Override
        Byte negate(Byte value)
        {
            return value > 0 ? (byte)-value : value;
        }

        @Override
        Byte abs(Byte value)
        {
            return value < 0 ? (byte)Math.abs(value) : value;
        }

        @Override
        Byte increment(Byte value)
        {
            return ++value;
        }

        public String toString()
        {
            return "tinyint";
        }
    }

    public static class VarintDataSet extends NumericDataSet<BigInteger>
    {
        public VarintDataSet(Random random)
        {
            super(random);
        }

        @Override
        BigInteger[] emptyValues()
        {
            return new BigInteger[NUMBER_OF_VALUES];
        }

        @Override
        BigInteger nextValue(Random random)
        {
            return new BigInteger(RandomInts.randomIntBetween(random, 16, 512), random);
        }

        @Override
        BigInteger negate(BigInteger value)
        {
            return value.signum() > 0 ? value.negate() : value;
        }

        @Override
        BigInteger abs(BigInteger value)
        {
            return value.signum() < 0 ? value.abs() : value;
        }

        @Override
        BigInteger increment(BigInteger value)
        {
            return value.add(BigInteger.ONE);
        }

        public String toString()
        {
            return "varint";
        }
    }

    public static class DecimalDataSet extends NumericDataSet<BigDecimal>
    {
        public DecimalDataSet(Random random)
        {
            super(random);
        }

        @Override
        BigDecimal[] emptyValues()
        {
            return new BigDecimal[NUMBER_OF_VALUES];
        }

        @Override
        BigDecimal nextValue(Random random)
        {
            return BigDecimal.valueOf(
                    RandomInts.randomIntBetween(random, -1_000_000, 1_000_000),
                    RandomInts.randomIntBetween(random, -64, 64));
        }

        @Override
        BigDecimal negate(BigDecimal value)
        {
            return value.signum() > 0 ? value.negate() : value;
        }

        @Override
        BigDecimal abs(BigDecimal value)
        {
            return value.signum() < 0 ? value.abs() : value;
        }

        @Override
        BigDecimal increment(BigDecimal value)
        {
            return value.add(BigDecimal.ONE);
        }

        public String toString()
        {
            return "decimal";
        }
    }


    public static class FloatDataSet extends NumericDataSet<Float>
    {
        public FloatDataSet(Random random)
        {
            super(random);
        }

        @Override
        Float[] emptyValues()
        {
            return new Float[NUMBER_OF_VALUES];
        }

        @Override
        Float nextValue(Random random)
        {
            return random.nextFloat();
        }

        @Override
        Float negate(Float value)
        {
            return value > 0 ? -value : value;
        }

        @Override
        Float abs(Float value)
        {
            return value < 0 ? Math.abs(value) : value;
        }

        @Override
        Float increment(Float value)
        {
            return ++value;
        }

        public String toString()
        {
            return "float";
        }
    }

    public static class DoubleDataSet extends NumericDataSet<Double>
    {
        public DoubleDataSet(Random random)
        {
            super(random);
        }

        @Override
        Double[] emptyValues()
        {
            return new Double[NUMBER_OF_VALUES];
        }

        @Override
        Double nextValue(Random random)
        {
            return random.nextDouble();
        }

        @Override
        Double negate(Double value)
        {
            return value > 0 ? -value : value;
        }

        @Override
        Double abs(Double value)
        {
            return value < 0 ? Math.abs(value) : value;
        }

        @Override
        Double increment(Double value)
        {
            return ++value;
        }

        public String toString()
        {
            return "double";
        }
    }

    public static class AsciiDataSet extends DataSet<String>
    {
        public AsciiDataSet(Random random)
        {
            values = new String[NUMBER_OF_VALUES];
            List<String> list = Arrays.asList(values);
            for (int index = 0; index < values.length; index++)
            {
                String value;
                while (true)
                {
                    value = RandomStrings.randomAsciiOfLengthBetween(random, 8, 256);
                    if (!list.contains(value))
                        break;
                }
                values[index] = value;
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.LiteralQuerySet(this);
        }

        public String toString()
        {
            return "ascii";
        }
    }

    public static class TextDataSet extends DataSet<String>
    {
        public TextDataSet(Random random)
        {
            values = new String[NUMBER_OF_VALUES];
            List<String> list = Arrays.asList(values);
            for (int index = 0; index < values.length; index++)
            {
                String value;
                while (true)
                {
                    value = RandomStrings.randomAsciiOfLengthBetween(random , 8, 256);
                    if (!list.contains(value))
                        break;
                }
                values[index] = value;
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.LiteralQuerySet(this);
        }

        public String toString()
        {
            return "text";
        }
    }

    public static class DateDataSet extends DataSet<Integer>
    {
        public DateDataSet(Random random)
        {
            values = new Integer[NUMBER_OF_VALUES];
            List<Integer> list = Arrays.asList(values);
            long min = TimeUnit.DAYS.toMillis(Integer.MIN_VALUE);
            long max = TimeUnit.DAYS.toMillis(Integer.MAX_VALUE);
            long range = max - min;

            for (int index = 0; index < values.length; index++)
            {
                Integer value;
                while (true)
                {
                    value = SimpleDateSerializer.timeInMillisToDay(min + Math.round(random.nextDouble() * range));
                    if (!list.contains(value))
                        break;
                }
                values[index] = value;
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.LiteralQuerySet(this);
        }

        public String toString()
        {
            return "date";
        }
    }

    public static class TimeDataSet extends DataSet<Long>
    {
        public TimeDataSet(Random random)
        {
            values = new Long[NUMBER_OF_VALUES];
            List<Long> list = Arrays.asList(values);
            for (int index = 0; index < values.length; index++)
            {
                Long value;
                while (true)
                {
                    int hours = RandomInts.randomIntBetween(random, 0, 23);
                    int minutes = RandomInts.randomIntBetween(random, 0, 59);
                    int seconds = RandomInts.randomIntBetween(random, 0, 59);
                    long nanos = RandomInts.randomIntBetween(random, 0, 1000000000);
                    value = TimeSerializer.timeStringToLong(String.format("%s:%s:%s.%s", hours, minutes, seconds, nanos));
                    if (!list.contains(value))
                        break;
                }
                values[index] = value;
            }
            Arrays.sort(values);
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.NumericQuerySet(this);
        }

        public String toString()
        {
            return "time";
        }
    }

    public static class TimestampDataSet extends DataSet<Date>
    {
        public TimestampDataSet(Random random)
        {
            values = new Date[NUMBER_OF_VALUES];
            List<Date> list = Arrays.asList(values);
            long min = Instant.EPOCH.getEpochSecond();
            long max = Instant.EPOCH.plus(100 * 365, ChronoUnit.DAYS).getEpochSecond();
            long range = max - min;

            for (int index = 0; index < values.length; index++)
            {
                Date value;
                while (true)
                {
                    value = Date.from(Instant.ofEpochSecond(min + Math.round(random.nextDouble() * range)));
                    if (!list.contains(value))
                        break;
                }
                values[index] = value;
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.LiteralQuerySet(this);
        }

        public String toString()
        {
            return "timestamp";
        }
    }

    public static class UuidDataSet extends DataSet<UUID>
    {
        public UuidDataSet(Random random)
        {
            values = new UUID[NUMBER_OF_VALUES];
            List<UUID> list = Arrays.asList(values);

            for (int index = 0; index < values.length; index++)
            {
                UUID value;
                while (true)
                {
                    value = UUID.randomUUID();
                    if (!list.contains(value))
                        break;
                }
                values[index] = value;
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.LiteralQuerySet(this);
        }

        public String toString()
        {
            return "uuid";
        }
    }

    public static class TimeuuidDataSet extends DataSet<UUID>
    {
        public TimeuuidDataSet(Random random)
        {
            values = new UUID[NUMBER_OF_VALUES];
            List<UUID> list = Arrays.asList(values);

            for (int index = 0; index < values.length; index++)
            {
                UUID value;
                while (true)
                {
                    value = UUIDGen.getTimeUUID();
                    if (!list.contains(value))
                        break;
                }
                values[index] = value;
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.LiteralQuerySet(this);
        }

        public String toString()
        {
            return "timeuuid";
        }
    }

    public static class InetDataSet extends DataSet<InetAddress>
    {
        public InetDataSet(Random random)
        {
            values = new InetAddress[NUMBER_OF_VALUES];
            List<InetAddress> list = Arrays.asList(values);

            for (int index = 0; index < values.length; index++)
            {
                InetAddress value;
                while (true)
                {
                    byte[] bytes;
                    if (random.nextBoolean())
                        bytes = new byte[4];
                    else
                        bytes = new byte[16];
                    random.nextBytes(bytes);
                    try
                    {
                        value = InetAddress.getByAddress(bytes);
                    }
                    catch (UnknownHostException e)
                    {
                        throw new RuntimeException(e);
                    }
                    if (!list.contains(value))
                        break;
                }
                values[index] = value;
            }
            Arrays.sort(values, (o1, o2) -> {
                return TypeUtil.compare(TypeUtil.encode(ByteBuffer.wrap(o1.getAddress()), InetAddressType.instance),
                                        TypeUtil.encode(ByteBuffer.wrap(o2.getAddress()), InetAddressType.instance),
                                        InetAddressType.instance);
            });
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.NumericQuerySet(this);
        }

        public String toString()
        {
            return "inet";
        }
    }
}
