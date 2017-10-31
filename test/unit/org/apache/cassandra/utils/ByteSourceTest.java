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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import com.datastax.shaded.netty.util.internal.ThreadLocalRandom;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;

import static org.junit.Assert.assertEquals;

public class ByteSourceTest
{
    String[] testStrings = new String[] { "", "\0", "\0\0", "\001", "A\0\0B", "A\0B\0", "0", "0\0", "00", "1", "\377" };
    Integer[] testInts = new Integer[] { null, Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -256, -255, -128, -127, -1, 0, 1, 127, 128, 255, 256, Integer.MAX_VALUE - 1, Integer.MAX_VALUE };
    Byte[] testBytes = new Byte[] { -128, -127, -1, 0, 1, 127 };
    Short[] testShorts = new Short[] { Short.MIN_VALUE, Short.MIN_VALUE + 1, -256, -255, -128, -127, -1, 0, 1, 127, 128, 255, 256, Short.MAX_VALUE - 1, Short.MAX_VALUE };
    Long[] testLongs = new Long[] { null, Long.MIN_VALUE, Long.MIN_VALUE + 1, Integer.MIN_VALUE - 1L, -256L, -255L, -128L, -127L, -1L, 0L, 1L, 127L, 128L, 255L, 256L, Integer.MAX_VALUE + 1L, Long.MAX_VALUE - 1, Long.MAX_VALUE };
    Double[] testDoubles = new Double[] { null, Double.NEGATIVE_INFINITY, -Double.MAX_VALUE, -1e+200, -1e3, -1e0, -1e-3, -1e-200, -Double.MIN_VALUE, -0.0, 0.0, Double.MIN_VALUE, 1e-200, 1e-3, 1e0, 1e3, 1e+200, Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NaN };
    Float[] testFloats = new Float[] { null, Float.NEGATIVE_INFINITY, -Float.MAX_VALUE, -1e+30f, -1e3f, -1e0f, -1e-3f, -1e-30f, -Float.MIN_VALUE, -0.0f, 0.0f, Float.MIN_VALUE, 1e-30f, 1e-3f, 1e0f, 1e3f, 1e+30f, Float.MAX_VALUE, Float.POSITIVE_INFINITY, Float.NaN };
    Boolean[] testBools = new Boolean[] { null, false, true };
    UUID[] testUUIDs = new UUID[] { null, UUIDGen.getTimeUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            UUIDGen.getTimeUUID(123, 234), UUIDGen.getTimeUUID(123, 234), UUIDGen.getTimeUUID(123),
            UUID.fromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8"), UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8"), UUID.fromString("e902893a-9d22-3c7e-a7b8-d6e313b71d9f"), UUID.fromString("74738ff5-5367-5958-9aee-98fffdcd1876")};
    // Instant.MIN/MAX fail Date.from.
    Date[] testDates = new Date[] { null, Date.from(Instant.ofEpochSecond(Integer.MIN_VALUE)), Date.from(Instant.ofEpochSecond(Short.MIN_VALUE)), Date.from(Instant.ofEpochMilli(-2000)), Date.from(Instant.EPOCH), Date.from(Instant.ofEpochMilli(2000)), Date.from(Instant.ofEpochSecond(Integer.MAX_VALUE)), Date.from(Instant.now()) };
    BigInteger[] testBigInts;

    {
        Set<BigInteger> bigs = new TreeSet<>();
        for (Long l : testLongs)
            if (l != null)
                bigs.add(BigInteger.valueOf(l));
        for (int i = 0; i < 11; ++i)
        {
            bigs.add(BigInteger.valueOf(i));
            bigs.add(BigInteger.valueOf(-i));

            bigs.add(BigInteger.valueOf((1L << 4*i) - 1));
            bigs.add(BigInteger.valueOf((1L << 4*i)));
            bigs.add(BigInteger.valueOf(-(1L << 4*i) - 1));
            bigs.add(BigInteger.valueOf(-(1L << 4*i)));
            String p = exp10(i);
            bigs.add(new BigInteger(p));
            bigs.add(new BigInteger("-" + p));
            p = exp10(1 << i);
            bigs.add(new BigInteger(p));
            bigs.add(new BigInteger("-" + p));

            BigInteger base = BigInteger.ONE.shiftLeft(512 * i);
            bigs.add(base);
            bigs.add(base.add(BigInteger.ONE));
            bigs.add(base.subtract(BigInteger.ONE));
            base = base.negate();
            bigs.add(base);
            bigs.add(base.add(BigInteger.ONE));
            bigs.add(base.subtract(BigInteger.ONE));
        }
        testBigInts = bigs.toArray(new BigInteger[0]);
    }
    BigDecimal[] testBigDecimals;
    {
        String vals = "0, 1, 1.1, 21, 98.9, 99, 99.9, 100, 100.1, 101, 331, 0.4, 0.07, 0.0700, 0.005, 6e4, 7e200, 6e-300, 8.1e2000, 8.1e-2000, 9e2000000000, 123456789012.34567890e-1000000000, 123456.78901234, 1.0000, 1234.56789012e2";
        List<BigDecimal> decs = new ArrayList<>();
        for (String s : vals.split(", "))
        {
            decs.add(new BigDecimal(s));
            decs.add(new BigDecimal("-" + s));
        }
        testBigDecimals = decs.toArray(new BigDecimal[0]);
    }
    
    static String exp10(int pow)
    {
        StringBuilder builder = new StringBuilder();
        builder.append('1');
        for (int i=0; i<pow; ++i)
            builder.append('0');
        return builder.toString();
    }
    
    Object[][] testValues = new Object[][] { testStrings, testInts, testBools, testDoubles, testBigInts, testBigDecimals };
    AbstractType[] testTypes = new AbstractType[] { AsciiType.instance, Int32Type.instance, BooleanType.instance, DoubleType.instance, IntegerType.instance, DecimalType.instance };
    
    @Test
    public void testStringsAscii()
    {
        testType(AsciiType.instance, testStrings);
    }

    @Test
    public void testStringsUTF8()
    {
        testType(UTF8Type.instance, testStrings);
    }

    @Test
    public void testBooleans()
    {
        testType(BooleanType.instance, testBools);
    }

    @Test
    public void testInts()
    {
        testType(Int32Type.instance, testInts);
        testDirect(x -> ByteSource.of(x), Integer::compare, testInts);
    }

    @Test
    public void randomTestInts()
    {
        Random rand = new Random();
        for (int i=0; i<10000; ++i)
        {
            int i1 = rand.nextInt();
            int i2 = rand.nextInt();
            assertComparesSame(Int32Type.instance, i1, i2);
        }
        
    }

    @Test
    public void testLongs()
    {
        testType(LongType.instance, testLongs);
        testDirect(x -> ByteSource.of(x), Long::compare, testLongs);
    }

    @Test
    public void testShorts()
    {
        testType(ShortType.instance, testShorts);
    }

    @Test
    public void testBytes()
    {
        testType(ByteType.instance, testBytes);
    }

    @Test
    public void testDoubles()
    {
        testType(DoubleType.instance, testDoubles);
    }

    @Test
    public void testFloats()
    {
        testType(FloatType.instance, testFloats);
    }

    @Test
    public void testBigInts()
    {
        testType(IntegerType.instance, testBigInts);
    }

    @Test
    public void testBigDecimals()
    {
        testType(DecimalType.instance, testBigDecimals);
    }

    @Test
    public void testBigDecimalInCombination()
    {
        BigDecimal b1 = new BigDecimal("123456.78901201");
        BigDecimal b2 = new BigDecimal("123456.789012");
        Boolean b = false;

        assertClusteringPairComparesSame(DecimalType.instance, BooleanType.instance, b1, b, b2, b);
        assertClusteringPairComparesSame(BooleanType.instance, DecimalType.instance, b, b1, b, b2);

        b1 = b1.negate();
        b2 = b2.negate();

        assertClusteringPairComparesSame(DecimalType.instance, BooleanType.instance, b1, b, b2, b);
        assertClusteringPairComparesSame(BooleanType.instance, DecimalType.instance, b, b1, b, b2);

        b1 = new BigDecimal("-123456.78901289");
        b2 = new BigDecimal("-123456.789012");

        assertClusteringPairComparesSame(DecimalType.instance, BooleanType.instance, b1, b, b2, b);
        assertClusteringPairComparesSame(BooleanType.instance, DecimalType.instance, b, b1, b, b2);

        b1 = new BigDecimal("1");
        b2 = new BigDecimal("1.1");

        assertClusteringPairComparesSame(DecimalType.instance, BooleanType.instance, b1, b, b2, b);
        assertClusteringPairComparesSame(BooleanType.instance, DecimalType.instance, b, b1, b, b2);

        b1 = b1.negate();
        b2 = b2.negate();

        assertClusteringPairComparesSame(DecimalType.instance, BooleanType.instance, b1, b, b2, b);
        assertClusteringPairComparesSame(BooleanType.instance, DecimalType.instance, b, b1, b, b2);
    }
    
    @Test
    public void testUUIDs()
    {
        testType(UUIDType.instance, testUUIDs);
    }
    
    @Test
    public void testTimeUUIDs()
    {
        testType(TimeUUIDType.instance, Arrays.stream(testUUIDs).filter(x -> x == null || x.version() == 1).toArray());
    }
    
    @Test
    public void testLexicalUUIDs()
    {
        testType(LexicalUUIDType.instance, testUUIDs);
    }
    
    @Test
    public void testSimpleDate()
    {
        testType(SimpleDateType.instance, Arrays.stream(testInts).filter(x -> x != null).toArray());
    }

    @Test
    public void testTimeType()
    {
        testType(TimeType.instance, Arrays.stream(testLongs).filter(x -> x != null && x >= 0 && x <= 24L * 60 * 60 * 1000 * 1000 * 1000).toArray());
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void testDateType()
    {
        testType(DateType.instance, testDates);
    }
    
    @Test
    public void testTimestampType()
    {
        testType(TimestampType.instance, testDates);
    }
    
    @Test
    public void testBytesType()
    {
        List<ByteBuffer> values = new ArrayList<>();
        for (int i = 0; i < testValues.length; ++i)
            for (Object o : testValues[i])
                values.add(testTypes[i].decompose(o));

        testType(BytesType.instance, values.toArray());
    }
    
    @Test
    public void testInetAddressType() throws UnknownHostException
    {
        InetAddress[] testInets = new InetAddress[] { null, InetAddress.getLocalHost(), InetAddress.getLoopbackAddress(),
                InetAddress.getByName("192.168.0.1"), InetAddress.getByName("fe80::428d:5cff:fe53:1dc9"), InetAddress.getByName("2001:610:3:200a:192:87:36:2"),
                InetAddress.getByName("10.0.0.1"), InetAddress.getByName("0a00:0001::"), InetAddress.getByName("::10.0.0.1") };
        testType(InetAddressType.instance, testInets);
    }
    
    @Test
    public void testEmptyType()
    {
        testType(EmptyType.instance, new Void[] { null });
    }
    
    @Test
    public void testPatitionerDefinedOrder()
    {
        List<ByteBuffer> values = new ArrayList<>();
        for (int i = 0; i < testValues.length; ++i)
            for (Object o : testValues[i])
                values.add(testTypes[i].decompose(o));

        testBuffers(new PartitionerDefinedOrder(Murmur3Partitioner.instance), values);
        testBuffers(new PartitionerDefinedOrder(RandomPartitioner.instance), values);
        testBuffers(new PartitionerDefinedOrder(ByteOrderedPartitioner.instance), values);
    }
    
    ClusteringPrefix.Kind[] kinds = new ClusteringPrefix.Kind[] {
            ClusteringPrefix.Kind.INCL_START_BOUND,
            ClusteringPrefix.Kind.CLUSTERING,
            ClusteringPrefix.Kind.EXCL_START_BOUND,
            };

    interface PairTester
    {
        void test(AbstractType t1, AbstractType t2, Object o1, Object o2, Object o3, Object o4);
    }

    void testCombinationSampling(Random rand, PairTester tester)
    {
        for (int i=0;i<testTypes.length;++i)
            for (int j=0;j<testTypes.length;++j)
            {
                Object[] tv1 = new Object[3];
                Object[] tv2 = new Object[3];
                for (int t=0; t<tv1.length; ++t)
                {
                    tv1[t] = testValues[i][rand.nextInt(testValues[i].length)];
                    tv2[t] = testValues[j][rand.nextInt(testValues[j].length)];
                }

                for (Object o1 : tv1)
                    for (Object o2 : tv2)
                        for (Object o3 : tv1)
                            for (Object o4 : tv2)

                {
                    tester.test(testTypes[i], testTypes[j], o1, o2, o3, o4);
                }
            }
    }

    @Test
    public void testCombinations()
    {
        Random rand = ThreadLocalRandom.current();
        testCombinationSampling(rand, this::assertClusteringPairComparesSame);
    }
    
    void assertClusteringPairComparesSame(AbstractType t1, AbstractType t2, Object o1, Object o2, Object o3, Object o4)
    {
        for (ClusteringPrefix.Kind k1 : kinds)
            for (ClusteringPrefix.Kind k2 : kinds)
            {
                ClusteringComparator comp = new ClusteringComparator(t1, t2);
                ByteBuffer[] b = new ByteBuffer[2]; 
                ByteBuffer[] d = new ByteBuffer[2]; 
                b[0] = t1.decompose(o1);
                b[1] = t2.decompose(o2);
                d[0] = t1.decompose(o3);
                d[1] = t2.decompose(o4);
                ClusteringPrefix c = ClusteringBound.create(k1, b);
                ClusteringPrefix e = ClusteringBound.create(k2, d);
                assertEquals(String.format("Failed comparing %s and %s, %s vs %s", safeStr(c.clusteringString(comp.subtypes())), safeStr(e.clusteringString(comp.subtypes())), comp.asByteComparableSource(c), comp.asByteComparableSource(e)),
                        Integer.signum(comp.compare(c, e)), Integer.signum(ByteSource.compare(comp.asByteComparableSource(c), comp.asByteComparableSource(e))));
                ClusteringComparator compR = new ClusteringComparator(ReversedType.getInstance(t1), ReversedType.getInstance(t2));
                assertEquals(String.format("Failed comparing reversed %s and %s, %s vs %s", safeStr(c.clusteringString(comp.subtypes())), safeStr(e.clusteringString(comp.subtypes())), compR.asByteComparableSource(c), compR.asByteComparableSource(e)),
                        Integer.signum(compR.compare(c, e)), Integer.signum(ByteSource.compare(compR.asByteComparableSource(c), compR.asByteComparableSource(e))));
            }
    }
    
    @Test
    public void testTupleType()
    {
        Random rand = ThreadLocalRandom.current();
        testCombinationSampling(rand, this::assertTupleComparesSame);
    }

    @Test
    public void testTupleTypeNonFull()
    {
        TupleType tt = new TupleType(ImmutableList.of(AsciiType.instance, Int32Type.instance));
        List<ByteBuffer> tests = ImmutableList.of
            (
                TupleType.buildValue(decomposeAndRandomPad(AsciiType.instance, ""), decomposeAndRandomPad(Int32Type.instance, 0)),
                TupleType.buildValue(decomposeAndRandomPad(AsciiType.instance, ""), decomposeAndRandomPad(Int32Type.instance, null)),
                TupleType.buildValue(decomposeAndRandomPad(AsciiType.instance, "")),
                TupleType.buildValue()
            );
        testBuffers(tt, tests);
    }

    void assertTupleComparesSame(AbstractType t1, AbstractType t2, Object o1, Object o2, Object o3, Object o4)
    {
        TupleType tt = new TupleType(ImmutableList.of(t1, t2));
        ByteBuffer b1 = TupleType.buildValue(t1.decompose(o1), t2.decompose(o2));
        ByteBuffer b2 = TupleType.buildValue(t1.decompose(o3), t2.decompose(o4));
        assertComparesSame(tt, b1, b2);
    }
    
    @Test
    public void testCompositeType()
    {
        Random rand = ThreadLocalRandom.current();
        testCombinationSampling(rand, this::assertCompositeComparesSame);
    }

    @Test
    public void testCompositeTypeNonFull()
    {
        CompositeType tt = CompositeType.getInstance(AsciiType.instance, Int32Type.instance);
        List<ByteBuffer> tests = ImmutableList.of
            (
                CompositeType.build(decomposeAndRandomPad(AsciiType.instance, ""), decomposeAndRandomPad(Int32Type.instance, 0)),
                CompositeType.build(decomposeAndRandomPad(AsciiType.instance, ""), decomposeAndRandomPad(Int32Type.instance, null)),
                CompositeType.build(decomposeAndRandomPad(AsciiType.instance, "")),
                CompositeType.build(),
                CompositeType.build(true, decomposeAndRandomPad(AsciiType.instance, "")),
                CompositeType.build(true)
            );
        for (ByteBuffer b : tests)
            tt.validate(b);
        testBuffers(tt, tests);
    }

    void assertCompositeComparesSame(AbstractType t1, AbstractType t2, Object o1, Object o2, Object o3, Object o4)
    {
        CompositeType tt = CompositeType.getInstance(t1, t2);
        ByteBuffer b1 = CompositeType.build(decomposeAndRandomPad(t1, o1), decomposeAndRandomPad(t2, o2));
        ByteBuffer b2 = CompositeType.build(decomposeAndRandomPad(t1, o3), decomposeAndRandomPad(t2, o4));
        assertComparesSame(tt, b1, b2);
    }
    
    @Test
    public void testDynamicComposite()
    {
        DynamicCompositeType tt = DynamicCompositeType.getInstance(DynamicCompositeTypeTest.aliases);
        UUID[] uuids = DynamicCompositeTypeTest.uuids;
        List<ByteBuffer> tests = ImmutableList.of
            (
                DynamicCompositeTypeTest.createDynamicCompositeKey("test1", null, -1, false, true),
                DynamicCompositeTypeTest.createDynamicCompositeKey("test1", uuids[0], 24, false, true),
                DynamicCompositeTypeTest.createDynamicCompositeKey("test1", uuids[0], 42, false, true),
                DynamicCompositeTypeTest.createDynamicCompositeKey("test2", uuids[0], -1, false, true),
                DynamicCompositeTypeTest.createDynamicCompositeKey("test2", uuids[1], 42, false, true)
            );
        for (ByteBuffer b : tests)
            tt.validate(b);
        testBuffers(tt, tests);
    }

    @Test
    public void testListTypeString()
    {
        testCollection(ListType.getInstance(AsciiType.instance, true), testStrings, () -> new ArrayList<>(), new Random());
    }

    @Test
    public void testListTypeLong()
    {
        testCollection(ListType.getInstance(LongType.instance, true), testLongs, () -> new ArrayList<>(), new Random());
    }

    @Test
    public void testSetTypeString()
    {
        testCollection(SetType.getInstance(AsciiType.instance, true), testStrings, () -> new HashSet<>(), new Random());
    }

    @Test
    public void testSetTypeLong()
    {
        testCollection(SetType.getInstance(LongType.instance, true), testLongs, () -> new HashSet<>(), new Random());
    }

    <T, CT extends Collection<T>> void testCollection(CollectionType<CT> tt, T[] values, Supplier<CT> gen, Random rand)
    {
        List<CT> tests = new ArrayList<>();
        tests.add(gen.get());
        for (int c = 1; c <= 3; ++c)
            for (int j = 0; j < 5; ++j)
            {
                CT l = gen.get();
                for (int i = 0; i < c; ++i)
                    l.add(values[rand.nextInt(values.length)]);

                tests.add(l);
            }
        testType(tt, tests.toArray());
    }

    @Test
    public void testMapTypeStringLong()
    {
        testMap(MapType.getInstance(AsciiType.instance, LongType.instance, true), testStrings, testLongs, () -> new HashMap<>(), new Random());
    }

    @Test
    public void testMapTypeStringLongTree()
    {
        testMap(MapType.getInstance(AsciiType.instance, LongType.instance, true), testStrings, testLongs, () -> new TreeMap<>(), new Random());
    }

    <K, V, M extends Map<K, V>>void testMap(MapType<K, V> tt, K[] keys, V[] values, Supplier<M> gen, Random rand)
    {
        List<M> tests = new ArrayList<>();
        tests.add(gen.get());
        for (int c = 1; c <= 3; ++c)
            for (int j = 0; j < 5; ++j)
            {
                M l = gen.get();
                for (int i = 0; i < c; ++i)
                    l.put(keys[rand.nextInt(keys.length)], values[rand.nextInt(values.length)]);

                tests.add(l);
            }
        testType(tt, tests.toArray());
    }

    public void testType(AbstractType type, Object[] values)
    {
        for (Object i : values) {
            ByteBuffer b = decomposeAndRandomPad(type, i);
            System.out.format("Value %s (%s) bytes %s ByteSource %s\n",
                    safeStr(i),
                    safeStr(type.getSerializer().toCQLLiteral(b)),
                    safeStr(ByteBufferUtil.bytesToHex(b)),
                    type.asByteComparableSource(b));
        }
        for (Object i : values)
            for (Object j : values)
                assertComparesSame(type, i, j);
        if (!type.isReversed())
            testType(ReversedType.getInstance(type), values);
    }
    
    public void testBuffers(AbstractType type, List<ByteBuffer> values)
    {
        try
        {
            for (Object i : values) {
                ByteBuffer b = decomposeAndRandomPad(type, i);
                System.out.format("Value %s bytes %s ByteSource %s\n",
                        safeStr(type.getSerializer().toCQLLiteral(b)),
                        safeStr(ByteBufferUtil.bytesToHex(b)),
                        type.asByteComparableSource(b));
            }
        }
        catch (UnsupportedOperationException e)
        {
            // Continue without listing values.
        }

        for (ByteBuffer i : values)
            for (ByteBuffer j : values)
                assertComparesSameBuffers(type, i, j);
    }
    
    void assertComparesSameBuffers(AbstractType type, ByteBuffer b1, ByteBuffer b2)
    {
        int expected = Integer.signum(type.compare(b1, b2));
        int actual = Integer.signum(ByteSource.compare(type.asByteComparableSource(b1), type.asByteComparableSource(b2)));
        assertEquals(String.format("Failed comparing %s and %s", ByteBufferUtil.bytesToHex(b1), ByteBufferUtil.bytesToHex(b2)), expected, actual);
    }

    private Object safeStr(Object i)
    {
        if (i == null)
            return null;
        String s = i.toString();
        if (s.length() > 100)
            s = s.substring(0, 100) + "...";
        return s.replaceAll("\0", "<0>");
    }

    public <T> void testDirect(Function<T, ByteSource> convertor, BiFunction<T, T, Integer> comparator, T[] values)
    {
        for (T i : values) {
            if (i == null)
                continue;

            System.out.format("Value %s ByteSource %s\n",
                    safeStr(i),
                    convertor.apply(i));
        }
        for (T i : values)
            if (i != null)
                for (T j : values)
                    if (j != null)
                        assertComparesSame(convertor, comparator, i, j);
    }

    <T> void assertComparesSame(Function<T, ByteSource> convertor, BiFunction<T, T, Integer> comparator, T v1, T v2)
    {
        ByteSource b1 = convertor.apply(v1);
        ByteSource b2 = convertor.apply(v2);
        int expected = Integer.signum(comparator.apply(v1, v2));
        int actual = Integer.signum(ByteSource.compare(b1, b2));
        assertEquals(String.format("Failed comparing %s and %s", v1, v2), expected, actual);
    }

    void assertComparesSame(AbstractType type, Object v1, Object v2)
    {
        ByteBuffer b1 = decomposeAndRandomPad(type, v1);
        ByteBuffer b2 = decomposeAndRandomPad(type, v2);
        int expected = Integer.signum(type.compare(b1, b2));
        int actual = Integer.signum(ByteSource.compare(type.asByteComparableSource(b1), type.asByteComparableSource(b2)));
        if (expected == actual)
            return;

        if (type.isReversed())
        {
            // This can happen for reverse of nulls and prefixes. Check that it's ok within multi-component
            ClusteringComparator cc = new ClusteringComparator(type);
            ByteSource c1 = cc.asByteComparableSource(Clustering.make(b1));
            ByteSource c2 = cc.asByteComparableSource(Clustering.make(b2));
            int actualcc = Integer.signum(ByteSource.compare(c1, c2));
            if (actualcc == expected)
                return;
            assertEquals(String.format("Failed comparing reversed %s(%s, %s) and %s(%s, %s) direct (%d) and as clustering", safeStr(v1), ByteBufferUtil.bytesToHex(b1), c1, safeStr(v2), ByteBufferUtil.bytesToHex(b2), c2, actual), expected, actualcc);
        }
        else
            assertEquals(String.format("Failed comparing %s(%s) and %s(%s)", safeStr(v1), ByteBufferUtil.bytesToHex(b1), safeStr(v2), ByteBufferUtil.bytesToHex(b2)), expected, actual);
    }

    ByteBuffer decomposeAndRandomPad(AbstractType type, Object v)
    {
        ByteBuffer b = type.decompose(v);
        Random rand = ThreadLocalRandom.current();
        int padBefore = rand.nextInt(16);
        int padAfter = rand.nextInt(16);
        ByteBuffer padded = ByteBuffer.allocate(b.remaining() + padBefore + padAfter);
        rand.ints(padBefore).forEach(x -> padded.put((byte) x));
        padded.put(b);
        rand.ints(padAfter).forEach(x -> padded.put((byte) x));
        padded.clear().limit(padded.capacity() - padAfter).position(padBefore);
        return padded;
    }
}
