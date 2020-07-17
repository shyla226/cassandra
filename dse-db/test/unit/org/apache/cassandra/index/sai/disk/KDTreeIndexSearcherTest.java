/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.index.sai.disk;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.NumberType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.Token;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.index.sai.utils.TokenFlow;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.flow.Flow;

public class KDTreeIndexSearcherTest extends NdiRandomizedTest
{
    private static final short EQ_TEST_LOWER_BOUND_INCLUSIVE = 0;
    private static final short EQ_TEST_UPPER_BOUND_EXCLUSIVE = 3;

    private static final short RANGE_TEST_LOWER_BOUND_INCLUSIVE = 0;
    private static final short RANGE_TEST_UPPER_BOUND_EXCLUSIVE = 10;

    @Test
    public void testRangeQueriesAgainstInt32Index() throws Exception
    {
        doTestRangeQueriesAgainstInt32Index(false);
    }

    @Test
    public void testRangeQueriesAgainstInt32IndexCrypto() throws Exception
    {
        doTestRangeQueriesAgainstInt32Index(true);
    }

    private void doTestRangeQueriesAgainstInt32Index(boolean crypto) throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildInt32Searcher(newIndexComponents(crypto), 0, 10);
        testRangeQueries(indexSearcher, Int32Type.instance, Int32Type.instance, Integer::valueOf);
    }

    @Test
    public void testEqQueriesAgainstInt32Index() throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildInt32Searcher(newIndexComponents(),
                EQ_TEST_LOWER_BOUND_INCLUSIVE, EQ_TEST_UPPER_BOUND_EXCLUSIVE);
        testEqQueries(indexSearcher, Int32Type.instance, Int32Type.instance, Integer::valueOf);
    }

    @Test
    public void testRangeQueriesAgainstLongIndex() throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildLongSearcher(newIndexComponents(), 0, 10);
        testRangeQueries(indexSearcher, LongType.instance, Int32Type.instance, Long::valueOf);
    }

    @Test
    public void testEqQueriesAgainstLongIndex() throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildLongSearcher(newIndexComponents(),
                EQ_TEST_LOWER_BOUND_INCLUSIVE, EQ_TEST_UPPER_BOUND_EXCLUSIVE);
        testEqQueries(indexSearcher, LongType.instance, Int32Type.instance, Long::valueOf);
    }

    @Test
    public void testRangeQueriesAgainstShortIndex() throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildShortSearcher(newIndexComponents(), (short) 0, (short) 10);
        testRangeQueries(indexSearcher, ShortType.instance, Int32Type.instance, Function.identity());
    }

    @Test
    public void testEqQueriesAgainstShortIndex() throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildShortSearcher(newIndexComponents(),
                EQ_TEST_LOWER_BOUND_INCLUSIVE, EQ_TEST_UPPER_BOUND_EXCLUSIVE);
        testEqQueries(indexSearcher, ShortType.instance, Int32Type.instance, Function.identity());
    }

    @Test
    public void testRangeQueriesAgainstDecimalIndex() throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildDecimalSearcher(newIndexComponents(),
                BigDecimal.ZERO, BigDecimal.valueOf(10L));
        testRangeQueries(indexSearcher, DecimalType.instance, DecimalType.instance, BigDecimal::valueOf,
                getLongsOnInterval(21L, 70L));
    }

    private List<Long> getLongsOnInterval(long lowerInclusive, long upperInclusive)
    {
        return LongStream.range(lowerInclusive, upperInclusive + 1L).boxed().collect(Collectors.toList());
    }

    @Test
    public void testEqQueriesAgainstDecimalIndex() throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildDecimalSearcher(newIndexComponents(),
                BigDecimal.valueOf(EQ_TEST_LOWER_BOUND_INCLUSIVE), BigDecimal.valueOf(EQ_TEST_UPPER_BOUND_EXCLUSIVE));
        testEqQueries(indexSearcher, DecimalType.instance, DecimalType.instance, BigDecimal::valueOf);
    }

    @Test
    public void testUnsupportedOperator() throws Exception
    {
        final IndexSearcher indexSearcher = KDTreeIndexBuilder.buildShortSearcher(newIndexComponents(), (short) 0, (short) 3);
        try
        {
            indexSearcher.search(new Expression(SAITester.createColumnContext("meh", ShortType.instance))
            {{
                operation = Op.NOT_EQ;
                lower = upper = new Bound(ShortType.instance.decompose((short) 0), Int32Type.instance, true);
            }}, SSTableQueryContext.forTest(), false);

            fail("Expect IllegalArgumentException thrown, but didn't");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    private <T extends Number> void testEqQueries(final IndexSearcher indexSearcher,
            final NumberType<T> rawType, final NumberType<?> encodedType,
            final Function<Short, T> rawValueProducer) throws Exception
    {
        TokenFlow results = indexSearcher.search(new Expression(SAITester.createColumnContext("meh", rawType))
        {{
            operation = Op.EQ;
            lower = upper = new Bound(rawType.decompose(rawValueProducer.apply(EQ_TEST_LOWER_BOUND_INCLUSIVE)), encodedType, true);
        }}, SSTableQueryContext.forTest(), false).blockingSingle();

        try (CloseableIterator<Token> iterator = Flow.toIterator(results.tokens()))
        {
            assertEquals(0L, iterator.next().get());
        }

        results = indexSearcher.search(new Expression(SAITester.createColumnContext("meh", rawType))
        {{
            operation = Op.EQ;
            lower = upper = new Bound(rawType.decompose(rawValueProducer.apply(EQ_TEST_UPPER_BOUND_EXCLUSIVE)), encodedType, true);
        }}, SSTableQueryContext.forTest(), false).blockingSingle();
        assertNoResult(results);
        indexSearcher.close();
    }

    private <T extends Number> void testRangeQueries(final IndexSearcher indexSearcher,
            final NumberType<T> rawType, final NumberType<?> encodedType,
            final Function<Short, T> rawValueProducer) throws Exception
    {
        List<Long> expectedTokenList = getLongsOnInterval(3L, 7L);
        testRangeQueries(indexSearcher, rawType, encodedType, rawValueProducer, expectedTokenList);
    }


    private <T extends Number> void testRangeQueries(final IndexSearcher indexSearcher,
            final NumberType<T> rawType, final NumberType<?> encodedType,
            final Function<Short, T> rawValueProducer, List<Long> expectedTokenList) throws Exception
    {
        TokenFlow results = indexSearcher.search(new Expression(SAITester.createColumnContext("meh", rawType))
        {{
            operation = Op.RANGE;
            lower = new Bound(rawType.decompose(rawValueProducer.apply((short)2)), encodedType, false);
            upper = new Bound(rawType.decompose(rawValueProducer.apply((short)7)), encodedType, true);
        }}, SSTableQueryContext.forTest(), false).blockingSingle();

        try (CloseableIterator<Token> iterator = Flow.toIterator(results.tokens()))
        {
            List<Long> actualTokenList = Lists.newArrayList(Iterators.transform(iterator, Token::get));
            assertEquals(expectedTokenList, actualTokenList);
        }

        results = indexSearcher.search(new Expression(SAITester.createColumnContext("meh", rawType))
        {{
            operation = Op.RANGE;
            lower = new Bound(rawType.decompose(rawValueProducer.apply(RANGE_TEST_UPPER_BOUND_EXCLUSIVE)), encodedType, true);
        }}, SSTableQueryContext.forTest(), false).blockingSingle();
        assertNoResult(results);

        results = indexSearcher.search(new Expression(SAITester.createColumnContext("meh", rawType))
        {{
            operation = Op.RANGE;
            upper = new Bound(rawType.decompose(rawValueProducer.apply(RANGE_TEST_LOWER_BOUND_INCLUSIVE)), encodedType, false);
        }}, SSTableQueryContext.forTest(), false).blockingSingle();
        assertNoResult(results);
        indexSearcher.close();
    }

    private void assertNoResult(TokenFlow result)
    {
        assertEquals(Collections.emptyList(), result.tokens().toList().blockingSingle());
    }
}
