/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.tools.nodesync;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import com.datastax.driver.core.Metadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static com.datastax.apollo.tools.nodesync.SubmitValidation.*;

@SuppressWarnings("unchecked")
public class SubmitValidationTest extends CQLTester
{
    @Test
    @SuppressWarnings("resource")
    public void testTokenFactory()
    {
        Metadata metadata = sessionNet().getCluster().getMetadata();
        assertEquals(DatabaseDescriptor.getPartitioner().getTokenFactory(),
                     SubmitValidation.tokenFactory(metadata));
    }

    @Test
    public void testParseRange()
    {
        Token.TokenFactory tokenFactory = new Murmur3Partitioner().getTokenFactory();

        testParseRange(tokenFactory, " (0,1]", range(0, 1));
        testParseRange(tokenFactory, "(0,-1] ", range(0, -1));
        testParseRange(tokenFactory, "(-1 ,0]", range(-1, -0));
        testParseRange(tokenFactory, "(-1, -2]", range(-1, -2));
        testParseRange(tokenFactory, " ( -15 , -04 ] ", range(-15, -4));

        testParseRange(tokenFactory, "[1,2]", String.format(UNSUPPORTED_RANGE_MESSAGE, "[1,2]", "1", "2"));
        testParseRange(tokenFactory, "[-1,2)", String.format(UNSUPPORTED_RANGE_MESSAGE, "[-1,2)", "-1", "2"));
        testParseRange(tokenFactory, "(1,-2)", String.format(UNSUPPORTED_RANGE_MESSAGE, "(1,-2)", "1", "-2"));
        testParseRange(tokenFactory, " [ 1 , 2 ] ", String.format(UNSUPPORTED_RANGE_MESSAGE, "[ 1 , 2 ]", "1", "2"));

        testParseRange(tokenFactory, "a", "Cannot parse range: a");
        testParseRange(tokenFactory, "", "Cannot parse range: ");
        testParseRange(tokenFactory, " ", "Cannot parse range: ");
        testParseRange(tokenFactory, "(]", "Cannot parse range: (]");

        testParseRange(tokenFactory, "(1,a]", "Cannot parse token: a");
        testParseRange(tokenFactory, "(a,1]", "Cannot parse token: a");
    }

    private static void testParseRange(Token.TokenFactory tokenFactory, String str, Range<Token> expectedRange)
    {
        assertEquals(expectedRange, SubmitValidation.parseRange(tokenFactory, str));
    }

    private static void testParseRange(Token.TokenFactory tokenFactory, String str, String expectedMsg)
    {
        try
        {
            SubmitValidation.parseRange(tokenFactory, str);
        }
        catch (Exception e)
        {
            assertSame(NodeSyncException.class, e.getClass());
            assertEquals(expectedMsg, e.getMessage());
            return;
        }
        fail("Expected nodesync exception with message: " + expectedMsg);
    }

    @Test
    @SuppressWarnings("resource")
    public void testParseRanges()
    {
        Metadata metadata = sessionNet().getCluster().getMetadata();

        testParseRanges(metadata, "(0,1]", range(0, 1));
        testParseRanges(metadata, "(0,-1]", range(Long.MIN_VALUE, -1), range(0, Long.MIN_VALUE));
        testParseRanges(metadata, "(-1,0]", range(-1, 0));
        testParseRanges(metadata, "(-1,-2]", range(Long.MIN_VALUE, -2), range(-1, Long.MIN_VALUE));

        testParseRanges(metadata, "(0,9223372036854775807]", range(0, Long.MAX_VALUE));
        testParseRanges(metadata, "(0,-9223372036854775808]", range(0, Long.MIN_VALUE));
        testParseRanges(metadata, "(-9223372036854775808,9223372036854775807]", range(Long.MIN_VALUE, Long.MAX_VALUE));
        testParseRanges(metadata, "(-9223372036854775808,-9223372036854775808]", range(Long.MIN_VALUE, Long.MIN_VALUE));

        testParseRanges(metadata, "(0,1] (-1,-2]", range(Long.MIN_VALUE, -2), range(-1, Long.MIN_VALUE));
        testParseRanges(metadata, "(0,-1] (-1,0]", range(Long.MIN_VALUE, Long.MIN_VALUE));
        testParseRanges(metadata, "(-1,0] (0,-1]", range(Long.MIN_VALUE, Long.MIN_VALUE));
        testParseRanges(metadata, "(-2,-1] (123,456]", range(-2, -1), range(123, 456));

        testParseRanges(metadata, "(1,20] (20,30] (50,60]", range(1, 30), range(50, 60));
        testParseRanges(metadata, "(1,10] (5,15] (14,17]", range(1, 17));
    }

    private void testParseRanges(Metadata metadata, String str, Range<Token>... expectedRanges)
    {
        List<String> args = Lists.newArrayList(StringUtils.split(str, " "));
        List<Range<Token>> actualRanges = SubmitValidation.parseRanges(metadata, args);
        assertEquals(Arrays.asList(expectedRanges), actualRanges);
    }

    @Test
    public void testFormat()
    {
        testFormat("0:1", range(0, 1));
        testFormat("-9223372036854775808:-1,0:-9223372036854775808", range(0, -1));
        testFormat("-1:0", range(-1, 0));
        testFormat("-9223372036854775808:-2,-1:-9223372036854775808", range(-1, -2));

        testFormat("0:9223372036854775807", range(0, Long.MAX_VALUE));
        testFormat("0:-9223372036854775808", range(0, Long.MIN_VALUE));
        testFormat("-9223372036854775808:9223372036854775807", range(Long.MIN_VALUE, Long.MAX_VALUE));
        testFormat("-9223372036854775808:-9223372036854775808", range(Long.MIN_VALUE, Long.MIN_VALUE));

        testFormat("-9223372036854775808:-2,-1:-9223372036854775808", range(0, 1), range(-1, -2));
        testFormat("-9223372036854775808:-9223372036854775808", range(0, -1), range(-1, 0));
        testFormat("-9223372036854775808:-9223372036854775808", range(-1, 0), range(0, -1));
        testFormat("-2:-1,123:456", range(-2, -1), range(123, 456));

        testFormat("1:30,50:60", range(1, 20), range(20, 30), range(50, 60));
        testFormat("1:17", range(1, 10), range(5, 15), range(14, 17));
    }

    private void testFormat(String expectedStr, Range<Token>... expectedRanges)
    {
        assertEquals(expectedStr, SubmitValidation.format(Arrays.asList(expectedRanges)));
    }

    private static Range<Token> range(long left, long right)
    {
        return new Range<>(token(left), token(right));
    }

    private static Token token(long n)
    {
        return new Murmur3Partitioner.LongToken(n);
    }
}
