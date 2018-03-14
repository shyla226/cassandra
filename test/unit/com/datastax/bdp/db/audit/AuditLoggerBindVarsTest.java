/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.db.audit;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Strings;
import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.serializers.UTF8Serializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AuditLoggerBindVarsTest
{
    @Test
    public void testEmpty()
    {
        StringBuilder b = new StringBuilder();
        AuditLogger.appendBindVariables(b, Collections.emptyList(), Collections.emptyList());
        assertEquals("[bind variable values unavailable]", b.toString());
    }

    @Test
    public void testSingleVariableNoTruncation()
    {
        StringBuilder b = new StringBuilder();

        String value = Strings.repeat("a", AuditLogger.MAX_VALUE_SIZE);

        AuditLogger.appendBindVariables(b,
                                        Collections.singletonList(new ColumnSpecification("k", "t", new ColumnIdentifier("foo", true), UTF8Type.instance)),
                                        Collections.singletonList(UTF8Serializer.instance.serialize(value)));

        assertEquals("[foo=" + value + ']', b.toString());
    }

    @Test
    public void testSingleVariableTruncation()
    {
        StringBuilder b = new StringBuilder();

        String value = Strings.repeat("ab", AuditLogger.MAX_VALUE_SIZE);

        AuditLogger.appendBindVariables(b,
                                        Collections.singletonList(new ColumnSpecification("k", "t", new ColumnIdentifier("foo", true), UTF8Type.instance)),
                                        Collections.singletonList(UTF8Serializer.instance.serialize(value)));

        assertEquals("[foo=" + value.substring(0, AuditLogger.MAX_VALUE_SIZE) + "... (truncated, " + AuditLogger.MAX_VALUE_SIZE + " chars omitted)]",
                     b.toString());
    }

    @Test
    public void testMultipleVariableNoTruncation()
    {
        StringBuilder b = new StringBuilder();

        AuditLogger.appendBindVariables(b,
                                        Arrays.asList(new ColumnSpecification("k", "t", new ColumnIdentifier("foo", true), UTF8Type.instance),
                                                      new ColumnSpecification("k", "t", new ColumnIdentifier("bar", true), UTF8Type.instance)),
                                        Arrays.asList(UTF8Serializer.instance.serialize("bar"),
                                                      UTF8Serializer.instance.serialize("baz")));

        assertEquals("[foo=bar,bar=baz]", b.toString());
    }

    @Test
    public void testCapping()
    {
        StringBuilder b = new StringBuilder();

        int noTruncationRepetiion = AuditLogger.MAX_SIZE / (3 + 1 + 3);

        List<ColumnSpecification> cols = new ArrayList<>(noTruncationRepetiion);
        List<ByteBuffer> vals = new ArrayList<>(noTruncationRepetiion);

        for (int i = 0; i <= noTruncationRepetiion; i++) {
            cols.add(new ColumnSpecification("k", "t", new ColumnIdentifier(String.format("%3d", i), true), UTF8Type.instance));
            vals.add(UTF8Serializer.instance.serialize("foo"));
        }

        AuditLogger.appendBindVariables(b, cols, vals);

        assertTrue(b.toString().endsWith(", ... (capped)]"));
    }

}
