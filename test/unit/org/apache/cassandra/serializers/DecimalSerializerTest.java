/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.serializers;

import java.math.BigDecimal;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DecimalSerializerTest
{
    @Test
    public void testDecimalScaleStringify()
    {
        BigDecimal d = new BigDecimal("1");
        assertEquals("1", DecimalSerializer.instance.toString(d));

        d = new BigDecimal("1e-" + DecimalSerializer.maxScale);
        StringBuilder s = new StringBuilder("0.");
        for (int i = 0; i < DecimalSerializer.maxScale - 1; i++)
            s.append('0');
        s.append('1');
        assertEquals(s.toString(), DecimalSerializer.instance.toString(d));

        d = new BigDecimal("1e-" + (DecimalSerializer.maxScale + 1));
        assertEquals("1E-" + (DecimalSerializer.maxScale + 1), DecimalSerializer.instance.toString(d));

        d = new BigDecimal("1e+" + DecimalSerializer.maxScale);
        s = new StringBuilder("1");
        for (int i = 0; i < DecimalSerializer.maxScale; i++)
            s.append('0');
        assertEquals(s.toString(), DecimalSerializer.instance.toString(d));

        d = new BigDecimal("1e+" + (DecimalSerializer.maxScale + 1));
        assertEquals("1E+" + (DecimalSerializer.maxScale + 1), DecimalSerializer.instance.toString(d));
    }
}
