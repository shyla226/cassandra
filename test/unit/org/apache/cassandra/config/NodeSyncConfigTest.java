/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.config;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.SizeValue;

import static org.junit.Assert.fail;

public class NodeSyncConfigTest
{
    @Test
    public void testPageSize()
    {
        NodeSyncConfig config = new NodeSyncConfig();

        // Integer.MAX_VALUE is the max value supported:
        config.setPageSize(SizeValue.of(Integer.MAX_VALUE, SizeUnit.BYTES));
        config.validate();

        // Otherwise it fails:
        try
        {
            config.setPageSize(SizeValue.of(((long) Integer.MAX_VALUE) + 1, SizeUnit.BYTES));
            config.validate();
            fail("Should have failed as Integer.MAX_VALUE is the max supported value.");
        }
        catch (ConfigurationException ex)
        {
            ex.printStackTrace();
        }
    }
}
