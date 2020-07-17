/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.index.sai.cql.types.collections.sets;

import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.cql.types.DataSet;
import org.apache.cassandra.index.sai.cql.types.IndexingTypeSupport;
import org.apache.cassandra.index.sai.cql.types.collections.CollectionDataSet;

@RunWith(Parameterized.class)
public class SetDecimalTest extends IndexingTypeSupport
{
    @Parameterized.Parameters(name = "dataset={0},wide={1},scenario={2}")
    public static Collection<Object[]> generateParameters()
    {
        return generateParameters(new CollectionDataSet.SetDataSet(random, new DataSet.DecimalDataSet(random)));
    }

    public SetDecimalTest(DataSet<?> dataset, boolean widePartitions, IndexingTypeSupport.Scenario scenario)
    {
        super(dataset, widePartitions, scenario);
    }

    @Test
    public void test() throws Throwable
    {
        runIndexQueryScenarios();
    }
}
