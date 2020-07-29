/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.index.sai.cql.types.collections.sets;

import java.util.Collection;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.cql.types.DataSet;
import org.apache.cassandra.index.sai.cql.types.IndexingTypeSupport;
import org.apache.cassandra.index.sai.cql.types.collections.CollectionDataSet;

@RunWith(Parameterized.class)
public class SetFrozenCollectionTest extends IndexingTypeSupport
{
    @Parameterized.Parameters(name = "dataset={0},wide={1},scenario={2}")
    public static Collection<Object[]> generateParameters()
    {
        DataSet<Set<Integer>> frozen = new CollectionDataSet.FrozenSetDataSet<>(random, new DataSet.IntDataSet(random));
        return generateParameters(new CollectionDataSet.SetDataSet<>(random, frozen));
    }

    public SetFrozenCollectionTest(DataSet<?> dataset, boolean widePartitions, Scenario scenario)
    {
        super(dataset, widePartitions, scenario);
    }

    @Test
    public void test() throws Throwable
    {
        runIndexQueryScenarios();
    }
}
