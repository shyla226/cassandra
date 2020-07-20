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
package org.apache.cassandra.index.sai.cql.types.collections;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import org.apache.cassandra.index.sai.cql.types.DataSet;
import org.apache.cassandra.index.sai.cql.types.QuerySet;

import static org.apache.cassandra.index.sai.cql.types.IndexingTypeSupport.NUMBER_OF_VALUES;

public abstract class CollectionDataSet<T> extends DataSet<T>
{
    public static class SetDataSet<T> extends CollectionDataSet<Set<T>>
    {
        protected DataSet<T> elementDataSet;

        public SetDataSet(Random random, DataSet<T> elementDataSet)
        {
            values = new Set[NUMBER_OF_VALUES];
            this.elementDataSet = elementDataSet;
            for (int index = 0; index < NUMBER_OF_VALUES; index++)
            {
                values[index] = new HashSet<>();
                for (int element = 0; element < RandomInts.randomIntBetween(random, 2, 8); element++)
                {
                    values[index].add(elementDataSet.values[RandomInts.randomIntBetween(random, 0, elementDataSet.values.length - 1)]);
                }
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.CollectionQuerySet(this, elementDataSet);
        }

        public String toString()
        {
            return String.format("set<%s>", elementDataSet);
        }
    }

    public static class FrozenSetDataSet<T> extends SetDataSet<T>
    {
        public FrozenSetDataSet(Random random, DataSet<T> elementDataSet)
        {
            super(random, elementDataSet);
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.FrozenCollectionQuerySet(this);
        }

        @Override
        public String decorateIndexColumn(String column)
        {
            return String.format("FULL(%s)", column);
        }

        public String toString()
        {
            return String.format("frozen<set<%s>>", elementDataSet);
        }
    }

    public static class ListDataSet<T> extends CollectionDataSet<List<T>>
    {
        protected DataSet<T> elementDataSet;

        public ListDataSet(Random random, DataSet<T> elementDataSet)
        {
            values = new List[NUMBER_OF_VALUES];
            this.elementDataSet = elementDataSet;
            for (int index = 0; index < NUMBER_OF_VALUES; index++)
            {
                values[index] = new ArrayList<>();
                for (int element = 0; element < RandomInts.randomIntBetween(random, 2, 8); element++)
                {
                    values[index].add(elementDataSet.values[RandomInts.randomIntBetween(random, 0, elementDataSet.values.length - 1)]);
                }
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.CollectionQuerySet(this, elementDataSet);
        }

        public String toString()
        {
            return String.format("list<%s>", elementDataSet);
        }
    }

    public static class FrozenListDataSet<T> extends ListDataSet<T>
    {
        public FrozenListDataSet(Random random, DataSet<T> elementDataSet)
        {
            super(random, elementDataSet);
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.FrozenCollectionQuerySet(this);
        }

        @Override
        public String decorateIndexColumn(String column)
        {
            return String.format("FULL(%s)", column);
        }

        public String toString()
        {
            return String.format("frozen<list<%s>>", elementDataSet);
        }
    }

    public static class MapDataSet<T> extends CollectionDataSet<Map<T, T>>
    {
        protected DataSet<T> elementDataSet;

        public MapDataSet(Random random, DataSet<T> elementDataSet)
        {
            values = new Map[NUMBER_OF_VALUES];
            this.elementDataSet = elementDataSet;
            for (int index = 0; index < NUMBER_OF_VALUES; index++)
            {
                values[index] = new HashMap<>();
                for (int element = 0; element < RandomInts.randomIntBetween(random, 2, 8); element++)
                {
                    T key = elementDataSet.values[RandomInts.randomIntBetween(random, 0, elementDataSet.values.length - 1)];
                    T value = elementDataSet.values[RandomInts.randomIntBetween(random, 0, elementDataSet.values.length - 1)];
                    values[index].put(key, value);
                }
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.MapValuesQuerySet(this, elementDataSet);
        }

        public String toString()
        {
            return String.format("map<%s,%s>", elementDataSet, elementDataSet);
        }
    }

    public static class FrozenMapValuesDataSet<T> extends MapDataSet<T>
    {
        public FrozenMapValuesDataSet(Random random, DataSet<T> elementDataSet)
        {
            super(random, elementDataSet);
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.FrozenCollectionQuerySet(this);
        }

        @Override
        public String decorateIndexColumn(String column)
        {
            return String.format("FULL(%s)", column);
        }

        public String toString()
        {
            return String.format("frozen<map<%s,%s>>", elementDataSet, elementDataSet);
        }
    }

    public static class MapKeysDataSet<T> extends MapDataSet<T>
    {
        public MapKeysDataSet(Random random, DataSet<T> elementDataSet)
        {
            super(random, elementDataSet);
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.MapKeysQuerySet(this, elementDataSet);
        }

        @Override
        public String decorateIndexColumn(String column)
        {
            return String.format("KEYS(%s)", column);
        }
    }

    public static class MapValuesDataSet<T> extends MapDataSet<T>
    {
        public MapValuesDataSet(Random random, DataSet<T> elementDataSet)
        {
            super(random, elementDataSet);
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.MapValuesQuerySet(this, elementDataSet);
        }

        @Override
        public String decorateIndexColumn(String column)
        {
            return String.format("VALUES(%s)", column);
        }
    }

    public static class MapEntriesDataSet<T> extends MapDataSet<T>
    {
        public MapEntriesDataSet(Random random, DataSet<T> elementDataSet)
        {
            super(random, elementDataSet);
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.MapEntriesQuerySet(this, elementDataSet);
        }

        @Override
        public String decorateIndexColumn(String column)
        {
            return String.format("ENTRIES(%s)", column);
        }
    }
}
