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

package org.apache.cassandra.auth.enums;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.testing.SampleElements;
import com.google.common.collect.testing.SetTestSuiteBuilder;
import com.google.common.collect.testing.TestSetGenerator;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

import junit.framework.TestSuite;

import static org.apache.cassandra.auth.enums.PartitionedEnumTestSupport.PartOne;
import static org.apache.cassandra.auth.enums.PartitionedEnumTestSupport.PartThree;
import static org.apache.cassandra.auth.enums.PartitionedEnumTestSupport.PartTwo;
import static org.apache.cassandra.auth.enums.PartitionedEnumTestSupport.TestPartitionedEnum;

@RunWith(AllTests.class)
public class PartitionedEnumSetGenTest
{
    // Generated suite to test conformity to the java.util.Set interface
    public static TestSuite suite()
    {

        return SetTestSuiteBuilder.using(new TestDataGenerator())
                                  .named("PartitionedEnumSet Tests")
                                  .withSetUp(() -> {
                                      Domains.unregisterType(TestPartitionedEnum.class);
                                      PartitionedEnum.registerDomainForType(TestPartitionedEnum.class, "PART_1", PartOne.class);
                                      PartitionedEnum.registerDomainForType(TestPartitionedEnum.class, "PART_2", PartTwo.class);
                                      PartitionedEnum.registerDomainForType(TestPartitionedEnum.class, "PART_3", PartThree.class);
                                  })
                                  .withFeatures(CollectionSize.ANY,
                                                CollectionFeature.NON_STANDARD_TOSTRING,
                                                CollectionFeature.SUPPORTS_ADD,
                                                CollectionFeature.SUPPORTS_REMOVE)
                                  .createTestSuite();
    }

    public static class TestDataGenerator implements TestSetGenerator<TestPartitionedEnum>
    {
        public SampleElements<TestPartitionedEnum> samples()
        {
            return new SampleElements<>(PartOne.ELEMENT_0,
                                        PartOne.ELEMENT_1,
                                        PartTwo.ELEMENT_0,
                                        PartTwo.ELEMENT_1,
                                        PartThree.ELEMENT_0);
        }

        public Set<TestPartitionedEnum> create(Object... objects)
        {
            TestPartitionedEnum[] elements = new TestPartitionedEnum[objects.length];
            int i = 0;
            for (Object object : objects)
                elements[i++] = (TestPartitionedEnum)object;

            return PartitionedEnumSet.of(TestPartitionedEnum.class, elements);
        }

        public TestPartitionedEnum[] createArray(int i)
        {
            return new TestPartitionedEnum[i];
        }

        public Iterable<TestPartitionedEnum> order(List<TestPartitionedEnum> list)
        {
            Collections.sort(list, (e1, e2) -> e1.getFullName().compareTo(e2.getFullName()));
            return list;
        }
    }
}
