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

import org.apache.commons.httpclient.methods.multipart.Part;
import org.junit.Before;
import org.junit.Test;

import static org.apache.cassandra.auth.enums.PartitionedEnumTestSupport.PartOne;
import static org.apache.cassandra.auth.enums.PartitionedEnumTestSupport.PartThree;
import static org.apache.cassandra.auth.enums.PartitionedEnumTestSupport.PartTwo;
import static org.apache.cassandra.auth.enums.PartitionedEnumTestSupport.TestPartitionedEnum;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PartitionedEnumSetTest
{
    // the generated guava tests verify the implementation of the Set interface,
    // but they don't exercise the specializations. In particular, the validation
    // and error checking & optimized  versions of Set operations where where we're
    // dealing 2 PartitionedEnumSet instances.

    @Before
    public void setup()
    {
        Domains.unregisterType(TestPartitionedEnum.class);
        PartitionedEnum.registerDomainForType(TestPartitionedEnum.class, "PART_1", PartOne.class);
        PartitionedEnum.registerDomainForType(TestPartitionedEnum.class, "PART_2", PartTwo.class);
        PartitionedEnum.registerDomainForType(TestPartitionedEnum.class, "PART_3", PartThree.class);
    }

    @Test
    public void testIsEmpty()
    {
        assertTrue(PartitionedEnumSet.of(TestPartitionedEnum.class).isEmpty());
        assertTrue(PartitionedEnumSet.noneOf(TestPartitionedEnum.class).isEmpty());
        assertFalse(PartitionedEnumSet.of(TestPartitionedEnum.class, PartOne.ELEMENT_0).isEmpty());

        PartitionedEnumSet<TestPartitionedEnum> set = PartitionedEnumSet.noneOf(TestPartitionedEnum.class);
        set.add(PartOne.ELEMENT_0);
        set.add(PartTwo.ELEMENT_0);
        set.add(PartThree.ELEMENT_0);
        assertFalse(set.isEmpty());
        set.remove(PartThree.ELEMENT_0);
        set.removeAll(PartitionedEnumSet.allOf(TestPartitionedEnum.class, PartTwo.class));
        set.retainAll(PartitionedEnumSet.allOf(TestPartitionedEnum.class, PartTwo.class));
        assertTrue(set.isEmpty());
    }

    @Test
    public void testSize()
    {
        PartitionedEnumSet<TestPartitionedEnum> set = PartitionedEnumSet.noneOf(TestPartitionedEnum.class);
        assertEquals(0, set.size());
        set.addAll(PartitionedEnumSet.allOf(TestPartitionedEnum.class, PartOne.class));
        assertEquals(5, set.size());
        set.addAll(PartitionedEnumSet.noneOf(TestPartitionedEnum.class));
        assertEquals(5, set.size());
        set.addAll(PartitionedEnumSet.allOf(TestPartitionedEnum.class, PartTwo.class));
        assertEquals(7, set.size());
        set.add(PartThree.ELEMENT_0);
        assertEquals(8, set.size());
        set.add(PartThree.ELEMENT_0);
        assertEquals(8, set.size());
        set.addAll(PartitionedEnumSet.allOf(TestPartitionedEnum.class, PartTwo.class));
        assertEquals(8, set.size());
        set.remove(PartThree.ELEMENT_0);
        assertEquals(7, set.size());
        set.removeAll(PartitionedEnumSet.allOf(TestPartitionedEnum.class, PartTwo.class));
        assertEquals(5, set.size());
        set.removeAll(PartitionedEnumSet.allOf(TestPartitionedEnum.class, PartTwo.class));
        assertEquals(5, set.size());
        set.removeAll(PartitionedEnumSet.allOf(TestPartitionedEnum.class, PartOne.class));
        assertEquals(0, set.size());
    }

    @Test
    public void onlyElementsOfRegisteredDomainsCanBeAdded()
    {
        // elements from an entirely unregistered domain should fail
        tryAndExpectIllegalArgs(() -> PartitionedEnumSet.of(TestPartitionedEnum.class, UnregisteredPart.ELEMENT_0));
        tryAndExpectIllegalArgs(() -> {
            PartitionedEnumSet<TestPartitionedEnum> p = PartitionedEnumSet.noneOf(TestPartitionedEnum.class);
            p.add(UnregisteredPart.ELEMENT_0);
        });
        tryAndExpectIllegalArgs(() -> PartitionedEnumSet.allOf(TestPartitionedEnum.class, UnregisteredPart.class));

        // buggy or malicious elements which spoof an already registered on should fail
        tryAndExpectIllegalArgs(() -> PartitionedEnumSet.of(TestPartitionedEnum.class, EvilPartOne.ELEMENT_0));
        tryAndExpectIllegalArgs(() -> {
            PartitionedEnumSet<TestPartitionedEnum> p = PartitionedEnumSet.noneOf(TestPartitionedEnum.class);
            p.add(EvilPartOne.ELEMENT_0);
        });
        tryAndExpectIllegalArgs(() -> PartitionedEnumSet.allOf(TestPartitionedEnum.class, EvilPartOne.class));
    }

    @Test
    public void testOptimizedAddAllWithContains()
    {
        PartitionedEnumSet<TestPartitionedEnum> t1 = PartitionedEnumSet.of(TestPartitionedEnum.class, PartThree.ELEMENT_0);
        t1.add(PartOne.ELEMENT_0);
        t1.add(PartOne.ELEMENT_1);
        PartitionedEnumSet<TestPartitionedEnum> t2 = PartitionedEnumSet.of(TestPartitionedEnum.class, PartThree.ELEMENT_0);
        t2.add(PartTwo.ELEMENT_0);
        t2.add(PartTwo.ELEMENT_1);
        assertTrue(t1.addAll(t2));
        assertEquals(5, t1.size());
        assertTrue(t1.contains(PartOne.ELEMENT_0));
        assertTrue(t1.contains(PartOne.ELEMENT_1));
        assertTrue(t1.contains(PartTwo.ELEMENT_0));
        assertTrue(t1.contains(PartTwo.ELEMENT_1));
        assertTrue(t1.contains(PartThree.ELEMENT_0));
    }

    @Test
    public void testOptimizedRetainAllWithContains()
    {
        PartitionedEnumSet<TestPartitionedEnum> t1 =PartitionedEnumSet.of(TestPartitionedEnum.class, PartThree.ELEMENT_0);
        t1.add(PartOne.ELEMENT_0);
        t1.add(PartOne.ELEMENT_1);

        PartitionedEnumSet<TestPartitionedEnum> t2 = PartitionedEnumSet.of(TestPartitionedEnum.class, PartThree.ELEMENT_0);
        t2.add(PartTwo.ELEMENT_0);
        t2.add(PartTwo.ELEMENT_1);

        assertTrue(t1.addAll(t2));
        assertEquals(5, t1.size());
        assertTrue(t1.contains(PartOne.ELEMENT_0));
        assertTrue(t1.contains(PartOne.ELEMENT_1));
        assertTrue(t1.contains(PartTwo.ELEMENT_0));
        assertTrue(t1.contains(PartTwo.ELEMENT_1));
        assertTrue(t1.contains(PartThree.ELEMENT_0));
    }

    @Test
    public void testIntersection()
    {
        // 2 empty sets
        PartitionedEnumSet<TestPartitionedEnum> set = emptySet();
        PartitionedEnumSet<TestPartitionedEnum> other = emptySet();
        assertFalse(set.intersects(other));
        set.retainAll(other);
        assertTrue(set.isEmpty());

        // single domain, same elements
        set = setOf(PartOne.ELEMENT_0);
        other = setOf(PartOne.ELEMENT_0);
        assertTrue(set.intersects(other));
        set.retainAll(other);
        assertEquals(set, setOf(PartOne.ELEMENT_0));

        // single domain, disjoint elements
        set = setOf(PartOne.ELEMENT_0);
        other = setOf(PartOne.ELEMENT_1);
        assertFalse(set.intersects(other));
        set.retainAll(other);
        assertTrue(set.isEmpty());

        // single domain, intersecting elements
        set = setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1);
        other = setOf(PartOne.ELEMENT_1, PartOne.ELEMENT_2);
        assertTrue(set.intersects(other));
        set.retainAll(other);
        assertEquals(set, setOf(PartOne.ELEMENT_1));

        // 1 empty, 1 single domain
        set = emptySet();
        other = setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1);
        assertFalse(set.intersects(other));
        set.retainAll(other);
        assertTrue(set.isEmpty());
        set = setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1);
        other = emptySet();
        assertFalse(set.intersects(other));
        set.retainAll(other);
        assertTrue(set.isEmpty());

        // 2 disjoint single domains
        set = setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1);
        other = setOf(PartTwo.ELEMENT_0, PartTwo.ELEMENT_1);
        assertFalse(set.intersects(other));
        set.retainAll(other);
        assertTrue(set.isEmpty());

        // multiple domains, all disjoint elements
        set = setOf(PartOne.ELEMENT_0, PartTwo.ELEMENT_0);
        other = setOf(PartOne.ELEMENT_1, PartTwo.ELEMENT_1);
        assertFalse(set.intersects(other));
        set.retainAll(other);
        assertTrue(set.isEmpty());

        // multiple domains, intersecting in 1 domain
        set = setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1, PartTwo.ELEMENT_0);
        other = setOf(PartOne.ELEMENT_3, PartTwo.ELEMENT_0, PartThree.ELEMENT_0);
        assertTrue(set.intersects(other));
        set.retainAll(other);
        assertEquals(set, setOf(PartTwo.ELEMENT_0));

        // multiple identical domains
        set = setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1, PartTwo.ELEMENT_0, PartTwo.ELEMENT_1);
        other = setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1, PartTwo.ELEMENT_0, PartTwo.ELEMENT_1);
        assertTrue(set.intersects(other));
        set.retainAll(other);
        assertEquals(set, setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1, PartTwo.ELEMENT_0, PartTwo.ELEMENT_1));
    }

    @Test
    public void testEquality()
    {
        // empty sets
        assertEquals(PartitionedEnumSet.of(TestPartitionedEnum.class),
                     PartitionedEnumSet.noneOf(TestPartitionedEnum.class));

        // single domain, same elements
        assertEquals(setOf(PartOne.ELEMENT_0), setOf(PartOne.ELEMENT_0));

        // single domain, disjoint elements
        assertFalse(setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1).equals(setOf(PartOne.ELEMENT_2, PartOne.ELEMENT_3)));

        // single domain, intersecting elements
        assertFalse(setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1).equals(setOf(PartOne.ELEMENT_1, PartOne.ELEMENT_2)));

        // 1 empty, 1 single domain
        assertFalse(emptySet().equals(setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1)));
        assertFalse(setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1).equals(emptySet()));

        // 2 disjoint single domains
        assertFalse(setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1).equals(setOf(PartTwo.ELEMENT_0, PartTwo.ELEMENT_1)));

        // multiple domains, all disjoint elements
        assertFalse(setOf(PartOne.ELEMENT_0, PartTwo.ELEMENT_0).equals(setOf(PartOne.ELEMENT_1, PartTwo.ELEMENT_1)));

        // multiple domains, intersecting in 1 domain
        assertFalse(setOf(PartOne.ELEMENT_0,
                          PartTwo.ELEMENT_0,
                          PartThree.ELEMENT_0)
                    .equals(setOf(PartOne.ELEMENT_1,
                                  PartTwo.ELEMENT_1,
                                  PartThree.ELEMENT_0)));

        // multiple identical domains
        assertEquals(setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1, PartTwo.ELEMENT_0, PartThree.ELEMENT_0),
                     setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1, PartTwo.ELEMENT_0, PartThree.ELEMENT_0));
    }

    @Test
    public void testUnion()
    {
        // 2 empty sets
        PartitionedEnumSet<TestPartitionedEnum> set = emptySet();
        set.addAll(emptySet());
        assertTrue(set.isEmpty());

        // single domain, same elements
        set = setOf(PartOne.ELEMENT_0);
        set.addAll(setOf(PartOne.ELEMENT_0));
        assertEquals(set, setOf(PartOne.ELEMENT_0));

        // single domain, disjoint elements
        set = setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1);
        set.addAll(setOf(PartOne.ELEMENT_2, PartOne.ELEMENT_3));
        assertEquals(set, setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1, PartOne.ELEMENT_2, PartOne.ELEMENT_3));

        // single domain, intersecting elements
        set = setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1);
        set.addAll(setOf(PartOne.ELEMENT_1, PartOne.ELEMENT_2));
        assertEquals(set, setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1, PartOne.ELEMENT_2));

        // 1 empty, 1 single domain
        set = emptySet();
        set.addAll(setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1));
        assertEquals(set, setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1));
        set = setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1);
        set.addAll(emptySet());
        assertEquals(set, setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1));

        // 2 disjoint single domains
        set = setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1);
        set.addAll(setOf(PartTwo.ELEMENT_0, PartTwo.ELEMENT_1));
        assertEquals(set, setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1, PartTwo.ELEMENT_0, PartTwo.ELEMENT_1));

        // multiple domains, all disjoint elements
        set = setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1, PartTwo.ELEMENT_0, PartThree.ELEMENT_0);
        set.addAll(setOf(PartOne.ELEMENT_2, PartTwo.ELEMENT_1));
        assertEquals(set, setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1, PartOne.ELEMENT_2,
                                PartTwo.ELEMENT_0, PartTwo.ELEMENT_1,
                                PartThree.ELEMENT_0));

        // multiple domains, intersecting in 1 domain
        set = setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1, PartTwo.ELEMENT_0, PartThree.ELEMENT_0);
        set.addAll(setOf(PartOne.ELEMENT_2, PartTwo.ELEMENT_1, PartThree.ELEMENT_0));
        assertEquals(set, setOf(PartOne.ELEMENT_0, PartOne.ELEMENT_1, PartOne.ELEMENT_2,
                                PartTwo.ELEMENT_0, PartTwo.ELEMENT_1,
                                PartThree.ELEMENT_0));

        // multiple identical domains
        set = setOf(PartOne.ELEMENT_0, PartTwo.ELEMENT_0, PartTwo.ELEMENT_1, PartThree.ELEMENT_0);
        set.addAll(setOf(PartOne.ELEMENT_0, PartTwo.ELEMENT_0, PartTwo.ELEMENT_1, PartThree.ELEMENT_0));
        assertEquals(set, setOf(PartOne.ELEMENT_0, PartTwo.ELEMENT_0, PartTwo.ELEMENT_1, PartThree.ELEMENT_0));

        // multiple intersection & disjoint domains
        set = setOf(PartOne.ELEMENT_0);
        set.addAll(setOf(PartOne.ELEMENT_1, PartTwo.ELEMENT_1));
        set.addAll(setOf(PartOne.ELEMENT_1));
        set.addAll(setOf(PartTwo.ELEMENT_0));
        set.addAll(setOf(PartOne.ELEMENT_2, PartTwo.ELEMENT_0));
        set.addAll(setOf(PartOne.ELEMENT_3, PartTwo.ELEMENT_0, PartThree.ELEMENT_0));
        set.addAll(setOf(PartOne.ELEMENT_1, PartTwo.ELEMENT_0, PartThree.ELEMENT_0));
        set.addAll(emptySet());
        assertEquals(set, setOf(PartOne.ELEMENT_0,
                                PartOne.ELEMENT_1,
                                PartOne.ELEMENT_2,
                                PartOne.ELEMENT_3,
                                PartTwo.ELEMENT_0,
                                PartTwo.ELEMENT_1,
                                PartThree.ELEMENT_0));
    }

    @Test
    public void immutableSetIsImmutable()
    {
        testImmutability(PartitionedEnumSet.immutableSetOf(TestPartitionedEnum.class,
                                                           PartOne.ELEMENT_0,
                                                           PartOne.ELEMENT_1,
                                                           PartOne.ELEMENT_2));
        testImmutability(PartitionedEnumSet.immutableEmptySetOf(TestPartitionedEnum.class));
        testImmutability(PartitionedEnumSet.immutableSetOfAll(TestPartitionedEnum.class, PartOne.class));
    }

    private void testImmutability(PartitionedEnumSet<TestPartitionedEnum> set)
    {
        tryAndExpectUnsupported(() -> set.add(PartOne.ELEMENT_3));
        tryAndExpectUnsupported(() -> set.remove(PartOne.ELEMENT_3));
        tryAndExpectUnsupported(() -> set.addAll(setOf(PartOne.ELEMENT_3)));
        tryAndExpectUnsupported(() -> set.retainAll(setOf(PartOne.ELEMENT_3)));
        tryAndExpectUnsupported(set::clear);
    }

    private void tryAndExpectUnsupported(Runnable r)
    {
        try
        {
            r.run();
            fail("Expected exception");
        }
        catch(UnsupportedOperationException e)
        {
            // expected
        }
    }

    private void tryAndExpectIllegalArgs(Runnable r)
    {
        try
        {
            r.run();
            fail("Expected exception");
        }
        catch(IllegalArgumentException e)
        {
            // expected
        }
    }

    private PartitionedEnumSet<TestPartitionedEnum> emptySet()
    {
        return PartitionedEnumSet.noneOf(TestPartitionedEnum.class);
    }

    private PartitionedEnumSet<TestPartitionedEnum> setOf(TestPartitionedEnum...elements)
    {
        return PartitionedEnumSet.of(TestPartitionedEnum.class, elements);
    }

    private enum UnregisteredPart implements TestPartitionedEnum
    {
        ELEMENT_0;

        public String domain()
        {
            return "UNREGISTERED";
        }
    }

    private enum EvilPartOne implements TestPartitionedEnum
    {
        ELEMENT_0;

        public String domain()
        {
            return "PART_1";
        }
    }
}

