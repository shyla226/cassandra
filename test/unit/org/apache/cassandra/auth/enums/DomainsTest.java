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

import java.util.UUID;

import static org.apache.cassandra.auth.enums.PartitionedEnumTestSupport.*;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

public class DomainsTest
{
    private Domains<TestPartitionedEnum> registry;

    @Before
    public void setup()
    {
        Domains.unregisterType(TestPartitionedEnum.class);
        Domains.registerDomain(TestPartitionedEnum.class, "PART_1", PartOne.class);
        Domains.registerDomain(TestPartitionedEnum.class, "PART_2", PartTwo.class);
        registry = Domains.getDomains(TestPartitionedEnum.class);
    }

    @Test
    public void domainsCanBeRegistered()
    {
        TestPartitionedEnum element = PartThree.ELEMENT_0;
        try
        {
            registry.get(element.domain(), element.name());
            // not registered yet
            fail("Expected an exception");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }

        registry.register("PART_3", PartThree.class);
        assertSame(element, registry.get(element.domain(), element.name()));
    }

    @Test
    public void nameLookupMethodsAreEquivalent()
    {
        assertSame(registry.get("PART_1", "ELEMENT_0"),
                   registry.get("part_1.element_0"));
    }

    @Test
    public void nameIsNotCaseSensitive()
    {
        assertSame(registry.get("PART_1", "ELEMENT_1"),
                   registry.get("part_1", "element_1"));
    }

    @Test
    public void getElementByDomainOrdinal()
    {
        assertSame(PartOne.ELEMENT_0, registry.get("PART_1", 0));
        assertSame(PartOne.ELEMENT_1, registry.get("PART_1", 1));
        assertSame(PartOne.ELEMENT_2, registry.get("PART_1", 2));

        try
        {
            registry.get("PART_1", 99);
            fail("Expected an exception");
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            // expected
        }
    }

    @Test
    public void cannotRegisterDuplicateDomains()
    {
        // Attempt to register 2 distinct enums with the same domain
        registry.register("DUPLICATE", DuplicateDomainEnum_0.class);
        try
        {
            registry.register("DUPLICATE", DuplicateDomainEnum_1.class);
            fail("Expected an exception");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    @Test
    public void canOnlyRegisterOnce()
    {
        // Attempt to register the same capability enum twice, with the same domain
        registry.register("PART_3", PartThree.class);
        try
        {
            registry.register("PART_3", PartThree.class);
            fail("Expected an exception");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    @Test
    public void acceptOnlyUpperCaseElementNames()
    {
        // forcing the declared names of all Capability constants, we
        // can accept either case in CQL strings without worrying about
        // edge cases where the constants may be lower/mixed case
        try
        {
            registry.register("MIXED_CASE", MixedCaseEnum.class);
            fail("Expected an exception");
        }
        catch (IllegalArgumentException e)
        {
            e.printStackTrace();
            // expected
        }
    }

    @Test
    public void declaredAndRegisteredDomainNamesMustMatch()
    {
        // the domain name being used to register the enum must
        // match the value returned by its domain() method
        try
        {
            registry.register(UUID.randomUUID().toString(), PartThree.class);
            fail("Expected an exception");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    @Test
    public void domainMustNotContainPeriod()
    {
        try
        {
            registry.register("INVALID.DOMAIN", WithPeriodInDomain.class);
            fail("Expected an error");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    private enum DuplicateDomainEnum_0 implements TestPartitionedEnum
    {
        FOO;

        public String domain()
        {
            return "DUPLICATE";
        }
    }

    private enum DuplicateDomainEnum_1 implements TestPartitionedEnum
    {
        BAR;

        public String domain()
        {
            return "DUPLICATE";
        }
    }

    private enum MixedCaseEnum implements TestPartitionedEnum
    {
        fOO, BAR, BAZ;

        public String domain()
        {
            return "MIXED_CASE";
        }
    }

    private enum WithPeriodInDomain implements TestPartitionedEnum
    {
        FOO, BAR, BAZ;

        public String domain()
        {
            return "INVALID.DOMAIN";
        }
    }
}
