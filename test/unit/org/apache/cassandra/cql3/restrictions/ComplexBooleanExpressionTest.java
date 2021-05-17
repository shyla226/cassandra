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

package org.apache.cassandra.cql3.restrictions;

import org.junit.Test;

import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.exceptions.SyntaxException;

import static org.junit.Assert.assertEquals;

public class ComplexBooleanExpressionTest
{
    @Test(expected = SyntaxException.class)
    public void cannotHaveEmptyWhereClause() throws Throwable
    {
        build("");
    }

    @Test
    public void singleRelationWithoutEnclosure() throws Throwable
    {
        ComplexBooleanExpression cbe = build("a = 1");
        assertEquals(1, cbe.relations().size());
        assertEquals("a = 1", cbe.toString());
    }

    @Test
    public void singleRelationWithEnclosure() throws Throwable
    {
        ComplexBooleanExpression cbe = build("(a = 1)");
        assertEquals(1, cbe.relations().size());
        assertEquals("a = 1", cbe.toString());
    }

    @Test
    public void simpleAndExpressionWithRelationsWithoutEnclosure() throws Throwable
    {
        ComplexBooleanExpression cbe = build("a = 1 and b = 1");
        assertEquals(2, cbe.relations().size());
        assertEquals("a = 1 AND b = 1", cbe.toString());
    }

    @Test
    public void simpleAndExpressionWithRelationsWithEnclosure() throws Throwable
    {
        ComplexBooleanExpression cbe = build("(a = 1 and b = 1)");
        assertEquals(2, cbe.relations().size());
        assertEquals("a = 1 AND b = 1", cbe.toString());
    }

    @Test
    public void multipleAndExpressionWithRelations() throws Throwable
    {
        ComplexBooleanExpression cbe = build("a = 1 and b = 1 and c = 1");
        assertEquals(3, cbe.relations().size());
        assertEquals("a = 1 AND b = 1 AND c = 1", cbe.toString());
    }

    @Test
    public void disjunctionExpression() throws Throwable
    {
        ComplexBooleanExpression cbe = build("a = 1 and b = 1 or c = 1");
        assertEquals(3, cbe.relations().size());
        assertEquals("(a = 1 AND b = 1) OR c = 1", cbe.toString());
    }

    @Test
    public void disjunctionExpressionWithPrecedence() throws Throwable
    {
        ComplexBooleanExpression cbe = build("a = 1 and (b = 1 or c = 1)");
        assertEquals(3, cbe.relations().size());
        assertEquals("(b = 1 OR c = 1) AND a = 1", cbe.toString());
    }

    @Test
    public void expressionIsSimplified() throws Throwable
    {
        ComplexBooleanExpression cbe = build("a = 1 and (a = 1 or b = 1 or c = 1)");
        assertEquals(1, cbe.relations().size());
        assertEquals("a = 1", cbe.toString());
    }

    private ComplexBooleanExpression build(String expression) throws Throwable
    {
        return WhereClause.parse(expression).complexBooleanExpression();
    }
}
