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

package org.apache.cassandra.index.sai.cql;

import java.util.HashMap;

import org.junit.Test;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Or;
import com.bpodgursky.jbool_expressions.Variable;
import com.bpodgursky.jbool_expressions.parsers.ExprParser;
import com.bpodgursky.jbool_expressions.rules.RuleSet;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.schema.CreateIndexStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.TableMetadata;

public class ComplexExpressionTest
{
    @Test
    public void testGrammar() throws Throwable
    {
        WhereClause whereClause = WhereClause.parse("a = 1 and b = 2 or c = 3");
        System.out.println(whereClause.toCQLString());
    }

//    @Test
//    public void testRestrictions() throws Throwable
//    {
//        createTable("CREATE TABLE %s (pk int primary key, a int, b int, c int)");
//        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
//        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
//        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
//
//        SelectStatement.RawStatement rawStatement = parseStatement(String.format("SELECT * FROM %s.%s WHERE a = 1 and b = 2 or c = 3",KEYSPACE, currentTable()));
//
//        SelectStatement statement = rawStatement.prepare(false);
//
//        System.out.println();
//    }

    @Test
    public void testJbool() throws Throwable
    {
        Expression<String> expression = And.of(Variable.of("a = 1"), Variable.of("b = 2"), Variable.of("c = 3"));
        System.out.println(expression);
//        expression = RuleSet.simplify(expression);
//        System.out.println(expression);

//        Expression<String> expression = RuleSet.simplify(ExprParser.parse("'a = 1' & 'b = 2' | 'c = 3'"));
//        System.out.println(expression);
    }

    private SelectStatement.RawStatement parseStatement(String query) throws Throwable
    {
        return CQLFragmentParser.parseAnyUnhandled(CqlParser::selectStatement, query);
    }
}
