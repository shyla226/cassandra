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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Or;
import com.bpodgursky.jbool_expressions.options.ExprOptions;
import com.bpodgursky.jbool_expressions.rules.RuleList;
import com.bpodgursky.jbool_expressions.rules.RuleSet;
import com.bpodgursky.jbool_expressions.util.ExprFactory;
import org.apache.cassandra.cql3.Relation;

public class ComplexBooleanExpression
{
    private final Expression<QueryExpression> root;
    private final List<Relation> relations;
    private final List<CustomIndexExpression> expressions;
    private final boolean containsDisjunction;

    private ComplexBooleanExpression(Builder builder)
    {
        this.root = builder.root;
        this.relations = builder.relations;
        this.expressions = builder.expressions;
        this.containsDisjunction = builder.containsDisjunction;
    }

    public List<Relation> relations()
    {
        return relations;
    }

    public List<CustomIndexExpression> expressions()
    {
        return expressions;
    }

    public boolean containsDisjunction()
    {
        return containsDisjunction;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relations, expressions);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;

        if (!(obj instanceof ComplexBooleanExpression))
            return false;

        ComplexBooleanExpression cbe = (ComplexBooleanExpression) obj;
        return relations.equals(cbe.relations) && expressions.equals(cbe.expressions);
    }

    @Override
    public String toString()
    {
        return super.toString();
    }

    public static class Builder
    {
        private String operator = "AND";
        private Expression<QueryExpression> root;
        private Deque<List<Expression<QueryExpression>>> expressionStack = new ArrayDeque<>();
        private Deque<String> operatorStack = new ArrayDeque<>();
        private List<Expression<QueryExpression>> current = new ArrayList<>();
        private boolean containsDisjunction;
        private List<Relation> relations;
        private List<CustomIndexExpression> expressions;

        public void add(QueryExpression queryExpression)
        {
            current.add(new InternalExpression(queryExpression));
        }

        public void startEnclosure()
        {
            expressionStack.push(current);
            current = new ArrayList<>();
            operatorStack.push(operator);
        }

        public void endEnclosure()
        {
            root = generate();
            current = expressionStack.pop();
            operator = operatorStack.pop();
        }

        public void setCurrentOperator(String operator)
        {
            this.operator = operator;
            if (operator.equalsIgnoreCase("OR"))
                containsDisjunction = true;
        }

        public ComplexBooleanExpression build()
        {
            root = RuleSet.simplify(generate());
            relations = root.getAllK().stream()
                            .filter(exp -> (exp instanceof Relation))
                            .map(exp -> ((Relation)exp))
                            .collect(Collectors.toList());
            expressions = root.getAllK().stream()
                              .filter(exp -> (exp instanceof CustomIndexExpression))
                              .map(exp -> ((CustomIndexExpression)exp))
                              .collect(Collectors.toList());
            return new ComplexBooleanExpression(this);
        }

        private Expression<QueryExpression> generate()
        {
            if (current.isEmpty())
                return root;
            if (root != null)
                current.add(root);
            return operator.equalsIgnoreCase("AND") ? And.of(current) : Or.of(current);
        }

        private static class InternalExpression extends Expression<QueryExpression>
        {
            private final QueryExpression queryExpression;

            public InternalExpression(QueryExpression queryExpression)
            {
                this.queryExpression = queryExpression;
            }

            @Override
            public Expression<QueryExpression> apply(RuleList<QueryExpression> ruleList, ExprOptions<QueryExpression> exprOptions)
            {
                return this;
            }

            @Override
            public List<Expression<QueryExpression>> getChildren()
            {
                return Collections.EMPTY_LIST;
            }

            @Override
            public Expression<QueryExpression> map(Function<Expression<QueryExpression>,
                                                           Expression<QueryExpression>> function,
                                                   ExprFactory<QueryExpression> exprFactory)
            {
                return this;
            }

            @Override
            public String getExprType()
            {
                return "QueryExpression";
            }

            @Override
            public Expression<QueryExpression> sort(Comparator<Expression> comparator)
            {
                return this;
            }

            @Override
            public void collectK(Set<QueryExpression> set, int i)
            {
                set.add(this.queryExpression);
            }

            @Override
            public Expression<QueryExpression> replaceVars(Map<QueryExpression,
                                                              Expression<QueryExpression>> map,
                                                           ExprFactory<QueryExpression> exprFactory)
            {
                return this;
            }

            @Override
            public String toString()
            {
                return queryExpression.toString();
            }
        }
    }
}
