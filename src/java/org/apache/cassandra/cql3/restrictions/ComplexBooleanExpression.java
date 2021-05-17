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

import com.google.common.collect.ImmutableList;

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
    private String cachedStringRepresentation;

    private ComplexBooleanExpression(Builder builder)
    {
        this.root = builder.root;
        this.relations = builder.relations.build();
        this.expressions = builder.expressions.build();
        this.containsDisjunction = builder.containsDisjunction;
    }

    public Expression<QueryExpression> root()
    {
        return root;
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
        if (cachedStringRepresentation == null)
        {
            if (root == null)
                cachedStringRepresentation = "";
            else
            {
                cachedStringRepresentation = root.sort(Expression.LEXICOGRAPHIC_COMPARATOR)
                                                 .toString()
                                                 .replaceAll("&", "AND")
                                                 .replaceAll("\\|", "OR");
                cachedStringRepresentation = cachedStringRepresentation.startsWith("(")
                                             ? cachedStringRepresentation.substring(1, cachedStringRepresentation.length() - 1)
                                             : cachedStringRepresentation;
            }
        }
        return cachedStringRepresentation;
    }

    public static class Builder
    {
        private final Deque<List<Expression<QueryExpression>>> expressionStack = new ArrayDeque<>();
        private final Deque<String> operatorStack = new ArrayDeque<>();

        private String operator = "";
        private Expression<QueryExpression> root;
        private List<Expression<QueryExpression>> current = new ArrayList<>();
        private boolean containsDisjunction;
        ImmutableList.Builder<Relation> relations = new ImmutableList.Builder<>();
        ImmutableList.Builder<CustomIndexExpression> expressions = new ImmutableList.Builder<>();

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
            if (!this.operator.isEmpty() && !this.operator.equalsIgnoreCase(operator))
            {
                root = generate();
                current.clear();
            }
            this.operator = operator;
            if (operator.equalsIgnoreCase("OR"))
                containsDisjunction = true;
        }

        public ComplexBooleanExpression build()
        {
            root = RuleSet.simplify(generate());
            root.getAllK().forEach(exp -> {
                if (exp instanceof Relation)
                    relations.add((Relation)exp);
                else
                    expressions.add((CustomIndexExpression)exp);
            });
            return new ComplexBooleanExpression(this);
        }

        private Expression<QueryExpression> generate()
        {
            if (current.isEmpty())
                return root;
            if (root != null)
                current.add(root);
            return operator.equalsIgnoreCase("AND") ? And.of(current)
                                                    : Or.of(current);
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
                return Collections.emptyList();
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
            public int hashCode()
            {
                return queryExpression.hashCode();
            }

            @Override
            public boolean equals(Object obj)
            {
                return (obj instanceof InternalExpression) && queryExpression.equals(((InternalExpression) obj).queryExpression);
            }

            @Override
            public String toString()
            {
                return queryExpression.toString();
            }
        }
    }
}
