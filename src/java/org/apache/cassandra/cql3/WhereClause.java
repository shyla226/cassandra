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
package org.apache.cassandra.cql3;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Or;
import com.bpodgursky.jbool_expressions.rules.Rule;
import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.cql3.restrictions.CustomIndexExpression;

import static java.lang.String.join;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;

public final class WhereClause
{
    private static final WhereClause EMPTY = new WhereClause(new Builder());

    private final List<Relation> relations;
    private final List<CustomIndexExpression> expressions;
    // Set for a WhereClause with either one predicate or multiple predicates all joined by AND
    private final boolean isPureConjunctionOrSingleton;
    // Set for a WhereClause with multiple predicates all joined by OR
    private final boolean isCompositeDisjunction;
    // This is a dead field; it was intended to help smuggle a tree structure through C* to SAI, where it would be
    // translated to an equivalent tree made out of SAI-internal AND/OR connectives
    private final Expression treeRoot;

    private WhereClause(Builder builder)
    {
        this.treeRoot = builder.currentTree;
        relations = builder.relations.build();
        expressions = builder.expressions.build();
        this.isPureConjunctionOrSingleton = builder.seenOnlyAnd;
        this.isCompositeDisjunction = !builder.seenOnlyAnd && builder.seenOnlyOr && 1 < this.relations.size() + this.expressions.size();
    }

    public static WhereClause empty()
    {
        return EMPTY;
    }

    public boolean isCompositeDisjunction()
    {
        return isCompositeDisjunction;
    }

    public boolean containsCustomExpressions()
    {
        return !expressions.isEmpty();
    }

    public List<Relation> getRelations()
    {

        Preconditions.checkState(isPureConjunctionOrSingleton);
        return getRelationsUnsafe();
    }

    public List<Relation> getRelationsUnsafe()
    {
        return relations;
    }

    public List<CustomIndexExpression> getExpressions()
    {
        Preconditions.checkState(isPureConjunctionOrSingleton);
        return expressions;
    }

    /**
     * Renames identifiers in all relations
     * @param from the old identifier
     * @param to the new identifier
     * @return a new WhereClause with with "from" replaced by "to" in all relations
     */
    public WhereClause renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
    {
        WhereClause.Builder builder = new WhereClause.Builder();

        relations.stream()
                 .map(r -> r.renameIdentifier(from, to))
                 .forEach(builder::add);

        expressions.forEach(builder::add);

        return builder.build();
    }

    public static WhereClause parse(String cql) throws RecognitionException
    {
        return CQLFragmentParser.parseAnyUnhandled(CqlParser::whereClause, cql).build();
    }

    @Override
    public String toString()
    {
        return toCQLString();
    }

    /**
     * Returns a CQL representation of this WHERE clause.
     *
     * @return a CQL representation of this WHERE clause
     */
    public String toCQLString()
    {
        return join(" AND ",
                    concat(transform(relations, Relation::toCQLString),
                           transform(expressions, CustomIndexExpression::toCQLString)));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof WhereClause))
            return false;

        WhereClause wc = (WhereClause) o;
        return relations.equals(wc.relations) && expressions.equals(wc.expressions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relations, expressions);
    }

    // This and CIEExpr both exist solely for the jbool_expressions tree, which is an unused stub from a
    // barely-initiated idea that wasn't fully implemented.  If the tree goes, these inner classes can go, too.
    public static class RelationExpression extends Expression<Relation>
    {
        private final Relation relation;

        public RelationExpression(Relation r)
        {
            this.relation = r;
        }

        @Override
        public Expression<Relation> apply(List<Rule<?, Relation>> list)
        {
            return this;
        }

        @Override
        public String getExprType()
        {
            return "Relation";
        }

        @Override
        public Expression<Relation> sort(Comparator<Expression> comparator)
        {
            return this;
        }
    }

    public static class CIEExpr extends Expression<CustomIndexExpression>
    {
        private final CustomIndexExpression cie;

        public CIEExpr(CustomIndexExpression cie)
        {
            this.cie = cie;
        }

        @Override
        public Expression<CustomIndexExpression> apply(List<Rule<?, CustomIndexExpression>> rules)
        {
            return this;
        }

        @Override
        public String getExprType()
        {
            return "CustomIndexExpression";
        }

        @Override
        public Expression<CustomIndexExpression> sort(Comparator<Expression> comparator)
        {
            return this;
        }
    }

    public static final class Builder
    {
        ImmutableList.Builder<Relation> relations = new ImmutableList.Builder<>();
        ImmutableList.Builder<CustomIndexExpression> expressions = new ImmutableList.Builder<>();
        // This is a hack; this field should be deleted, and the associated state should be pushed down into the
        // ANTLR-generated parser, by specifying rules like
        //      a=relationOrExpression K_OR  b=relationOrExpression { builder.or(a, b);  }
        //    | a=relationOrExpression K_AND b=relationOrExpression { builder.and(a, b); }
        // (I had to hand this issue off before getting to this change)
        private String currentOp;
        // Eligible for removal along the same lines as tree, CIEExpr, RelationExpr
        private Expression currentTree;
        private boolean seenOnlyAnd = true;
        private boolean seenOnlyOr = true;

        public Builder add(Relation relation)
        {
            relations.add(relation);
            addToTree(new RelationExpression(relation));
            return this;
        }

        public Builder add(CustomIndexExpression expression)
        {
            expressions.add(expression);
            addToTree(new CIEExpr(expression));
            return this;
        }

        private void addToTree(Expression e)
        {
            try
            {
                if (null == currentTree)
                {
                    currentTree = e;
                }
                else
                {
                    //Preconditions.checkState(null != currentOp);
                    if (null == currentOp)
                    {
                        currentOp = "AND";
                    }
                    if ("AND".equalsIgnoreCase(currentOp))
                    {
                        currentTree = And.of(currentTree, e);
                    }
                    else if ("OR".equalsIgnoreCase(currentOp))
                    {
                        currentTree = Or.of(currentTree, e);
                    }
                    else
                    {
                        throw new IllegalArgumentException("Unsupported operation: \"" + currentOp + "\"");
                    }
                }
            }
            finally
            {
                currentOp = null;
            }
        }

        public Builder setCurrentOperator(String op)
        {
            currentOp = op;
            seenOnlyAnd &= "AND".equalsIgnoreCase(op);
            seenOnlyOr &= "OR".equalsIgnoreCase(op);
            return this;
        }

        public WhereClause build()
        {
            return new WhereClause(this);
        }
    }
}
