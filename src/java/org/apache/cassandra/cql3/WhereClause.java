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

import java.util.List;
import java.util.Objects;

import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.cql3.restrictions.ComplexBooleanExpression;
import org.apache.cassandra.cql3.restrictions.CustomIndexExpression;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.join;

public final class WhereClause
{
    private static final WhereClause EMPTY = new WhereClause(new Builder());

    private final ComplexBooleanExpression complexBooleanExpression;

    private WhereClause(Builder builder)
    {
        complexBooleanExpression = builder.complexBooleanExpression;
    }

    public static WhereClause empty()
    {
        return EMPTY;
    }

    public boolean containsCustomExpressions()
    {
        return !complexBooleanExpression.expressions().isEmpty();
    }

    public List<Relation> relations()
    {

        return complexBooleanExpression.relations();
    }

    public List<CustomIndexExpression> expressions()
    {
        return complexBooleanExpression.expressions();
    }

    public boolean containsDisjunction()
    {
        return complexBooleanExpression.containsDisjunction();
    }

    public ComplexBooleanExpression complexBooleanExpression()
    {
        return complexBooleanExpression;
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

        complexBooleanExpression.relations().stream()
                                            .map(r -> r.renameIdentifier(from, to))
                                            .forEach(builder::add);

        complexBooleanExpression.expressions().forEach(builder::add);

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
        return complexBooleanExpression.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof WhereClause))
            return false;

        WhereClause wc = (WhereClause) o;
        return complexBooleanExpression.equals(wc.complexBooleanExpression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(complexBooleanExpression);
    }

    public static final class Builder
    {
        ComplexBooleanExpression.Builder builder = new ComplexBooleanExpression.Builder();
        ComplexBooleanExpression complexBooleanExpression;

        public Builder add(Relation relation)
        {
            builder.add(relation);
            return this;
        }

        public Builder add(CustomIndexExpression expression)
        {
            builder.add(expression);
            return this;
        }

        public Builder startEnclosure()
        {
            builder.startEnclosure();
            return this;
        }

        public Builder endEnclosure()
        {
            builder.endEnclosure();
            return this;
        }

        public Builder setCurrentOperator(String operator)
        {
            builder.setCurrentOperator(operator);
            return this;
        }

        public WhereClause build()
        {
            complexBooleanExpression = builder.build();
            return new WhereClause(this);
        }
    }
}
