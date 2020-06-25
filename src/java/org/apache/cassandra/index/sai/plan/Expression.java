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
package org.apache.cassandra.index.sai.plan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.IndexWriterConfig;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class Expression
{
    private static final Logger logger = LoggerFactory.getLogger(Expression.class);

    public enum Op
    {
        EQ, MATCH, PREFIX, NOT_EQ, RANGE;

        public static Op valueOf(Operator operator)
        {
            switch (operator)
            {
                case EQ:
                    return EQ;

                case NEQ:
                    return NOT_EQ;

                case LT:
                case GT:
                case LTE:
                case GTE:
                    return RANGE;

                case LIKE_PREFIX:
                    return PREFIX;

                case LIKE_MATCHES:
                    return MATCH;

                default:
                    return null;
            }
        }
    }

    private final QueryController controller;

    public final AbstractAnalyzer analyzer;

    public final ColumnContext context;
    public final AbstractType<?> validator;

    @VisibleForTesting
    protected Op operation;

    public Bound lower, upper;
    
    final List<ByteBuffer> exclusions = new ArrayList<>();

    public Expression(Expression other)
    {
        this(other.controller, other.context);
        operation = other.operation;
    }

    public Expression(QueryController controller, ColumnContext columnContext)
    {
        this.controller = controller;
        this.context = columnContext;
        this.analyzer = columnContext.getAnalyzer();
        this.validator = columnContext.getValidator();
    }

    @VisibleForTesting
    public Expression(String name, AbstractType<?> validator)
    {
        this(null, new ColumnContext("test_ks", "test_cf",
                                     UTF8Type.instance, 
                                     new ClusteringComparator(), 
                                     ColumnMetadata.regularColumn("sai", "internal", name, validator),
                                     IndexWriterConfig.emptyConfig()));
    }

    public Expression setLower(Bound newLower)
    {
        lower = newLower == null ? null : new Bound(newLower.value, newLower.inclusive);
        return this;
    }

    public Expression setUpper(Bound newUpper)
    {
        upper = newUpper == null ? null : new Bound(newUpper.value, newUpper.inclusive);
        return this;
    }

    public Expression setOp(Op op)
    {
        this.operation = op;
        return this;
    }

    public Expression add(Operator op, ByteBuffer value)
    {
        value = TypeUtil.encode(value, validator);

        boolean lowerInclusive = false, upperInclusive = false;
        switch (op)
        {
            case LIKE_PREFIX:
            case LIKE_MATCHES:
            case EQ:
                lower = new Bound(value, true);
                upper = lower;
                operation = Op.valueOf(op);
                break;

            case NEQ:
                // index expressions are priority sorted
                // and NOT_EQ is the lowest priority, which means that operation type
                // is always going to be set before reaching it in case of RANGE or EQ.
                if (operation == null)
                {
                    operation = Op.NOT_EQ;
                    lower = new Bound(value, true);
                    upper = lower;
                }
                else
                    exclusions.add(value);
                break;

            case LTE:
                if (context.getDefinition().isReversedType())
                    lowerInclusive = true;
                else
                    upperInclusive = true;
            case LT:
                operation = Op.RANGE;
                if (context.getDefinition().isReversedType())
                    lower = new Bound(value, lowerInclusive);
                else
                    upper = new Bound(value, upperInclusive);
                break;

            case GTE:
                if (context.getDefinition().isReversedType())
                    upperInclusive = true;
                else
                    lowerInclusive = true;
            case GT:
                operation = Op.RANGE;
                if (context.getDefinition().isReversedType())
                    upper = new Bound(value, upperInclusive);
                else
                    lower = new Bound(value, lowerInclusive);

                break;
        }

        return this;
    }

    public boolean isSatisfiedBy(ByteBuffer value)
    {
        value = TypeUtil.encode(value, validator);

        if (!TypeUtil.isValid(value, validator))
        {
            logger.error(context.logMessage("Value is not valid for indexed column {} with {}"), context.getColumnName(), validator);
            return false;
        }

        if (lower != null)
        {
            // suffix check
            if (TypeUtil.isString(validator))
            {
                if (!validateStringValue(value, lower.value))
                    return false;
            }
            else
            {
                // range or (not-)equals - (mainly) for numeric values
                int cmp = TypeUtil.compare(lower.value, value, validator);

                // in case of (NOT_)EQ lower == upper
                if (operation == Op.EQ || operation == Op.NOT_EQ)
                    return cmp == 0;

                if (cmp > 0 || (cmp == 0 && !lower.inclusive))
                    return false;
            }
        }

        if (upper != null && lower != upper)
        {
            // string (prefix or suffix) check
            if (TypeUtil.isString(validator))
            {
                if (!validateStringValue(value, upper.value))
                    return false;
            }
            else
            {
                // range - mainly for numeric values
                int cmp = TypeUtil.compare(upper.value, value, validator);
                if (cmp < 0 || (cmp == 0 && !upper.inclusive))
                    return false;
            }
        }

        // as a last step let's check exclusions for the given field,
        // this covers EQ/RANGE with exclusions.
        for (ByteBuffer term : exclusions)
        {
            if (TypeUtil.isString(validator) && validateStringValue(value, term) || TypeUtil.compare(term, value, validator) == 0)
                return false;
        }

        return true;
    }

    private boolean validateStringValue(ByteBuffer columnValue, ByteBuffer requestedValue)
    {
        analyzer.reset(columnValue.duplicate());
        while (analyzer.hasNext())
        {
            ByteBuffer term = analyzer.next();

            boolean isMatch = false;
            switch (operation)
            {
                case EQ:
                case MATCH:
                // Operation.isSatisfiedBy handles conclusion on !=,
                // here we just need to make sure that term matched it
                case NOT_EQ:
                    isMatch = validator.compare(term, requestedValue) == 0;
                    break;
                case RANGE:
                    isMatch = isLowerSatisfiedBy(term) && isUpperSatisfiedBy(term);
                    break;

                case PREFIX:
                    isMatch = ByteBufferUtil.startsWith(term, requestedValue);
                    break;
            }

            if (isMatch)
                return true;
        }

        return false;
    }

    public Op getOp()
    {
        return operation;
    }

    private boolean hasLower()
    {
        return lower != null;
    }

    private boolean hasUpper()
    {
        return upper != null;
    }

    private boolean isLowerSatisfiedBy(ByteBuffer value)
    {
        if (!hasLower())
            return true;

        int cmp = validator.compare(value, lower.value);
        return cmp > 0 || cmp == 0 && lower.inclusive;
    }

    private boolean isUpperSatisfiedBy(ByteBuffer value)
    {
        if (!hasUpper())
            return true;

        int cmp = validator.compare(value, upper.value);
        return cmp < 0 || cmp == 0 && upper.inclusive;
    }

    public String toString()
    {
        return String.format("Expression{name: %s, op: %s, lower: (%s, %s), upper: (%s, %s), exclusions: %s}",
                             context.getColumnName(),
                             operation,
                             lower == null ? "null" : validator.getString(lower.value),
                             lower != null && lower.inclusive,
                             upper == null ? "null" : validator.getString(upper.value),
                             upper != null && upper.inclusive,
                             Iterators.toString(Iterators.transform(exclusions.iterator(), validator::getString)));
    }

    public int hashCode()
    {
        return new HashCodeBuilder().append(context.getColumnName())
                                    .append(operation)
                                    .append(validator)
                                    .append(lower).append(upper)
                                    .append(exclusions).build();
    }

    public boolean equals(Object other)
    {
        if (!(other instanceof Expression))
            return false;

        if (this == other)
            return true;

        Expression o = (Expression) other;

        return Objects.equals(context.getColumnName(), o.context.getColumnName())
                && validator.equals(o.validator)
                && operation == o.operation
                && Objects.equals(lower, o.lower)
                && Objects.equals(upper, o.upper)
                && exclusions.equals(o.exclusions);
    }

    public static class Bound
    {
        public final ByteBuffer value;
        public final boolean inclusive;

        public Bound(ByteBuffer value, boolean inclusive)
        {
            this.value = value;
            this.inclusive = inclusive;
        }

        public boolean equals(Object other)
        {
            if (!(other instanceof Bound))
                return false;

            Bound o = (Bound) other;
            return value.equals(o.value) && inclusive == o.inclusive;
        }

        public int hashCode()
        {
            HashCodeBuilder builder = new HashCodeBuilder();
            builder.append(value);
            builder.append(inclusive);
            return builder.toHashCode();
        }
    }
}
