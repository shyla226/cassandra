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

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.IndexName;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public final class IndexRestrictions
{
    /**
     * The empty {@code IndexRestrictions}.
     */
    private static final IndexRestrictions EMPTY_RESTRICTIONS = new IndexRestrictions(ImmutableList.of(), ImmutableList.of());

    public static final String INDEX_NOT_FOUND = "Invalid index expression, index %s not found for %s.%s";
    public static final String INVALID_INDEX = "Target index %s cannot be used to query %s.%s";
    public static final String CUSTOM_EXPRESSION_NOT_SUPPORTED = "Index %s does not support custom expressions";
    public static final String NON_CUSTOM_INDEX_IN_EXPRESSION = "Only CUSTOM indexes may be used in custom index expressions, %s is not valid";
    public static final String MULTIPLE_EXPRESSIONS = "Multiple custom index expressions in a single query are not supported";

    private final ImmutableList<Restrictions> regularRestrictions;
    private final ImmutableList<ExternalRestriction> externalRestrictions;

    private IndexRestrictions(ImmutableList<Restrictions> regularRestrictions, ImmutableList<ExternalRestriction> externalExpressions){
        this.regularRestrictions = regularRestrictions;
        this.externalRestrictions = externalExpressions;
    }

    /**
     * Returns an empty {@code IndexRestrictions}.
     * @return an empty {@code IndexRestrictions}
     */
    public static IndexRestrictions of()
    {
        return EMPTY_RESTRICTIONS;
    }

    /**
     * Creates a new {@code IndexRestrictions.Builder} instance.
     * @return a new {@code IndexRestrictions.Builder} instance.
     */
    public static Builder builder()
    {
        return new IndexRestrictions.Builder();
    }

    public boolean isEmpty()
    {
        return regularRestrictions.isEmpty() && externalRestrictions.isEmpty();
    }

    /**
     * Returns the regular restrictions.
     * @return the regular restrictions
     */
    public ImmutableList<Restrictions> getRestrictions()
    {
        return regularRestrictions;
    }

    /**
     * Returns the external restrictions.
     * @return the external restrictions
     */
    public ImmutableList<ExternalRestriction> getExternalExpressions()
    {
        return externalRestrictions;
    }

    static InvalidRequestException invalidIndex(IndexName indexName, CFMetaData cfm)
    {
        return invalidRequest(INVALID_INDEX, indexName.getIdx(), cfm.ksName, cfm.cfName);
    }

    static InvalidRequestException indexNotFound(IndexName indexName, CFMetaData cfm)
    {
        return invalidRequest(INDEX_NOT_FOUND,indexName.getIdx(), cfm.ksName, cfm.cfName);
    }

    static InvalidRequestException nonCustomIndexInExpression(IndexName indexName)
    {
        return invalidRequest(NON_CUSTOM_INDEX_IN_EXPRESSION, indexName.getIdx());
    }

    static InvalidRequestException customExpressionNotSupported(IndexName indexName)
    {
        return invalidRequest(CUSTOM_EXPRESSION_NOT_SUPPORTED, indexName.getIdx());
    }

    /**
     * Builder for IndexRestrictions.
     */
    public static final class Builder
    {
        /**
         * Builder for the regular restrictions.
         */
        private ImmutableList.Builder<Restrictions> regularRestrictions = ImmutableList.builder();

        /**
         * Builder for the custom expressions.
         */
        private ImmutableList.Builder<ExternalRestriction> externalRestrictions = ImmutableList.builder();

        private Builder() {}

        /**
         * Adds the specified restrictions.
         *
         * @param restrictions the restrictions to add
         * @return this {@code Builder}
         */
        public Builder add(Restrictions restrictions)
        {
            regularRestrictions.add(restrictions);
            return this;
        }

        /**
         * Adds the restrictions and custom expressions from the specified {@code IndexRestrictions}.
         *
         * @param restrictions the restrictions and custom expressions to add
         * @return this {@code Builder}
         */
        public Builder add(IndexRestrictions restrictions)
        {
            regularRestrictions.addAll(restrictions.regularRestrictions);
            externalRestrictions.addAll(restrictions.externalRestrictions);
            return this;
        }

        /**
         * Adds the specified index expression.
         *
         * @param restriction the index expression to add
         * @return this {@code Builder}
         */
        public Builder add(ExternalRestriction restriction)
        {
            externalRestrictions.add(restriction);
            return this;
        }

        /**
         * Builds a new {@code IndexRestrictions} instance
         * @return a new {@code IndexRestrictions} instance
         */
        public IndexRestrictions build()
        {
            return new IndexRestrictions(regularRestrictions.build(), externalRestrictions.build());
        }
    }
}
