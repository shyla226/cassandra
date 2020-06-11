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
package org.apache.cassandra.index.sai.analyzer;

import java.util.Map;

import com.google.common.base.Strings;

import org.apache.cassandra.exceptions.InvalidRequestException;

public class NonTokenizingOptions
{
    public static final String NORMALIZE = "normalize";
    public static final String CASE_SENSITIVE = "case_sensitive";

    private boolean caseSensitive;
    private boolean normalized;

    boolean isCaseSensitive()
    {
        return caseSensitive;
    }

    void setCaseSensitive(boolean caseSensitive)
    {
        this.caseSensitive = caseSensitive;
    }
    
    boolean isNormalized()
    {
        return this.normalized;
    }
    
    private void setNormalized(boolean normalized)
    {
        this.normalized = normalized;
    }

    public static class OptionsBuilder
    {
        private boolean caseSensitive = true;
        private boolean normalized = false;

        OptionsBuilder() {}

        OptionsBuilder caseSensitive(boolean caseSensitive)
        {
            this.caseSensitive = caseSensitive;
            return this;
        }

        OptionsBuilder normalized(boolean normalized)
        {
            this.normalized = normalized;
            return this;
        }

        public NonTokenizingOptions build()
        {
            NonTokenizingOptions options = new NonTokenizingOptions();
            options.setCaseSensitive(caseSensitive);
            options.setNormalized(normalized);
            return options;
        }
    }

    static NonTokenizingOptions fromMap(Map<String, String> options)
    {
        OptionsBuilder builder = new OptionsBuilder();

        for (Map.Entry<String, String> entry : options.entrySet())
        {
            switch (entry.getKey())
            {
                case CASE_SENSITIVE:
                {
                    boolean boolValue = validateBoolean(entry.getValue(), CASE_SENSITIVE);
                    builder = builder.caseSensitive(boolValue);
                    break;
                }
                
                case NORMALIZE:
                {
                    boolean boolValue = validateBoolean(entry.getValue(), NORMALIZE);
                    builder = builder.normalized(boolValue);
                    break;
                }
            }
        }
        return builder.build();
    }

    private static boolean validateBoolean(String value, String option)
    {
        if (Strings.isNullOrEmpty(value))
        {
            throw new InvalidRequestException("Empty value for boolean option '" + option + "'");
        }

        if (!value.equalsIgnoreCase(Boolean.TRUE.toString()) && !value.equalsIgnoreCase(Boolean.FALSE.toString()))
        {
            throw new InvalidRequestException("Illegal value for boolean option '" + option + "': " + value);
        }

        return Boolean.parseBoolean(value);
    }

    static NonTokenizingOptions getDefaultOptions()
    {
        return new OptionsBuilder().caseSensitive(true).normalized(false).build();
    }
}
