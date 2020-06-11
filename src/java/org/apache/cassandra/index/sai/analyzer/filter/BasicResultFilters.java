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
package org.apache.cassandra.index.sai.analyzer.filter;

import java.text.Normalizer;
import java.util.Locale;

/**
 * Basic/General Token Filters
 */
public class BasicResultFilters
{
    private static final Locale DEFAULT_LOCALE = Locale.getDefault();

    public static class LowerCase extends FilterPipelineTask
    {
        private final Locale locale;

        public LowerCase()
        {
            this.locale = DEFAULT_LOCALE;
        }

        public String process(String input)
        {
            return input.toLowerCase(locale);
        }
    }

    public static class Normalize extends FilterPipelineTask
    {
        public Normalize() { }

        public String process(String input)
        {   
            return input == null || Normalizer.isNormalized(input, Normalizer.Form.NFC) ? input : Normalizer.normalize(input, Normalizer.Form.NFC);
        }
    }

    public static class NoOperation extends FilterPipelineTask
    {
        public String process(String input)
        {
            return input;
        }
    }
}
