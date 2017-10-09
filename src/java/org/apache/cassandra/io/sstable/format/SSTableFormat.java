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
package org.apache.cassandra.io.sstable.format;

import com.google.common.base.CharMatcher;

import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.trieindex.TrieIndexFormat;

/**
 * Provides the accessors to data on disk.
 */
public interface SSTableFormat
{
    static boolean enableSSTableDevelopmentTestMode = Boolean.getBoolean("cassandra.test.sstableformatdevelopment");


    Version getLatestVersion();
    Version getVersion(String version);

    SSTableWriter.Factory getWriterFactory();
    SSTableReader.Factory getReaderFactory();

    public static SSTableFormat current()
    {
        return Type.current().info;
    }

    public static Type streamWriteFormat()
    {
        // When we receive streams we write them in trie format regardless if they came from big format source.
        // This relies on compatible sstable formats.
        return Type.current();
    }

    /**
     * @param ver SSTable version
     * @return True if the given version string matches the format.
     */
    public boolean validateVersion(String ver);

    public enum Type
    {
        //The original sstable format
        BIG("big", BigFormat.instance),

        //Sstable format with trie indices
        TRIE_INDEX("bti", TrieIndexFormat.instance);

        public final SSTableFormat info;
        public final String name;

        public static Type current()
        {
            return TRIE_INDEX;
        }

        Type(String name, SSTableFormat info)
        {
            //Since format comes right after generation
            //we disallow formats with numeric names
            assert !CharMatcher.DIGIT.matchesAllOf(name);

            this.name = name;
            this.info = info;
        }

        /**
         * @param ver SSTable version
         * @return True if the given version string matches the format.
         */
        public boolean validateVersion(String ver)
        {
            return info.validateVersion(ver);
        }

        public static Type validate(String name)
        {
            for (Type valid : Type.values())
            {
                if (valid.name.equalsIgnoreCase(name))
                    return valid;
            }

            throw new IllegalArgumentException("No Type constant " + name);
        }
    }
}
