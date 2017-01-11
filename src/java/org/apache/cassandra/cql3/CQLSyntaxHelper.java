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

import java.util.Iterator;
import java.util.Map;

/**
 * Simple static helper methods to create valid CQL syntax.
 */
public class CQLSyntaxHelper
{
    public static String toCQLMap(Map<String, String> map)
    {
        if (map.isEmpty())
            return "{}";


        StringBuilder sb = new StringBuilder();
        sb.append('{');

        Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();
        appendMapEntry(sb, iterator.next());
        while (iterator.hasNext())
            appendMapEntry(sb.append(", "), iterator.next());

        sb.append('}');
        return sb.toString();
    }

    private static void appendMapEntry(StringBuilder sb, Map.Entry<String, String> entry)
    {
        sb.append(toCQLString(entry.getKey())).append(": ").append(toCQLString(entry.getValue()));
    }

    public static String toCQLString(String str)
    {
        return '\'' + str.replaceAll("'", "''") + '\'';
    }
}
