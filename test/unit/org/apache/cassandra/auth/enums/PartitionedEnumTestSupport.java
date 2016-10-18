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

package org.apache.cassandra.auth.enums;

public class PartitionedEnumTestSupport
{
    public interface TestPartitionedEnum extends PartitionedEnum
    { }

    public enum PartOne implements TestPartitionedEnum
    {
        ELEMENT_0, ELEMENT_1, ELEMENT_2, ELEMENT_3, ELEMENT_4;

        public String domain()
        {
            return "PART_1";
        }
    }

    public enum PartTwo implements TestPartitionedEnum
    {
        ELEMENT_0, ELEMENT_1;

        public String domain()
        {
            return "PART_2";
        }
    }

    public enum PartThree implements TestPartitionedEnum
    {
        ELEMENT_0;

        public String domain()
        {
            return "PART_3";
        }
    }
}
