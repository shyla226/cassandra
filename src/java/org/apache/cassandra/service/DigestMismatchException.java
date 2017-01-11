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
package org.apache.cassandra.service;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.flow.Flow;

@SuppressWarnings("serial")
public class DigestMismatchException extends Exception implements Flow.NonWrappableException
{
    public DigestMismatchException(ReadCommand command, ByteBuffer digest1, ByteBuffer digest2)
    {
        super(String.format("Mismatch for %s (%s vs %s)",
                            mismatchRegion(command),
                            ByteBufferUtil.bytesToHex(digest1),
                            ByteBufferUtil.bytesToHex(digest2)));
    }

    private static String mismatchRegion(ReadCommand command)
    {
        if (command instanceof SinglePartitionReadCommand)
            return "key " + ((SinglePartitionReadCommand)command).partitionKey();
        else
            return "range " + ((PartitionRangeReadCommand)command).dataRange().toCQLString(command.metadata());
    }
}
