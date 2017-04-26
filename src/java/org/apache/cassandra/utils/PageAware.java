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
package org.apache.cassandra.utils;

import java.io.IOException;

import org.apache.cassandra.io.util.DataOutputPlus;

public interface PageAware
{

    public static final int PAGE_SIZE = 4096;

    public static long pageLimit(long dstPos)
    {
        return (dstPos | (PAGE_SIZE - 1)) + 1;
    }

    public static long pageStart(long dstPos)
    {
        return (dstPos & ~(PAGE_SIZE - 1));
    }

    public static long padded(long dstPos)
    {
        return pageLimit(dstPos - 1);
    }

    public static void pad(DataOutputPlus dest) throws IOException
    {
        long position = dest.position();
        long bytesLeft = pageLimit(position) - position;
        if (bytesLeft < PAGE_SIZE)
            dest.write(EmptyPage.EMPTY_PAGE, 0, (int) bytesLeft);
    }

    class EmptyPage
    {
        static final byte[] EMPTY_PAGE = new byte[PAGE_SIZE];
    }
}