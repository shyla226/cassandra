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

import org.junit.Test;

import org.apache.cassandra.io.sstable.format.SSTableFormat;

/**
 * Tests randomly causing re-read with NotInCacheException.
 */
public class AsyncReadBigFormatTest extends AsyncReadTestImpl
{
    @Test
    public void testWideIndexingForwardBig() throws Throwable
    {
        testWideIndexingForward(SSTableFormat.Type.BIG);
    }

    @Test
    public void testWideIndexingReversedBig() throws Throwable
    {
        testWideIndexingReversed(SSTableFormat.Type.BIG);
    }

    @Test
    public void testWideIndexForwardInBig() throws Throwable
    {
        testWideIndexForwardIn(SSTableFormat.Type.BIG);
    }

    @Test
    public void testWideIndexReversedInBig() throws Throwable
    {
        testWideIndexReversedIn(SSTableFormat.Type.BIG);
    }

    @Test
    public void testForwardBig() throws Throwable
    {
        testForward(SSTableFormat.Type.BIG);
    }

    @Test
    public void testReversedBig() throws Throwable
    {
        testReversed(SSTableFormat.Type.BIG);
    }

    @Test
    public void testRangeQueriesBig() throws Throwable
    {
        testRangeQueries(SSTableFormat.Type.BIG);
    }
}