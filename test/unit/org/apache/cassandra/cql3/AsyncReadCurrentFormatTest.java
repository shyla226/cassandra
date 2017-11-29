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

/**
 * Tests randomly causing re-read with NotInCacheException.
 */
public class AsyncReadCurrentFormatTest extends AsyncReadTestImpl
{
    @Test
    public void testWideIndexingForward() throws Throwable
    {
        testWideIndexingForward(null);
    }

    @Test
    public void testWideIndexingReversed() throws Throwable
    {
        testWideIndexingReversed(null);
    }

    @Test
    public void testWideIndexForwardIn() throws Throwable
    {
        testWideIndexForwardIn(null);
    }

    @Test
    public void testWideIndexReversedIn() throws Throwable
    {
        testWideIndexReversedIn(null);
    }

    @Test
    public void testForward() throws Throwable
    {
        testForward(null);
    }

    @Test
    public void testReversed() throws Throwable
    {
        testReversed(null);
    }

    @Test
    public void testRangeQueries() throws Throwable
    {
        testRangeQueries(null);
    }
}