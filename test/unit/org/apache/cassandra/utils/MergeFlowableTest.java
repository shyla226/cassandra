/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils;

import java.util.Arrays;

import com.google.common.collect.Ordering;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.reactivex.Flowable;

public class MergeFlowableTest
{
    Flowable<String> all = null, cat = null, a = null, b = null, c = null, d = null;

    @Before
    public void clear()
    {
        all = Flowable.fromArray("1", "2", "3", "3", "4", "5", "6", "7", "8", "8", "9");
        cat = Flowable.just("1", "2", "33", "4", "5", "6", "7", "88", "9");
        a = Flowable.just("1", "3", "5", "8");
        b = Flowable.just("2", "4", "6");
        c = Flowable.just("3", "7", "8", "9");
        d = Flowable.empty();
    }

    /** Test that duplicate values are concatted. */
    @Test
    public void testManyToOne() throws Exception
    {
        Reducer<String,String> reducer = new Reducer<String,String>()
        {
            String concatted = "";

            @Override
            public void reduce(int idx, String current)
            {
                concatted += current;
            }

            public String getReduced()
            {
                String tmp = concatted;
                concatted = "";
                return tmp;
            }
        };
        Flowable<String> smi = MergeFlowable.get(Arrays.asList(a, b, c, d),
                Ordering.<String>natural(),
                reducer);
        Assert.assertTrue(Flowable.sequenceEqual(cat, smi).blockingGet());
    }
}
