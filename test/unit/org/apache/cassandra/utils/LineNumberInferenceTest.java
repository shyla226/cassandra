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

import java.util.function.Function;

import org.junit.Test;

import org.junit.Assert;

// This test is line number sensitive ¯\_(ツ)_/¯
public class LineNumberInferenceTest
{
    @Test
    public void lineNumbersTest() throws Throwable
    {
        LineNumberInference lineNumbers = new LineNumberInference(Tag.class::isAssignableFrom);

        TaggedFunction<Short, String> lambda = (a) -> Short.toString(a); // Line #35
        TaggedFunction<Short, String> lambda2 = (a) -> Short.toString(a);
        Function<Short, String> lambda3 = (a) -> Short.toString(a);
        Function<Long, String> methodRef = toFn(this::methodAsReference);

        lineNumbers.preloadLambdas();

        lineNumbers.maybeProcessClass(lambda.getClass());
        lineNumbers.maybeProcessClass(lambda3.getClass());
        lineNumbers.maybeProcessClass(FunInst.class);
        lineNumbers.maybeProcessClass(FunInstStatic.class);
        lineNumbers.maybeProcessClass(AbstractFunInst.class);
        lineNumbers.maybeProcessClass(methodRef.getClass());
        lineNumbers.maybeProcessClass(LineNumbersFn.class);

        Assert.assertEquals(lineNumbers.getLine(lambda.getClass()).right.intValue(), 35);
        Assert.assertEquals(lineNumbers.getLine(lambda2.getClass()).right.intValue(), 36);
        Assert.assertEquals(lineNumbers.getLine(lambda3.getClass()).right.intValue(), -1); // Does not inherit from Tag interface
        Assert.assertEquals(lineNumbers.getLine(LineNumberInference.class).right.intValue(), -1); // Does not inherit from Tag interface
        Assert.assertEquals(lineNumbers.getLine(methodRef.getClass()).right.intValue(), 68);
        Assert.assertEquals(lineNumbers.getLine(FunInst.class).right.intValue(), 79);
        Assert.assertEquals(lineNumbers.getLine(FunInstStatic.class).right.intValue(), 87);
        Assert.assertEquals(lineNumbers.getLine(AbstractFunInst.class).right.intValue(), 95);
    }

    private TaggedFunction<Long, String> toFn(TaggedFunction<Long, String> fn)
    {
        return fn;
    }

    // Method to be cast to method reference
    private String methodAsReference(Long l) // Line # 68
    {
        return Long.toString(l);
    }

    public interface Tag
    {
    }

    public interface TaggedFunction<A, B> extends Function<A, B>, Tag
    {
    }

    public class FunInst implements Function<Integer, String>, Tag // Line #79
    {
        public String apply(Integer integer)
        {
            return Integer.toString(integer);
        }
    }

    public static class FunInstStatic implements Function<Integer, String>, Tag // Line #87
    {
        public String apply(Integer integer)
        {
            return Integer.toString(integer);
        }
    }

    public abstract class AbstractFunInst implements Function<Integer, String>, Tag // Line #95
    {
    }
}
