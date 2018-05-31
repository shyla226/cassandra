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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.junit.Test;

import org.junit.Assert;

// This test is line number sensitive ¯\_(ツ)_/¯
public class LineNumberInferenceTest
{
    @Test
    public void lineNumbersTest() throws Throwable
    {
        LineNumberInference lineNumbers = new LineNumberInference();

        TaggedFunction<Short, String> lambda = (a) -> Short.toString(a); // Line #38
        TaggedFunction<Short, String> lambda2 = (a) -> Short.toString(a); // Line #39
        Function<Short, String> lambda3 = (a) -> Short.toString(a); // Line #40
        Function<Long, String> thisMethodRef = toFn(this::methodAsReference);
        Function<Long, String> methodRef = toFn(new ConcreteStaticClass()::methodOfStaticClass);
        Function<Long, String> abstractMethodRef = toFn(((StaticClass)new ConcreteStaticClass())::abstractMethodOfStaticClass);
        Function<Long, String> abstractMethodConcreteClassRef = toFn((new ConcreteStaticClass())::abstractMethodOfStaticClass);
        Function<Long, ConstructorAsReferenceClass> constructorAsReferenceClassFunction = toFn(ConstructorAsReferenceClass::new);

        lineNumbers.preloadLambdas();

        List<Class> classes = Arrays.asList(lambda.getClass(), lambda2.getClass(), lambda3.getClass(),
                                            FunInst.class, FunInstStatic.class, AbstractFunInst.class, LineNumbersFn.class,
                                            thisMethodRef.getClass(), methodRef.getClass(), abstractMethodRef.getClass(),
                                            abstractMethodConcreteClassRef.getClass(), constructorAsReferenceClassFunction.getClass());
        Collections.shuffle(classes);

        for (Class klass : classes)
        {
            Assert.assertTrue(lineNumbers.maybeProcessClass(klass));
            Assert.assertFalse(lineNumbers.maybeProcessClass(klass)); // Make sure that subsequent calls are skipped
        }

        Assert.assertEquals(lineNumbers.getLine(lambda.getClass()).line(), 38);
        Assert.assertEquals(lineNumbers.getLine(lambda2.getClass()).line(), 39);
        Assert.assertEquals(lineNumbers.getLine(lambda3.getClass()).line(), 40);
        Assert.assertEquals(lineNumbers.getLine(thisMethodRef.getClass()).line(), 82);
        Assert.assertEquals(lineNumbers.getLine(methodRef.getClass()).line(), 118);
        Assert.assertEquals(lineNumbers.getLine(abstractMethodRef.getClass()).line(), 114);
        Assert.assertEquals(lineNumbers.getLine(abstractMethodConcreteClassRef.getClass()).line(), 128);
        Assert.assertEquals(lineNumbers.getLine(constructorAsReferenceClassFunction.getClass()).line(), 135);
        Assert.assertEquals(lineNumbers.getLine(FunInst.class).line(), 94);
        Assert.assertEquals(lineNumbers.getLine(FunInstStatic.class).line(), 102);
        Assert.assertEquals(lineNumbers.getLine(AbstractFunInst.class).line(), 110);
    }

    private TaggedFunction<Long, String> toFn(TaggedFunction<Long, String> fn)
    {
        return fn;
    }

    // Method to be cast to method reference
    private String methodAsReference(Long l)
    {
        return Long.toString(l); // Line #82
    }

    private Function<Long, ConstructorAsReferenceClass> toFn(Function<Long, ConstructorAsReferenceClass> fn)
    {
        return fn;
    }

    public interface TaggedFunction<A, B> extends Function<A, B>
    {
    }

    public class FunInst implements Function<Integer, String> // Line #94
    {
        public String apply(Integer integer)
        {
            return Integer.toString(integer);
        }
    }

    public static class FunInstStatic implements Function<Integer, String> // Line #102
    {
        public String apply(Integer integer)
        {
            return Integer.toString(integer);
        }
    }

    public abstract class AbstractFunInst implements Function<Integer, String> // Line #110
    {
    }

    static abstract class StaticClass
    {
        public String methodOfStaticClass(Long i)
        {
            return Long.toString(i); // Line #118
        }

        public abstract String abstractMethodOfStaticClass(Long i);
    }

    static class ConcreteStaticClass extends StaticClass
    {
        public String abstractMethodOfStaticClass(Long i)
        {
            return Long.toString(i); // Line #128
        }
    }

    public static class ConstructorAsReferenceClass
    {
        public ConstructorAsReferenceClass(Long l)
        { // Line #135
        }
    }
}
