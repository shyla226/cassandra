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
    public void lineNumbersTest()
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

        List<Class> classes = Arrays.asList(lambda.getClass(), lambda2.getClass(), lambda3.getClass(),
                                            FunInst.class, FunInstStatic.class, AbstractFunInst.class, LineNumbersFn.class,
                                            thisMethodRef.getClass(), methodRef.getClass(), abstractMethodRef.getClass(),
                                            abstractMethodConcreteClassRef.getClass(), constructorAsReferenceClassFunction.getClass());
        Collections.shuffle(classes);

        for (Class klass : classes)
        {
            Assert.assertTrue(klass.getName() + " in " + classes, lineNumbers.maybeProcessClass(klass));
            Assert.assertFalse(klass.getName() + " in " + classes, lineNumbers.maybeProcessClass(klass)); // Make sure that subsequent calls are skipped
        }

        Assert.assertEquals(38, lineNumbers.getLine(lambda.getClass()).line());
        Assert.assertEquals(39, lineNumbers.getLine(lambda2.getClass()).line());
        Assert.assertEquals(40, lineNumbers.getLine(lambda3.getClass()).line());
        Assert.assertEquals(80, lineNumbers.getLine(thisMethodRef.getClass()).line());
        Assert.assertEquals(116, lineNumbers.getLine(methodRef.getClass()).line());
        Assert.assertEquals(112, lineNumbers.getLine(abstractMethodRef.getClass()).line());
        Assert.assertEquals(126, lineNumbers.getLine(abstractMethodConcreteClassRef.getClass()).line());
        Assert.assertEquals(133, lineNumbers.getLine(constructorAsReferenceClassFunction.getClass()).line());
        Assert.assertEquals(96, lineNumbers.getLine(FunInst.class).line());
        Assert.assertEquals(104, lineNumbers.getLine(FunInstStatic.class).line());
        Assert.assertEquals(108, lineNumbers.getLine(AbstractFunInst.class).line());
    }

    private TaggedFunction<Long, String> toFn(TaggedFunction<Long, String> fn)
    {
        return fn;
    }

    // Method to be cast to method reference
    private String methodAsReference(Long l)
    {
        return Long.toString(l); // Line #80
    }

    private Function<Long, ConstructorAsReferenceClass> toFn(Function<Long, ConstructorAsReferenceClass> fn)
    {
        return fn;
    }

    interface TaggedFunction<A, B> extends Function<A, B>
    {
    }

    public class FunInst implements Function<Integer, String>
    {
        public String apply(Integer integer)
        {
            return Integer.toString(integer); // Line #96
        }
    }

    public static class FunInstStatic implements Function<Integer, String>
    {
        public String apply(Integer integer)
        {
            return Integer.toString(integer); // Line #104
        }
    }

    abstract class AbstractFunInst implements Function<Integer, String> // Line #108
    {
    }

    static abstract class StaticClass
    {
        String methodOfStaticClass(Long i)
        {
            return Long.toString(i); // Line #116
        }

        public abstract String abstractMethodOfStaticClass(Long i);
    }

    static class ConcreteStaticClass extends StaticClass
    {
        public String abstractMethodOfStaticClass(Long i)
        {
            return Long.toString(i); // Line #126
        }
    }

    public static class ConstructorAsReferenceClass
    {
        ConstructorAsReferenceClass(Long l)
        { // Line #133
        }
    }
}
