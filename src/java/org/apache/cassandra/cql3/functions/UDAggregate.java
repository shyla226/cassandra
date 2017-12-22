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
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.*;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Base class for user-defined-aggregates.
 */
public class UDAggregate extends AbstractFunction implements AggregateFunction
{
    protected static final Logger logger = LoggerFactory.getLogger(UDAggregate.class);

    private final UDFDataType stateType;
    private final List<UDFDataType> argumentTypes;
    private final UDFDataType resultType;
    protected final ByteBuffer initcond;
    private final ScalarFunction stateFunction;
    private final ScalarFunction finalFunction;
    private final boolean deterministic;

    public UDAggregate(FunctionName name,
                       List<AbstractType<?>> argTypes,
                       AbstractType<?> returnType,
                       ScalarFunction stateFunc,
                       ScalarFunction finalFunc,
                       ByteBuffer initcond,
                       boolean deterministic)
    {
        super(name, argTypes, returnType);
        this.stateFunction = stateFunc;
        this.finalFunction = finalFunc;
        this.argumentTypes = UDFDataType.wrap(argTypes, false);
        this.resultType = UDFDataType.wrap(returnType, false);
        this.stateType = stateFunc != null ? UDFDataType.wrap(stateFunc.returnType(), false) : null;
        this.initcond = initcond;
        this.deterministic = deterministic;
    }

    public static UDAggregate create(Functions functions,
                                     FunctionName name,
                                     List<AbstractType<?>> argTypes,
                                     AbstractType<?> returnType,
                                     FunctionName stateFunc,
                                     FunctionName finalFunc,
                                     AbstractType<?> stateType,
                                     ByteBuffer initcond,
                                     boolean deterministic)
    throws InvalidRequestException
    {
        List<AbstractType<?>> stateTypes = new ArrayList<>(argTypes.size() + 1);
        stateTypes.add(stateType);
        stateTypes.addAll(argTypes);
        List<AbstractType<?>> finalTypes = Collections.singletonList(stateType);
        return new UDAggregate(name,
                               argTypes,
                               returnType,
                               resolveScalar(functions, name, stateFunc, stateTypes),
                               finalFunc != null ? resolveScalar(functions, name, finalFunc, finalTypes) : null,
                               initcond,
                               deterministic);
    }

    public static UDAggregate createBroken(FunctionName name,
                                           List<AbstractType<?>> argTypes,
                                           AbstractType<?> returnType,
                                           ByteBuffer initcond,
                                           boolean deterministic,
                                           InvalidRequestException reason)
    {
        return new UDAggregate(name, argTypes, returnType, null, null, initcond, deterministic)
        {
            public Aggregate newAggregate() throws InvalidRequestException
            {
                throw new InvalidRequestException(String.format("Aggregate '%s' exists but hasn't been loaded successfully for the following reason: %s. "
                                                                + "Please see the server log for more details",
                                                                this,
                                                                reason.getMessage()));
            }
        };
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }

    @Override
    public Arguments newArguments(ProtocolVersion version)
    {
        return FunctionArguments.newInstanceForUdf(version, argumentTypes);
    }

    public boolean hasReferenceTo(Function function)
    {
        return stateFunction == function || finalFunction == function;
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        functions.add(this);
        if (stateFunction != null)
        {
            stateFunction.addFunctionsTo(functions);

            if (finalFunction != null)
                finalFunction.addFunctionsTo(functions);
        }
    }

    public boolean isAggregate()
    {
        return true;
    }

    public boolean isNative()
    {
        return false;
    }

    public ScalarFunction stateFunction()
    {
        return stateFunction;
    }

    public ScalarFunction finalFunction()
    {
        return finalFunction;
    }

    public ByteBuffer initialCondition()
    {
        return initcond;
    }

    public AbstractType<?> stateType()
    {
        return stateType == null ? null : stateType.toAbstractType();
    }

    public Aggregate newAggregate() throws InvalidRequestException
    {
        return new Aggregate()
        {
            private long stateFunctionCount;
            private long stateFunctionDuration;

            private Object state;
            private boolean needsInit = true;

            public void addInput(Arguments arguments) throws InvalidRequestException
            {
                maybeInit(arguments.getProtocolVersion());

                long startTime = System.nanoTime();
                stateFunctionCount++;
                if (stateFunction instanceof UDFunction)
                {
                    UDFunction udf = (UDFunction)stateFunction;
                    if (udf.isCallableWrtNullable(arguments))
                        state = udf.executeForAggregate(state, arguments);
                }
                else
                {
                    throw new UnsupportedOperationException("UDAs only support UDFs");
                }
                stateFunctionDuration += (System.nanoTime() - startTime) / 1000;
            }

            private void maybeInit(ProtocolVersion protocolVersion)
            {
                if (needsInit)
                {
                    state = initcond != null ? stateType.compose(protocolVersion, initcond.duplicate()) : null;
                    stateFunctionDuration = 0;
                    stateFunctionCount = 0;
                    needsInit = false;
                }
            }

            public ByteBuffer compute(ProtocolVersion protocolVersion) throws InvalidRequestException
            {
                maybeInit(protocolVersion);

                // final function is traced in UDFunction
                Tracing.trace("Executed UDA {}: {} call(s) to state function {} in {}\u03bcs", name(), stateFunctionCount, stateFunction.name(), stateFunctionDuration);
                if (finalFunction == null)
                    return stateType.decompose(protocolVersion, state);

                if (finalFunction instanceof UDFunction)
                {
                    UDFunction udf = (UDFunction)finalFunction;
                    Object result = udf.executeForAggregate(state, FunctionArguments.emptyInstance(protocolVersion));
                    return resultType.decompose(protocolVersion, result);
                }
                throw new UnsupportedOperationException("UDAs only support UDFs");
            }

            public void reset()
            {
                needsInit = true;
            }
        };
    }

    private static ScalarFunction resolveScalar(Functions functions, FunctionName aName, FunctionName fName, List<AbstractType<?>> argTypes) throws InvalidRequestException
    {
        Optional<Function> fun = functions.find(fName, argTypes);
        if (!fun.isPresent())
            throw new InvalidRequestException(String.format("Referenced state function '%s %s' for aggregate '%s' does not exist",
                                                            fName,
                                                            AbstractType.asCQLTypeStringList(argTypes),
                                                            aName));

        if (!(fun.get() instanceof ScalarFunction))
            throw new InvalidRequestException(String.format("Referenced state function '%s %s' for aggregate '%s' is not a scalar function",
                                                            fName,
                                                            AbstractType.asCQLTypeStringList(argTypes),
                                                            aName));
        return (ScalarFunction) fun.get();
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof UDAggregate))
            return false;

        UDAggregate that = (UDAggregate) o;
        return Objects.equals(name, that.name)
            && Functions.typesMatch(argTypes, that.argTypes)
            && Functions.typesMatch(returnType, that.returnType)
            && Objects.equals(stateFunction, that.stateFunction)
            && Objects.equals(finalFunction, that.finalFunction)
            && ((stateType == that.stateType) || ((stateType != null) && stateType.equals(that.stateType)))
            && Objects.equals(initcond, that.initcond);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, Functions.typeHashCode(argTypes), Functions.typeHashCode(returnType), stateFunction, finalFunction, stateType, initcond);
    }
}
