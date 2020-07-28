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
package org.apache.cassandra.index.sai.analyzer;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.utils.TypeUtil;

public abstract class AbstractAnalyzer implements Iterator<ByteBuffer>
{
    static final Set<AbstractType<?>> ANALYZABLE_TYPES = ImmutableSet.of(UTF8Type.instance, AsciiType.instance); 
    
    protected ByteBuffer next = null;
    String nextLiteral = null;

    /**
     * @return true if index value is transformed, eg. normalized or lower-cased or tokenized.
     */
    public abstract boolean transformValue();

    /**
     * Note: This method does not advance, as we rely on {@link #hasNext()} to buffer the next value.
     *
     * @return the raw value currently buffered by this iterator
     */
    public ByteBuffer next()
    {
        return next;
    }

    /**
     * Note: This method does not advance, as we rely on {@link #hasNext()} to buffer the next value.
     *
     * @return the string value currently buffered by this iterator
     */
    public String nextLiteral(AbstractType<?> validator)
    {
        if (nextLiteral != null)
        {
            return nextLiteral;
        }

        assert next != null;
        return TypeUtil.getString(next, validator);
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    protected abstract void resetInternal(ByteBuffer input);

    public void reset(ByteBuffer input)
    {
        this.next = null;
        this.nextLiteral = null;

        resetInternal(input);
    }

    public static AbstractAnalyzer fromOptions(AbstractType<?> type, Map<String, String> options)
    {
        if (options.get(NonTokenizingOptions.CASE_SENSITIVE) != null || options.get(NonTokenizingOptions.NORMALIZE) != null)
        {
            if (!ANALYZABLE_TYPES.contains(type))
            {
                throw new InvalidRequestException("CQL type " + type.asCQL3Type() + " cannot be analyzed.");
            }
            
            return new NonTokenizingAnalyzer(type, options);
        }
        
        return new NoOpAnalyzer();
    }
}
