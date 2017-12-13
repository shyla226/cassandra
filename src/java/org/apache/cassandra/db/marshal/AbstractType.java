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
package org.apache.cassandra.db.marshal;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.MarshalException;

import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FastByteOperations;
import org.github.jamm.Unmetered;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;

import static org.apache.cassandra.db.marshal.AbstractType.ComparisonType.*;

/**
 * Specifies a Comparator for a specific type of ByteBuffer.
 *
 * Note that empty ByteBuffer are used to represent "start at the beginning"
 * or "stop at the end" arguments to get_slice, so the Comparator
 * should always handle those values even if they normally do not
 * represent a valid ByteBuffer for the type being compared.
 */
@Unmetered
public abstract class AbstractType<T> implements Comparator<ByteBuffer>, AssignmentTestable
{
    public static final int VARIABLE_LENGTH = -1;

    public enum ComparisonType
    {
        /**
         * This type should never be compared
         */
        NOT_COMPARABLE,

        /**
         * This type is always compared by its sequence of unsigned bytes
         */
        BYTE_ORDER,

        /**
         * Fixed return comparison.
         */
        FIXED_COMPARE,

        /**
         * This type can be compared by deserializing the value and comparing.
         */
        FIXED_SIZE_VALUE,

        /**
         * This type can only be compared by calling the type's compareCustom() method, which may be expensive.
         * Support for this may be removed in a major release of Cassandra, however upgrade facilities will be
         * provided if and when this happens.
         */
        CUSTOM
    }

    public enum FixedSizeType
    {
        BOOLEAN, BYTE, SHORT, INT32, LONG, FLOAT, DOUBLE, NONE
    }

    protected final ComparisonType comparisonType;
    protected final FixedSizeType fixedSizeType;
    protected final int fixedCompareReturns;
    private final boolean isReversed;
    private final int valueLength;

    protected AbstractType(ComparisonType comparisonType)
    {
        this(comparisonType, VARIABLE_LENGTH, false);
    }
    protected AbstractType(ComparisonType comparisonType, int valueLength)
    {
        this(comparisonType, valueLength, false);
    }

    protected AbstractType(ComparisonType comparisonType, int valueLength, boolean isReversed)
    {
        this(comparisonType, valueLength, FixedSizeType.NONE, 0, isReversed);
    }

    protected AbstractType(ComparisonType comparisonType, int valueLength, FixedSizeType fixedSizeType, int fixedCompareReturns)
    {
        this(comparisonType, valueLength, fixedSizeType, fixedCompareReturns, false);
    }

    protected AbstractType(ComparisonType comparisonType, int valueLength, FixedSizeType fixedSizeType, int fixedCompareReturns, boolean isReversed)
    {
        this.comparisonType = comparisonType;
        this.valueLength = valueLength;
        this.fixedSizeType = fixedSizeType;
        this.fixedCompareReturns = fixedCompareReturns;
        this.isReversed = isReversed;

        if (comparisonType == FIXED_SIZE_VALUE && fixedSizeType == FixedSizeType.NONE)
            throw new IllegalArgumentException("FIXED_SIZE_VALUE must have valid type");
        if (comparisonType != FIXED_SIZE_VALUE && fixedSizeType != FixedSizeType.NONE)
            throw new IllegalArgumentException("only FIXED_SIZE_VALUE can have fixed sized type");

        try
        {
            Method custom = getClass().getMethod("compareCustom", ByteBuffer.class, ByteBuffer.class);
            if ((custom.getDeclaringClass() == AbstractType.class) == (comparisonType == CUSTOM) && !isReversed)
                throw new IllegalStateException((comparisonType == CUSTOM ? "compareCustom must be overridden if ComparisonType is CUSTOM"
                                                                         : "compareCustom should not be overridden if ComparisonType is not CUSTOM")
                                                + " (" + getClass().getSimpleName() + ")");
        }
        catch (NoSuchMethodException e)
        {
            throw new IllegalStateException();
        }
    }

    public static List<String> asCQLTypeStringList(List<AbstractType<?>> abstractTypes)
    {
        List<String> r = new ArrayList<>(abstractTypes.size());
        for (AbstractType<?> abstractType : abstractTypes)
            r.add(abstractType.asCQL3Type().toString());
        return r;
    }

    public T compose(ByteBuffer bytes)
    {
        return getSerializer().deserialize(bytes);
    }

    public ByteBuffer decompose(T value)
    {
        return getSerializer().serialize(value);
    }

    /** get a string representation of the bytes used for various identifier (NOT just for log messages) */
    public String getString(ByteBuffer bytes)
    {
        if (bytes == null)
            return "null";

        TypeSerializer<T> serializer = getSerializer();
        serializer.validate(bytes);

        return serializer.toString(serializer.deserialize(bytes));
    }

    /** get a byte representation of the given string. */
    public abstract ByteBuffer fromString(String source) throws MarshalException;

    /** Given a parsed JSON string, return a byte representation of the object.
     * @param parsed the result of parsing a json string
     **/
    public abstract Term fromJSONObject(Object parsed) throws MarshalException;

    /**
     * Converts the specified value into its JSON representation.
     * <p>
     * The buffer position will stay the same.
     * </p>
     *
     * @param buffer the value to convert
     * @param protocolVersion the protocol version to use for the conversion
     * @return a JSON string representing the specified value
     */
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return '"' + getSerializer().deserialize(buffer).toString() + '"';
    }

    /* validate that the byte array is a valid sequence for the type we are supposed to be comparing */
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        getSerializer().validate(bytes);
    }

    @Override
    public final int compare(ByteBuffer left, ByteBuffer right)
    {
        if (comparisonType == FIXED_COMPARE)
            return fixedCompareReturns;

        if (isReversed)
        {
            ByteBuffer t = left;
            left = right;
            right = t;
        }

        if (!left.hasRemaining() || !right.hasRemaining())
            return left.hasRemaining() ? 1 : right.hasRemaining() ? -1 : 0;

        if (comparisonType == FIXED_SIZE_VALUE)
        {
            switch (fixedSizeType)
            {
                case BOOLEAN:
                    return BooleanType.compareType(left, right);
                case BYTE:
                    return ByteType.compareType(left, right);
                case SHORT:
                    return ShortType.compareType(left, right);
                case INT32:
                    return Int32Type.compareType(left, right);
                case FLOAT:
                    return FloatType.compareType(left, right);
                case LONG:
                    return LongType.compareType(left, right);
                case DOUBLE:
                    return DoubleType.compareType(left, right);
                default:
                    throw new IllegalStateException();
            }
        }

        if (comparisonType == BYTE_ORDER)
            return FastByteOperations.compareUnsigned(left, right);

        if(isReversed)
            return ((ReversedType)this).baseType.compareCustom(left, right);
        else
            return compareCustom(left, right);
    }

    /**
     * Implement IFF ComparisonType is CUSTOM. <p>
     *
     * No need to handle the common empty buffer case, which is tackled in {@link #compare(ByteBuffer, ByteBuffer)}. <p>
     *
     * Compares the ByteBuffer representation of two instances of this class, for types where this cannot be done by
     * simple in-order comparison of the unsigned bytes. <p>
     *
     * Standard Java compare semantics
     */
    public int compareCustom(ByteBuffer left, ByteBuffer right)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Validate cell value. Unlike {@linkplain #validate(java.nio.ByteBuffer)},
     * cell value is passed to validate its content.
     * Usually, this is the same as validate except collection.
     *
     * @param cellValue ByteBuffer representing cell value
     * @throws MarshalException
     */
    public void validateCellValue(ByteBuffer cellValue) throws MarshalException
    {
        validate(cellValue);
    }

    /* Most of our internal type should override that. */
    public CQL3Type asCQL3Type()
    {
        return new CQL3Type.Custom(this);
    }

    /**
     * Same as compare except that this ignore ReversedType. This is to be use when
     * comparing 2 values to decide for a CQL condition (see Operator.isSatisfiedBy) as
     * for CQL, ReversedType is simply an "hint" to the storage engine but it does not
     * change the meaning of queries per-se.
     */
    public int compareForCQL(ByteBuffer v1, ByteBuffer v2)
    {
        return compare(v1, v2);
    }

    public abstract TypeSerializer<T> getSerializer();

    /**
     * Returns the deserializer used to deserialize the function arguments of this type.
     * @return the deserializer used to deserialize the function arguments of this type.
     */
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return new DefaultArgumentDerserializer(this);
    }

    /* convenience method */
    public String getString(Collection<ByteBuffer> names)
    {
        StringBuilder builder = new StringBuilder();
        for (ByteBuffer name : names)
        {
            builder.append(getString(name)).append(",");
        }
        return builder.toString();
    }

    public boolean isCounter()
    {
        return false;
    }

    public boolean isFrozenCollection()
    {
        return isCollection() && !isMultiCell();
    }

    public final boolean isReversed()
    {
        return isReversed;
    }

    public static AbstractType<?> parseDefaultParameters(AbstractType<?> baseType, TypeParser parser) throws SyntaxException
    {
        Map<String, String> parameters = parser.getKeyValueParameters();
        String reversed = parameters.get("reversed");
        if (reversed != null && (reversed.isEmpty() || reversed.equals("true")))
        {
            return ReversedType.getInstance(baseType);
        }
        else
        {
            return baseType;
        }
    }

    /**
     * Returns true if this comparator is compatible with the provided
     * previous comparator, that is if previous can safely be replaced by this.
     * A comparator cn should be compatible with a previous one cp if forall columns c1 and c2,
     * if   cn.validate(c1) and cn.validate(c2) and cn.compare(c1, c2) == v,
     * then cp.validate(c1) and cp.validate(c2) and cp.compare(c1, c2) == v.
     *
     * Note that a type should be compatible with at least itself and when in
     * doubt, keep the default behavior of not being compatible with any other comparator!
     */
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        return this.equals(previous);
    }

    /**
     * Returns true if values of the other AbstractType can be read and "reasonably" interpreted by the this
     * AbstractType. Note that this is a weaker version of isCompatibleWith, as it does not require that both type
     * compare values the same way.
     *
     * The restriction on the other type being "reasonably" interpreted is to prevent, for example, IntegerType from
     * being compatible with all other types.  Even though any byte string is a valid IntegerType value, it doesn't
     * necessarily make sense to interpret a UUID or a UTF8 string as an integer.
     *
     * Note that a type should be compatible with at least itself.
     */
    public boolean isValueCompatibleWith(AbstractType<?> otherType)
    {
        return isValueCompatibleWithInternal((otherType instanceof ReversedType) ? ((ReversedType) otherType).baseType : otherType);
    }

    /**
     * Needed to handle ReversedType in value-compatibility checks.  Subclasses should implement this instead of
     * isValueCompatibleWith().
     */
    protected boolean isValueCompatibleWithInternal(AbstractType<?> otherType)
    {
        return isCompatibleWith(otherType);
    }

    /**
     * An alternative comparison function used by CollectionsType in conjunction with CompositeType.
     *
     * This comparator is only called to compare components of a CompositeType. It gets the value of the
     * previous component as argument (or null if it's the first component of the composite).
     *
     * Unless you're doing something very similar to CollectionsType, you shouldn't override this.
     */
    public int compareCollectionMembers(ByteBuffer v1, ByteBuffer v2, ByteBuffer collectionName)
    {
        return compare(v1, v2);
    }

    /**
     * An alternative validation function used by CollectionsType in conjunction with CompositeType.
     *
     * This is similar to the compare function above.
     */
    public void validateCollectionMember(ByteBuffer bytes, ByteBuffer collectionName) throws MarshalException
    {
        validate(bytes);
    }

    public boolean isCollection()
    {
        return false;
    }

    public boolean isUDT()
    {
        return false;
    }

    public boolean isTuple()
    {
        return false;
    }

    public boolean isMultiCell()
    {
        return false;
    }

    public boolean isFreezable()
    {
        return false;
    }

    public AbstractType<?> freeze()
    {
        return this;
    }

    /**
     * Returns an AbstractType instance that is equivalent to this one, but with all nested UDTs and collections
     * explicitly frozen.
     *
     * This is only necessary for {@code 2.x -> 3.x} schema migrations, and can be removed in Cassandra 4.0.
     *
     * See CASSANDRA-11609 and CASSANDRA-11613.
     */
    public AbstractType<?> freezeNestedMulticellTypes()
    {
        return this;
    }

    /**
     * Returns {@code true} for types where empty should be handled like {@code null} like {@link Int32Type}.
     */
    public boolean isEmptyValueMeaningless()
    {
        return false;
    }

    /**
     * @param ignoreFreezing if true, the type string will not be wrapped with FrozenType(...), even if this type is frozen.
     */
    public String toString(boolean ignoreFreezing)
    {
        return this.toString();
    }

    /**
     * The number of subcomponents this type has.
     * This is always 1, i.e. the type has only itself as "subcomponents", except for CompositeType.
     */
    public int componentsCount()
    {
        return 1;
    }

    /**
     * Return a list of the "subcomponents" this type has.
     * This always return a singleton list with the type itself except for CompositeType.
     */
    public List<AbstractType<?>> getComponents()
    {
        return Collections.<AbstractType<?>>singletonList(this);
    }

    /**
     * The length of values for this type if all values are of fixed length, -1 otherwise.
     */
    public final int valueLengthIfFixed()
    {
        return valueLength;
    }

    // This assumes that no empty values are passed
    public void writeValue(ByteBuffer value, DataOutputPlus out) throws IOException
    {
        assert value.hasRemaining();
        if (valueLengthIfFixed() >= 0)
            out.write(value);
        else
            ByteBufferUtil.writeWithVIntLength(value, out);
    }

    public int writtenLength(ByteBuffer value)
    {
        assert value.hasRemaining();
        return valueLengthIfFixed() >= 0
             ? value.remaining()
             : TypeSizes.sizeofWithVIntLength(value);
    }

    public ByteBuffer readValue(DataInputPlus in, int maxValueSize) throws IOException
    {
        int length = valueLengthIfFixed();

        if (length >= 0)
            return ByteBufferUtil.read(in, length);
        else
        {
            int l = (int)in.readUnsignedVInt();
            if (l < 0)
                throw new IOException("Corrupt (negative) value length encountered");

            if (l > maxValueSize)
                throw new IOException(String.format("Corrupt value length %d encountered, as it exceeds the maximum of %d, " +
                                                    "which is set via max_value_size_in_mb in cassandra.yaml",
                                                    l, maxValueSize));

            return ByteBufferUtil.read(in, l);
        }
    }

    public void skipValue(DataInputPlus in) throws IOException
    {
        int length = valueLengthIfFixed();
        if (length >= 0)
            in.skipBytesFully(length);
        else
            ByteBufferUtil.skipWithVIntLength(in);
    }

    public boolean referencesUserType(String userTypeName)
    {
        return false;
    }

    public boolean referencesDuration()
    {
        return false;
    }

    /**
     * Tests whether a CQL value having this type can be assigned to the provided receiver.
     */
    public AssignmentTestable.TestResult testAssignment(AbstractType<?> receiverType)
    {
        // testAssignement is for CQL literals and native protocol values, none of which make a meaningful
        // difference between frozen or not and reversed or not.

        if (isFreezable() && !isMultiCell())
            receiverType = receiverType.freeze();

        if (isReversed() && !receiverType.isReversed())
            receiverType = ReversedType.getInstance(receiverType);

        if (equals(receiverType))
            return AssignmentTestable.TestResult.EXACT_MATCH;

        if (receiverType.isValueCompatibleWith(this))
            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;

        return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
    }

    /**
     * This must be overriden by subclasses if necessary so that for any
     * AbstractType, this == TypeParser.parse(toString()).
     *
     * Note that for backwards compatibility this includes the full classname.
     * For CQL purposes the short name is fine.
     */
    @Override
    public String toString()
    {
        return getClass().getName();
    }

    /**
     * Checks to see if two types are equal when ignoring or not ignoring differences in being frozen, depending on
     * the value of the ignoreFreezing parameter.
     * @param other type to compare
     * @param ignoreFreezing if true, differences in the types being frozen will be ignored
     */
    public boolean equals(Object other, boolean ignoreFreezing)
    {
        return this.equals(other);
    }

    public void checkComparable()
    {
        if (comparisonType == NOT_COMPARABLE)
            throw new IllegalArgumentException(this + " cannot be used in comparisons, so cannot be used as a clustering column");

    }

    public final AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
    {
        return testAssignment(receiver.type);
    }

    /**
     * Converts the given clustering of prefix into a ByteSource which preserves this comparator's order when compared
     * byte by byte.
     */
    public ByteSource asByteComparableSource(ByteBuffer byteBuffer)
    {
        if (comparisonType == BYTE_ORDER)
            return ByteSource.of(byteBuffer);
        else
            // default is only good for byte comparable
            throw new UnsupportedOperationException(getClass().getSimpleName() + " does not implement asByteComparableSource");
    }

    /**
     * {@code ArgumentDeserializer} that use the type deserialization.
     */
    protected static class DefaultArgumentDerserializer implements ArgumentDeserializer
    {
        private final AbstractType<?> type;

        public DefaultArgumentDerserializer(AbstractType<?> type)
        {
           this.type = type;
        }

        @Override
        public Object deserialize(ProtocolVersion protocolVersion, ByteBuffer buffer)
        {
            if (buffer == null || (!buffer.hasRemaining() && type.isEmptyValueMeaningless()))
                return null;

            return type.compose(buffer);
        }
    }
}
