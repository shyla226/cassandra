/*
 * Copyright DataStax, Inc.
 */

package org.apache.cassandra.auth.enums;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Provides similar functionality, speed and memory efficiency to {@link EnumSet}, but for namespaced enum types as
 * implemented with {@link PartitionedEnum} and {@link Domains}.
 */
public class PartitionedEnumSet<E extends PartitionedEnum> implements Set<E>
{
    private final Domains<E> registry;
    private final BitSet bits;
    private final Class<E> elementType;
    private final boolean isImmutable;


    // Static factory methods
    // Domains (the 'concrete' enums which make up the partitioned enum) must be registered before
    // use. This registration process ensures that each domain both extends java.lang.Enum and
    // implements the specific PartitionedEnum interface.
    /**
     * Return a PartitionedEnumSet of type E, containing all elements defined in the domain enum
     * @param elementType The subtype of PartitionedEnum representing the entire enum
     * @param domain A concrete enum, containing elements of elementType,
     * @param <E> enum type
     * @return PartitionedEnumSet containing all elements from domain
     */
    public static <E extends PartitionedEnum> PartitionedEnumSet<E> allOf(Class<E> elementType, Class<? extends E> domain)
    {
       return of(elementType, domain.getEnumConstants());
    }

    /**
     * Return an immutable PartitionedEnumSet of type E, containing all elements defined in the domain enum. Calls to
     * methods which mutate this instance will throw {@link UnsupportedOperationException}
     * @param elementType The subtype of PartitionedEnum representing the entire enum
     * @param domain A concrete enum, containing elements of elementType,
     * @param <E> enum type
     * @return Immutable PartitionedEnumSet containing all elements from domain
     */
    public static <E extends PartitionedEnum> PartitionedEnumSet<E> immutableSetOfAll(Class<E> elementType, Class<? extends E> domain)
    {
        return immutableSetOf(elementType, domain.getEnumConstants());
    }

    /**
     * Return a PartitionedEnumSet of type E, containing all elements supplied as arguments.
     * @param elementType The subtype of PartitionedEnum representing the entire enum
     * @param elements Elements from one or more concrete enum, all implementing elementType,
     * @param <E> enum type
     * @return PartitionedEnumSet containing the supplied elements
     */
    public static <E extends PartitionedEnum> PartitionedEnumSet<E> of(Class<E> elementType, E...elements)
    {
        @SuppressWarnings("unchecked")
        Domains<E> domains = Domains.getDomains(elementType);
        if (domains == null)
            throw new IllegalArgumentException("Unknown PartitionedEnumType " + elementType.getName());

        return new PartitionedEnumSet<>(domains, Arrays.asList(elements), false);
    }

    /**
     * Return a PartitionedEnumSet of type E, containing all elements supplied as arguments.
     * @param elementType The subtype of PartitionedEnum representing the entire enum
     * @param elements Elements from one or more concrete enum, all implementing elementType,
     * @param <E> enum type
     * @return PartitionedEnumSet containing the supplied elements
     */
    public static <E extends PartitionedEnum> PartitionedEnumSet<E> of(Class<E> elementType, Iterable<E> elements)
    {
        @SuppressWarnings("unchecked")
        Domains<E> domains = Domains.getDomains(elementType);
        if (domains == null)
            throw new IllegalArgumentException("Unknown PartitionedEnumType " + elementType.getName());

        return new PartitionedEnumSet<>(domains, elements, false);
    }

    /**
     * Return an immutable PartitionedEnumSet of type E, containing all elements supplied as arguments. Calls to
     * methods which mutate this instance will throw {@link UnsupportedOperationException}
     * @param elementType The subtype of PartitionedEnum representing the entire enum
     * @param elements Elements from one or more concrete enum, all implementing elementType,
     * @param <E> enum type
     * @return Immutable PartitionedEnumSet containing the supplied elements
     */
    public static <E extends PartitionedEnum> PartitionedEnumSet<E> immutableSetOf(Class<E> elementType, E...elements)
    {
        @SuppressWarnings("unchecked")
        Domains<E> domains = Domains.getDomains(elementType);
        if (domains == null)
            throw new IllegalArgumentException("Unknown PartitionedEnumType " + elementType.getName());

        return new PartitionedEnumSet<>(domains, Arrays.asList(elements), true);
    }

    /**
     * Return an immutable PartitionedEnumSet of type E, containing all elements supplied as arguments. Calls to
     * methods which mutate this instance will throw {@link UnsupportedOperationException}
     * @param elementType The subtype of PartitionedEnum representing the entire enum
     * @param elements Elements from one or more concrete enum, all implementing elementType,
     * @param <E> enum type
     * @return Immutable PartitionedEnumSet containing the supplied elements
     */
    public static <E extends PartitionedEnum> PartitionedEnumSet<E> immutableSetOf(Class<E> elementType, Iterable<E> elements)
    {
        @SuppressWarnings("unchecked")
        Domains<E> domains = Domains.getDomains(elementType);
        if (domains == null)
            throw new IllegalArgumentException("Unknown PartitionedEnumType " + elementType.getName());

        return new PartitionedEnumSet<>(domains, elements, true);
    }

    /**
     * Return an empty PartitionedEnum set of type E
     * @param elementType The subtype of PartitionedEnum representing the entire enum
     * @param <E> enum type
     * @return Empty PartitionedEnumSet
     */
    public static <E extends PartitionedEnum> PartitionedEnumSet<E> noneOf(Class<E> elementType)
    {
        @SuppressWarnings("unchecked")
        Domains<E> domains = Domains.getDomains(elementType);
        if (domains == null)
            throw new IllegalArgumentException("Unknown PartitionedEnumType " + elementType.getName());

        return new PartitionedEnumSet<>(domains, Collections.emptyList(), false);
    }

    /**
     * Return an immutable empty PartitionedEnum set of type E. Calls to methods which mutate the instance will
     * throw {@link UnsupportedOperationException}
     * @param elementType The subtype of PartitionedEnum representing the entire enum
     * @param <E> enum type
     * @return Empty immutable PartitionedEnumSet
     */
    public static <E extends PartitionedEnum> PartitionedEnumSet<E> immutableEmptySetOf(Class<E> elementType)
    {
        @SuppressWarnings("unchecked")
        Domains<E> domains = Domains.getDomains(elementType);
        if (domains == null)
            throw new IllegalArgumentException("Unknown PartitionedEnumType " + elementType.getName());

        return new PartitionedEnumSet<>(domains, Collections.emptyList(), true);
    }

    private PartitionedEnumSet(Domains<E> registry, Iterable<E> elements, boolean isImmutable)
    {
        this.registry = registry;
        this.elementType = registry.getType();
        if (elements instanceof PartitionedEnumSet)
        {
            PartitionedEnumSet src = (PartitionedEnumSet) elements;
            bits = (BitSet) src.bits.clone();
        }
        else
        {
            bits = new BitSet();
            elements.forEach(this::add);
        }
        this.isImmutable = isImmutable;
    }

    // Methods from java.util.Set
    public int size()
    {
        if (bits.isEmpty())
            return 0;

        return bits.cardinality();
    }

    public boolean contains(Object o)
    {
        if (o == null)
            return false;

        if (!(o instanceof PartitionedEnum))
            return false;

        PartitionedEnum element = (PartitionedEnum)o;
        int bit = checkElementGetBit(element);

        return bits.get(bit);
    }

    /**
     * Non-mutating method to check whether this set intersects with another
     * @param other the other enumset
     * @return true if there is an intersection, false otherwise
     */
    public boolean intersects(PartitionedEnumSet<E> other)
    {
        return bits.intersects(other.bits);
    }

    public boolean isEmpty()
    {
        return bits.isEmpty();
    }

    public Iterator<E> iterator()
    {
        ImmutableList.Builder<E> builder = ImmutableList.builder();
        forEach(builder::add);
        return builder.build().iterator();
    }

    public void forEach(Consumer<? super E> action)
    {
        for (Domains.Domain d : registry.domains())
        {
            int off = d.bitOffset;
            for (Enum e : d.enumType.getEnumConstants())
            {
                if (bits.get(off + e.ordinal()))
                    action.accept((E) e);
            }
        }
    }

    public Stream<E> stream()
    {
        List<E> lst = new ArrayList<>();
        forEach(lst::add);
        return lst.stream();
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        forEach((e) -> {
            builder.append(e.getFullName());
            builder.append(", ");
        });
        if (builder.length() >=2)
            builder.setLength(builder.length() - 2);
        return String.format("PartitionedEnumSet [%s]", builder.toString());
    }

    private ImmutableSet<E> asImmutableSet()
    {
        ImmutableSet.Builder<E> builder = ImmutableSet.builder();
        forEach(builder::add);
        return builder.build();
    }

    private int checkElementGetBit(PartitionedEnum element)
    {
        Domains.Domain d = registry.domain(element.domain());

        if (d == null || d.enumType.getEnumConstants()[element.ordinal()] != element)
            throw new IllegalArgumentException(String.format("Supplied and registered elements " +
                                                             "are not equal (%s)",
                                                             element));

        return d.bitOffset + element.ordinal();
    }

    /**
     * Intersect a supplied PartitionedEnumSet with this one.
     *
     * @param other PartitionedBitSet to intersect with
     * @return true if this set was modified by performing the intersection, otherwise false.
     */
    private boolean intersect(PartitionedEnumSet<E> other)
    {
        boolean modified = false;
        // different types, there can be no commonality so remove everything
        if (other.elementType != elementType)
        {
            modified = !isEmpty();
            clear();
            return modified;
        }


        long[] before = bits.toLongArray();
        bits.and(other.bits);
        return !Arrays.equals(before, bits.toLongArray());
    }

    /**
     * Combine this set with another by adding all its elements to this.
     * @param other PartitionedEnumSet to combine with
     * @return true if this set was modified as a result of the union, otherwise false
     */
    private boolean union(PartitionedEnumSet<E> other)
    {
        long[] before = bits.toLongArray();
        bits.or(other.bits);
        return !Arrays.equals(before, bits.toLongArray());
    }

    /**
     * Check whether this set completely contains another.
     * @param other PartitionedEnumSet to compare
     * @return true if this set is strictly a subset of the other, false otherwise.
     */
    private boolean contains(PartitionedEnumSet<E> other)
    {
        BitSet otherBits = other.bits;
        BitSet otherCopy = new BitSet();
        otherCopy.or(otherBits);
        otherCopy.and(bits);
        return otherCopy.equals(otherBits);
    }

    /**
     * Remove all elements present in another PartitionedEnumSet from this one, where present.
     * @param other PartitionedEnumSet who's elements are to be removed from this
     * @return true if this set was modified by the operation, otherwise false
     */
    private boolean remove(PartitionedEnumSet<E> other)
    {
        long[] before = bits.toLongArray();
        bits.andNot(other.bits);
        return !Arrays.equals(before, bits.toLongArray());
    }

    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (o instanceof PartitionedEnumSet)
        {
            PartitionedEnumSet<?> other = (PartitionedEnumSet) o;
            return elementType == other.elementType && bits.equals(other.bits);
        }

        return o instanceof Set && asImmutableSet().equals(o);
    }

    public int hashCode()
    {
        return new Consumer<E>()
        {
            int sum;

            public void accept(E e)
            {
                sum += e.hashCode();
            }

            {
                forEach(this);
            }
        }.sum;
    }

    public Object[] toArray()
    {
        Object[] r = new Object[bits.cardinality()];
        return toArrayInt(r);
    }

    private Object[] toArrayInt(Object[] r)
    {
        int i = 0;
        for (Domains.Domain d : registry.domains())
        {
            Enum[] enums = d.enumType.getEnumConstants();
            for (int n = 0; n < enums.length; n++)
            {
                if (bits.get(d.bitOffset + n))
                    r[i++] = enums[n];
            }
        }
        return r;
    }

    public <T> T[] toArray(T[] a)
    {
        int size = size();
        @SuppressWarnings("unchecked")
        T[] array = a.length >= size ? a : (T[])java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
        T[] r = (T[]) toArrayInt(array);
        for (int i = size; i < a.length; i++)
            r[i] = null;
        return r;
    }

    private void checkIsMutable()
    {
       if (isImmutable)
           throw new UnsupportedOperationException("This PartitionedEnumSet is immutable");
    }

    public boolean add(E element)
    {
        checkIsMutable();
        int bit = checkElementGetBit(element);
        boolean present = bits.get(bit);
        bits.set(bit);
        return !present;
    }

    public boolean remove(Object o)
    {
        checkIsMutable();
        if (!contains(o))
            return false;

        PartitionedEnum element = (PartitionedEnum)o;

        int bit = checkElementGetBit(element);

        boolean present = bits.get(bit);
        if (!present)
            return false;

        bits.clear(bit);
        return true;
    }

    @SuppressWarnings("unchecked")
    public boolean containsAll(@Nullable Collection<?> c)
    {
        if (c == null)
            throw new NullPointerException();

        if (c instanceof PartitionedEnumSet<?>)
        {
            PartitionedEnumSet p = (PartitionedEnumSet) c;
            return p.elementType == elementType && contains(p);
        }

        for (Object o : c)
            if(!contains(o))
                return false;

        return true;
    }

    @SuppressWarnings("unchecked")
    public boolean addAll(@Nullable Collection<? extends E> c)
    {
        checkIsMutable();
        if (c == null)
            throw new NullPointerException();

        // optimized version where we can just union the bitsets
        if (c instanceof PartitionedEnumSet<?>)
        {
            PartitionedEnumSet p = (PartitionedEnumSet) c;
            return p.elementType == elementType && union(p);
        }

        boolean added = false;
        for (E e : c)
            if (add(e))
                added = true;

        return added;
    }

    @SuppressWarnings("unchecked")
    public boolean retainAll(@Nullable Collection<?> c)
    {
        checkIsMutable();
        if (c == null)
            throw new NullPointerException();

        if (c.isEmpty())
        {
            if (isEmpty())
                return false;

            clear();
            return true;
        }

        // optimized version where we can just intersect bitsets
        if (c instanceof PartitionedEnumSet<?>)
        {
            PartitionedEnumSet p = (PartitionedEnumSet)c;
            return p.elementType == elementType && intersect(p);
        }

        boolean removed = false;
        Iterator<E> iter = iterator();
        while (iter.hasNext())
        {
            E e = iter.next();
            if (!c.contains(e))
                removed = remove(e);
        }
        return removed;
    }

    @SuppressWarnings("unchecked")
    public boolean removeAll(@Nullable Collection<?> c)
    {
        checkIsMutable();
        if (c == null)
            throw new NullPointerException();

        // optimized version where we can just andNot the bitsets
        if (c instanceof PartitionedEnumSet<?>)
        {
            PartitionedEnumSet p = (PartitionedEnumSet) c;
            return p.elementType == elementType && remove((PartitionedEnumSet) c);
        }

        boolean removed = false;
        for (Object o : c)
            if (remove(o))
                removed = true;

        return removed;
    }

    public void clear()
    {
        checkIsMutable();
        bits.clear();
    }
}
