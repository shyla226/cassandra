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
    private final Map<String, BitSet> domains = new HashMap<>();
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
        elements.forEach(this::add);
        this.isImmutable = isImmutable;
    }

    // Methods from java.util.Set
    public int size()
    {
        if (domains.isEmpty())
            return 0;

        return domains.values().stream().mapToInt(BitSet::cardinality).sum();
    }

    public boolean contains(Object o)
    {
        if (o == null)
            return false;

        if (!(o instanceof PartitionedEnum))
            return false;

        PartitionedEnum element = (PartitionedEnum)o;
        checkElement(element);

        BitSet b = domains.get(element.domain());
        return b != null && b.get(element.ordinal());
    }

    /**
     * Non-mutating method to check whether this set intersects with another
     * @param other the other enumset
     * @return true if there is an intersection, false otherwise
     */
    public boolean intersects(PartitionedEnumSet<E> other)
    {
        Iterator<Map.Entry<String, BitSet>> iter = domains.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry<String, BitSet> domain = iter.next();
            BitSet otherDomain = other.domains.get(domain.getKey());
            if (otherDomain == null)
                continue;

            BitSet mine = domain.getValue();
            if (mine.intersects(otherDomain))
                return true;
        }
        return false;
    }

    public boolean isEmpty()
    {
        return domains.isEmpty();
    }

    public Iterator<E> iterator()
    {
        ImmutableList.Builder<E> builder = ImmutableList.builder();
        stream().forEach(builder::add);
        return builder.build().iterator();
    }

    public Stream<E> stream()
    {
        return domains.entrySet()
                      .stream()
                      .flatMap(d -> domainToElements(d.getKey(), d.getValue()));
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        stream().forEach((e) -> {
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
        stream().forEach(builder::add);
        return builder.build();
    }

    private Stream<E> domainToElements(String domain, BitSet bits)
    {
        List<E> list = new ArrayList<>();
        for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1))
            list.add(registry.get(domain, i));
        return list.stream();
    }

    private void checkElement(PartitionedEnum element)
    {
        if (registry.get(element.domain(), element.ordinal()) != element)
            throw new IllegalArgumentException(String.format("Supplied and registered elements " +
                                                             "are not equal (%s)",
                                                             element));
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

        Iterator<Map.Entry<String, BitSet>> iter = domains.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry<String, BitSet> domain = iter.next();
            BitSet otherDomain = other.domains.get(domain.getKey());
            BitSet mine = domain.getValue();

            // there are no elements of this domain in other
            if (otherDomain == null)
            {
                if (!mine.isEmpty())
                    modified = true;

                iter.remove();
                continue;
            }

            long[] before = mine.toLongArray();
            mine.and(otherDomain);
            if (!Arrays.equals(before, mine.toLongArray()))
                modified = true;

            if (mine.isEmpty())
                iter.remove();
        }
        return modified;
    }

    /**
     * Combine this set with another by adding all its elements to this.
     * @param other PartitionedEnumSet to combine with
     * @return true if this set was modified as a result of the union, otherwise false
     */
    private boolean union(PartitionedEnumSet<E> other)
    {
        boolean modified = false;
        for (Map.Entry<String, BitSet> otherDomain : other.domains.entrySet())
        {
            BitSet mine = domains.computeIfAbsent(otherDomain.getKey(), d -> new BitSet());
            long[] before = mine.toLongArray();
            mine.or(otherDomain.getValue());
            if (!Arrays.equals(before, mine.toLongArray()))
                modified = true;
        }
        return modified;
    }

    /**
     * Check whether this set completely contains another.
     * @param other PartitionedEnumSet to compare
     * @return true if this set is strictly a subset of the other, false otherwise.
     */
    private boolean contains(PartitionedEnumSet<E> other)
    {
        for (Map.Entry<String, BitSet> otherDomain : other.domains.entrySet())
        {
            BitSet mine = domains.get(otherDomain.getKey());
            if (mine == null)
                return false;

            // if (other & mine) != other then other is not a subset of mine
            BitSet otherBits = otherDomain.getValue();
            BitSet otherCopy = new BitSet();
            otherCopy.or(otherBits);
            otherCopy.and(mine);
            if (!otherCopy.equals(otherBits))
                return false;
        }
        return true;
    }

    /**
     * Remove all elements present in another PartitionedEnumSet from this one, where present.
     * @param other PartitionedEnumSet who's elements are to be removed from this
     * @return true if this set was modified by the operation, otherwise false
     */
    private boolean remove(PartitionedEnumSet<E> other)
    {
        boolean modified = false;
        for (Map.Entry<String, BitSet> otherDomain : other.domains.entrySet())
        {
            BitSet mine = domains.get(otherDomain.getKey());
            if (mine == null)
                continue;

            long[] before = mine.toLongArray();
            mine.andNot(otherDomain.getValue());
            if (!Arrays.equals(before, mine.toLongArray()))
                modified = true;

            if (mine.isEmpty())
                domains.remove(otherDomain.getKey());
        }
        return modified;
    }

    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (o instanceof PartitionedEnumSet)
        {
            PartitionedEnumSet<?> other = (PartitionedEnumSet) o;
            if (elementType != other.elementType)
                return false;
            if (size() != other.size())
                return false;

            for (Map.Entry<String, BitSet> domain : domains.entrySet())
            {
                BitSet otherBits = other.domains.get(domain.getKey());
                BitSet bits = domain.getValue();

                if (otherBits == null && bits.isEmpty())
                    continue;

                if (!bits.equals(otherBits))
                    return false;
            }

            return true;
        }

        return o instanceof Set && asImmutableSet().equals(o);
    }

    public int hashCode()
    {
        // A set's hashcode should equal the sum of its elements
        if (domains.isEmpty())
            return 0;

        return stream().mapToInt(Object::hashCode) .sum();
    }

    public Object[] toArray()
    {
        return stream().toArray();
    }

    public <T> T[] toArray(T[] a)
    {
        int size = size();
        @SuppressWarnings("unchecked")
        T[] array = a.length >= size ? a : (T[])java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
        Consumer<E> consumer = new Consumer<E>()
        {
            int idx = 0;
            public void accept(E e)
            {
                array[idx++] = (T)e;
            }
        };
        stream().forEach(consumer);
        // if we filled the supplied array and it has spare capacity,
        // set the first additional element to null as per the set contract
        if (size < a.length)
            array[size] = null;
        return array;
    }

    private void checkIsMutable()
    {
       if (isImmutable)
           throw new UnsupportedOperationException("This PartitionedEnumSet is immutable");
    }

    public boolean add(E element)
    {
        checkIsMutable();
        checkElement(element);
        boolean present = contains(element);
        BitSet bits = domains.computeIfAbsent(element.domain(), d -> new BitSet());
        bits.set(element.ordinal());
        return !present;
    }

    public boolean remove(Object o)
    {
        checkIsMutable();
        if (!contains(o))
            return false;

        PartitionedEnum element = (PartitionedEnum)o;
        checkElement(element);

        BitSet bits = domains.get(element.domain());
        if (bits == null)
            return false;

        if (!bits.get(element.ordinal()))
            return false;

        bits.clear(element.ordinal());
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
        domains.clear();
    }
}
