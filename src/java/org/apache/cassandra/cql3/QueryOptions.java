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

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.ImmutableList;

import io.netty.buffer.ByteBuf;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.CBCodec;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.Pair;

/**
 * Options for a query.
 */
public abstract class QueryOptions
{
    public static final QueryOptions DEFAULT = new DefaultQueryOptions(ConsistencyLevel.ONE,
                                                                       Collections.<ByteBuffer>emptyList(),
                                                                       false,
                                                                       SpecificOptions.DEFAULT,
                                                                       ProtocolVersion.CURRENT);

    public static final CBCodec<QueryOptions> codec = new Codec();

    // A cache of bind values parsed as JSON, see getJsonColumnValue for details.
    private List<Map<ColumnIdentifier, Term>> jsonValuesCache;

    public static QueryOptions fromThrift(ConsistencyLevel consistency, List<ByteBuffer> values)
    {
        return new DefaultQueryOptions(consistency, values, false, SpecificOptions.DEFAULT, ProtocolVersion.V3);
    }

    public static QueryOptions forInternalCalls(ConsistencyLevel consistency, List<ByteBuffer> values)
    {
        return new DefaultQueryOptions(consistency, values, false, SpecificOptions.DEFAULT, ProtocolVersion.V3);
    }

    public static QueryOptions forInternalCalls(List<ByteBuffer> values)
    {
        return new DefaultQueryOptions(ConsistencyLevel.ONE, values, false, SpecificOptions.DEFAULT, ProtocolVersion.V3);
    }

    public static QueryOptions forProtocolVersion(ProtocolVersion protocolVersion)
    {
        return new DefaultQueryOptions(null, null, true, null, protocolVersion);
    }

    public static QueryOptions create(ConsistencyLevel consistency, List<ByteBuffer> values, boolean skipMetadata, int pageSize, PagingState pagingState, ConsistencyLevel serialConsistency, ProtocolVersion version)
    {
        return new DefaultQueryOptions(consistency, values, skipMetadata, new SpecificOptions(new PagingOptions(PageSize.rowsSize(pageSize), PagingOptions.Mechanism.SINGLE, pagingState), serialConsistency, -1L), version);
    }

    public static QueryOptions addColumnSpecifications(QueryOptions options, List<ColumnSpecification> columnSpecs)
    {
        return new OptionsWithColumnSpecifications(options, columnSpecs);
    }

    public abstract ConsistencyLevel getConsistency();
    public abstract List<ByteBuffer> getValues();
    public abstract boolean skipMetadata();

    /**
     * Returns the term corresponding to column {@code columnName} in the JSON value of bind index {@code bindIndex}.
     *
     * This is functionally equivalent to:
     *   {@code Json.parseJson(UTF8Type.instance.getSerializer().deserialize(getValues().get(bindIndex)), expectedReceivers).get(columnName)}
     * but this cache the result of parsing the JSON so that while this might be called for multiple columns on the same {@code bindIndex}
     * value, the underlying JSON value is only parsed/processed once.
     *
     * Note: this is a bit more involved in CQL specifics than this class generally is but we as we need to cache this per-query and in an object
     * that is available when we bind values, this is the easier place to have this.
     *
     * @param bindIndex the index of the bind value that should be interpreted as a JSON value.
     * @param columnName the name of the column we want the value of.
     * @param expectedReceivers the columns expected in the JSON value at index {@code bindIndex}. This is only used when parsing the
     * json initially and no check is done afterwards. So in practice, any call of this method on the same QueryOptions object and with the same
     * {@code bindIndx} values should use the same value for this parameter, but this isn't validated in any way.
     *
     * @return the value correspong to column {@code columnName} in the (JSON) bind value at index {@code bindIndex}. This may return null if the
     * JSON value has no value for this column.
     */
    public Term getJsonColumnValue(int bindIndex, ColumnIdentifier columnName, Collection<ColumnDefinition> expectedReceivers) throws InvalidRequestException
    {
        if (jsonValuesCache == null)
            jsonValuesCache = new ArrayList<>(Collections.<Map<ColumnIdentifier, Term>>nCopies(getValues().size(), null));

        Map<ColumnIdentifier, Term> jsonValue = jsonValuesCache.get(bindIndex);
        if (jsonValue == null)
        {
            ByteBuffer value = getValues().get(bindIndex);
            if (value == null)
                throw new InvalidRequestException("Got null for INSERT JSON values");

            jsonValue = Json.parseJson(UTF8Type.instance.getSerializer().deserialize(value), expectedReceivers);
            jsonValuesCache.set(bindIndex, jsonValue);
        }

        return jsonValue.get(columnName);
    }

    /**
     * Tells whether or not this <code>QueryOptions</code> contains the column specifications for the bound variables.
     * <p>The column specifications will be present only for prepared statements.</p>
     * @return <code>true</code> this <code>QueryOptions</code> contains the column specifications for the bound
     * variables, <code>false</code> otherwise.
     */
    public boolean hasColumnSpecifications()
    {
        return false;
    }

    /**
     * Returns the column specifications for the bound variables (<i>optional operation</i>).
     *
     * <p>The column specifications will be present only for prepared statements.</p>
     *
     * <p>Invoke the {@link hasColumnSpecifications} method before invoking this method in order to ensure that this
     * <code>QueryOptions</code> contains the column specifications.</p>
     *
     * @return the option names
     * @throws UnsupportedOperationException If this <code>QueryOptions</code> does not contains the column
     * specifications.
     */
    public ImmutableList<ColumnSpecification> getColumnSpecifications()
    {
        throw new UnsupportedOperationException();
    }

    /**  Serial consistency for conditional updates. */
    public ConsistencyLevel getSerialConsistency()
    {
        return getSpecificOptions().serialConsistency;
    }

    public long getTimestamp(QueryState state)
    {
        long tstamp = getSpecificOptions().timestamp;
        return tstamp != Long.MIN_VALUE ? tstamp : state.getTimestamp();
    }

    /**
     * The protocol version for the query. Will be 3 if the object don't come from
     * a native protocol request (i.e. it's been allocated locally or by CQL-over-thrift).
     */
    public abstract ProtocolVersion getProtocolVersion();

    // Mainly for the sake of BatchQueryOptions
    abstract SpecificOptions getSpecificOptions();

    /**
     * The paging options for this query, if paging is requested.
     *
     * @return the paging options for this query if paging is requested,
     * {@code null} otherwise.
     */
    public PagingOptions getPagingOptions()
    {
        return getSpecificOptions().pagingOptions;
    }

    /**
     * @return true if the client has requested pages to be pushed continuously.
     */
    public boolean continuousPagesRequested()
    {
        PagingOptions pagingOptions = getPagingOptions();
        return pagingOptions != null && pagingOptions.isContinuous();
    }

    public QueryOptions prepare(List<ColumnSpecification> specs)
    {
        return this;
    }

    static class DefaultQueryOptions extends QueryOptions
    {
        private final ConsistencyLevel consistency;
        private final List<ByteBuffer> values;
        private final boolean skipMetadata;

        private final SpecificOptions options;

        private final transient ProtocolVersion protocolVersion;

        DefaultQueryOptions(ConsistencyLevel consistency, List<ByteBuffer> values, boolean skipMetadata, SpecificOptions options, ProtocolVersion protocolVersion)
        {
            this.consistency = consistency;
            this.values = values;
            this.skipMetadata = skipMetadata;
            this.options = options;
            this.protocolVersion = protocolVersion;
        }

        public ConsistencyLevel getConsistency()
        {
            return consistency;
        }

        public List<ByteBuffer> getValues()
        {
            return values;
        }

        public boolean skipMetadata()
        {
            return skipMetadata;
        }

        public ProtocolVersion getProtocolVersion()
        {
            return protocolVersion;
        }

        SpecificOptions getSpecificOptions()
        {
            return options;
        }
    }

    static class QueryOptionsWrapper extends QueryOptions
    {
        protected final QueryOptions wrapped;

        QueryOptionsWrapper(QueryOptions wrapped)
        {
            this.wrapped = wrapped;
        }

        public List<ByteBuffer> getValues()
        {
            return this.wrapped.getValues();
        }

        public ConsistencyLevel getConsistency()
        {
            return wrapped.getConsistency();
        }

        public boolean skipMetadata()
        {
            return wrapped.skipMetadata();
        }

        public ProtocolVersion getProtocolVersion()
        {
            return wrapped.getProtocolVersion();
        }

        SpecificOptions getSpecificOptions()
        {
            return wrapped.getSpecificOptions();
        }

        @Override
        public QueryOptions prepare(List<ColumnSpecification> specs)
        {
            wrapped.prepare(specs);
            return this;
        }
    }

    /**
     * <code>QueryOptions</code> decorator that provides access to the column specifications.
     */
    static class OptionsWithColumnSpecifications extends QueryOptionsWrapper
    {
        private final ImmutableList<ColumnSpecification> columnSpecs;

        OptionsWithColumnSpecifications(QueryOptions wrapped, List<ColumnSpecification> columnSpecs)
        {
            super(wrapped);
            this.columnSpecs = ImmutableList.copyOf(columnSpecs);
        }

        @Override
        public boolean hasColumnSpecifications()
        {
            return true;
        }

        @Override
        public ImmutableList<ColumnSpecification> getColumnSpecifications()
        {
            return columnSpecs;
        }
    }

    static class OptionsWithNames extends QueryOptionsWrapper
    {
        private final List<String> names;
        private List<ByteBuffer> orderedValues;

        OptionsWithNames(DefaultQueryOptions wrapped, List<String> names)
        {
            super(wrapped);
            this.names = names;
        }

        @Override
        public QueryOptions prepare(List<ColumnSpecification> specs)
        {
            super.prepare(specs);

            orderedValues = new ArrayList<ByteBuffer>(specs.size());
            for (int i = 0; i < specs.size(); i++)
            {
                String name = specs.get(i).name.toString();
                for (int j = 0; j < names.size(); j++)
                {
                    if (name.equals(names.get(j)))
                    {
                        orderedValues.add(wrapped.getValues().get(j));
                        break;
                    }
                }
            }
            return this;
        }

        @Override
        public List<ByteBuffer> getValues()
        {
            assert orderedValues != null; // We should have called prepare first!
            return orderedValues;
        }
    }

    /**
     * Options that are not necessarily present in all queries.
     */
    static class SpecificOptions
    {
        private static final SpecificOptions DEFAULT = new SpecificOptions(null, null, Long.MIN_VALUE);

        private final PagingOptions pagingOptions;
        private final ConsistencyLevel serialConsistency;
        private final long timestamp;

        private SpecificOptions(PagingOptions pagingOptions, ConsistencyLevel serialConsistency, long timestamp)
        {
            this.pagingOptions = pagingOptions;
            this.serialConsistency = serialConsistency == null ? ConsistencyLevel.SERIAL : serialConsistency;
            this.timestamp = timestamp;
        }
    }

    /**
     * The paging options requested by the client, these include the page size,
     * the paging mechanism and other paging parameters.
     */
    public static class PagingOptions
    {
        private enum Mechanism
        {
            /** The client requests only a single page */
            SINGLE,

            /**
             * The client requests up to maxPages, or all pages if maxPages <= 0,
             * to be pushed continuously with a rate not exceeding maxPagesPerSecond per second, if >0,
             * or as quickly as possible.
             */
            CONTINUOUS
        };

        private final PageSize pageSize;
        private final Mechanism mechanism;
        private final PagingState pagingState;
        private final int maxPages;
        private final int maxPagesPerSecond;

        private PagingOptions(PageSize pageSize, Mechanism mechanism, PagingState pagingState)
        {
            this(pageSize, mechanism, pagingState, 0, 0);
        }

        private PagingOptions(PageSize pageSize, Mechanism mechanism, PagingState pagingState, int maxPages, int maxPagesPerSecond)
        {
            this.pageSize = pageSize;
            this.mechanism = mechanism;
            this.pagingState = pagingState;
            this.maxPages = maxPages <= 0 ? Integer.MAX_VALUE : maxPages;
            this.maxPagesPerSecond = maxPagesPerSecond;
        }

        public boolean isContinuous()
        {
            return mechanism == Mechanism.CONTINUOUS;
        }

        public PageSize pageSize()
        {
            return pageSize;
        }

        public PagingState state()
        {
            return pagingState;
        }

        public int maxPages()
        {
            return maxPages;
        }

        public int maxPagesPerSecond()
        {
            return maxPagesPerSecond;
        }

        @Override
        public final int hashCode()
        {
            return Objects.hash(pageSize, mechanism, pagingState, maxPages, maxPagesPerSecond);
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof PagingOptions))
                return false;

            PagingOptions that = (PagingOptions)o;
            return Objects.equals(this.pageSize, that.pageSize)
                && Objects.equals(this.mechanism, that.mechanism)
                && Objects.equals(this.pagingState, that.pagingState)
                && this.maxPages == that.maxPages
                && this.maxPagesPerSecond == that.maxPagesPerSecond;
        }

        @Override
        public String toString()
        {
            return String.format("%s %s (max %d, %d per second) with state %s",
                                 pageSize, mechanism, maxPages, maxPagesPerSecond, pagingState);
        }
    }

    private static class Codec implements CBCodec<QueryOptions>
    {
        private enum Flag
        {
            // public flags
            VALUES(0),
            SKIP_METADATA(1),
            PAGE_SIZE(2),
            PAGING_STATE(3),
            SERIAL_CONSISTENCY(4),
            TIMESTAMP(5),
            NAMES_FOR_VALUES(6),

            // private flags
            PAGE_SIZE_BYTES(30),
            CONTINUOUS_PAGING(31);

            /** The position of the bit that must be set to one to indicate a flag is set */
            private final int pos;

            Flag(int pos)
            {
                this.pos = pos;
            }

            private static final Flag[] ALL_VALUES = values();

            public static EnumSet<Flag> deserialize(int flags)
            {
                EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
                for (Flag flag : ALL_VALUES)
                {
                    if ((flags & (1 << flag.pos)) != 0)
                        set.add(flag);
                }
                return set;
            }

            public static int serialize(EnumSet<Flag> flags)
            {
                int i = 0;
                for (Flag flag : flags)
                    i |= 1 << flag.pos;
                return i;
            }
        }

        public QueryOptions decode(ByteBuf body, ProtocolVersion version)
        {
            ConsistencyLevel consistency = CBUtil.readConsistencyLevel(body);
            EnumSet<Flag> flags = Flag.deserialize(version.isGreaterOrEqualTo(ProtocolVersion.V5)
                                                   ? (int)body.readUnsignedInt()
                                                   : (int)body.readByte());

            List<ByteBuffer> values = Collections.<ByteBuffer>emptyList();
            List<String> names = null;
            if (flags.contains(Flag.VALUES))
            {
                if (flags.contains(Flag.NAMES_FOR_VALUES))
                {
                    Pair<List<String>, List<ByteBuffer>> namesAndValues = CBUtil.readNameAndValueList(body, version);
                    names = namesAndValues.left;
                    values = namesAndValues.right;
                }
                else
                {
                    values = CBUtil.readValueList(body, version);
                }
            }

            boolean skipMetadata = flags.contains(Flag.SKIP_METADATA);
            flags.remove(Flag.VALUES);
            flags.remove(Flag.SKIP_METADATA);

            SpecificOptions options = SpecificOptions.DEFAULT;
            if (!flags.isEmpty())
            {
                PageSize pageSize = null;
                if (flags.contains(Flag.PAGE_SIZE))
                {
                    try
                    {
                        pageSize = new PageSize(body.readInt(),
                                                flags.contains(Flag.PAGE_SIZE_BYTES) ? PageSize.PageUnit.BYTES : PageSize.PageUnit.ROWS);
                    }
                    catch (IllegalArgumentException e)
                    {
                        throw new ProtocolException(String.format("Invalid page size: " + e.getMessage()));
                    }
                }

                PagingState pagingState = flags.contains(Flag.PAGING_STATE) ? PagingState.deserialize(CBUtil.readValue(body), version) : null;

                ConsistencyLevel serialConsistency = flags.contains(Flag.SERIAL_CONSISTENCY) ? CBUtil.readConsistencyLevel(body) : ConsistencyLevel.SERIAL;

                long timestamp = Long.MIN_VALUE;
                if (flags.contains(Flag.TIMESTAMP))
                {
                    long ts = body.readLong();
                    if (ts == Long.MIN_VALUE)
                        throw new ProtocolException(String.format("Out of bound timestamp, must be in [%d, %d] (got %d)", Long.MIN_VALUE + 1, Long.MAX_VALUE, ts));
                    timestamp = ts;
                }

                boolean hasContinuousPaging = flags.contains(Flag.CONTINUOUS_PAGING);
                if (hasContinuousPaging)
                {
                    if (version.isSmallerThan(ProtocolVersion.DSE_V1))
                        throw new ProtocolException("Continuous paging requires DSE_V1 or higher");
                    if (pageSize == null)
                        throw new ProtocolException("Cannot use continuous paging without indicating a page size");
                }
                else if (pageSize != null && !pageSize.isInRows())
                {
                    throw new ProtocolException("Page size in bytes is only supported with continuous paging");
                }

                PagingOptions pagingOptions = hasContinuousPaging
                    ? new PagingOptions(pageSize, PagingOptions.Mechanism.CONTINUOUS, pagingState, body.readInt(), body.readInt())
                    : new PagingOptions(pageSize, PagingOptions.Mechanism.SINGLE, pagingState);

                options = new SpecificOptions(pagingOptions, serialConsistency, timestamp);
            }

            DefaultQueryOptions opts = new DefaultQueryOptions(consistency, values, skipMetadata, options, version);
            return names == null ? opts : new OptionsWithNames(opts, names);
        }

        public void encode(QueryOptions options, ByteBuf dest, ProtocolVersion version)
        {
            PagingOptions pagingOptions = options.getPagingOptions();

            CBUtil.writeConsistencyLevel(options.getConsistency(), dest);

            EnumSet<Flag> flags = gatherFlags(options);
            if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
                dest.writeInt(Flag.serialize(flags));
            else
                dest.writeByte((byte)Flag.serialize(flags));

            if (flags.contains(Flag.VALUES))
                CBUtil.writeValueList(options.getValues(), dest);
            if (flags.contains(Flag.PAGE_SIZE))
                dest.writeInt(pagingOptions.pageSize().rawSize());
            if (flags.contains(Flag.PAGING_STATE))
                CBUtil.writeValue(pagingOptions.state().serialize(version), dest);
            if (flags.contains(Flag.SERIAL_CONSISTENCY))
                CBUtil.writeConsistencyLevel(options.getSerialConsistency(), dest);
            if (flags.contains(Flag.TIMESTAMP))
                dest.writeLong(options.getSpecificOptions().timestamp);
            if (flags.contains(Flag.CONTINUOUS_PAGING))
            {
                dest.writeInt(pagingOptions.maxPages);
                dest.writeInt(pagingOptions.maxPagesPerSecond);
            }

            // Note that we don't really have to bother with NAMES_FOR_VALUES server side,
            // and in fact we never really encode QueryOptions, only decode them, so we
            // don't bother.
        }

        public int encodedSize(QueryOptions options, ProtocolVersion version)
        {
            PagingOptions pagingOptions = options.getPagingOptions();
            int size = 0;

            size += CBUtil.sizeOfConsistencyLevel(options.getConsistency());

            EnumSet<Flag> flags = gatherFlags(options);
            size += (version.isGreaterOrEqualTo(ProtocolVersion.V5) ? 4 : 1);

            if (flags.contains(Flag.VALUES))
                size += CBUtil.sizeOfValueList(options.getValues());
            if (flags.contains(Flag.PAGE_SIZE))
                size += 4;
            if (flags.contains(Flag.PAGING_STATE))
                size += CBUtil.sizeOfValue(pagingOptions.state().serializedSize(version));
            if (flags.contains(Flag.SERIAL_CONSISTENCY))
                size += CBUtil.sizeOfConsistencyLevel(options.getSerialConsistency());
            if (flags.contains(Flag.TIMESTAMP))
                size += 8;
            if (flags.contains(Flag.CONTINUOUS_PAGING))
                size += 8;

            return size;
        }

        private EnumSet<Flag> gatherFlags(QueryOptions options)
        {
            EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
            if (options.getValues().size() > 0)
                flags.add(Flag.VALUES);
            if (options.skipMetadata())
                flags.add(Flag.SKIP_METADATA);
            PagingOptions pagingOptions = options.getPagingOptions();
            if (pagingOptions != null)
            {
                flags.add(Flag.PAGE_SIZE);
                if (pagingOptions.pageSize.isInBytes())
                    flags.add(Flag.PAGE_SIZE_BYTES);
                if (pagingOptions.state() != null)
                    flags.add(Flag.PAGING_STATE);
                if (pagingOptions.isContinuous())
                    flags.add(Flag.CONTINUOUS_PAGING);
            }
            if (options.getSerialConsistency() != ConsistencyLevel.SERIAL)
                flags.add(Flag.SERIAL_CONSISTENCY);
            if (options.getSpecificOptions().timestamp != Long.MIN_VALUE)
                flags.add(Flag.TIMESTAMP);

            return flags;
        }
    }
}
