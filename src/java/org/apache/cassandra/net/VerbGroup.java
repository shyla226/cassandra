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
package org.apache.cassandra.net;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.cassandra.concurrent.ExecutorSupplier;
import org.apache.cassandra.concurrent.Schedulable;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.db.WriteVerbs;
import org.apache.cassandra.db.monitoring.Monitor;
import org.apache.cassandra.db.monitoring.Monitorable;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.TimeoutSupplier;
import org.apache.cassandra.utils.versioning.Versioned;


/**
 * A grouping of multiple related {@link Verb}s.
 * <p>
 * Any {@link Verb} is intrinsically linked to a {@link VerbGroup} to which it belongs. Groups are a code organization
 * facility: the code has modules of different purpose and each uses a number of verbs to work, so groups provide a way
 * to group all the verb of a module in one place. In other words, a group is meant to provide the messaging interface
 * of a particular module.
 * <p>
 * In practice, 2 main aspects are dealt with at the group level: versioning and serialization codes. Each group has an
 * associated version that reflects any change made to the serialization to any of its verb. The messaging protocol
 * version is thus mainly defined as the union of the versions of its verb groups (see {@link MessagingVersion}).
 * Further, each group assign serialization codes of its verb independently of other groups (and those codes depend on
 * the version, thus providing an easy way to drop/add verbs in a particular version).
 * <p>
 * Although this is not strictly enforced statically or dynamically, any group implementation is meant to declare all
 * verbs it is composed of in its constructor using the {@link RegistrationHelper} facility. See the {@link WriteVerbs}
 * ctor for an example of how this is meant.
 *
 * @param <V> the type of the version used by this group. Groups is our main level of granularity for versioning for the
 *           internode messaging and so each group declares it's own version enum; this is the type of that enum.
 */
public abstract class VerbGroup<V extends Enum<V> & Version<V>> implements Iterable<Verb<?, ?>>
{
    private final Verbs.Group id;
    private final boolean isInternal;
    private final Class<V> versionClass;

    private final List<Verb<?, ?>> verbs = new ArrayList<>();

    private final EnumMap<V, VersionSerializers> versionedSerializers;

    private boolean helperCreated;

    protected VerbGroup(Verbs.Group id,
                        boolean isInternal,
                        Class<V> versionClass)
    {
        this.id = id;
        this.isInternal = isInternal;
        this.versionClass = versionClass;

        this.versionedSerializers = new EnumMap<>(versionClass);
        for (V v : versionClass.getEnumConstants())
            versionedSerializers.put(v, new VersionSerializers(id.name(), v));
    }

    /**
     * The {@link Verbs.Group} enum value associated to this group.
     */
    public Verbs.Group id()
    {
        return id;
    }

    /**
     * The internal helper to use for registering verbs for the group. This method should only be called once in the
     * group ctor and all verbs should then be registered against the returned instance.
     * <p>
     * For groups where the request stage is the same (or almost always the same) for every verb, it is convenient to set
     * the stage at the helper level through the {@link RegistrationHelper#stage(Stage)} method. Otherwise, each verb
     * can be assigned its own verb.
     */
    protected RegistrationHelper helper()
    {
        if (helperCreated)
            throw new IllegalStateException("Should only create a single RegistrationHelper per group");

        helperCreated = true;
        return new RegistrationHelper();
    }

    /**
     * Whether this is an "internal" group, where "internal" means that the verbs of group are used a part of user
     * requests but rather for some internal mechanism.
     * <p>
     * Note: this isn't of tremendous importance and only influences which stage ({@link Stage#INTERNAL_RESPONSE} versus
     * {@link Stage#REQUEST_RESPONSE}) we use to execute responses for the verbs of this group. We haven't used this
     * terribly consistently in the past however so it's not even clean separating such stage even brings much benefits.
     * This might also go away with TPC.
     */
    public boolean isInternal()
    {
        return isInternal;
    }

    /**
     * Return the serialization information for the verbs of this group for the provided version.
     *
     * @param version the version for which to return serialization infos.
     * @return the serialization information for the verb of this group at version {@code version}.
     */
    VersionSerializers forVersion(V version)
    {
        return versionedSerializers.get(version);
    }

    public Iterator<Verb<?, ?>> iterator()
    {
        return verbs.iterator();
    }

    @Override
    public String toString()
    {
        return id.toString();
    }

    private static <T> ExecutorSupplier<T> maybeGetRequestExecutor(Class<T> requestClass, Stage defaultStage)
    {
        if (Schedulable.class.isAssignableFrom(requestClass))
            return (p) -> ((Schedulable)p).getOperationExecutor();

        return defaultStage == null ? null : (p) -> StageManager.getStage(defaultStage);
    }

    // Note that the response executor depends on the _request_ payload, not the _response_ one. This is because
    // the request is what contains the most information on the actual request-response exchange in practice (so, is
    // more likely to be useful at deciding the executor of its response. In fact, in case like reads, we need to
    // know the executor before we truly deserialize the payload, so making the executor choice based on the response
    // payload wouldn't work (of course, we could make imagine making the decision be based on both the request
    // and response payload, but that would require a new 'ExecutorSupplier' class taking 2 arguments and we simply
    // don't need that for now).
    private static <T> ExecutorSupplier<T> maybeGetResponseExecutor(Class<T> requestClass, boolean isInternal)
    {
        if (Schedulable.class.isAssignableFrom(requestClass))
            return (p) -> ((Schedulable)p).getOperationExecutor();

        return (p) -> StageManager.getStage(isInternal ? Stage.INTERNAL_RESPONSE : Stage.REQUEST_RESPONSE);
    }

    @SuppressWarnings("unchecked")
    private static <T> Serializer<T> maybeGetSerializer(Class<T> klass)
    {
        try
        {
            Field field = klass.getField("serializer");
            if (!field.getType().equals(Serializer.class) || !Modifier.isStatic(field.getModifiers()))
                return null;

            return (Serializer<T>) field.get(null);
        }
        catch (Exception e)
        {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private <T> Function<V, Serializer<T>> maybeGetVersionedSerializers(Class<T> klass)
    {
        try
        {
            Field field = klass.getField("serializers");
            if (!field.getType().equals(Versioned.class) || !Modifier.isStatic(field.getModifiers()))
                return null;

            Versioned<V, Serializer<T>> versioned = (Versioned<V, Serializer<T>>) field.get(null);
            return versioned::get;
        }
        catch (Exception e)
        {
            return null;
        }
    }

    /**
     * An helper to create and register the verb of a group (see {@link #helper()} for how and when this should be used).
     * <p>
     * Each verb registration starts by the call to one of:
     * <ul>
     *     <li>{@link #oneWay}: to declare a {@link Verb.OneWay} verb.</li>
     *     <li>{@link #ackedRequest}: to declare a {@link Verb.AckedRequest} verb.</li>
     *     <li>{@link #requestResponse}: to declare a {@link Verb.RequestResponse} verb.</li>
     *     <li>{@link #monitoredRequestResponse}: to declare a {@link Verb.RequestResponse} verb that uses a
     *         monitoring. This is the same than {@link #requestResponse} except that the handler is provided with
     *         a {@link Monitor} to use for monitoring (see {@link VerbHandlers.MonitoredRequestResponse} for details)</li>
     * </ul>
     */
    protected class RegistrationHelper
    {
        private int idx;
        private final int[] versionCodes = new int[versionClass.getEnumConstants().length];
        private Stage defaultStage;
        private DroppedMessages.Group defaultDroppedGroup;
        private boolean executeOnIOScheduler;

        public RegistrationHelper stage(Stage defaultStage)
        {
            this.defaultStage = defaultStage;
            return this;
        }

        public RegistrationHelper droppedGroup(DroppedMessages.Group defaultDroppedGroup)
        {
            this.defaultDroppedGroup = defaultDroppedGroup;
            return this;
        }

        public <P> OneWayBuilder<P> oneWay(String verbName, Class<P> klass)
        {
            return new OneWayBuilder<>(verbName, idx++, klass);
        }

        public <P> AckedRequestBuilder<P> ackedRequest(String verbName, Class<P> klass)
        {
            return new AckedRequestBuilder<>(verbName, idx++, klass);
        }

        public <P, Q> RequestResponseBuilder<P, Q> requestResponse(String verbName, Class<P> requestClass, Class<Q> responseClass)
        {
            return new RequestResponseBuilder<>(verbName, idx++, requestClass, responseClass, executeOnIOScheduler);
        }

        public <P extends Monitorable, Q> MonitoredRequestResponseBuilder<P, Q> monitoredRequestResponse(String verbName, Class<P> requestClass, Class<Q> responseClass)
        {
            return new MonitoredRequestResponseBuilder<>(verbName, idx++, requestClass, responseClass);
        }

        public RegistrationHelper executeOnIOScheduler()
        {
            this.executeOnIOScheduler = true;
            return this;
        }

        public class VerbBuilder<P, Q, T>
        {
            private final String name;
            private final int groupIdx;
            private final boolean isOneWay;

            private TimeoutSupplier<P> timeoutSupplier;
            private ExecutorSupplier<P> requestExecutor;
            private ExecutorSupplier<P> responseExecutor;

            private Serializer<P> requestSerializer;
            private Serializer<Q> responseSerializer;

            private Function<V, Serializer<P>> requestSerializerFct;
            private Function<V, Serializer<Q>> responseSerializerFct;

            private DroppedMessages.Group droppedGroup;

            private V sinceVersion;
            private V untilVersion;

            private boolean supportsBackPressure;

            private VerbBuilder(String name, int groupIdx, boolean isOneWay, Class<P> requestClass, Class<Q> responseClass)
            {
                this.name = name;
                this.groupIdx = groupIdx;
                this.isOneWay = isOneWay;
                this.requestExecutor = maybeGetRequestExecutor(requestClass, defaultStage);
                this.responseExecutor = maybeGetResponseExecutor(requestClass, isInternal);
                this.requestSerializer = maybeGetSerializer(requestClass);
                this.responseSerializer = maybeGetSerializer(responseClass);
                this.requestSerializerFct = maybeGetVersionedSerializers(requestClass);
                this.responseSerializerFct = maybeGetVersionedSerializers(responseClass);
                this.droppedGroup = defaultDroppedGroup;
            }

            @SuppressWarnings("unchecked")
            private T us()
            {
                return (T)this;
            }

            public T timeout(TimeoutSupplier<P> supplier)
            {
                this.timeoutSupplier = supplier;
                return us();
            }

            public T timeout(Supplier<Long> supplier)
            {
                this.timeoutSupplier = request -> supplier.get();
                return us();
            }

            public T timeout(int timeout, TimeUnit unit)
            {
                long timeoutMillis = unit.toMillis(timeout);
                return timeout(request -> timeoutMillis);
            }

            public T requestStage(Stage stage)
            {
                this.requestExecutor = (p) -> StageManager.getStage(stage);
                return us();
            }

            public T requestExecutor(TracingAwareExecutor tae)
            {
                this.requestExecutor = (p) -> tae;
                return us();
            }

            public T responseExecutor(ExecutorSupplier<P> responseExecutor)
            {
                this.responseExecutor = responseExecutor;
                return us();
            }

            public T droppedGroup(DroppedMessages.Group droppedGroup)
            {
                this.droppedGroup = droppedGroup;
                return us();
            }

            public T withRequestSerializer(Serializer<P> serializer)
            {
                this.requestSerializer = serializer;
                return us();
            }

            public T withRequestSerializer(Function<V, Serializer<P>> serializerFunction)
            {
                this.requestSerializerFct = serializerFunction;
                return us();
            }

            public T withResponseSerializer(Serializer<Q> serializer)
            {
                assert !isOneWay : "Shouldn't set the response serializer of one-way verbs";
                this.responseSerializer = serializer;
                return us();
            }

            public T withResponseSerializer(Function<V, Serializer<Q>> serializerFunction)
            {
                assert !isOneWay : "Shouldn't set the response serializer of one-way verbs";
                this.responseSerializerFct = serializerFunction;
                return us();
            }

            public T withBackPressure()
            {
                this.supportsBackPressure = true;
                return us();
            }

            /**
             * First version of which the verb is part of (inclusive).
             */
            public T since(V version)
            {
                if (sinceVersion != null)
                    throw new IllegalStateException("since() should be called at most once for each verb");
                this.sinceVersion = version;
                return us();
            }

            /**
             * Last version of which the verb is part of (inclusive).
             */
            public T until(V version)
            {
                if (untilVersion != null)
                    throw new IllegalStateException("until() should be called at most once for each verb");
                this.untilVersion = version;
                return us();
            }

            protected Verb.Info<P> info()
            {
                if (requestExecutor == null)
                    throw new IllegalStateException("Unless the request payload implements the Schedulable interface, a request stage is required (either at the RegistrationHelper level or at the VerbBuilder one)");
                if (isOneWay && supportsBackPressure)
                    throw new IllegalStateException("Back pressure doesn't make sense for one-way message (no response is sent so we can't keep track of in-flight requests to an host)");
                if (!isOneWay && droppedGroup == null)
                    throw new IllegalStateException("Missing 'dropped group', should be indicated either at the RegistrationHelper lever or at the VerbBuilder one");

                return new Verb.Info<>(VerbGroup.this, groupIdx, name, requestExecutor, responseExecutor, supportsBackPressure, isOneWay ? null : droppedGroup);
            }

            TimeoutSupplier<P> timeoutSupplier()
            {
                if (isOneWay && timeoutSupplier != null)
                    throw new IllegalStateException("One way verb should not define a timeout supplier, we'll never timeout them");

                if (!isOneWay && timeoutSupplier == null)
                    throw new IllegalStateException("Non-one way verb must define a timeout supplier");

                return timeoutSupplier;
            }

            <X extends Verb<P, Q>> X add(X verb)
            {
                // Add the verb itself to our list
                verbs.add(verb);

                for (V v : versionClass.getEnumConstants())
                {
                    if (sinceVersion != null && v.compareTo(sinceVersion) < 0)
                        continue;

                    if (untilVersion != null && v.compareTo(untilVersion) > 0)
                        break;

                    int code = versionCodes[v.ordinal()]++;

                    Serializer<P> reqSerializer = requestSerializerFct == null ? requestSerializer : requestSerializerFct.apply(v);
                    if (reqSerializer == null)
                        throw new IllegalStateException(String.format("No request serializer defined for verb %s and no default one found.", name));

                    Serializer<Q> respSerializer = null;
                    if (!isOneWay)
                    {
                        respSerializer = responseSerializerFct == null ? responseSerializer : responseSerializerFct.apply(v);
                        if (respSerializer == null)
                            throw new IllegalStateException(String.format("No response serializer defined for verb %s and no default one found.", name));
                    }
                    versionedSerializers.get(v).add(verb, code, reqSerializer, respSerializer);
                }
                return verb;
            }
        }

        public class OneWayBuilder<P> extends VerbBuilder<P, NoResponse, OneWayBuilder<P>>
        {
            private OneWayBuilder(String name, int groupIdx, Class<P> requestClass)
            {
                super(name, groupIdx, true, requestClass, NoResponse.class);
            }

            public Verb.OneWay<P> handler(VerbHandler<P, NoResponse> handler)
            {
                return add(new Verb.OneWay<>(info(), handler));
            }

            public Verb.OneWay<P> handler(VerbHandlers.OneWay<P> handler)
            {
                return add(new Verb.OneWay<>(info(), handler));
            }
        }

        public class AckedRequestBuilder<P> extends VerbBuilder<P, EmptyPayload, AckedRequestBuilder<P>>
        {
            private AckedRequestBuilder(String name, int groupIdx, Class<P> requestClass)
            {
                super(name, groupIdx, false, requestClass, EmptyPayload.class);
            }

            public Verb.AckedRequest<P> handler(VerbHandlers.AckedRequest<P> handler)
            {
                return add(new Verb.AckedRequest<>(info(), timeoutSupplier(), handler));
            }

            public Verb.AckedRequest<P> syncHandler(VerbHandlers.SyncAckedRequest<P> handler)
            {
                return add(new Verb.AckedRequest<>(info(), timeoutSupplier(), handler));
            }
        }

        public class RequestResponseBuilder<P, Q> extends VerbBuilder<P, Q, RequestResponseBuilder<P, Q>>
        {
            private RequestResponseBuilder(String name, int groupIdx, Class<P> requestClass, Class<Q> responseClass, boolean executeOnIOScheduler)
            {
                super(name, groupIdx, false, requestClass, responseClass);
            }

            public Verb.RequestResponse<P, Q> handler(VerbHandlers.RequestResponse<P, Q> handler)
            {
                return add(new Verb.RequestResponse<>(info(), timeoutSupplier(), handler));
            }

            public Verb.RequestResponse<P, Q> syncHandler(VerbHandlers.SyncRequestResponse<P, Q> handler)
            {
                return add(new Verb.RequestResponse<>(info(), timeoutSupplier(), handler));
            }
        }

        public class MonitoredRequestResponseBuilder<P extends Monitorable, Q> extends VerbBuilder<P, Q, MonitoredRequestResponseBuilder<P, Q>>
        {
            private MonitoredRequestResponseBuilder(String name, int groupIdx, Class<P> requestClass, Class<Q> responseClass)
            {
                super(name, groupIdx, false, requestClass, responseClass);
            }

            public Verb.RequestResponse<P, Q> handler(VerbHandlers.MonitoredRequestResponse<P, Q> handler)
            {
                return add(new Verb.RequestResponse<>(info(), timeoutSupplier(), handler));
            }

            public Verb.RequestResponse<P, Q> syncHandler(VerbHandlers.SyncMonitoredRequestResponse<P, Q> handler)
            {
                return add(new Verb.RequestResponse<>(info(), timeoutSupplier(), handler));
            }
        }
    }
}
