/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework;


import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.UUID;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.factories.ReflectionSerializerFactory;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.googlecode.mobilityrpc.controller.impl.MobilityControllerImpl;
import com.googlecode.mobilityrpc.serialization.impl.KryoSerializer;
import com.googlecode.mobilityrpc.session.MobilitySession;
import com.googlecode.mobilityrpc.session.impl.MobilitySessionImpl;
import com.googlecode.mobilityrpc.session.impl.MobilitySessionInternal;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * Hack to make lambda serialization work in our integration tests.
 */
public class DseMobilityControllerImpl extends MobilityControllerImpl
{
    @Override
    public MobilitySession newSession()
    {
        MobilitySession mobilitySession = super.newSession();
        configureForLambdas(mobilitySession);
        return mobilitySession;
    }

    @Override
    public MobilitySessionInternal getMessageHandlingSession(UUID sessionId)
    {
        MobilitySessionInternal messageHandlingSession = super.getMessageHandlingSession(sessionId);
        configureForLambdas(messageHandlingSession);
        return messageHandlingSession;
    }

    private void configureForLambdas(MobilitySession mobilitySession)
    {
        try
        {
            Field kryoSerializerField = MobilitySessionImpl.class.getDeclaredField("defaultSerializer");
            kryoSerializerField.setAccessible(true);
            KryoSerializer serializer = (KryoSerializer) kryoSerializerField.get(mobilitySession);
            Field kryoField = KryoSerializer.class.getDeclaredField("kryo");
            kryoField.setAccessible(true);
            Kryo kryo = (Kryo) kryoField.get(serializer);
            kryo.register(SerializedLambda.class);
            kryo.register(Class.forName(Kryo.class.getName() + "$Closure"), new ClosureSerializer());
            //Fix the serialisation of exceptions so we get the stack trace.
            kryo.addDefaultSerializer(Throwable.class, new Serializer<Throwable>()
            {


                @Override
                public void write(Kryo kryo, Output output, Throwable e)
                {
                    ReflectionSerializerFactory reflectionSerializerFactory = new ReflectionSerializerFactory(FieldSerializer.class);
                    Serializer defaultSerializer = reflectionSerializerFactory.makeSerializer(kryo, e.getClass());
                    defaultSerializer.write(kryo, output, e);
                    kryo.writeObject(output, e.getStackTrace());
                }

                @Override
                public Throwable read(Kryo kryo, Input input, Class<Throwable> aClass)
                {

                    ReflectionSerializerFactory reflectionSerializerFactory = new ReflectionSerializerFactory(FieldSerializer.class);
                    Serializer defaultSerializer = reflectionSerializerFactory.makeSerializer(kryo, aClass);
                    Throwable e = (Throwable) defaultSerializer.read(kryo, input, aClass);
                    StackTraceElement[] stackTrace = kryo.readObject(input, StackTraceElement[].class);
                    e.setStackTrace(stackTrace);
                    return e;
                }
            });

            //This serializer makes sure that we don't try and serialize any of the base test fields and that also constructors are not called.
            kryo.addDefaultSerializer(DseTestRunner.class, new Serializer()
            {
                @Override
                public void write(Kryo kryo, Output output, Object o)
                {
                    FieldSerializer serializer = getFieldSerializer(kryo, o.getClass());
                    serializer.write(kryo, output, o);
                }

                @Override
                public Object read(Kryo kryo, Input input, Class aClass)
                {

                    FieldSerializer serializer = getFieldSerializer(kryo, aClass);
                    return serializer.read(kryo, input, aClass);
                }

                @javax.validation.constraints.NotNull
                private FieldSerializer getFieldSerializer(Kryo kryo, Class clazz)
                {
                    FieldSerializer serializer = new FieldSerializer(kryo, clazz)
                    {
                        @Override
                        protected Object create(Kryo kryo, Input input, Class type)
                        {
                            return new StdInstantiatorStrategy().newInstantiatorOf(type).newInstance();
                        }
                    };

                    for (FieldSerializer.CachedField field : Arrays.asList(serializer.getFields()))
                    {
                        Class<?> declaringClass = field.getField().getDeclaringClass();
                        if (declaringClass.equals(DseTestRunner.class) || declaringClass.equals(DseTestRunner.class))
                        {
                            serializer.removeField(field);
                        }
                    }
                    return serializer;
                }
            });
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
