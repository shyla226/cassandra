/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.MBeanServerBuilder;
import javax.management.MBeanServerDelegate;
import javax.management.ObjectName;

/**
 * Some parts of C* codebase register MBeans, and unfortunately this cannot be avoided.
 * The default MBean server does not allow to register the same mbean more than once, i.e. it throws {@link InstanceAlreadyExistsException}.
 * This situation can occur in tests, causing the test failures. Thus, we need a MBean server that is more tolerant - if a bean is already
 * registered, this implementation will unregister and register it again.
 */
public class LenientMBeanServerBuilder extends MBeanServerBuilder
{
    @Override
    public MBeanServer newMBeanServer(String domain, final MBeanServer server, MBeanServerDelegate delegate)
    {
        try
        {
            final MBeanServer jmxServer = new MBeanServerBuilder().newMBeanServer(domain, null, delegate);
            final Method unregisterMBeanMethod = MBeanServer.class.getMethod("unregisterMBean", new Class[]{ ObjectName.class });
            return (MBeanServer) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{ MBeanServer.class }, new InvocationHandler()
            {
                @Override
                public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
                {
                    try
                    {
                        if (method.getName().equals("registerMBean"))
                        {
                            try
                            {
                                return method.invoke(jmxServer, args);
                            }
                            catch (InvocationTargetException e)
                            {
                                if (e.getTargetException() instanceof InstanceAlreadyExistsException)
                                {
                                    unregisterMBeanMethod.invoke(jmxServer, new Object[]{ args[1] });
                                    return method.invoke(jmxServer, args);
                                }
                                throw e.getTargetException();
                            }
                        } else
                        {
                            return method.invoke(jmxServer, args);
                        }
                    }
                    catch (InvocationTargetException e)
                    {
                        throw e.getTargetException();
                    }
                }
            });
        }
        catch (Exception e)
        {
            throw new RuntimeException();
        }
    }
}
