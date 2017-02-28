/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jboss.byteman.test.ng.InjectorHelper;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.utils.SigarLibrary;
import org.hyperic.sigar.ProcState;
import org.hyperic.sigar.Sigar;

import static com.datastax.dse.framework.DseTestRunner.executeCodeOnNode;

public class Util
{
    private final static Logger logger = LoggerFactory.getLogger(Util.class);

    public final static Supplier<Sigar> sigar = Sigar::new;

    public static void generateConfig(CassandraYamlBuilder cassandraYamlBuilder) throws IOException
    {
        cassandraYamlBuilder.build();
        System.setProperty("cassandra.config", cassandraYamlBuilder.getConfigFilePath().toUri().toString());
    }

    public static void generateConfigs() throws IOException
    {
        CassandraYamlBuilder cassandraYamlBuilder = CassandraYamlBuilder.newInstance().withSnitch(SimpleSnitch.class);

        generateConfig(cassandraYamlBuilder);
    }

    /**
     * @return string representation of the thread dump, ready for printing
     */
    public static String getThreadDump()
    {
        final StringBuilder dump = new StringBuilder();
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
        for (ThreadInfo threadInfo : threadInfos)
        {
            if (threadInfo != null)
            {
                dump.append('"');
                dump.append(threadInfo.getThreadName());
                dump.append("\" ");
                final Thread.State state = threadInfo.getThreadState();
                dump.append("\n   java.lang.Thread.State: ");
                dump.append(state);
                final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
                for (final StackTraceElement stackTraceElement : stackTraceElements)
                {
                    dump.append("\n        at ");
                    dump.append(stackTraceElement);
                }
                dump.append("\n\n");
            }
        }
        return dump.toString();
    }

    /**
     * @return collection of ip addresses the machine has
     */
    public static Collection<InetAddress> allIpAddresses()
    {
        Collection<InetAddress> allAddresses = new ArrayList<>();
        try
        {
            Enumeration<NetworkInterface> allIfs = NetworkInterface.getNetworkInterfaces();
            while (allIfs.hasMoreElements())
            {
                Enumeration<InetAddress> addresses = allIfs.nextElement().getInetAddresses();
                while (addresses.hasMoreElements())
                {
                    allAddresses.add(addresses.nextElement());
                }
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return allAddresses;
    }

    /**
     * @return true if test run within docker container
     */
    public static boolean isInsideDockerContainer()
    {
        return Boolean.parseBoolean(System.getProperty("useDocker"));
    }

    /**
     * Sends the given signal to the entire process tree. Works only on Unix-like OS.
     * For signal reference go to <a href="https://en.wikipedia.org/wiki/Unix_signal">this website</a>.
     * The most common signal is SIGINT, which has the number 2.
     */
    public static void sendSignalToProcessesTree(int signal, int parentProcessID)
    {
        Preconditions.checkState(SystemUtils.IS_OS_UNIX, "This method can be run only on Unix-like OS");
        int result;
        try
        {
            // TODO: fix this
            //String cmd = DseTestRunner.testEnvironment.dseHome.resolve("test/scripts/kill-process-tree").toString();
            String cmd = null;
            result = new ProcessBuilder(cmd, String.valueOf(signal), String.valueOf(parentProcessID))
                     .inheritIO().start().waitFor();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to run kill command for pid: " + parentProcessID, e);
        }

        if (result != 0)
        {
            throw new RuntimeException("Failed to kill process " + parentProcessID);
        }
    }

    /**
     * Returns an enum value of a given string or default value if the given string is blank or null.
     */
    @SuppressWarnings("unchecked")
    public static <T extends Enum<T>> Enum<T> toEnum(String raw, Enum<T> defaultValue)
    {
        if (StringUtils.isBlank(raw))
        {
            return defaultValue;
        }
        else
        {
            return T.valueOf(defaultValue.getClass(), raw);
        }
    }

    private static int getUnixProcessID(Process process)
    {
        Preconditions.checkState(SystemUtils.IS_OS_UNIX, "This method can be run only on Unix-like OS");
        try
        {
            Class cls = Class.forName("java.lang.UNIXProcess");
            Field field = FieldUtils.getDeclaredField(cls, "pid", true);
            return field.getInt(process);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to get PID of a process " + process, e);
        }
    }

    /**
     * Return PID of the given process. It uses the right implementation depending on which operating
     * system this program is running on.
     */
    public static int getProcessID(Process process)
    {
        if (SystemUtils.IS_OS_UNIX)
        {
            return getUnixProcessID(process);
        }
        else
        {
            throw new IllegalStateException("This method is not supported on this operation system");
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T deserializeWithClassLoader(byte[] objectData, ClassLoader classLoader)
    {
        final ByteArrayInputStream objectDataIn = new ByteArrayInputStream(objectData);

        try (ClassLoaderObjectInputStream in = new ClassLoaderObjectInputStream(classLoader, objectDataIn))
        {
            return (T) in.readObject();
        }
        catch (final ClassNotFoundException ex)
        {
            throw new SerializationException("ClassNotFoundException while reading cloned object data", ex);
        }
        catch (final IOException ex)
        {
            throw new SerializationException("IOException while reading cloned object data", ex);
        }
    }

    public static Integer readInjectedCounter(final String counterName) throws Exception
    {
        return readInjectedCounter(1, counterName);
    }

    public static Integer readInjectedCounter(int node, final String counterName) throws Exception
    {
        return executeCodeOnNode(node, new DseTestRunner.SerializableCallable<Integer>()
        {
            @Override
            public Integer call() throws Exception
            {
                return InjectorHelper.readCounter(counterName);
            }
        }).get();
    }

    public static int readInjectedCounter(int node, final String counterName, int defaultValue) throws Exception
    {
        Integer value = executeCodeOnNode(node, new DseTestRunner.SerializableCallable<Integer>()
        {
            @Override
            public Integer call() throws Exception
            {
                return InjectorHelper.readCounter(counterName);
            }
        }).get();
        return value == null ? defaultValue : value;
    }

    public static void resetInjectedCounter(final String counterName)
    {
        resetInjectedCounter(1, counterName);
    }

    public static void resetInjectedCounter(final int node, final String counterName)
    {
        try
        {
            executeCodeOnNode(node, new DseTestRunner.SerializableCallable<Integer>()
            {
                @Override
                public Integer call() throws Exception
                {
                    InjectorHelper.resetCounter(counterName);
                    return null;
                }
            }).get();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Multimap<Long, Long> getPpidToPidMap()
    {
        HashMultimap<Long, Long> result = HashMultimap.create();
        try
        {
            long[] procList = sigar.get().getProcList();
            for (long procId : procList)
            {
                try
                {
                    ProcState procStat = sigar.get().getProcState(procId);
                    if (procStat != null)
                    {
                        result.put(procStat.getPpid(), procId);
                    }
                }
                catch (Throwable ex)
                {
                    logger.warn("Failed to get process {} stat: {}", procId, ex.getMessage());
                }
            }
            return result;
        }
        catch (Throwable ex)
        {
            logger.warn("Failed to get processes map: {}", ex);
        }
        return result;
    }

    /**
     * Kills the whole processes tree rooted at given process. Does not throw exception on failure.
     */
    public static void killProcessTree(long processId, int signal)
    {
        Queue<Long> queue = new LinkedList<>();
        queue.add(processId);

        Multimap<Long, Long> processesMap = getPpidToPidMap();
        while (!queue.isEmpty())
        {
            long nextPid = queue.remove();
            Collection<Long> childrenPids = processesMap.get(nextPid);
            if (childrenPids != null)
            {
                queue.addAll(processesMap.get(nextPid));
            }

            kill(nextPid, signal);
        }
    }

    public static Set<Long> findProcess(long rootProcessId, Predicate<Long> filter)
    {
        Set<Long> collectedProcesses = Sets.newHashSet();
        Queue<Long> queue = new LinkedList<>();
        queue.add(rootProcessId);

        Multimap<Long, Long> processesMap = getPpidToPidMap();
        while (!queue.isEmpty())
        {
            long nextPid = queue.remove();
            Collection<Long> childrenPids = processesMap.get(nextPid);
            if (childrenPids != null)
            {
                queue.addAll(processesMap.get(nextPid));
            }

            if (filter.test(nextPid))
            {
                collectedProcesses.add(nextPid);
            }
        }

        return collectedProcesses;
    }

    /**
     * Kills the process with the specified signal. Does not throw any exception in case of failure.
     */
    public static void kill(long processId, int signal)
    {
        logger.info("Killing process {} with signal {}", processId, signal);
        try
        {
            sigar.get().kill(processId, signal);
        }
        catch (Throwable ex)
        {
            logger.warn("Could not kill process {}: {}", processId, ex.getMessage());
        }
    }

    /**
     * Returns a cartesian product of two given arrays of objects, so that each element of resulting array is an array
     * of objects from {@code objects1} and {@code objects2}. This is useful for preparing parametrized tests from
     * partial sets of parameters. For example:
     * <pre>
     *     Object[] params1 = new Object[] {
     *         new Object[] {1, 2},
     *         new Object[] {3, 4},
     *         new Object[] {5, 6}
     *     }
     *     Object[] params2 = new Object[] {
     *         new Object[] {"a", "b"},
     *         new Object[] {"c", "d"},
     *         new Object[] {"e", "f"}
     *     }
     *
     *     Object[] result = cartesian(params1, params2);
     *     // result = {{1, 2, "a", "b"}, {1, 2, "c", "d"}, {1, 2, "e", "f"},
     *     //           {3, 4, "a", "b"}, {3, 4, "c", "d"}, {3, 4, "e", "f"},
     *     //           {5, 6, "a", "b"}, {5, 6, "c", "d"}, {5, 6, "e", "f"}}
     * </pre>
     *
     * @param objects1 array of objects or array of arrays of objects
     * @param objects2 array of objects or array of arrays of objects
     * @return array of arrays of objects
     */
    public static Object[] cartesian(Object[] objects1, Object[] objects2)
    {
        int lenght = objects1.length * objects2.length;
        Object[] result = new Object[lenght];
        int cnt = 0;
        for (Object o1 : objects1)
        {
            Object[] arr1;
            if (o1 instanceof Object[])
            {
                arr1 = (Object[]) o1;
            }
            else
            {
                arr1 = new Object[]{ o1 };
            }
            for (Object o2 : objects2)
            {
                Object[] arr2;
                if (o2 instanceof Object[])
                {
                    arr2 = (Object[]) o2;
                }
                else
                {
                    arr2 = new Object[]{ o2 };
                }
                Object[] arr = new Object[arr1.length + arr2.length];
                System.arraycopy(arr1, 0, arr, 0, arr1.length);
                System.arraycopy(arr2, 0, arr, arr1.length, arr2.length);
                result[cnt++] = arr;
            }
        }

        return result;
    }
}
