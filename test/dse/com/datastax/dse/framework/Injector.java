/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;

import org.jboss.byteman.agent.submit.Submit;

public abstract class Injector
{
    private static final String COUNT =
    "RULE _count_$ADDRESS_$ID\n"
    + "CLASS %s\n"
    + "METHOD %s\n"
    + "HELPER org.jboss.byteman.test.ng.InjectorHelper\n"
    + "AT %s\n"
    + "IF isEnabled(\"$ID\")\n"
    + "DO storeAndIncrementCounter(\"$NAME\");\n"
    + "ENDRULE";
    private static final String CRASH =
    "RULE _crash_$ADDRESS_$ID\n"
    + "CLASS %s\n"
    + "METHOD %s\n"
    + "HELPER org.jboss.byteman.test.ng.InjectorHelper\n"
    + "AT %s\n"
    + "IF isEnabled(\"$ID\")\n"
    + "DO throw new %s(%s)\n"
    + "ENDRULE";
    private static final String CRASH_ONCE =
    "RULE _crash_$ADDRESS_$ID\n"
    + "CLASS %s\n"
    + "METHOD %s\n"
    + "HELPER org.jboss.byteman.test.ng.InjectorHelper\n"
    + "AT %s\n"
    + "IF isEnabled(\"$ID\")\n"
    + "DO org.jboss.byteman.test.ng.InjectorHelper.setEnabled(\"$ID\", false); throw new %s(%s)\n"
    + "ENDRULE";
    private static final String CRASH_INCREMENT =
    "RULE _crash_increment_$ADDRESS_$ID\n"
    + "INTERFACE %s\n"
    + "METHOD %s\n"
    + "HELPER org.jboss.byteman.test.ng.InjectorHelper\n"
    + "AT %s\n"
    + "IF isEnabled(\"$ID\")\n"
    + "DO storeAndIncrementCounter(\"$NAME\");throw new %s(%s);\n"
    + "ENDRULE";
    private static final String PAUSE =
    "RULE _pause_$ADDRESS_$ID\n"
    + "CLASS %s\n"
    + "METHOD %s\n"
    + "HELPER org.jboss.byteman.test.ng.InjectorHelper\n"
    + "AT %s\n"
    + "IF isEnabled(\"$ID\")\n"
    + "DO Thread.sleep(%s)\n"
    + "ENDRULE";
    private static final String TRACE =
    "RULE _trace_$ADDRESS_$ID\n"
    + "CLASS %s\n"
    + "METHOD %s\n"
    + "HELPER org.jboss.byteman.test.ng.InjectorHelper\n"
    + "AT %s\n"
    + "IF isEnabled(\"$ID\")\n"
    + "DO traceOpen(\"$NAME\", \"%s\");\n"
    + "traceln(\"$NAME\", $METHOD + \" - %s\")\n"
    + "ENDRULE";
    private static final String TRACE_PARAMETER =
    "RULE _trace_$ADDRESS_$ID\n"
    + "CLASS %s\n"
    + "METHOD %s\n"
    + "HELPER org.jboss.byteman.test.ng.InjectorHelper\n"
    + "AT %s\n"
    + "IF isEnabled(\"$ID\")\n"
    + "DO traceOpen(\"$NAME\", \"%s\");\n"
    + "traceln(\"$NAME\", \"\" + $1);\n"
    + "traceClose(\"$NAME\")\n"
    + "ENDRULE";
    private static final String WAIT_FOR =
    "RULE _waitfor_$ADDRESS_$ID\n"
    + "CLASS %s\n"
    + "METHOD %s\n"
    + "HELPER org.jboss.byteman.test.ng.InjectorHelper\n"
    + "AT %s\n"
    + "BIND monitor = \"%s\"\n"
    + "IF isEnabled(\"$ID\")\n"
    + "DO waitFor(monitor);\n"
    + "ENDRULE";
    private static final String SIGNAL_WAKE =
    "RULE _signalwake_$ADDRESS_$ID\n"
    + "CLASS %s\n"
    + "METHOD %s\n"
    + "HELPER org.jboss.byteman.test.ng.InjectorHelper\n"
    + "AT %s\n"
    + "BIND monitor = \"%s\"\n"
    + "IF isEnabled(\"$ID\")\n"
    + "DO signalWake(monitor);\n"
    + "ENDRULE";
    private static final String AWAIT_BARRIER =
    "RULE _barrier_$ADDRESS_$ID\n"
    + "CLASS %s\n"
    + "METHOD %s\n"
    + "HELPER org.jboss.byteman.test.ng.InjectorHelper\n"
    + "AT %s\n"
    + "IF isEnabled(\"$ID\")\n"
    + "DO awaitBarrierInternal(\"$NAME\");\n"
    + "ENDRULE";
    private static final String AWAIT_DOUBLE_BARRIER =
    "RULE _doubleBarrierEntry_$ADDRESS_$ID\n"
    + "CLASS $CLASS\n"
    + "METHOD $METHOD\n"
    + "HELPER org.jboss.byteman.test.ng.InjectorHelper\n"
    + "AT ENTRY\n"
    + "IF isEnabled(\"$ID\")\n"
    + "DO awaitBarrierInternal(\"$ENTRY\");\n"
    + "ENDRULE;\n"
    + "\n"
    + "RULE _doubleBarrierExit_$ADDRESS_$ID\n"
    + "CLASS $CLASS\n"
    + "METHOD $METHOD\n"
    + "HELPER org.jboss.byteman.test.ng.InjectorHelper\n"
    + "AT EXIT\n"
    + "IF isEnabled(\"$ID\")\n"
    + "DO awaitBarrierInternal(\"$EXIT\");\n"
    + "ENDRULE\n";
    private static final String GENERIC_ACTION =
    "RULE _crash_$ADDRESS_$ID\n"
    + "CLASS %s\n"
    + "METHOD %s\n"
    + "HELPER org.jboss.byteman.test.ng.InjectorHelper\n"
    + "AT %s\n"
    + "IF isEnabled(\"$ID\")\n"
    + "DO %s\n"
    + "ENDRULE";

    private final Submit submitter;
    private final String targetAddress;

    private final ConcurrentMap<String, String> rules = new ConcurrentHashMap<>();
    private volatile boolean doSubmit = true;

    public Injector(String targetAddress)
    {
        this.targetAddress = targetAddress;
        this.submitter = new Submit(targetAddress, Submit.DEFAULT_PORT);
    }

    public Map<String, String> getRules()
    {
        return ImmutableMap.copyOf(rules);
    }

    public String script() throws Exception
    {
        try
        {
            doSubmit = false;
            inject();
            return StringUtils.join(rules.values(), '\n');
        }
        finally
        {
            doSubmit = true;
        }
    }

    protected abstract void inject() throws Exception;

    protected final void countAt(String id, Class targetClass, String targetMethod, String invokePoint, String counterName) throws Exception
    {
        countAt(id, targetClass.getName(), targetMethod, invokePoint, counterName);
    }

    protected final void countAt(String id, String targetClass, String targetMethod, String invokePoint, String counterName) throws Exception
    {
        String rule = String.format(COUNT,
                                    targetClass,
                                    targetMethod,
                                    invokePoint == null ? "ENTRY" : (invokePoint.equals("EXIT") ? invokePoint : "INVOKE " + invokePoint))
                            .replaceAll("\\$NAME", counterName);
        maybeSubmit(id, rule);
    }

    protected final void crashAt(String id, Class targetClass, String targetMethod, String invokePoint, String exception, String message) throws Exception
    {
        crashAt(id, targetClass.getName(), targetMethod, invokePoint, exception, message);
    }

    protected final void crashAt(String id, String targetClass, String targetMethod, String invokePoint, String exception, String message) throws Exception
    {
        String rule = String.format(CRASH,
                                    targetClass,
                                    targetMethod,
                                    invokePoint == null ? "ENTRY" : ("INVOKE " + invokePoint),
                                    exception != null ? exception : RuntimeException.class.getSimpleName(),
                                    message == null ? "" : "\"" + message + "\"");
        maybeSubmit(id, rule);
    }

    protected final void crashOnceAt(String id, Class targetClass, String targetMethod, String invokePoint, String exception, String message) throws Exception
    {
        crashOnceAt(id, targetClass.getName(), targetMethod, invokePoint, exception, message);
    }

    protected final void crashOnceAt(String id, String targetClass, String targetMethod, String invokePoint, String exception, String message) throws Exception
    {
        String rule = String.format(CRASH_ONCE,
                                    targetClass,
                                    targetMethod,
                                    invokePoint == null ? "ENTRY" : ("INVOKE " + invokePoint),
                                    exception != null ? exception : RuntimeException.class.getSimpleName(),
                                    message == null ? "" : "\"" + message + "\"");
        maybeSubmit(id, rule);
    }

    protected final void crashAtAndIncrementInterface(String id, String targetInterface, String targetMethod, String counterName, String invokePoint, String exception, String message) throws Exception
    {
        String rule = String.format(CRASH_INCREMENT,
                                    targetInterface,
                                    targetMethod,
                                    invokePoint == null ? "ENTRY" : ("INVOKE " + invokePoint),
                                    exception != null ? exception : RuntimeException.class.getSimpleName(),
                                    message == null ? "" : "\"" + message + "\"")
                            .replaceAll("\\$NAME", counterName);
        maybeSubmit(id, rule);
    }

    protected final void pauseAt(String id, Class targetClass, String targetMethod, String invokePoint, long millis) throws Exception
    {
        pauseAt(id, targetClass.getName(), targetMethod, invokePoint, millis);
    }

    protected final void pauseAt(String id, String targetClass, String targetMethod, String invokePoint, long millis) throws Exception
    {
        String rule = String.format(PAUSE,
                                    targetClass,
                                    targetMethod,
                                    invokePoint == null ? "ENTRY" : ("INVOKE " + invokePoint),
                                    Long.toString(millis));
        maybeSubmit(id, rule);
    }

    protected final void traceAt(String id, Class targetClass, String targetMethod,
                                 String invokePoint,
                                 String fileName,
                                 String message) throws Exception
    {
        traceAt(id, targetClass.getName(), targetMethod, invokePoint, fileName, message);
    }

    protected final void traceAt(String id, String targetClass, String targetMethod, String invokePoint, String fileName, String message) throws Exception
    {
        String rule = String.format(TRACE,
                                    targetClass,
                                    targetMethod,
                                    invokePoint == null ? "ENTRY" : ("INVOKE " + invokePoint),
                                    fileName,
                                    message)
                            .replaceAll("\\$NAME", UUID.randomUUID().toString());

        File output = new File(fileName);
        if (output.exists())
        {
            output.delete();
        }

        maybeSubmit(id, rule);
    }

    protected final void traceParameter(String id, Class targetClass, String targetMethod,
                                        String invokePoint,
                                        String fileName) throws Exception
    {
        traceParameter(id, targetClass.getName(), targetMethod, invokePoint, fileName);
    }

    protected final void traceParameter(String id, String targetClass, String targetMethod, String invokePoint, String fileName) throws Exception
    {
        String rule = String.format(TRACE_PARAMETER,
                                    targetClass,
                                    targetMethod,
                                    invokePoint == null ? "ENTRY" : ("INVOKE " + invokePoint),
                                    fileName)
                            .replaceAll("\\$NAME", UUID.randomUUID().toString());

        File output = new File(fileName);
        if (output.exists())
        {
            output.delete();
        }

        maybeSubmit(id, rule);
    }

    protected final void waitFor(String id, Class targetClass, String targetMethod, String invokePoint, String identifier) throws Exception
    {
        waitFor(id, targetClass.getName(), targetMethod, invokePoint, identifier);
    }

    protected final void waitFor(String id, String targetClass, String targetMethod, String invokePoint, String identifier) throws Exception
    {
        String rule = String.format(WAIT_FOR,
                                    targetClass,
                                    targetMethod,
                                    invokePoint == null ? "ENTRY" : ("INVOKE " + invokePoint),
                                    identifier);
        maybeSubmit(id, rule);
    }

    protected final void signalWake(String id, String identifier, Class<? extends Callable<String>> waker) throws Exception
    {
        String rule = String.format(SIGNAL_WAKE,
                                    waker.getName(),
                                    "call",
                                    "ENTRY",
                                    identifier);
        maybeSubmit(id, rule);
    }

    protected final void awaitBarrier(String id, String targetClass, String targetMethod, String invokePoint, String barrierName) throws Exception
    {
        // Make sure it works for inner too
        String cn = targetClass.replaceAll("\\$", "\\\\\\$");

        String rule = String.format(AWAIT_BARRIER,
                                    cn,
                                    targetMethod,
                                    invokePoint == null ? "ENTRY" : invokePoint)
                            .replaceAll("\\$NAME", barrierName);
        maybeSubmit(id, rule);
    }

    protected final void generic(String id, String targetClass, String targetMethod, String invokePoint, String action) throws Exception
    {
        String rule = String.format(GENERIC_ACTION,
                                    targetClass,
                                    targetMethod,
                                    invokePoint == null ? "ENTRY" : invokePoint,
                                    action);
        maybeSubmit(id, rule);
    }

    protected final void deleteAll() throws Exception
    {
        submitter.deleteAllRules();
    }

    protected void maybeSubmit(String id, String rule) throws Exception
    {
        rule = rule.
                   replaceAll("\\$ID", id).
                                          replaceAll("\\$ADDRESS", targetAddress);

        rules.put(id, rule);

        if (doSubmit)
        {
            submitter.addRulesFromResources(
            Arrays.<InputStream>asList(
            new ByteArrayInputStream(
                                    rule.getBytes(Charset.forName("UTF-8")))));
        }
        else
        {
            // create rules that are enabled by default
            // (this is needed when we inject byteman rules at JVM bootstrap
            rules.put(id, rule.replaceAll("isEnabled\\(\"" + id + "\"\\)", "true"));
        }
    }

    /**
     * Wrap a method between two barriers, one on entry and one on exit.
     * Both rules need to be in the same script because Byteman will drop the second one if not...
     *
     * @param id         common id to both rules
     * @param className  the class of the method
     * @param methodName the method to wrap
     * @param entry      the name of the entry barrier
     * @param exit       the name of the exit barrier.
     * @throws Exception when things go south
     */
    protected void awaitDoubleBarrier(String id, String className, String methodName, String entry, String exit)
    throws Exception
    {
        // Make sure inner classes still works
        String cn = className.replaceAll("\\$", "\\\\\\$");

        String rule = AWAIT_DOUBLE_BARRIER.replaceAll("\\$ENTRY", entry)
                                          .replaceAll("\\$EXIT", exit)
                                          .replaceAll("\\$METHOD", methodName)
                                          .replaceAll("\\$CLASS", cn);
        maybeSubmit(id, rule);
    }

    public static class DeleteAll extends Injector
    {
        public DeleteAll(String targetAddress)
        {
            super(targetAddress);
        }

        @Override
        protected void inject() throws Exception
        {
            try
            {
                deleteAll();
            }
            catch (Throwable e)
            {
                // swallow it
            }
        }
    }
}