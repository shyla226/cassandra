/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.junit.Assert;

import jboss.byteman.test.ng.InjectorHelper;

/**
 * Wrapper for pausing a background process on a node, stepping and synchronizing in a unit test.
 */
public class PauseController
{
    private final String host;
    private final String id;
    private final String finishBarrier;
    private final String startBarrier;
    private boolean active = true;

    public PauseController(String host)
    throws ExecutionException, InterruptedException
    {
        this.host = host;
        id = String.valueOf(hashCode());
        startBarrier = "start_" + id;
        finishBarrier = "finish_" + id;
    }

    /**
     * Do one iteration of the run() method.
     * This method returns after the run() was completed.
     */
    public synchronized void step() throws Exception
    {
        Assert.assertTrue(active);

        DseTestRunner.executeCodeOnNode(DseTestRunner.getNodeForHost(host),
                                        new StepCommand(startBarrier, finishBarrier));
    }

    public synchronized void resume() throws Exception
    {
        Assert.assertTrue(active);

        DseTestRunner.executeCodeOnNode(DseTestRunner.getNodeForHost(host),
                                        new ResumeCommand(startBarrier, finishBarrier, host, id));

        active = false;
    }

    public String getId()
    {
        return id;
    }

    public String getHost()
    {
        return host;
    }

    public static class Setup extends Injector implements Callable<String>
    {
        private final String className;
        private final String id;
        private final String hookedMethod;
        private final String start;
        private final String finish;

        public Setup(String address, String id, String className, String hookedMethod)
        {
            super(address);
            this.className = className;
            this.hookedMethod = hookedMethod;
            this.id = id;

            start = "start_" + id;
            finish = "finish_" + id;
        }

        @Override
        public String call() throws Exception
        {
            inject();
            return null;
        }

        @Override
        protected void inject() throws Exception
        {
            InjectorHelper.addBarrier(start, 2);
            InjectorHelper.addBarrier(finish, 2);
            InjectorHelper.setEnabled(id, true);
            awaitDoubleBarrier(id, className, hookedMethod, start, finish);
        }
    }

    public static class StepCommand implements DseTestRunner.SerializableCallable<Boolean>
    {
        private final String startBarrier, finishBarrier;

        public StepCommand(String startBarrier, String finishBarrier)
        {
            this.startBarrier = startBarrier;
            this.finishBarrier = finishBarrier;
        }

        @Override
        public Boolean call() throws Exception
        {
            // Wait until the hooked method actually gets to the start barrier
            InjectorHelper.awaitBarrier(startBarrier);

            // Hooked method is running

            InjectorHelper.awaitBarrier(finishBarrier);
            // Hooked method is done and so are we

            return true;
        }
    }

    public static class ResumeCommand implements DseTestRunner.SerializableCallable<Boolean>
    {
        private final String startBarrier, finishBarrier, host, id;

        public ResumeCommand(String startBarrier, String finishBarrier, String host, String id)
        {
            this.startBarrier = startBarrier;
            this.finishBarrier = finishBarrier;
            this.host = host;
            this.id = id;
        }

        @Override
        public Boolean call() throws Exception
        {
            // Disable waiting at barriers for hooked method
            InjectorHelper.setEnabled(id, false);
            InjectorHelper.resetBarrier(startBarrier);
            InjectorHelper.resetBarrier(finishBarrier);
            return true;
        }
    }
}