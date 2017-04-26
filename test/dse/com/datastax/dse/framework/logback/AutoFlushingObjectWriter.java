/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework.logback;

import java.io.IOException;
import java.io.ObjectOutputStream;

import ch.qos.logback.core.net.ObjectWriter;

/**
 * Automatically flushes the underlying {@link ObjectOutputStream} immediately after calling
 * it's {@link ObjectOutputStream#writeObject(Object)} method.
 *
 * @author Sebastian Gr&ouml;bler
 */
public class AutoFlushingObjectWriter implements ObjectWriter
{
    private final ObjectOutputStream objectOutputStream;
    private final int resetFrequency;
    private int writeCounter = 0;

    /**
     * Creates a new instance for the given {@link ObjectOutputStream}.
     *
     * @param objectOutputStream the stream to write to
     * @param resetFrequency     the frequency with which the given stream will be
     *                           automatically reset to prevent a memory leak
     */
    public AutoFlushingObjectWriter(ObjectOutputStream objectOutputStream, int resetFrequency)
    {
        this.objectOutputStream = objectOutputStream;
        this.resetFrequency = resetFrequency;
    }

    @Override
    public void write(Object object) throws IOException
    {
        objectOutputStream.writeObject(object);
        objectOutputStream.flush();
        preventMemoryLeak();
    }

    /**
     * Failing to reset the object output stream every now and then creates a serious memory leak which
     * is why the underlying stream will be reset according to the {@code resetFrequency}.
     */
    private void preventMemoryLeak() throws IOException
    {
        if (++writeCounter >= resetFrequency)
        {
            objectOutputStream.reset();
            writeCounter = 0;
        }
    }
}
