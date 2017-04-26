/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework.logback;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import ch.qos.logback.core.CoreConstants;

/**
 * Factory for {@link ch.qos.logback.core.net.ObjectWriter} instances.
 *
 * @author Sebastian Gr&ouml;bler
 */
public class ObjectWriterFactory {

    /**
     * Creates a new {@link ch.qos.logback.core.net.AutoFlushingObjectWriter} instance.
     *
     * @param outputStream the underlying {@link OutputStream} to write to
     * @return a new {@link ch.qos.logback.core.net.AutoFlushingObjectWriter} instance
     * @throws IOException if an I/O error occurs while writing stream header
     */
    public AutoFlushingObjectWriter newAutoFlushingObjectWriter(OutputStream outputStream) throws IOException {
        return new AutoFlushingObjectWriter(new ObjectOutputStream(outputStream), CoreConstants.OOS_RESET_FREQUENCY);
    }
}
