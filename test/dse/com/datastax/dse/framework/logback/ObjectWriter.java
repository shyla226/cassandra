/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework.logback;

import java.io.IOException;

/**
 * Writes objects to an output.
 *
 * @author Sebastian Gr&ouml;bler
 */
public interface ObjectWriter {

    /**
     * Writes an object to an output.
     *
     * @param object the {@link Object} to write
     * @throws IOException in case input/output fails, details are defined by the implementation
     */
    void write(Object object) throws IOException;

}
