/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.net;

import java.io.IOException;

import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.util.DataInputPlus;

class MessageDeserializationException extends RuntimeException
{
    /**
     * Negative if the exception is not recoverable, the number of bytes to skip to recover after this exception
     * otherwise.
     */
    private final int unreadBytesOfMessage;

    private MessageDeserializationException(int unreadBytesOfMessage, Throwable cause)
    {
        super("Error deserializing inter-node message", cause);
        this.unreadBytesOfMessage = unreadBytesOfMessage;
    }

    static MessageDeserializationException forPayloadDeserializationException(Exception e,
                                                                              Verb<?, ?> verb,
                                                                              int unreadBytesOfMessage)
    {
        // For now, UnknownTableException is the only exception we deem recoverable. When we get it, we know this
        // is most likely not a corrupt/buggy message, but simply a race with a drop/create of the table. As such
        // simply skipping the message bytes is likely to leave the stream in a valid state.
        // We can probably add other case over time.
        // Side-note: we also use unreadBytesOfMessage < 0 to mean "don't recover". That's mostly here for tests.
        if (unreadBytesOfMessage < 0 || !(e instanceof UnknownTableException))
            return new MessageDeserializationException(-1, e);

        // The following will log a warning if deemed necessary for the verb
        verb.errorHandler().handleError((UnknownTableException)e);
        return new MessageDeserializationException(unreadBytesOfMessage, e);
    }

    boolean isRecoverable()
    {
        return unreadBytesOfMessage >= 0;
    }

    void recover(DataInputPlus in) throws IOException
    {
        assert isRecoverable();
        in.skipBytesFully(unreadBytesOfMessage);
    }
}
