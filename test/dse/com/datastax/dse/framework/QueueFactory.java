/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Factory for {@link java.util.Queue} instances.
 *
 * @author Sebastian Gr&ouml;bler
 */
public class QueueFactory
{

    /**
     * Creates a new {@link LinkedBlockingDeque} with the given {@code capacity}.
     * In case the given capacity is smaller than one it will automatically be
     * converted to one.
     *
     * @param capacity the capacity to use for the queue
     * @param <E> the type of elements held in the queue
     * @return a new instance of {@link ArrayBlockingQueue}
     */
    public <E> LinkedBlockingDeque<E> newLinkedBlockingDeque(int capacity) {
        final int actualCapacity = capacity < 1 ? 1 : capacity;
        return new LinkedBlockingDeque<E>(actualCapacity);
    }
}
