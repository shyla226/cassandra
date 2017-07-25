/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.transform;

import org.apache.cassandra.utils.Throwables;

// a shared internal interface, that is hidden to provide type-safety to the user
interface MoreContents<I extends AutoCloseable> extends AutoCloseable
{
    public abstract I moreContents();

    public default boolean closeNonAttachedContents()
    {
        return true;
    }

    /** Close the contents that were not attached yet */
    @SuppressWarnings("resource") // unsure why eclipse-warnings thinks next is not closed, it looks OK to me
    public default void close()
    {
        if (!closeNonAttachedContents())
            return;

        Throwable err = null;
        I next = moreContents();
        while (next != null)
        {
            try
            {
                next.close();
            }
            catch (Throwable t)
            {
                err = Throwables.merge(err, t);
            }

            next = moreContents();
        }

        Throwables.maybeFail(err);
    }
}

