/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3;

import java.util.Objects;

import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

/**
 * The (requested) size of page of result, expressed in either a number of rows
 * or number of bytes.
 */
public class PageSize
{
    public enum PageUnit
    {
        BYTES,
        ROWS
    }

    private final int size;
    private final PageUnit unit;

    /**
     * Creates a new page size given a raw size and its unit.
     *
     * @throws IllegalArgumentException if the size is not strictly positive.
     */
    public PageSize(int size, PageUnit unit)
    {
        if (size <= 0)
            throw new IllegalArgumentException("A page size should be strictly positive, got " + size);

        this.size = size;
        this.unit = unit;
    }

    /**
     * Creates a page size representing {@code count} rows.
     *
     * @throws IllegalArgumentException if the size is not strictly positive.
     */
    public static PageSize rowsSize(int count)
    {
        return new PageSize(count, PageUnit.ROWS);
    }

    /**
     * Creates a page size representing {@code size} bytes.
     *
     * @throws IllegalArgumentException if the size is not strictly positive.
     */
    public static PageSize bytesSize(int size)
    {
        return new PageSize(size, PageUnit.BYTES);
    }

    /**
     * The raw size this represent, which can be in rows or in bytes.
     * <p>
     * This method should be used with care as the unit of the value it returns
     * depends on the unit of this {@code PageSize}, and it should thus
     * generally only be used after calling one of {@code isInRows()} or
     * {@code isInBytes()}.
     */
    public int rawSize()
    {
        return size;
    }

    /**
     * Whether the page size is expressed in rows.
     */
    public boolean isInRows()
    {
        return unit == PageUnit.ROWS;
    }

    /**
     * Whether the page size is expressed in bytes.
     */
    public boolean isInBytes()
    {
        return unit == PageUnit.BYTES;
    }

    /**
     * The exact number of rows this page size represents.
     * <p>
     * This is only valid to call if the size this represent is expressed in
     * rows. Otherwise, {@link #inEstimatedRows} should be used instead since
     * that's the best we can do.
     *
     * @return the exact number of rows this size represents if it expresses a
     * size in rows.
     *
     * @throws InvalidRequestException if the size this represents is not in
     * rows.
     */
    public int inRows()
    {
        checkTrue(unit == PageSize.PageUnit.ROWS,
                  "A page size expressed in bytes is unsupported");
        return size;
    }

    /**
     * The estimated number of rows this page size represents.
     * <p>
     * This is estimated in general because when the page size was requested in
     * bytes by the user, we have to estimate this based on {@code avgRowSize}.
     * This will be an exact value if the page size was requested in rows
     * however.
     *
     * @param avgRowSize the average size of rows in the table this is a page
     * size for. This is used to estimate the number of rows the pages of this
     * size will represent when the page size is expressed in bytes. This must
     * be <b>strictly</b> positive.
     * @return an estimate of the number of rows a page of this size will hold.
     */
    public int inEstimatedRows(int avgRowSize)
    {
        assert unit == PageUnit.ROWS || avgRowSize > 0;
        return unit == PageUnit.ROWS
             ? size
             : Math.max(1, size / avgRowSize);
    }

    /**
     * The estimated number of bytes this page size represents.
     * <p>
     * This is estimated in general because when the page size was requested in
     * rows by the user, we have to estimate this based on {@code avgRowSize}.
     * This will be an exact value if the page size was requested in bytes
     * directly however.
     *
     * @param avgRowSize the average size of rows in the table this is a page
     * size for. This is used to estimate the number of bytes the pages of this
     * size will represent when the page size is expressed in rows. This must
     * be <b>strictly</b> positive.
     * @return an estimate of the number of bytes a page of this size will be.
     */
    public int inEstimatedBytes(int avgRowSize)
    {
        assert unit == PageUnit.BYTES || avgRowSize > 0;
        return unit == PageUnit.BYTES
             ? size
             : size * avgRowSize;
    }

    /**
     * Returns whether a page that contains the provided number of rows and is of
     * the provided byte size is bigger than the size represented by this object
     * (said page is thus complete).
     *
     * @param rowCount the number of rows in the page to check.
     * @param sizeInBytes the size in bytes of the page to check.
     * @return whether the page represented by {@code rowCount} and {@code
     * sizeInBytes} is complete.
     */
    public boolean isComplete(int rowCount, int sizeInBytes)
    {
        return isInRows() ? rowCount >= size : sizeInBytes >= size;
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(size, unit);
    }

    @Override
    public final boolean equals(Object o)
    {
        if(!(o instanceof PageSize))
            return false;

        PageSize that = (PageSize)o;
        return this.size == that.size && this.unit == that.unit;
    }

    @Override
    public String toString()
    {
        return String.format("%d %s", size, unit);
    }
}
