/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.config;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.units.RateUnit;
import org.apache.cassandra.utils.units.RateValue;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.SizeValue;

/**
 * The YAML options for NodeSync.
 */
public class NodeSyncConfig
{
    private static final boolean DEFAULT_ENABLED = true;
    private static final long DEFAULT_RATE_KB = 1024;
    private static final long DEFAULT_PAGE_SIZE_KB = 100;

    private static final int DEFAULT_MIN_THREADS = 1;
    // Note that while we're trying to limit how often our threads wait on blocking operations, it's not perfect, so it
    // can absolutely make sense to have more threads than processors. But as NodeSync is also a background task, keep the
    // default number reasonable.
    private static final int DEFAULT_MAX_THREADS = 8;
    // Dealing with a segment validation has blocking parts (waiting on replica), so having at least 2 in-flight ones so the
    // blocking parts of one can be filled by processing the other one feels like a good idea. And 2 inflight validations
    // aren't going to hit too much resources.
    private static final int DEFAULT_MIN_VALIDATIONS = 2;
    // It generally make sense to have more validations in flights than number of threads (again, because part of dealing
    // with a segment involves waiting replica, during which threads can process other segments). And given that the
    // "waiting on replica" part can get a bit long when multi-DCs are involved, it's worth allowing a fair amount of
    // in-flights validations to compensate. That said, each validation does consume resources, so don't get totally crazy.
    private static final int DEFAULT_MAX_VALIDATIONS = DEFAULT_MAX_THREADS * 2;

    private boolean enabled = DEFAULT_ENABLED;
    private long rate_in_kb = DEFAULT_RATE_KB;

    private volatile long page_size_in_kb = DEFAULT_PAGE_SIZE_KB;

    private volatile int min_threads = DEFAULT_MIN_THREADS;
    private volatile int max_threads = DEFAULT_MAX_THREADS;
    private volatile int min_inflight_validations = DEFAULT_MIN_VALIDATIONS;
    private volatile int max_inflight_validations = DEFAULT_MAX_VALIDATIONS;

    public final RateLimiter rateLimiter = RateLimiter.create(SizeUnit.BYTES.convert(DEFAULT_RATE_KB, SizeUnit.KILOBYTES));

    /* Setters: most don't follow code-style in their naming being they are used by reflection by snakeYaml. Avoid
     * using in the rest of the code for consistency however and prefer properly named setters. */

    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    public void setRate_in_kb(long rate_in_kb)
    {
        this.rate_in_kb = rate_in_kb;
        this.rateLimiter.setRate(SizeUnit.BYTES.convert(rate_in_kb, SizeUnit.KILOBYTES));
    }

    public void setPage_size_in_kb(long page_size_in_kb)
    {
        this.page_size_in_kb = page_size_in_kb;
    }

    public void setMin_threads(int min_threads)
    {
        this.min_threads = min_threads;
    }

    public void setMax_threads(int max_threads)
    {
        this.max_threads = max_threads;
    }

    public void setMin_inflight_validations(int min_inflight_validations)
    {
        this.min_inflight_validations = min_inflight_validations;
    }

    public void setMax_inflight_validations(int max_inflight_validations)
    {
        this.max_inflight_validations = max_inflight_validations;
    }

    /* Proper setters and getters for access in the rest of the code. */

    public boolean isEnabled()
    {
        return enabled;
    }

    public long getPageSize(SizeUnit unit)
    {
        return unit.convert(page_size_in_kb, SizeUnit.KILOBYTES);
    }

    public SizeValue getPageSize()
    {
        return SizeValue.of(page_size_in_kb, SizeUnit.KILOBYTES);
    }

    public void setPageSize(SizeValue pageSize)
    {
        setPage_size_in_kb(pageSize.in(SizeUnit.KILOBYTES));
    }

    public RateValue getRate()
    {
        return RateValue.of(rate_in_kb, RateUnit.KB_S);
    }

    public void setRate(RateValue rate)
    {
        setRate_in_kb(rate.in(RateUnit.KB_S));
    }

    public int getMinThreads()
    {
        return min_threads;
    }

    public int getMaxThreads()
    {
        return max_threads;
    }

    public int getMinInflightValidations()
    {
        return min_inflight_validations;
    }

    public int getMaxInflightValidations()
    {
        return max_inflight_validations;
    }

    public void validate() throws ConfigurationException
    {
        validateMinMax(min_threads, max_threads, "threads", false);
        validateMinMax(min_inflight_validations, max_inflight_validations, "inflight_segments", false);

        if (max_threads > max_inflight_validations)
            throw new ConfigurationException(String.format("max_threads value (%d) should be less than or equal to max_inflight_validations (%d)",
                                                           max_threads, max_inflight_validations));
        
        if (SizeValue.of(page_size_in_kb, SizeUnit.KILOBYTES).in(SizeUnit.BYTES) > Integer.MAX_VALUE)
            throw new ConfigurationException(String.format("Max page_size_in_kb supported is %d, got: %d",
                                                           SizeValue.of(Integer.MAX_VALUE, SizeUnit.BYTES).in(SizeUnit.KILOBYTES),
                                                           page_size_in_kb));
    }

    private void validateMinMax(long min, long max, String option, boolean allowZeros) throws ConfigurationException
    {
        if (min < 0 || (!allowZeros && min == 0))
            throw new ConfigurationException(String.format("Invalid value for min_%s, must be %spositive: got %d",
                                                           option, allowZeros ? "" : "strictly ", min));

        if (max < 0 || (!allowZeros && max == 0))
            throw new ConfigurationException(String.format("Invalid value for max_%s, must be %spositive: got %d",
                                                           option, allowZeros ? "" : "strictly ", max));

        if (min > max)
            throw new ConfigurationException(String.format("min_%s value (%d) must be less than or equal to max_%s value(%d)",
                                                           option, min, option, max));
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof NodeSyncConfig))
            return false;

        NodeSyncConfig that = (NodeSyncConfig) other;
        return this.enabled == that.enabled
               && this.rate_in_kb == that.rate_in_kb
               && this.page_size_in_kb == that.page_size_in_kb
               && this.min_threads == that.min_threads
               && this.max_threads == that.max_threads
               && this.min_inflight_validations == that.min_inflight_validations
               && this.max_inflight_validations == that.max_inflight_validations;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(enabled,
                            rate_in_kb,
                            page_size_in_kb,
                            min_threads,
                            max_threads,
                            min_inflight_validations,
                            max_inflight_validations);
    }

    private Map<String, String> toStringMap()
    {
        Map<String, String> m = new TreeMap<>();
        m.put("enabled", Boolean.toString(enabled));
        m.put("rate_in_kb", Long.toString(rate_in_kb));
        m.put("page_size_in_kb", Long.toString(page_size_in_kb));
        m.put("min_threads", Integer.toString(min_threads));
        m.put("max_threads", Integer.toString(max_threads));
        m.put("max_inflight_validations", Integer.toString(max_inflight_validations));
        m.put("min_inflight_validations", Integer.toString(min_inflight_validations));
        return m;
    }

    @Override
    public String toString()
    {
        return '{' + Joiner.on(", ").withKeyValueSeparator("=").join(toStringMap()) + '}';
    }
}
