/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.mos;

import java.util.Map;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;

public class MemoryOnlyStrategyOptions
{
    // For MOS, a default minimumCompactionThreshold of 2 makes a lot more sense than the default default of 4;
    // after all the entire point of MOS is to sacrifice CPU time for lower latency.  Unfortunately, CFMetaData
    // grabs this option before this constructor is called, and it is not so easy to change this: if we change
    // the minCompactionThreshold via CFMetaData's function, Cassandra assumes that this is a user change coming
    // from JMX . . . which means that if we alter the compaction strategy to sizetiered, the setting remains
    // until the server is rebooted when it goes back to the SizeTiered default of 4.  This is obviously Not Good,
    // so we just have our own values which default to 2 and 32 respectively.
    // EDIT post integration (DB-342): it is possible to now rely on the compaction params but we'd have to support
    // both the mos_ and the plain version, due to lack of time this has not been done yet
    public static final String MOS_MINCOMPACTIONTHRESHOLD = "mos_min_threshold";
    public static final String MOS_MAXCOMPACTIONTHRESHOLD = "mos_max_threshold";
    public static final String MOS_MAXACTIVECOMPACTIONS = "mos_max_active_compactions";
    public static final int MOS_DEFAULT_MINCOMPACTIONTHRESHOLD = 2;
    public static final int MOS_DEFAULT_MAXCOMPACTIONTHRESHOLD = 32;
    public static final int MOS_DEFAULT_MAXACTIVECOMPACTIONS = 100;
    public final int minCompactionThreshold;
    public final int maxCompactionThreshold;
    public final int maxActiveCompactions;

    public MemoryOnlyStrategyOptions(Map<String, String> options)
    {
        try
        {
            this.minCompactionThreshold = getIntegerOption(options, MOS_MINCOMPACTIONTHRESHOLD, MOS_DEFAULT_MINCOMPACTIONTHRESHOLD);
            this.maxCompactionThreshold = getIntegerOption(options, MOS_MAXCOMPACTIONTHRESHOLD, MOS_DEFAULT_MAXCOMPACTIONTHRESHOLD);
            this.maxActiveCompactions = getIntegerOption(options, MOS_MAXACTIVECOMPACTIONS, MOS_DEFAULT_MAXACTIVECOMPACTIONS);
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);  // Really should not happen after validateOptions
        }
    }

    public static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws ConfigurationException
    {
        int minThreshold = getIntegerOption(options, MOS_MINCOMPACTIONTHRESHOLD, MOS_DEFAULT_MINCOMPACTIONTHRESHOLD);
        int maxThreshold = getIntegerOption(options, MOS_MAXCOMPACTIONTHRESHOLD, MOS_DEFAULT_MAXCOMPACTIONTHRESHOLD);
        int maxActiveCompactions = getIntegerOption(options, MOS_MAXACTIVECOMPACTIONS, MOS_DEFAULT_MAXACTIVECOMPACTIONS);

        if (minThreshold < 2)
        {
            throw new ConfigurationException(String.format("'%s' must be at least 2!", MOS_MINCOMPACTIONTHRESHOLD));
        }
        if (maxThreshold < 2)
        {
            throw new ConfigurationException(String.format("'%s' must be at least 2!", MOS_MAXCOMPACTIONTHRESHOLD));
        }
        if (minThreshold > maxThreshold)
        {
            throw new ConfigurationException(String.format("'%s' (%d) is greater than '%s' (%d)!",
                                                           MOS_MINCOMPACTIONTHRESHOLD, minThreshold,
                                                           MOS_MAXCOMPACTIONTHRESHOLD, maxThreshold));
        }
        if (maxActiveCompactions <= 0)
        {
            throw new ConfigurationException(String.format("'%s' must be greater than 0, or your table will never compact!",
                                                           MOS_MAXACTIVECOMPACTIONS));
        }

        uncheckedOptions.remove(MOS_MINCOMPACTIONTHRESHOLD);
        uncheckedOptions.remove(MOS_MAXCOMPACTIONTHRESHOLD);
        uncheckedOptions.remove(MOS_MAXACTIVECOMPACTIONS);
        return uncheckedOptions;
    }

    private static int getIntegerOption(Map<String, String> options, String optionName, int defaultValue) throws ConfigurationException
    {
        if (options.containsKey(optionName))
        {
            try
            {
                return Integer.parseInt(options.get(optionName));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("Option '%s' must be a number, not '%s'",
                                                               optionName, options.get(optionName)));
            }
        }

        return defaultValue;
    }
}
