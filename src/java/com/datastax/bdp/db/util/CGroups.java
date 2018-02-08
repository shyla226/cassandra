/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 *
 */
package com.datastax.bdp.db.util;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.BitSet;

/**
 * Utility class to get (some) cgroups limitations.
 */
public final class CGroups
{
    /**
     * Get the effective group name for the {@code cpuset} controller.
     */
    static String controllerGroup(String procCGroup, String controller)
    {
        if (procCGroup == null || controller == null)
            return "/";

        // example line in /proc/<pid>/cgroup:
        // 7:cpuset:/docker/1cd79847a997156bcea3178aa51dd8073775aff0575a5a2dff7ae0faf67b403b

        for (String cgroupLine : procCGroup.split("\n"))
        {
            String[] parts = cgroupLine.split(":");
            if (parts.length < 3)
                continue;
            String[] controllers = parts[1].split(",");
            for (String ctl : controllers)
            {
                if (controller.equals(ctl))
                    return parts[2].isEmpty() ? "/" : parts[2];
            }
        }

        return "/";
    }

    /**
     * Reads the {@code /proc/PID/cgroup} file.
     */
    private static String procCGroupsFileContents()
    {
        File procCGroups = new File("/proc/self/cgroup");
        if (!procCGroups.isFile())
            return null;

        return readFile(procCGroups);
    }

    private static String readFile(File f)
    {
        if (f == null)
            return null;
        try
        {
            return new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
        }
        catch (IOException e)
        {
            return null;
        }
    }

    /**
     * Get the cgroup file containing for {@code cpuset.cpus} for the specified cgroup
     */
    static File controllerFile(String procCGroup, String controller, String file)
    {
        if (file == null)
            return null;

        String cgroupPart = controllerGroup(procCGroup, controller);
        if (!cgroupPart.endsWith("/"))
            cgroupPart += "/";
        return new File("/sys/fs/cgroup/" + controller + cgroupPart + file);
    }

    static Integer countCpus(String cpus)
    {
        if (cpus == null || cpus.isEmpty())
            return null;

        BitSet bits = new BitSet();

        try
        {
            for (String elem : cpus.split(","))
            {
                elem = elem.trim();
                if (elem.isEmpty())
                    continue;

                if (elem.indexOf('-') != -1)
                {
                    String[] pair = elem.split("-");
                    int start = Integer.parseInt(pair[0].trim());
                    int end = Integer.parseInt(pair[1].trim());
                    for (int n = start; n <= end; n++)
                        bits.set(n);
                }
                else
                    bits.set(Integer.parseInt(elem));
            }

            return bits.cardinality();
        }
        catch (Exception e)
        {
            return null;
        }
    }

    public static Integer countCpus()
    {
        File f = controllerFile(procCGroupsFileContents(), "cpuset", "cpuset.cpus");
        String cpus = readFile(f);

        return countCpus(cpus);
    }

    /**
     * The value is utopic - it represents roughly 4 exabytes (=4096 PB or 4194304 TB).
     */
    public static long MEM_UNLIMITED = 4611686018427387904L;

    public static long memoryLimit()
    {
        String procCGroup = procCGroupsFileContents();
        long softLimit = longValue(controllerFile(procCGroup, "memory", "memory.soft_limit_in_bytes"));
        long limit = longValue(controllerFile(procCGroup, "memory", "memory.limit_in_bytes"));
        return Math.min(softLimit, limit);
    }

    static long longValue(File f)
    {
        return longValue(readFile(f));
    }

    static long longValue(String s)
    {
        if (s == null)
            return MEM_UNLIMITED;
        BigInteger val;
        long v = Long.MAX_VALUE;
        try
        {
            val = new BigInteger(s);
            if (val.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) <= 0 && val.compareTo(BigInteger.ZERO) > 0)
                v = val.longValue();
        }
        catch (Exception e)
        {
            // ignore bad values
        }
        return v >= MEM_UNLIMITED ? MEM_UNLIMITED : v;
    }

    public static boolean blkioThrottled()
    {
        String procCGroup = procCGroupsFileContents();
        return notEmpty(controllerFile(procCGroup, "blkio", "blkio.throttle.read_iops_device"))
            || notEmpty(controllerFile(procCGroup, "blkio", "blkio.throttle.read_bps_device"))
            || notEmpty(controllerFile(procCGroup, "blkio", "blkio.throttle.write_iops_device"))
            || notEmpty(controllerFile(procCGroup, "blkio", "blkio.throttle.write_bps_device"));
    }

    private static boolean notEmpty(File file)
    {
        String s = readFile(file);
        return s != null && !s.isEmpty();
    }
}
