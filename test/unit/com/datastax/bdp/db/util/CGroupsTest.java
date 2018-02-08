/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 *
 */
package com.datastax.bdp.db.util;

import java.io.File;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CGroupsTest
{
    @Test
    public void testLongParsing()
    {
        assertEquals(CGroups.MEM_UNLIMITED, CGroups.longValue("184467440737095516151844674407370955161518446744073709551615")); // imaginary
        assertEquals(CGroups.MEM_UNLIMITED, CGroups.longValue("-184467440737095516151844674407370955161518446744073709551615")); // imaginary
        assertEquals(CGroups.MEM_UNLIMITED, CGroups.longValue("18446744073709551615")); // value seen on a Linux box
        assertEquals(CGroups.MEM_UNLIMITED, CGroups.longValue("9223372036854771712")); // value seen on a Linux box
        assertEquals(CGroups.MEM_UNLIMITED, CGroups.longValue("9223372036854775807")); // 9223372036854775807 == Long.MAX_VALUE
        assertEquals(CGroups.MEM_UNLIMITED, CGroups.longValue(Long.toString(Long.MIN_VALUE)));
        assertEquals(CGroups.MEM_UNLIMITED, CGroups.longValue(Long.toString(Long.MAX_VALUE)));
        assertEquals(CGroups.MEM_UNLIMITED, CGroups.longValue(Long.toString(0)));
        assertEquals(CGroups.MEM_UNLIMITED, CGroups.longValue("-0"));
        assertEquals(1L, CGroups.longValue(Long.toString(1)));
        assertEquals(CGroups.MEM_UNLIMITED, CGroups.longValue((String) null));
        assertEquals(CGroups.MEM_UNLIMITED, CGroups.longValue("-1"));
        assertEquals(CGroups.MEM_UNLIMITED, CGroups.longValue("Not a number"));
        assertEquals(CGroups.MEM_UNLIMITED, CGroups.longValue(""));
        // that's valid though
        assertEquals(CGroups.MEM_UNLIMITED - 1L, CGroups.longValue(Long.toString(CGroups.MEM_UNLIMITED - 1)));
        // 256 GB memory
        assertEquals(256L*1024*1024*1024, CGroups.longValue(Long.toString(256L*1024*1024*1024)));
    }

    @Test
    public void testControllerCGroup()
    {
        assertEquals("/", CGroups.controllerGroup(procSelfCgroup, "cpuset"));
        assertEquals("/user.slice", CGroups.controllerGroup(procSelfCgroup, "cpu"));
        assertEquals("/user.slice", CGroups.controllerGroup(procSelfCgroup, "cpuacct"));
        assertEquals("/", CGroups.controllerGroup(procSelfCgroup, "somethingmagic"));

        assertEquals(new File("/sys/fs/cgroup/" + "cpu" + "/user.slice" + '/' + "filename"), CGroups.controllerFile(procSelfCgroup, "cpu", "filename"));
        assertEquals(new File("/sys/fs/cgroup/" + "somethingmagic" + "" + '/' + "filename"), CGroups.controllerFile(procSelfCgroup, "somethingmagic", "filename"));
        assertEquals(new File("/sys/fs/cgroup/" + "net_cls" + "" + '/' + "filename"), CGroups.controllerFile(procSelfCgroup, "net_cls", "filename"));
    }

    @Test
    public void testCountCpus()
    {
        assertNull(CGroups.countCpus("foo"));
        assertNull(CGroups.countCpus(null));
        assertEquals(32, CGroups.countCpus("0-31").intValue());
        assertEquals(33, CGroups.countCpus("0-31,33").intValue());
        assertEquals(5, CGroups.countCpus("0,1,2,3,4").intValue());
        assertEquals(5, CGroups.countCpus("0,11,22,33,44").intValue());
    }

    private static final String procSelfCgroup = "12:hugetlb:/\n" +
                                                 "11:perf_event:/\n" +
                                                 "10:rdma:/\n" +
                                                 "9:devices:/user.slice\n" +
                                                 "8:blkio:/user.slice\n" +
                                                 "7:net_cls,net_prio:/\n" +
                                                 "6:cpuset:/\n" +
                                                 "5:memory:/user.slice\n" +
                                                 "4:freezer:/\n" +
                                                 "3:pids:/user.slice/user-1000.slice/user@1000.service\n" +
                                                 "2:cpu,cpuacct:/user.slice\n" +
                                                 "1:name=systemd:/user.slice/user-1000.slice/user@1000.service/gnome-terminal-server.service\n" +
                                                 "0::/user.slice/user-1000.slice/user@1000.service/gnome-terminal-server.service\n";
}
