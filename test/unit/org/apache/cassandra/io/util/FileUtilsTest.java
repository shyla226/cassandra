/*
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
 */
package org.apache.cassandra.io.util;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FileUtilsTest
{

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testCpuIdVendorId() throws Exception
    {
        assertEquals("GenuineIntel", FileUtils.cpuIdVendorId("   0x00000000 0x00: eax=0x00000014 ebx=0x756e6547 ecx=0x6c65746e edx=0x49656e69\n"));
    }

    @Test
    public void testBrokenCpuid()
    {
        // virtualization detection must not break startup procedure, if cpuid executable
        // is not installed.

        String[] save = FileUtils.CPUID_COMMANDLINE;
        try
        {
            FileUtils.CPUID_COMMANDLINE = new String[]{"this_crazy_thing_might_exist_on_someones_computer_somewhere_in_the_universe"};
            FileUtils.detectVirtualization();
        }
        finally
        {
            FileUtils.CPUID_COMMANDLINE = save;
        }
    }

    @Test
    public void testDetectVirtualization()
    {
        FBUtilities.CpuInfo cpuInfoVirtual = FBUtilities.CpuInfo.loadFrom(Arrays.asList(cpuInfoHyperv.split("\n")));
        FBUtilities.CpuInfo cpuInfoBareMetal = FBUtilities.CpuInfo.loadFrom(Arrays.asList(cpuInfoNoVirt.split("\n")));

        // bare metal - all good
        String result = FileUtils.detectVirtualization(() -> baremetalCpuid,
                                                       Collections::emptyList,
                                                       () -> null,
                                                       () -> cpuInfoBareMetal,
                                                       filter -> null);
        assertNull(result);
        result = FileUtils.detectVirtualization(() -> baremetalCpuid,
                                                Collections::emptyList,
                                                () -> new File[0],
                                                () -> cpuInfoBareMetal,
                                                filter -> new File[0]);
        assertNull(result);

        // VMwareVMware vendor-ID in CPUID
        //0000000   G   e   n   u   i   n   e   I   n   t   e   l  \n
        //0000000    6547    756e    6e69    4965    746e    6c65    000a
        //0000000   V   M   w   a   r   e   V   M   w   a   r   e  \n
        //0000000    4d56    6177    6572    4d56    6177    6572    000a
        result = FileUtils.detectVirtualization(() -> baremetalCpuid.replace("ebx=0x756e6547 ecx=0x6c65746e edx=0x49656e69", // GenuineIntel
                                                                             "ebx=0x61774d56 ecx=0x65726177 edx=0x4d566572"), // VMwareVMware
                                                Collections::emptyList,
                                                () -> null,
                                                () -> cpuInfoBareMetal,
                                                filter -> null);
        assertEquals("VMWare", result);

        // VirtualBox returns "GenuineIntel" as the Vendor-ID for CPUID.
        // But bit 31 of ECX for CPUID leaf 1 is set.
        result = FileUtils.detectVirtualization(() -> virtualboxCpuid,
                                                Collections::emptyList,
                                                () -> null,
                                                () -> cpuInfoBareMetal,
                                                filter -> null);
        assertEquals("unknown (CPUID leaf 1 ECX bit 31 set)", result);

        // VirtualBox returns "GenuineIntel" as the Vendor-ID for CPUID.
        // Bit 31 of ECX for CPUID leaf 1 is is cleared in this test
        // but the hypervisor specific leafs are present.
        result = FileUtils.detectVirtualization(() -> virtualboxCpuid.replace("0x00000001 0x00: eax=0x00040661 ebx=0x02040800 ecx=0xdef82203 edx=0x178bfbff",
                                                                              "0x00000001 0x00: eax=0x00040661 ebx=0x02040800 ecx=0x5ef82203 edx=0x178bfbff"),
                                                Collections::emptyList,
                                                () -> null,
                                                () -> cpuInfoBareMetal,
                                                filter -> null);
        assertEquals("unknown (CPUID hypervisor leafs)", result);

        // In case the 'cpuid' binary is not available, the 'hypervisor' flag _might_ be present in /proc/cpuinfo
        result = FileUtils.detectVirtualization(() -> { throw new RuntimeException("foo bar baz"); },
                                                Collections::emptyList,
                                                () -> null,
                                                () -> cpuInfoVirtual,
                                                filter -> null);
        assertEquals("unknown (hypervisor CPU flag present)", result);

        // detection via /sys/hypervisor/properties/capabilities
        result = FileUtils.detectVirtualization(() -> baremetalCpuid,
                                                () -> Collections.singletonList("xen-3.0-x86_64 xen-3.0-x86_32p hvm-3.0-x86_32 hvm-3.0-x86_32p hvm-3.0-x86_64"),
                                                () -> null,
                                                () -> cpuInfoBareMetal,
                                                filter -> null);
        assertEquals("Xen HVM", result);

        // detection via /sys/hypervisor/properties/capabilities
        result = FileUtils.detectVirtualization(() -> baremetalCpuid,
                                                () -> Collections.singletonList("xen-3.0-x86_64 xen-3.0-x86_32p"),
                                                () -> null,
                                                () -> cpuInfoBareMetal,
                                                filter -> null);
        assertEquals("Xen", result);

        // detection whether there are Xen devices
        result = FileUtils.detectVirtualization(() -> baremetalCpuid,
                                                Collections::emptyList,
                                                () -> new File[]{ new File("/foobarbaz") },
                                                () -> cpuInfoBareMetal,
                                                filter -> null);
        assertEquals("Xen", result);

        // detection via disks-by-id
        result = FileUtils.detectVirtualization(() -> baremetalCpuid,
                                                Collections::emptyList,
                                                () -> null,
                                                () -> cpuInfoBareMetal,
                                                filter -> {
                                                    switch (filter)
                                                    {
                                                        case "-QEMU_":
                                                            return new File[]{
                                                            new File("/dev/disk/by-id/ata-QEMU_HARDDISK_VBc7f3f571-49b6d797")
                                                            };
                                                        case "-VBOX_":
                                                        default:
                                                            return null;
                                                    }
                                                });
        assertEquals("Xen/KVM", result);

        result = FileUtils.detectVirtualization(() -> baremetalCpuid,
                                                Collections::emptyList,
                                                () -> null,
                                                () -> cpuInfoBareMetal,
                                                filter -> {
                                                    switch (filter)
                                                    {
                                                        case "-VBOX_":
                                                            return new File[]{
                                                            new File("/dev/disk/by-id/ata-VBOX_HARDDISK_VBc7f3f571-49b6d797")
                                                            };
                                                        case "-QEMU_":
                                                        default:
                                                            return null;
                                                    }
                                                });
        assertEquals("VirtualBox", result);
    }

    @Test
    public void testGetDiskPartitions() throws IOException
    {
        String procMounts =
        "sysfs /sys sysfs rw,nosuid,nodev,noexec,relatime 0 0\n" +
        "proc /proc proc rw,nosuid,nodev,noexec,relatime 0 0\n" +
        "/dev/nvme0n1p2 / ext4 rw,relatime,errors=remount-ro,data=ordered 0 0\n" +
        "/dev/sda3 /foobar ext4 rw,relatime,errors=remount-ro,data=ordered 0 0\n" +
        "tmpfs /tmp tmpfs rw,nosuid,nodev,noatime 0 0\n" +
        "securityfs /sys/kernel/security securityfs rw,nosuid,nodev,noexec,relatime 0 0\n" +
        "cgroup /sys/fs/cgroup/systemd cgroup rw,nosuid,nodev,noexec,relatime,xattr,release_agent=/lib/systemd/systemd-cgroups-agent,name=systemd 0 0\n" +
        "fusectl /sys/fs/fuse/connections fusectl rw,relatime 0 0\n" +
        "/dev/nvme0n1p1 /boot/efi vfat rw,relatime,fmask=0077,dmask=0077,codepage=437,iocharset=iso8859-1,shortname=mixed,errors=remount-ro 0 0\n" +
        "binfmt_misc /proc/sys/fs/binfmt_misc binfmt_misc rw,relatime 0 0\n" +
        "gvfsd-fuse /run/user/1000/gvfs fuse.gvfsd-fuse rw,nosuid,nodev,relatime,user_id=1000,group_id=1000 0 0\n" +
        "/dev/nvme0n1p2 /var/lib/docker/aufs ext4 rw,relatime,errors=remount-ro,data=ordered 0 0\n" +
        "diskstation:/volume1/backup-bear /home/snazy/backup-bear nfs4 rw,relatime,vers=4.0 0 0\n";

        Map<Path, FileUtils.MountPoint> expected = new LinkedHashMap<>();
        // The order is important! We want the path with the most components first.
        expected.put(Paths.get("/var/lib/docker/aufs"), new FileUtils.MountPoint(Paths.get("/var/lib/docker/aufs"),
                                                                                 "nvme0n1p2",
                                                                                 "ext4"));
        expected.put(Paths.get("/home/snazy/backup-bear"), new FileUtils.MountPoint(Paths.get("/home/snazy/backup-bear"),
                                                                                    "diskstation:/volume1/backup-bear",
                                                                                    "nfs4"));
        expected.put(Paths.get("/boot/efi"), new FileUtils.MountPoint(Paths.get("/boot/efi"),
                                                                      "nvme0n1p1",
                                                                      "vfat"));
        expected.put(Paths.get("/foobar"), new FileUtils.MountPoint(Paths.get("/foobar"),
                                                                    "sda3",
                                                                    "ext4"));
        expected.put(Paths.get("/tmp"), new FileUtils.MountPoint(Paths.get("/tmp"),
                                                                 "tmpfs",
                                                                 "tmpfs"));
        expected.put(Paths.get("/"), new FileUtils.MountPoint(Paths.get("/"),
                                                              "nvme0n1p2",
                                                              "ext4"));

        Map<Path, FileUtils.MountPoint> mounts = FileUtils.getDiskPartitions(new StringReader(procMounts));
        ArrayList<Map.Entry<Path, FileUtils.MountPoint>> mountsList = new ArrayList<>(mounts.entrySet());
        ArrayList<Map.Entry<Path, FileUtils.MountPoint>> expectedList = new ArrayList<>(expected.entrySet());
        assertEquals(expectedList, mountsList);
    }

    @Test
    public void testTruncate() throws IOException
    {
        File file = FileUtils.createDeletableTempFile("testTruncate", "1");
        final String expected = "The quick brown fox jumps over the lazy dog";

        Files.write(file.toPath(), expected.getBytes());
        assertTrue(file.exists());

        byte[] b = Files.readAllBytes(file.toPath());
        assertEquals(expected, new String(b, StandardCharsets.UTF_8));

        FileUtils.truncate(file.getAbsolutePath(), 10);
        b = Files.readAllBytes(file.toPath());
        assertEquals("The quick ", new String(b, StandardCharsets.UTF_8));

        FileUtils.truncate(file.getAbsolutePath(), 0);
        b = Files.readAllBytes(file.toPath());
        assertEquals(0, b.length);
    }

    @Test
    public void testFolderSize() throws Exception
    {
        File folder = createFolder(Paths.get(DatabaseDescriptor.getAllDataFileLocations()[0], "testFolderSize"));
        folder.deleteOnExit();

        File childFolder = createFolder(Paths.get(folder.getPath(), "child"));

        File[] files = {
                       createFile(new File(folder, "001"), 10000),
                       createFile(new File(folder, "002"), 1000),
                       createFile(new File(folder, "003"), 100),
                       createFile(new File(childFolder, "001"), 1000),
                       createFile(new File(childFolder, "002"), 2000),
        };

        assertEquals(0, FileUtils.folderSize(new File(folder, "i_dont_exist")));
        assertEquals(files[0].length(), FileUtils.folderSize(files[0]));

        long size = FileUtils.folderSize(folder);
        assertEquals(Arrays.stream(files).mapToLong(f -> f.length()).sum(), size);
    }

    private File createFolder(Path path)
    {
        File folder = path.toFile();
        FileUtils.createDirectory(folder);
        return folder;
    }

    private File createFile(File file, long size)
    {
        try (RandomAccessFile f = new RandomAccessFile(file, "rw"))
        {
            f.setLength(size);
        }
        catch (Exception e)
        {
            System.err.println(e);
        }
        return file;
    }

    static final String virtualboxCpuid = "CPU:\n" +
                                          "   0x00000000 0x00: eax=0x0000000d ebx=0x756e6547 ecx=0x6c65746e edx=0x49656e69\n" +
                                          "   0x00000001 0x00: eax=0x00040661 ebx=0x02040800 ecx=0xdef82203 edx=0x178bfbff\n" +
                                          "   0x00000002 0x00: eax=0x76036301 ebx=0x00f0b5ff ecx=0x00000000 edx=0x00c10000\n" +
                                          "   0x00000003 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                          "   0x00000004 0x00: eax=0x0c000121 ebx=0x01c0003f ecx=0x0000003f edx=0x00000000\n" +
                                          "   0x00000004 0x01: eax=0x0c000122 ebx=0x01c0003f ecx=0x0000003f edx=0x00000000\n" +
                                          "   0x00000004 0x02: eax=0x0c000143 ebx=0x01c0003f ecx=0x000001ff edx=0x00000000\n" +
                                          "   0x00000004 0x03: eax=0x0c000163 ebx=0x02c0003f ecx=0x00001fff edx=0x00000006\n" +
                                          "   0x00000004 0x04: eax=0x0c000183 ebx=0x03c0f03f ecx=0x00001fff edx=0x00000004\n" +
                                          "   0x00000005 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                          "   0x00000006 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                          "   0x00000007 0x00: eax=0x00000000 ebx=0x00002000 ecx=0x00000000 edx=0x00000000\n" +
                                          "   0x00000008 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                          "   0x00000009 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                          "   0x0000000a 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                          "   0x0000000b 0x00: eax=0x00000000 ebx=0x00000001 ecx=0x00000100 edx=0x00000002\n" +
                                          "   0x0000000b 0x01: eax=0x00000002 ebx=0x00000004 ecx=0x00000201 edx=0x00000002\n" +
                                          "   0x0000000c 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                          "   0x0000000d 0x00: eax=0x00000007 ebx=0x00000340 ecx=0x00000340 edx=0x00000000\n" +
                                          "   0x0000000d 0x01: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                          "   0x0000000d 0x02: eax=0x00000100 ebx=0x00000240 ecx=0x00000000 edx=0x00000000\n" +
                                          "   0x40000000 0x00: eax=0x40000001 ebx=0x4b4d564b ecx=0x564b4d56 edx=0x0000004d\n" +
                                          "   0x40000001 0x00: eax=0x01000089 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                          "   0x80000000 0x00: eax=0x80000008 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                          "   0x80000001 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000021 edx=0x28100800\n" +
                                          "   0x80000002 0x00: eax=0x65746e49 ebx=0x2952286c ecx=0x726f4320 edx=0x4d542865\n" +
                                          "   0x80000003 0x00: eax=0x37692029 ebx=0x3839342d ecx=0x20514830 edx=0x20555043\n" +
                                          "   0x80000004 0x00: eax=0x2e322040 ebx=0x48473038 ecx=0x0000007a edx=0x00000000\n" +
                                          "   0x80000005 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                          "   0x80000006 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x01006040 edx=0x00000000\n" +
                                          "   0x80000007 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000100\n" +
                                          "   0x80000008 0x00: eax=0x00003027 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                          "   0x80860000 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                          "   0xc0000000 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n";
    static final String baremetalCpuid = "CPU:\n" +
                                         "   0x00000000 0x00: eax=0x00000014 ebx=0x756e6547 ecx=0x6c65746e edx=0x49656e69\n" +
                                         "   0x00000001 0x00: eax=0x000406f1 ebx=0x0a100800 ecx=0x7ffefbbf edx=0xbfebfbff\n" +
                                         "   0x00000002 0x00: eax=0x76036301 ebx=0x00f0b5ff ecx=0x00000000 edx=0x00c30000\n" +
                                         "   0x00000003 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                         "   0x00000004 0x00: eax=0x1c004121 ebx=0x01c0003f ecx=0x0000003f edx=0x00000000\n" +
                                         "   0x00000004 0x01: eax=0x1c004122 ebx=0x01c0003f ecx=0x0000003f edx=0x00000000\n" +
                                         "   0x00000004 0x02: eax=0x1c004143 ebx=0x01c0003f ecx=0x000001ff edx=0x00000000\n" +
                                         "   0x00000004 0x03: eax=0x1c03c163 ebx=0x04c0003f ecx=0x00003fff edx=0x00000006\n" +
                                         "   0x00000005 0x00: eax=0x00000040 ebx=0x00000040 ecx=0x00000003 edx=0x00002120\n" +
                                         "   0x00000006 0x00: eax=0x00000077 ebx=0x00000002 ecx=0x00000001 edx=0x00000000\n" +
                                         "   0x00000007 0x00: eax=0x00000000 ebx=0x021cbfbb ecx=0x00000000 edx=0x00000000\n" +
                                         "   0x00000008 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                         "   0x00000009 0x00: eax=0x00000001 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                         "   0x0000000a 0x00: eax=0x07300403 ebx=0x00000000 ecx=0x00000000 edx=0x00000603\n" +
                                         "   0x0000000b 0x00: eax=0x00000001 ebx=0x00000002 ecx=0x00000100 edx=0x0000000a\n" +
                                         "   0x0000000b 0x01: eax=0x00000004 ebx=0x00000010 ecx=0x00000201 edx=0x0000000a\n" +
                                         "   0x0000000c 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                         "   0x0000000d 0x00: eax=0x00000007 ebx=0x00000340 ecx=0x00000340 edx=0x00000000\n" +
                                         "   0x0000000d 0x01: eax=0x00000001 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                         "   0x0000000d 0x02: eax=0x00000100 ebx=0x00000240 ecx=0x00000000 edx=0x00000000\n" +
                                         "   0x0000000e 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                         "   0x0000000f 0x00: eax=0x00000000 ebx=0x0000003f ecx=0x00000000 edx=0x00000002\n" +
                                         "   0x0000000f 0x01: eax=0x00000000 ebx=0x00008000 ecx=0x0000003f edx=0x00000007\n" +
                                         "   0x00000010 0x00: eax=0x00000000 ebx=0x00000002 ecx=0x00000000 edx=0x00000000\n" +
                                         "   0x00000010 0x01: eax=0x00000013 ebx=0x000c0000 ecx=0x00000004 edx=0x0000000f\n" +
                                         "   0x00000011 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                         "   0x00000012 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                         "   0x00000013 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                         "   0x00000014 0x00: eax=0x00000000 ebx=0x00000001 ecx=0x00000001 edx=0x00000000\n" +
                                         "   0x80000000 0x00: eax=0x80000008 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                         "   0x80000001 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000121 edx=0x2c100800\n" +
                                         "   0x80000002 0x00: eax=0x65746e49 ebx=0x2952286c ecx=0x726f4320 edx=0x4d542865\n" +
                                         "   0x80000003 0x00: eax=0x37692029 ebx=0x3039362d ecx=0x43204b30 edx=0x40205550\n" +
                                         "   0x80000004 0x00: eax=0x322e3320 ebx=0x7a484730 ecx=0x00000000 edx=0x00000000\n" +
                                         "   0x80000005 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                         "   0x80000006 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x01006040 edx=0x00000000\n" +
                                         "   0x80000007 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000100\n" +
                                         "   0x80000008 0x00: eax=0x0000302e ebx=0x00000000 ecx=0x00000000 edx=0x00000000\n" +
                                         "   0x80860000 0x00: eax=0x00000000 ebx=0x00000001 ecx=0x00000001 edx=0x00000000\n" +
                                         "   0xc0000000 0x00: eax=0x00000000 ebx=0x00000001 ecx=0x00000001 edx=0x00000000\n";

    private static final String cpuInfoHyperv = "processor : 0\n" +
                                                "vendor_id : GenuineIntel\n" +
                                                "cpu family : 6\n" +
                                                "model : 70\n" +
                                                "model name : Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz\n" +
                                                "stepping : 1\n" +
                                                "microcode : 0x13\n" +
                                                "cpu MHz : 2194.322\n" +
                                                "cache size : 6144 KB\n" +
                                                "physical id : 0\n" +
                                                "siblings : 1\n" +
                                                "core id : 0\n" +
                                                "cpu cores : 1\n" +
                                                "apicid : 0\n" +
                                                "initial apicid : 0\n" +
                                                "fpu : yes\n" +
                                                "fpu_exception : yes\n" +
                                                "cpuid level : 13\n" +
                                                "wp : yes\n" +
                                                "flags : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts mmx fxsr sse sse2 ss syscall nx pdpe1gb rdtscp lm constant_tsc arch_perfmon pebs bts nopl xtopology tsc_reliable nonstop_tsc aperfmperf pni pclmulqdq vmx ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt aes xsave avx f16c rdrand hypervisor lahf_lm ida arat epb pln pts dtherm tpr_shadow vnmi ept vpid fsgsbase smep\n" +
                                                "bogomips : 4389.82\n" +
                                                "clflush size : 64\n" +
                                                "cache_alignment : 64\n" +
                                                "address sizes : 40 bits physical, 48 bits virtual\n" +
                                                "power management:\n" +
                                                "\n" +
                                                "processor : 1\n" +
                                                "vendor_id : GenuineIntel\n" +
                                                "cpu family : 6\n" +
                                                "model : 70\n" +
                                                "model name : Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz\n" +
                                                "stepping : 1\n" +
                                                "microcode : 0x13\n" +
                                                "cpu MHz : 2194.322\n" +
                                                "cache size : 6144 KB\n" +
                                                "physical id : 2\n" +
                                                "siblings : 1\n" +
                                                "core id : 0\n" +
                                                "cpu cores : 1\n" +
                                                "apicid : 2\n" +
                                                "initial apicid : 2\n" +
                                                "fpu : yes\n" +
                                                "fpu_exception : yes\n" +
                                                "cpuid level : 13\n" +
                                                "wp : yes\n" +
                                                "flags : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts mmx fxsr sse sse2 ss syscall nx pdpe1gb rdtscp lm constant_tsc arch_perfmon pebs bts nopl xtopology tsc_reliable nonstop_tsc aperfmperf pni pclmulqdq vmx ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt aes xsave avx f16c rdrand hypervisor lahf_lm ida arat epb pln pts dtherm tpr_shadow vnmi ept vpid fsgsbase smep\n" +
                                                "bogomips : 4389.82\n" +
                                                "clflush size : 64\n" +
                                                "cache_alignment : 64\n" +
                                                "address sizes : 40 bits physical, 48 bits virtual\n" +
                                                "power management: \n";

    private static final String cpuInfoNoVirt = "processor : 0\n" +
                                                "vendor_id : GenuineIntel\n" +
                                                "cpu family : 6\n" +
                                                "model : 70\n" +
                                                "model name : Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz\n" +
                                                "stepping : 1\n" +
                                                "microcode : 0x13\n" +
                                                "cpu MHz : 2194.322\n" +
                                                "cache size : 6144 KB\n" +
                                                "physical id : 0\n" +
                                                "siblings : 1\n" +
                                                "core id : 0\n" +
                                                "cpu cores : 1\n" +
                                                "apicid : 0\n" +
                                                "initial apicid : 0\n" +
                                                "fpu : yes\n" +
                                                "fpu_exception : yes\n" +
                                                "cpuid level : 13\n" +
                                                "wp : yes\n" +
                                                "flags : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts mmx fxsr sse sse2 ss syscall nx pdpe1gb rdtscp lm constant_tsc arch_perfmon pebs bts nopl xtopology tsc_reliable nonstop_tsc aperfmperf pni pclmulqdq vmx ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt aes xsave avx f16c rdrand lahf_lm ida arat epb pln pts dtherm tpr_shadow vnmi ept vpid fsgsbase smep\n" +
                                                "bogomips : 4389.82\n" +
                                                "clflush size : 64\n" +
                                                "cache_alignment : 64\n" +
                                                "address sizes : 40 bits physical, 48 bits virtual\n" +
                                                "power management:\n" +
                                                "\n" +
                                                "processor : 1\n" +
                                                "vendor_id : GenuineIntel\n" +
                                                "cpu family : 6\n" +
                                                "model : 70\n" +
                                                "model name : Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz\n" +
                                                "stepping : 1\n" +
                                                "microcode : 0x13\n" +
                                                "cpu MHz : 2194.322\n" +
                                                "cache size : 6144 KB\n" +
                                                "physical id : 2\n" +
                                                "siblings : 1\n" +
                                                "core id : 0\n" +
                                                "cpu cores : 1\n" +
                                                "apicid : 2\n" +
                                                "initial apicid : 2\n" +
                                                "fpu : yes\n" +
                                                "fpu_exception : yes\n" +
                                                "cpuid level : 13\n" +
                                                "wp : yes\n" +
                                                "flags : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts mmx fxsr sse sse2 ss syscall nx pdpe1gb rdtscp lm constant_tsc arch_perfmon pebs bts nopl xtopology tsc_reliable nonstop_tsc aperfmperf pni pclmulqdq vmx ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt aes xsave avx f16c rdrand lahf_lm ida arat epb pln pts dtherm tpr_shadow vnmi ept vpid fsgsbase smep\n" +
                                                "bogomips : 4389.82\n" +
                                                "clflush size : 64\n" +
                                                "cache_alignment : 64\n" +
                                                "address sizes : 40 bits physical, 48 bits virtual\n" +
                                                "power management: \n";
}
