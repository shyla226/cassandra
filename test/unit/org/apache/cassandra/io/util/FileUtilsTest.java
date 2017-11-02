/**
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileUtilsTest
{

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testGetDiskPartitions() throws IOException
    {
        String procMounts =
        "sysfs /sys sysfs rw,nosuid,nodev,noexec,relatime 0 0\n" +
        "proc /proc proc rw,nosuid,nodev,noexec,relatime 0 0\n" +
        "tmpfs /run tmpfs rw,nosuid,noexec,relatime,size=6587372k,mode=755 0 0\n" +
        "/dev/nvme0n1p2 / ext4 rw,relatime,errors=remount-ro,data=ordered 0 0\n" +
        "/dev/sda3 /foobar ext4 rw,relatime,errors=remount-ro,data=ordered 0 0\n" +
        "securityfs /sys/kernel/security securityfs rw,nosuid,nodev,noexec,relatime 0 0\n" +
        "cgroup /sys/fs/cgroup/systemd cgroup rw,nosuid,nodev,noexec,relatime,xattr,release_agent=/lib/systemd/systemd-cgroups-agent,name=systemd 0 0\n" +
        "fusectl /sys/fs/fuse/connections fusectl rw,relatime 0 0\n" +
        "/dev/nvme0n1p1 /boot/efi vfat rw,relatime,fmask=0077,dmask=0077,codepage=437,iocharset=iso8859-1,shortname=mixed,errors=remount-ro 0 0\n" +
        "binfmt_misc /proc/sys/fs/binfmt_misc binfmt_misc rw,relatime 0 0\n" +
        "tmpfs /run/user/1000 tmpfs rw,nosuid,nodev,relatime,size=6587368k,mode=700,uid=1000,gid=1000 0 0\n" +
        "gvfsd-fuse /run/user/1000/gvfs fuse.gvfsd-fuse rw,nosuid,nodev,relatime,user_id=1000,group_id=1000 0 0\n" +
        "/dev/nvme0n1p2 /var/lib/docker/aufs ext4 rw,relatime,errors=remount-ro,data=ordered 0 0\n" +
        "diskstation:/volume1/backup-bear /home/snazy/backup-bear nfs4 rw,relatime,vers=4.0 0 0\n";

        Map<Path, String> expected = new LinkedHashMap<>();
        // The order is important! We want the path with the most components first.
        expected.put(Paths.get("/var/lib/docker/aufs"), "nvme0n1p2");
        expected.put(Paths.get("/home/snazy/backup-bear"), "WE_DO_NOT_KNOW");
        expected.put(Paths.get("/boot/efi"), "nvme0n1p1");
        expected.put(Paths.get("/foobar"), "sda3");
        expected.put(Paths.get("/"), "nvme0n1p2");

        Map<Path, String> mounts = FileUtils.getDiskPartitions(new StringReader(procMounts));
        ArrayList<Map.Entry<Path, String>> mountsList = new ArrayList<>(mounts.entrySet());
        ArrayList<Map.Entry<Path, String>> expectedList = new ArrayList<>(expected.entrySet());
        assertEquals(expectedList, mountsList);
    }

    @Test
    public void testTruncate() throws IOException
    {
        File file = FileUtils.createTempFile("testTruncate", "1");
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
}
