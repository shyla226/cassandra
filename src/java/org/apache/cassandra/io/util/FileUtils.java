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
package org.apache.cassandra.io.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.nio.ch.DirectBuffer;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.io.*;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;

public final class FileUtils
{
    public static final Charset CHARSET = StandardCharsets.UTF_8;

    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);
    public static final long ONE_KB = 1024;
    public static final long ONE_MB = 1024 * ONE_KB;
    public static final long ONE_GB = 1024 * ONE_MB;
    public static final long ONE_TB = 1024 * ONE_GB;

    private static final DecimalFormat df = new DecimalFormat("#.##");
    public static final boolean isCleanerAvailable;
    private static final AtomicReference<Optional<FSErrorHandler>> fsErrorHandler = new AtomicReference<>(Optional.empty());

    static String[] CPUID_COMMANDLINE = { "cpuid", "-1", "-r" };

    static
    {
        boolean canClean = false;
        try
        {
            ByteBuffer buf = ByteBuffer.allocateDirect(1);
            ((DirectBuffer) buf).cleaner().clean();
            canClean = true;
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.info("Cannot initialize un-mmaper.  (Are you using a non-Oracle JVM?)  Compacted data files will not be removed promptly.  Consider using an Oracle JVM or using standard disk access mode");
        }
        isCleanerAvailable = canClean;
    }

    public static void createHardLink(String from, String to)
    {
        createHardLink(new File(from), new File(to));
    }

    public static void createHardLink(File from, File to)
    {
        if (to.exists())
            throw new RuntimeException("Tried to create duplicate hard link to " + to);
        if (!from.exists())
            throw new RuntimeException("Tried to hard link to file that does not exist " + from);

        try
        {
            Files.createLink(to.toPath(), from.toPath());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, to);
        }
    }

    public static File createTempFile(String prefix, String suffix, File directory)
    {
        try
        {
            return File.createTempFile(prefix, suffix, directory);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, directory);
        }
    }

    public static File createTempFile(String prefix, String suffix)
    {
        return createTempFile(prefix, suffix, new File(System.getProperty("java.io.tmpdir")));
    }

    public static File createDeletableTempFile(String prefix, String suffix)
    {
        File f = createTempFile(prefix, suffix, new File(System.getProperty("java.io.tmpdir")));
        f.deleteOnExit();
        return f;
    }

    public static Throwable deleteWithConfirm(String filePath, boolean expect, Throwable accumulate)
    {
        return deleteWithConfirm(new File(filePath), expect, accumulate);
    }

    public static Throwable deleteWithConfirm(File file, boolean expect, Throwable accumulate)
    {
        boolean exists = file.exists();
        assert exists || !expect : "attempted to delete non-existing file " + file.getName();
        try
        {
            if (exists)
                Files.delete(file.toPath());
        }
        catch (Throwable t)
        {
            try
            {
                throw new FSWriteError(t, file);
            }
            catch (Throwable t2)
            {
                accumulate = merge(accumulate, t2);
            }
        }
        return accumulate;
    }

    public static void deleteWithConfirm(String file)
    {
        deleteWithConfirm(new File(file));
    }

    public static void deleteWithConfirm(File file)
    {
        maybeFail(deleteWithConfirm(file, true, null));
    }

    public static void renameWithOutConfirm(String from, String to)
    {
        try
        {
            atomicMoveWithFallback(new File(from).toPath(), new File(to).toPath());
        }
        catch (IOException e)
        {
            if (logger.isTraceEnabled())
                logger.trace("Could not move file "+from+" to "+to, e);
        }
    }

    public static void renameWithConfirm(String from, String to)
    {
        renameWithConfirm(new File(from), new File(to));
    }

    public static void renameWithConfirm(File from, File to)
    {
        assert from.exists();
        if (logger.isTraceEnabled())
            logger.trace("Renaming {} to {}", from.getPath(), to.getPath());
        // this is not FSWE because usually when we see it it's because we didn't close the file before renaming it,
        // and Windows is picky about that.
        try
        {
            atomicMoveWithFallback(from.toPath(), to.toPath());
        }
        catch (IOException e)
        {
            throw new RuntimeException(String.format("Failed to rename %s to %s", from.getPath(), to.getPath()), e);
        }
    }

    /**
     * Move a file atomically, if it fails, it falls back to a non-atomic operation
     * @param from
     * @param to
     * @throws IOException
     */
    private static void atomicMoveWithFallback(Path from, Path to) throws IOException
    {
        try
        {
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        }
        catch (AtomicMoveNotSupportedException e)
        {
            logger.trace("Could not do an atomic move", e);
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
        }

    }
    public static void truncate(String path, long size)
    {
        try(FileChannel channel = FileChannel.open(Paths.get(path), StandardOpenOption.READ, StandardOpenOption.WRITE))
        {
            channel.truncate(size);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void closeQuietly(Closeable c)
    {
        try
        {
            if (c != null)
                c.close();
        }
        catch (Exception e)
        {
            logger.warn("Failed closing {}", c, e);
        }
    }

    public static void closeQuietly(AutoCloseable c)
    {
        try
        {
            if (c != null)
                c.close();
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            logger.warn("Failed closing {}", c, e);
        }
    }

    public static void close(Closeable... cs) throws IOException
    {
        close(Arrays.asList(cs));
    }

    public static void close(Iterable<? extends Closeable> cs) throws IOException
    {
        IOException e = null;
        for (Closeable c : cs)
        {
            try
            {
                if (c != null)
                    c.close();
            }
            catch (IOException ex)
            {
                e = ex;
                logger.warn("Failed closing stream {}", c, ex);
            }
        }
        if (e != null)
            throw e;
    }

    public static void closeQuietly(Iterable<? extends AutoCloseable> cs)
    {
        for (AutoCloseable c : cs)
        {
            try
            {
                if (c != null)
                    c.close();
            }
            catch (Exception ex)
            {
                logger.warn("Failed closing {}", c, ex);
            }
        }
    }

    public static String getCanonicalPath(String filename)
    {
        try
        {
            return new File(filename).getCanonicalPath();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filename);
        }
    }

    public static String getCanonicalPath(File file)
    {
        try
        {
            return file.getCanonicalPath();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, file);
        }
    }

    /** Return true if file is contained in folder */
    public static boolean isContained(File folder, File file)
    {
        String folderPath = getCanonicalPath(folder);
        String filePath = getCanonicalPath(file);

        return filePath.startsWith(folderPath);
    }

    /** Convert absolute path into a path relative to the base path */
    public static String getRelativePath(String basePath, String path)
    {
        try
        {
            return Paths.get(basePath).relativize(Paths.get(path)).toString();
        }
        catch(Exception ex)
        {
            String absDataPath = FileUtils.getCanonicalPath(basePath);
            return Paths.get(absDataPath).relativize(Paths.get(path)).toString();
        }
    }

    public static void clean(ByteBuffer buffer)
    {
        clean(buffer, false);
    }

    public static void clean(ByteBuffer buffer, boolean cleanAttachment)
    {
        if (buffer == null)
            return;
        if (isCleanerAvailable && buffer.isDirect())
        {
            DirectBuffer db = (DirectBuffer) buffer;
            if (db.cleaner() != null)
            {
                db.cleaner().clean();
            }
            else
            {
                // When dealing with sliced buffers we
                // attach the root buffer we used to align
                // so we can properly free it
                if (cleanAttachment && buffer.isDirect())
                {
                    Object attach = UnsafeByteBufferAccess.getAttachment(buffer);
                    if (attach != null && attach instanceof ByteBuffer && attach != buffer)
                        clean((ByteBuffer) attach);
                }
            }
        }
    }

    public static void createDirectory(String directory)
    {
        createDirectory(new File(directory));
    }

    public static void createDirectory(File directory)
    {
        if (!directory.exists())
        {
            if (!directory.mkdirs())
                throw new FSWriteError(new IOException("Failed to mkdirs " + directory), directory);
        }
    }

    public static boolean delete(String file)
    {
        File f = new File(file);
        return f.delete();
    }

    public static void delete(File... files)
    {
        for ( File file : files )
        {
            file.delete();
        }
    }

    public static void deleteAsync(final String file)
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                deleteWithConfirm(new File(file));
            }
        };
        ScheduledExecutors.nonPeriodicTasks.execute(runnable);
    }

    public static String stringifyFileSize(double value)
    {
        double d;
        if ( value >= ONE_TB )
        {
            d = value / ONE_TB;
            String val = df.format(d);
            return val + " TiB";
        }
        else if ( value >= ONE_GB )
        {
            d = value / ONE_GB;
            String val = df.format(d);
            return val + " GiB";
        }
        else if ( value >= ONE_MB )
        {
            d = value / ONE_MB;
            String val = df.format(d);
            return val + " MiB";
        }
        else if ( value >= ONE_KB )
        {
            d = value / ONE_KB;
            String val = df.format(d);
            return val + " KiB";
        }
        else
        {
            String val = df.format(value);
            return val + " bytes";
        }
    }

    /**
     * Deletes all files and subdirectories under "dir".
     * @param dir Directory to be deleted
     * @throws FSWriteError if any part of the tree cannot be deleted
     */
    public static void deleteRecursive(File dir)
    {
        if (dir.isDirectory())
        {
            String[] children = dir.list();
            for (String child : children)
                deleteRecursive(new File(dir, child));
        }

        // The directory is now empty so now it can be smoked
        deleteWithConfirm(dir);
    }

    /**
     * Schedules deletion of all file and subdirectories under "dir" on JVM shutdown.
     * @param dir Directory to be deleted
     */
    public static void deleteRecursiveOnExit(File dir)
    {
        if (dir.isDirectory())
        {
            String[] children = dir.list();
            for (String child : children)
                deleteRecursiveOnExit(new File(dir, child));
        }

        logger.trace("Scheduling deferred deletion of file: {}", dir);
        dir.deleteOnExit();
    }

    public static void handleCorruptSSTable(CorruptSSTableException e)
    {
        fsErrorHandler.get().ifPresent(handler -> handler.handleCorruptSSTable(e));
    }

    public static void handleFSError(FSError e)
    {
        fsErrorHandler.get().ifPresent(handler -> handler.handleFSError(e));
    }

    /**
     * Get the size of a directory in bytes
     * @param folder The directory for which we need size.
     * @return The size of the directory
     */
    public static long folderSize(File folder)
    {
        final long [] sizeArr = {0L};
        try
        {
            Files.walkFileTree(folder.toPath(), new SimpleFileVisitor<Path>()
            {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                {
                    sizeArr[0] += attrs.size();
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        catch (IOException e)
        {
            logger.error("Error while getting {} folder size. {}", folder, e.getMessage());
        }
        return sizeArr[0];
    }

    public static void copyTo(DataInput in, OutputStream out, int length) throws IOException
    {
        byte[] buffer = new byte[64 * 1024];
        int copiedBytes = 0;

        while (copiedBytes + buffer.length < length)
        {
            in.readFully(buffer);
            out.write(buffer);
            copiedBytes += buffer.length;
        }

        if (copiedBytes < length)
        {
            int left = length - copiedBytes;
            in.readFully(buffer, 0, left);
            out.write(buffer, 0, left);
        }
    }

    public static boolean isSubDirectory(File parent, File child) throws IOException
    {
        parent = parent.getCanonicalFile();
        child = child.getCanonicalFile();

        File toCheck = child;
        while (toCheck != null)
        {
            if (parent.equals(toCheck))
                return true;
            toCheck = toCheck.getParentFile();
        }
        return false;
    }

    public static void append(File file, String ... lines)
    {
        if (file.exists())
            write(file, Arrays.asList(lines), StandardOpenOption.APPEND);
        else
            write(file, Arrays.asList(lines), StandardOpenOption.CREATE);
    }

    public static void appendAndSync(File file, String ... lines)
    {
        if (file.exists())
            write(file, Arrays.asList(lines), StandardOpenOption.APPEND, StandardOpenOption.SYNC);
        else
            write(file, Arrays.asList(lines), StandardOpenOption.CREATE, StandardOpenOption.SYNC);
    }

    public static void replace(File file, String ... lines)
    {
        write(file, Arrays.asList(lines), StandardOpenOption.TRUNCATE_EXISTING);
    }

    public static void write(File file, List<String> lines, StandardOpenOption ... options)
    {
        try
        {
            Files.write(file.toPath(),
                        lines,
                        CHARSET,
                        options);
        }
        catch (IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    public static String readLine(File file)
    {
        List<String> lines = readLines(file);
        return lines == null || lines.size() < 1 ? null : lines.get(0);
    }

    public static List<String> readLines(File file)
    {
        try
        {
            return Files.readAllLines(file.toPath(), CHARSET);
        }
        catch (IOException ex)
        {
            if (ex instanceof NoSuchFileException)
                return Collections.emptyList();

            throw new RuntimeException(ex);
        }
    }

    public static void setFSErrorHandler(FSErrorHandler handler)
    {
        fsErrorHandler.getAndSet(Optional.ofNullable(handler));
    }

    public static class MountPoint
    {
        // the default sector size applies to all devices on Linux but only takes effect if logical_block_size does not exist
        // or for platforms other than Linux
        private static final Supplier<String> defaultSectorSize = () -> System.getProperty("dse.io.default.sector.size", "512");

        public static final MountPoint DEFAULT = new MountPoint(Paths.get(".").getRoot(), "unknown", "unknown", true, Integer.parseInt(defaultSectorSize.get()));

        private static final Map<Path, MountPoint> mountPoints = getDiskPartitions();

        public final Path mountpoint;
        public final String device;
        public final String fstype;
        public final boolean onSSD;
        public final int sectorSize;

        /**
         * Lists the physical disk devices the data directories are on.
         * This only works on Linux.
         */
        private static Map<Path, MountPoint> getDiskPartitions()
        {
            if (!FBUtilities.isLinux)
                return Collections.emptyMap();

            try
            {
                try (Reader source = new InputStreamReader(new FileInputStream("/proc/mounts"), "UTF-8"))
                {
                    return getDiskPartitions(source);
                }
            }
            catch (Throwable t)
            {
                logger.error("Failed to retrieve disk partitions", t);
                return Collections.emptyMap();
            }
        }

        @VisibleForTesting
        static Map<Path, MountPoint> getDiskPartitions(Reader source) throws IOException
        {
            assert FBUtilities.isLinux;

            Map<Path, MountPoint> dirToDisk = new TreeMap<>((a,b) ->
                                                            {
                                                                int cmp = -Integer.compare(a.getNameCount(), b.getNameCount());
                                                                if (cmp != 0)
                                                                    return cmp;
                                                                return a.toString().compareTo(b.toString());
                                                            });
            try (BufferedReader bufferedReader =  new BufferedReader(source))
            {
                String line;
                while ((line = bufferedReader.readLine()) != null)
                {
                    String[] parts = line.split(" +");
                    if (parts.length <= 2)
                        continue;

                    String partition =  parts[0];

                    String fstype = parts[2];

                    if (!checkRamdisk(fstype) && partition.indexOf('/') == -1)
                        // this _should_ be a Linux internal thing (sysfs, procfs, tmpfs, cgroup, whatever)
                        continue;

                    if (partition.startsWith("/dev/"))
                    {
                        // we need the full disk partition name here and look up the device later
                        // possible patterns:
                        //   /dev/sdb23
                        //   /dev/nvme0n1p2
                        partition = partition.substring("/dev/".length());
                    }

                    // /home, /dev/sda
                    Path mp = Paths.get(parts[1]);
                    dirToDisk.put(mp, new MountPoint(mp, partition, fstype));
                }
            }

            return dirToDisk;
        }

        MountPoint(Path mountpoint, String device, String fstype) throws IOException
        {
            this(mountpoint, device, fstype, getQueueFolder(device));
        }

        private MountPoint(Path mountpoint, String device, String fstype, Path queueFolder) throws IOException
        {
            this(mountpoint, device, fstype,
                 checkRamdisk(fstype) ? true : isPartitionOnSSD(queueFolder),
                 checkRamdisk(fstype) ? DEFAULT.sectorSize : getSectorSize(queueFolder, device));
        }

        private MountPoint(Path mountpoint, String device, String fstype, boolean onSSD, int sectorSize)
        {
            this.mountpoint = mountpoint;
            this.device = device;
            this.fstype = fstype;
            this.onSSD = onSSD;
            this.sectorSize = sectorSize;
        }

        private static Path getQueueFolder(String device) throws IOException
        {
            assert FBUtilities.isLinux;

            Path sysClassBlock = Paths.get("/sys/class/block/" + device);
            // not a block device
            if (!sysClassBlock.toFile().exists())
                return null;

            // read symlink from /sys/class/block/... to /sys/devices/...
            sysClassBlock = sysClassBlock.toRealPath();

            // If 'device' points to a partition, there will be no "queue" folder, because partitions don't have
            // that - but the device does. The device is found in the parent folder.
            // If 'device' directly points to a device (e.g. /dev/loopX or /dev/sda), then the "queue" folder will
            // be there.

            // pointers to the device itself (e.g. via /sys/class/block/sda) have the "queue" folder in there
            if (sysClassBlock.resolve("queue").toFile().isDirectory())
                return sysClassBlock.resolve("queue");

            // pointers to the partition (e.g. /sys/class/block/sda1) have the "queue" folder in the parent, which is the device
            Path devDir = sysClassBlock.getParent();
            return devDir.resolve("queue");
        }

        /**
         * Detects if a given partition (i.e. sda2, nvme0n1p2) is rotational or ssd
         *
         * This only works on Linux
         *
         * @param queue the folder containing files for the block device
         * @return true for ssd
         * @throws IOException
         */
        private static boolean isPartitionOnSSD(Path queue) throws IOException
        {
            assert FBUtilities.isLinux;

            // not a block device - so can't be an SSD
            if (queue == null)
                return false;

            Path rotational = queue.resolve("rotational");
            if (!rotational.toFile().exists())
                // assume it's rotational, if that file's not there
                return false;

            // "rotational" contains 0 for non-rotational
            return "0".equals(Files.lines(rotational).findFirst().orElse("1"));
        }

        private static int getSectorSize(Path queue, String device) throws IOException
        {
            assert FBUtilities.isLinux;

            // if for whatever reason the content of logical_block_size does not contain the correct sector size,
            // an operator can specify a sector size manually for a specific device which will override the content of
            // logical_block_size
            int operatorSectorSize = Integer.parseInt(System.getProperty(String.format("dse.io.%s.sector.size", device), "0"));
            if (operatorSectorSize > 0)
            {
                logger.info("Using operator sector size {} for {}", operatorSectorSize, device);
                return operatorSectorSize;
            }

            if (queue == null)
            {
                logger.warn("Could not determine sector size for {}, assuming {}", device, defaultSectorSize.get());
                return Integer.parseInt(defaultSectorSize.get());
            }

            // according to https://lwn.net/Articles/12032/, for AIO alignment the requirement is to align to what
            // bdev_hardsect_size() returns, which can be queried with ioctl() using BLKSSZGET, which derives from
            // the hardsect_size array. This should be the logical block size, not the physical block size. For this
            // reason we are reading the logical block size. However, it may be that for performance of HDDs using
            // advanced formatting and 512 emulation, we should really read the physical block size. I'm not sure
            // if there is ever a case where this could cause problems though.
            Path logical_block_size = queue.resolve("logical_block_size");
            if (!logical_block_size.toFile().exists())
            {
                logger.warn("Could not determine sector size for {}, assuming {}", device, defaultSectorSize.get());
                return Integer.parseInt(defaultSectorSize.get());
            }

            return Integer.parseInt(Files.lines(logical_block_size).findFirst().orElseGet(defaultSectorSize));
        }

        public static MountPoint mountPointForDirectory(String dir)
        {
            if (mountPoints.isEmpty())
                return DEFAULT; // a dummy mount-point for non linux partitions

            try
            {
                Path path = Paths.get(dir).toAbsolutePath();
                for (Map.Entry<Path, MountPoint> entry : mountPoints.entrySet())
                {
                    if (path.startsWith(entry.getKey()))
                    {
                        return entry.getValue();
                    }
                }

                logger.debug("Could not find mount-point for {}", dir);
                return DEFAULT;
            }
            catch (Throwable t)
            {
                logger.debug("Failed to detect mount point for directory {}: {}", dir, t.getMessage());
                return DEFAULT;
            }
        }

        public static boolean hasMountPoints()
        {
            return !mountPoints.isEmpty();
        }

        @Override
        public String toString()
        {
            return "MountPoint{" +
                   "mountpoint=" + mountpoint +
                   ", device='" + device + '\'' +
                   ", fstype='" + fstype + '\'' +
                   ", onSSD=" + onSSD +
                   ", sectorSize=" + sectorSize +
                   '}';
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MountPoint that = (MountPoint) o;
            return onSSD == that.onSSD &&
                   sectorSize == that.sectorSize &&
                   Objects.equals(mountpoint, that.mountpoint) &&
                   Objects.equals(device, that.device) &&
                   Objects.equals(fstype, that.fstype);
        }

        public int hashCode()
        {

            return Objects.hash(mountpoint, device, fstype, onSSD, sectorSize);
        }
    }

    private static boolean checkRamdisk(String fstype)
    {
        return "tmpfs".equals(fstype);
    }

    /**
     * Best-effort checks to detect whether and eventually on which virtualization we're running.
     *
     * @return {@code null} if unable to detect virtualization (which is usually a good sign) or
     *         the name of the virtualization type.
     */
    public static String detectVirtualization()
    {
        if (!FBUtilities.isLinux)
            return null;

        return detectVirtualization(() ->
                                    {
                                        try
                                        {
                                            Process proc = Runtime.getRuntime().exec(CPUID_COMMANDLINE);
                                            proc.waitFor(1, TimeUnit.SECONDS);
                                            InputStream in = proc.getInputStream();
                                            byte[] buf = new byte[in.available()];
                                            for (int p = 0; p < buf.length; )
                                            {
                                                int rd = in.read(buf, p, buf.length - p);
                                                if (rd < 0)
                                                    break;
                                                p += rd;
                                            }
                                            return new String(buf, StandardCharsets.UTF_8);
                                        }
                                        catch (Exception e)
                                        {
                                            throw new RuntimeException(e);
                                        }
                                    },
                                    () ->
                                    {
                                        File hypervisorCaps = new File("/sys/hypervisor/properties/capabilities");
                                        if (hypervisorCaps.exists() && hypervisorCaps.canRead())
                                        {
                                            return FileUtils.readLines(hypervisorCaps);
                                        }
                                        return null;
                                    },
                                    () -> new File("/sys/bus/xen/devices/").listFiles(),
                                    FBUtilities.CpuInfo::load,
                                    (filter) -> new File("/dev/disk/by-id").listFiles(n -> n.getName().contains(filter)));
    }

    @VisibleForTesting
    static String detectVirtualization(Supplier<String> cpuid,
                                       Supplier<List<String>> hypervisorCaps,
                                       Supplier<File[]> xenDevices,
                                       Supplier<FBUtilities.CpuInfo> cpuInfoSupplier,
                                       Function<String, File[]> disksByIdFilter)
    {
        try
        {
            // The most reliable way to detect virtualization/hypervisor is to check the CPUID.
            // Bit 31 of ECX of CPUID is reserved for this reason. 
            // https://kb.vmware.com/s/article/1009458
            // https://en.wikipedia.org/wiki/CPUID

            try
            {
                String cpuidOutput = cpuid.get();
                String[] cpuidLines = cpuidOutput.split("\n");
                String vendorId = cpuIdVendorId(cpuidLine(cpuidLines, "0x00000000", "0x00"));
                String virtByVendorId = virtualizationFromVendorId(vendorId);
                if (virtByVendorId != null)
                    return virtByVendorId;

                int[] cpuidLeaf1 = cpuIdParse(cpuidLine(cpuidLines, "0x00000001", "0x00"));
                boolean hypervisorFlag = (cpuidLeaf1[2] & 0x80000000) != 0;
                if (hypervisorFlag)
                    return "unknown (CPUID leaf 1 ECX bit 31 set)";

                // 0x40000000..0x400000FF reserved for hypervisor custom stuff
                boolean hypervisorLeafsPresent = Arrays.stream(cpuidLines).anyMatch(s -> s.startsWith("   0x400000"));
                if (hypervisorLeafsPresent)
                    return "unknown (CPUID hypervisor leafs)";
            }
            catch (Exception e)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Unable to parse CPUID information. This is usually because 'cpuid' tool is not installed on the system.", e);
                else
                    logger.debug("Unable to parse CPUID information ({}). This is usually because 'cpuid' tool is not installed on the system.", e.toString());
            }

            // the easy way - read sysfs
            // http://www.brendangregg.com/blog/2014-05-09/xen-feature-detection.html

            List<String> hCaps = hypervisorCaps.get();
            if (hCaps != null)
            {
                Set<String> caps = hypervisorCaps.get().stream()
                                                 .flatMap(s -> Arrays.stream(s.split(" ")))
                                                 .map(s -> s.split("-")[0])
                                                 .collect(Collectors.toSet());

                if (caps.contains("xen") && caps.contains("hvm"))
                    return "Xen HVM";

                if (caps.contains("xen"))
                    return "Xen";

                if (!caps.isEmpty())
                    return "unknwon (hypervisor capabilities)";
            }

            File[] xenDevs = xenDevices.get();
            if (xenDevs != null && xenDevs.length > 0)
                return "Xen";

            // disk by-id method
            // https://techglimpse.com/xen-kvm-virtualbox-vm-detection-command/

            File[] disks = disksByIdFilter.apply("-QEMU_");
            if (disks != null && disks.length > 0)
                return "Xen/KVM";
            disks = disksByIdFilter.apply("-VBOX_");
            if (disks != null && disks.length > 0)
                return "VirtualBox";

            // Uses the 'vendor_id' field and 'hypervisor' CPU flags from /proc/cpuinfo to identify
            // whether the system is a virtual machine. This is similar to the check that is using
            // the 'cpuid' binary - however, the check above is probably more reliable.
            try
            {
                FBUtilities.CpuInfo cpuInfo = cpuInfoSupplier.get();
                if (!cpuInfo.getProcessors().isEmpty())
                {
                    FBUtilities.CpuInfo.PhysicalProcessor processor = cpuInfo.getProcessors().get(0);
                    String virtByVendorId = virtualizationFromVendorId(processor.getVendorId());
                    if (virtByVendorId != null)
                        return virtByVendorId;

                    if (processor.hasFlag("hypervisor"))
                        return "unknown (hypervisor CPU flag present)";
                }
            }
            catch (Exception e)
            {
                // ignore
            }
        }
        catch (Exception e)
        {
            if (logger.isTraceEnabled())
                logger.warn("Unable to detect virtualization/hypervisor", e);
            else
                logger.warn("Unable to detect virtualization/hypervisor");
        }

        return null;
    }

    private static String virtualizationFromVendorId(String vendorId)
    {
        switch (vendorId)
        {
            case "AuthenticAMD":
            case "GenuineIntel":
            case "CentaurHauls":
            case "AMDisbetter!":
            case "CyrixInstead":
            case "TransmetaCPU":
            case "GenuineTMx86":
            case "Geode by NSC":
            case "NexGenDriven":
            case "RiseRiseRise":
            case "SiS SiS SiS ":
            case "UMC UMC UMC ":
            case "VIA VIA VIA ":
            case "Vortex86 SoC":
                // these are known vendor-IDs of "bare metal" CPUs
                break;
            case "bhyve bhyve ":
                return "bhyve";
            case "KVMKVMKVMKVM":
                return "KVM";
            case "Microsoft Hv":
                return "MS Hyper-V";
            case " lrpepyh vr":
                return "Parallels";
            case "VMwareVMware":
                return "VMWare";
            case "XenVMMXenVMM":
                return "Xen HVM";
            default:
                return vendorId + " (unknown vendor in CPUID)";
        }
        return null;
    }

    private static String cpuidLine(String[] cpuidLines, String leaf, String ecx)
    {
        return Arrays.stream(cpuidLines)
                     .map(String::trim)
                     .filter(s -> s.startsWith(leaf + ' ' + ecx + ':'))
                     .findFirst()
                     .orElse(null);
    }

    static int[] cpuIdParse(String cpuidLine)
    {
        StringTokenizer vendorIdLine = new StringTokenizer(cpuidLine);
        vendorIdLine.nextToken(); // EAX input
        vendorIdLine.nextToken(); // ECX input
        int eax = (int) Long.parseLong(vendorIdLine.nextToken().substring(6), 16);
        int ebx = (int) Long.parseLong(vendorIdLine.nextToken().substring(6), 16);
        int ecx = (int) Long.parseLong(vendorIdLine.nextToken().substring(6), 16);
        int edx = (int) Long.parseLong(vendorIdLine.nextToken().substring(6), 16);
        return new int[]{ eax, ebx, ecx, edx };
    }

    static String cpuIdVendorId(String cpuidLine) throws IOException
    {
        int[] registers = cpuIdParse(cpuidLine);
        byte[] bytes = new byte[12];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        // Order for VendorID is EBX, EDX, ECX
        bb.putInt(registers[1]);
        bb.putInt(registers[3]);
        bb.putInt(registers[2]);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Returns the size of the specified partition.
     * <p>This method handles large file system by returning {@code Long.MAX_VALUE} if the  size overflow.
     * See <a href='https://bugs.openjdk.java.net/browse/JDK-8179320'>JDK-8179320</a> for more information.</p>
     *
     * @param file the partition
     * @return the size, in bytes, of the partition or {@code 0L} if the abstract pathname does not name a partition
     */
    public static long getTotalSpace(File file)
    {
        return handleLargeFileSystem(file.getTotalSpace());
    }

    /**
     * Returns the number of unallocated bytes on the specified partition.
     * <p>This method handles large file system by returning {@code Long.MAX_VALUE} if the  number of unallocated bytes
     * overflow. See <a href='https://bugs.openjdk.java.net/browse/JDK-8179320'>JDK-8179320</a> for more information</p>
     *
     * @param file the partition
     * @return the number of unallocated bytes on the partition or {@code 0L}
     * if the abstract pathname does not name a partition.
     */
    public static long getFreeSpace(File file)
    {
        return handleLargeFileSystem(file.getFreeSpace());
    }

    /**
     * Returns the number of available bytes on the specified partition.
     * <p>This method handles large file system by returning {@code Long.MAX_VALUE} if the  number of available bytes
     * overflow. See <a href='https://bugs.openjdk.java.net/browse/JDK-8179320'>JDK-8179320</a> for more information</p>
     *
     * @param file the partition
     * @return the number of available bytes on the partition or {@code 0L}
     * if the abstract pathname does not name a partition.
     */
    public static long getUsableSpace(File file)
    {
        return handleLargeFileSystem(file.getUsableSpace());
    }

    /**
     * Returns the {@link FileStore} representing the file store where a file
     * is located. This {@link FileStore} handles large file system by returning {@code Long.MAX_VALUE}
     * from {@code FileStore#getTotalSpace()}, {@code FileStore#getUnallocatedSpace()} and {@code FileStore#getUsableSpace()}
     * it the value is bigger than {@code Long.MAX_VALUE}. See <a href='https://bugs.openjdk.java.net/browse/JDK-8162520'>JDK-8162520</a>
     * for more information.
     *
     * @param path the path to the file
     * @return the file store where the file is stored
     */
    public static FileStore getFileStore(Path path) throws IOException
    {
        return new SafeFileStore(Files.getFileStore(path));
    }

    /**
     * Handle large file system by returning {@code Long.MAX_VALUE} when the size overflows.
     * @param size returned by the Java's FileStore methods
     * @return the size or {@code Long.MAX_VALUE} if the size was bigger than {@code Long.MAX_VALUE}
     */
    private static long handleLargeFileSystem(long size)
    {
        return size < 0 ? Long.MAX_VALUE : size;
    }

    /**
     * Private constructor as the class contains only static methods.
     */
    private FileUtils()
    {
    }

    /**
     * FileStore decorator used to safely handle large file system.
     *
     * <p>Java's FileStore methods (getTotalSpace/getUnallocatedSpace/getUsableSpace) are limited to reporting bytes as
     * signed long (2^63-1), if the filesystem is any bigger, then the size overflows. {@code SafeFileStore} will
     * return {@code Long.MAX_VALUE} if the size overflow.</p>
     *
     * See {@code https://bugs.openjdk.java.net/browse/JDK-8162520}.
     */
    private static final class SafeFileStore extends FileStore
    {
        /**
         * The decorated {@code FileStore}
         */
        private final FileStore fileStore;

        public SafeFileStore(FileStore fileStore)
        {
            this.fileStore = fileStore;
        }

        @Override
        public String name()
        {
            return fileStore.name();
        }

        @Override
        public String type()
        {
            return fileStore.type();
        }

        @Override
        public boolean isReadOnly()
        {
            return fileStore.isReadOnly();
        }

        @Override
        public long getTotalSpace() throws IOException
        {
            return handleLargeFileSystem(fileStore.getTotalSpace());
        }

        @Override
        public long getUsableSpace() throws IOException
        {
            return handleLargeFileSystem(fileStore.getUsableSpace());
        }

        @Override
        public long getUnallocatedSpace() throws IOException
        {
            return handleLargeFileSystem(fileStore.getUnallocatedSpace());
        }

        @Override
        public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type)
        {
            return fileStore.supportsFileAttributeView(type);
        }

        @Override
        public boolean supportsFileAttributeView(String name)
        {
            return fileStore.supportsFileAttributeView(name);
        }

        @Override
        public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type)
        {
            return fileStore.getFileStoreAttributeView(type);
        }

        @Override
        public Object getAttribute(String attribute) throws IOException
        {
            return fileStore.getAttribute(attribute);
        }
    }
}
