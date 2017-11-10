/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.locator;

import java.io.File;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.google.common.io.Files;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.*;

public class SimpleSeedProviderTest
{
    String seedPattern = "seed_provider:\n" +
                         "    - class_name: org.apache.cassandra.locator.SimpleSeedProvider\n" +
                         "      parameters:\n" +
                         "          - seeds: %s\n" +
                         "commitlog_sync: batch\n" +
                         "commitlog_sync_batch_window_in_ms: 1000\n" +
                         "partitioner: org.apache.cassandra.dht.Murmur3Partitioner\n" +
                         "endpoint_snitch: org.apache.cassandra.locator.SimpleSnitch\n" +
                         "commitlog_directory: build/test/cassandra/commitlog\n" +
                         "hints_directory: build/test/cassandra/hints\n" +
                         "saved_caches_directory: build/test/cassandra/saved_caches\n" +
                         "data_file_directories:\n" +
                         "    - build/test/cassandra/data\n";

    @Test
    public void testSeedConfigChange() throws Throwable
    {
        File f = File.createTempFile("SimpleSeedProviderTest-testIfModifiedSince", ".yaml");
        f.deleteOnExit();

        Files.write(String.format(seedPattern, "\"127.0.0.1\"").getBytes(StandardCharsets.UTF_8), f);
        long lm = f.lastModified();
        assertTrue(f.setLastModified(lm - 3000L));

        System.setProperty("cassandra.config", f.toURI().toURL().toString());

        SimpleSeedProvider seedProvider = new SimpleSeedProvider(null);
        List<InetAddress> seeds = seedProvider.getSeeds();
        assertNotNull(seeds);
        assertEquals(Collections.singletonList(InetAddress.getByName("127.0.0.1")), seeds);

        // memoized, file did not change
        assertSame(seeds, seedProvider.getSeeds());

        assertTrue(f.setLastModified(lm - 2000L));

        // memoized, file changed, but config value did not change
        assertSame(seeds, seedProvider.getSeeds());

        // update seeds

        Files.write(String.format(seedPattern, "127.0.0.1, 127.0.0.2").getBytes(StandardCharsets.UTF_8), f);
        assertTrue(f.setLastModified(lm - 1000L));

        List<InetAddress> newSeeds = seedProvider.getSeeds();
        assertNotNull(newSeeds);
        assertNotSame(seeds, newSeeds);
        assertEquals(Arrays.asList(InetAddress.getByName("127.0.0.1"), InetAddress.getByName("127.0.0.2")), newSeeds);

        // update to invalid value - should memoize previous seeds

        Files.write(String.format(seedPattern, "\"127.0.0.1, ").getBytes(StandardCharsets.UTF_8), f);
        assertTrue(f.setLastModified(lm - 0L));

        assertSame(newSeeds, seedProvider.getSeeds());
    }
}
