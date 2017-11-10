/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.config;

import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import com.google.common.io.Files;
import org.junit.Test;

import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class YamlConfigurationLoaderTest
{
    private static final String config = "seed_provider:\n" +
                                         "    - class_name: org.apache.cassandra.locator.SimpleSeedProvider\n" +
                                         "      parameters:\n" +
                                         "          - seeds: 127.0.0.1\n" +
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
    public void testIfModifiedSince() throws Throwable
    {
        File f = File.createTempFile("YamlConfigurationLoaderTest-testIfModifiedSince", ".yaml");
        f.deleteOnExit();

        Files.write(config.getBytes(StandardCharsets.UTF_8), f);

        URL url = f.toURI().toURL();

        System.setProperty("cassandra.config", url.toString());

        YamlConfigurationLoader configurationLoader = new YamlConfigurationLoader();

        Pair<Config, Long> pair = configurationLoader.loadConfig(url, 0L);
        assertNotNull(pair);
        assertNotNull(pair.left);
        assertEquals(f.lastModified(), pair.right.longValue());

        assertNull(configurationLoader.loadConfig(url, pair.right));

        assertTrue(f.setLastModified(f.lastModified() + 10000L));

        assertNull(configurationLoader.loadConfig(url, f.lastModified()));

        pair = configurationLoader.loadConfig(url, f.lastModified() - 1);
        assertNotNull(pair);
        assertNotNull(pair.left);
        assertEquals(f.lastModified(), pair.right.longValue());
    }
}
