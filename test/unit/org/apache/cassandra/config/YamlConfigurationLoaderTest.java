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

import static org.junit.Assert.*;

public class YamlConfigurationLoaderTest
{
    @Test
    public void testIfModifiedSince() throws Throwable
    {
        File f = File.createTempFile("YamlConfigurationLoaderTest-testIfModifiedSince", ".yaml");
        f.deleteOnExit();

        Files.write("".getBytes(StandardCharsets.UTF_8), f);

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
