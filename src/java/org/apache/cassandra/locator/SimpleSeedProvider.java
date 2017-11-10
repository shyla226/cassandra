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
package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSeedProvider implements SeedProvider
{
    private static final Logger logger = LoggerFactory.getLogger(SimpleSeedProvider.class);

    private volatile String memoizedSeedsValue;
    private volatile List<InetAddress> memoizedSeeds;

    public SimpleSeedProvider(Map<String, String> args) {}

    public List<InetAddress> getSeeds()
    {
        Config conf;
        try
        {
            conf = DatabaseDescriptor.loadConfig();
        }
        catch (Exception e)
        {
            if (memoizedSeeds != null)
            {
                // Should not fail when configuration file changed but cannot be parsed as long
                // as we have a valid seed list. Gossip stuff depends on this functionality and
                // should not fail to process requests/events.

                logger.error("Cannot load seeds from configuration - reusing memoized seeds: " + memoizedSeeds, e);
                return memoizedSeeds;
            }
            throw new AssertionError(e);
        }

        String newSeeds = conf.seed_provider.parameters.get("seeds");
        if (newSeeds.equals(memoizedSeedsValue))
            // Skip resolving host names, if memoized config value has not changed
            return memoizedSeeds;

        String[] hosts = newSeeds.split(",", -1);
        List<InetAddress> seeds = new ArrayList<>(hosts.length);
        for (String host : hosts)
        {
            try
            {
                seeds.add(InetAddress.getByName(host.trim()));
            }
            catch (UnknownHostException ex)
            {
                // treat wrong seed nodes as an error in the log file
                logger.error("Seed provider couldn't lookup host {}", host);
            }
        }

        List<InetAddress> result = memoizedSeeds = Collections.unmodifiableList(seeds);
        memoizedSeedsValue = newSeeds;

        return result;
    }
}
