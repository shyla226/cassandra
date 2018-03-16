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
package org.apache.cassandra.config;

import java.net.URL;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

public interface ConfigurationLoader
{
    /**
     * Creates a ConfigurationLoader instance based on the value of "cassandra.config.loader" system property.
     * When no value is present in "cassandra.config.loader" property, it creates a {@link YamlConfigurationLoader} instance.
     * @return ConfigurationLoader instance
     * @throws ConfigurationException if the provided class cannot be constructed.
     */
    public static ConfigurationLoader create() throws ConfigurationException
    {
        String loaderClass = System.getProperty("cassandra.config.loader");
        return loaderClass == null ? new YamlConfigurationLoader()
                                   : FBUtilities.<ConfigurationLoader>construct(loaderClass, "configuration loading");
    }

    /**
     * Loads a {@link Config} object to use to configure a node.
     *
     * @return the {@link Config} to use.
     * @throws ConfigurationException if the configuration cannot be properly loaded.
     */
    Config loadConfig() throws ConfigurationException;

    /**
     * Loads a {@link Config} object to use to configure a node.
     *
     * @param url configuration location.
     * @return the {@link Config} to use.
     * @throws ConfigurationException if the configuration cannot be properly loaded.
     */
    Config loadConfig(URL url) throws ConfigurationException;
}
