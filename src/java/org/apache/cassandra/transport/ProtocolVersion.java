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

/**
 * Copyright DataStax, Inc.
 *
 * Modified by DataStax, Inc.
 */


package org.apache.cassandra.transport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.ArrayUtils;

/**
 * The native (CQL binary) protocol version.
 *
 * DSE versions have the 7th most significant bit of the version number set to 1,
 * so DSE version 1 starts at 0x41 or d65.
 *
 * Some versions may be in beta, which means that the client must
 * specify the beta flag in the frame for the version to be considered valid.
 * Beta versions must have the word "beta" in their description, this is mandated
 * by the specs.
 *
 */
public enum ProtocolVersion implements Comparable<ProtocolVersion>
{
    // The order is important as it defines the chronological history of versions, which is used
    // to determine if a feature is supported or some serdes formats
    V1(1, "v1", false), // no longer supported
    V2(2, "v2", false), // no longer supported
    V3(3, "v3", false),
    V4(4, "v4", false),
    V5(5, "v5-beta", true),
    DSE_V1(65, "dse-v1", false);

    /** The version number, for OS version this is a number from 1 to 64, for DSE versions from 65 to 127 */
    private final int num;

    /** A description of the version, beta versions should have the word "-beta" */
    private final String descr;

    /** Set this to true for beta versions */
    private final boolean beta;

    ProtocolVersion(int num, String descr, boolean beta)
    {
        this.num = num;
        this.descr = descr;
        this.beta = beta;
    }

    /** Some utility constants for decoding DSE versions */
    private static final byte DSE_VERSION_BIT = 0x40; // 0100 0000
    private static final byte DSE_VERSION_MASK = 0x4f; // 0011 1111

    /** The supported OS versions */
    final static ProtocolVersion[] OS_VERSIONS = new ProtocolVersion[] { V3, V4, V5 };
    final static ProtocolVersion MIN_OS_VERSION = OS_VERSIONS[0];
    final static ProtocolVersion MAX_OS_VERSION = OS_VERSIONS[OS_VERSIONS.length - 1];

    /** The supported DSE versions */
    final static ProtocolVersion[] DSE_VERSIONS = new ProtocolVersion[] { DSE_V1 };
    final static ProtocolVersion MIN_DSE_VERSION = DSE_VERSIONS[0];
    final static ProtocolVersion MAX_DSE_VERSION = DSE_VERSIONS[DSE_VERSIONS.length - 1];

    /** All supported versions */
    public final static EnumSet<ProtocolVersion> SUPPORTED = EnumSet.copyOf(Arrays.asList((ProtocolVersion[])
                                                                                          ArrayUtils.addAll(OS_VERSIONS, DSE_VERSIONS)));

    /** Old unsupported versions, this is OK as long as we never add newer unsupported versions */
    public final static EnumSet<ProtocolVersion> UNSUPPORTED = EnumSet.complementOf(SUPPORTED);

    /** The preferred versions */
    public final static ProtocolVersion CURRENT = V4;
    public final static Optional<ProtocolVersion> BETA = Optional.empty();

    public static List<String> supportedVersions()
    {
        List<String> ret = new ArrayList<>(SUPPORTED.size());
        for (ProtocolVersion version : SUPPORTED)
            ret.add(version.toString());
        return ret;
    }

    public static ProtocolVersion decode(int versionNum)
    {
        ProtocolVersion ret = null;
        boolean isDse = isDse(versionNum);
        if (isDse)
        { // DSE version
            int dseVersionNum = versionNum & DSE_VERSION_MASK;
            if (dseVersionNum >= MIN_DSE_VERSION.num && dseVersionNum <= MAX_DSE_VERSION.num)
                ret = DSE_VERSIONS[dseVersionNum - MIN_DSE_VERSION.num];
        }
        else
        { // OS version
            if (versionNum >= MIN_OS_VERSION.num && versionNum <= MAX_OS_VERSION.num)
                ret = OS_VERSIONS[versionNum - MIN_OS_VERSION.num];
        }

        if (ret == null)
        {
            // if this is not a supported version check the old versions
            for (ProtocolVersion version : UNSUPPORTED)
            {
                // if it is an old version that is no longer supported this ensures that we reply
                // with that same version
                if (version.num == versionNum)
                    throw new ProtocolException(ProtocolVersion.invalidVersionMessage(versionNum), version);
            }

            // If the version is invalid reply with the highest version of the same kind that we support
            throw new ProtocolException(invalidVersionMessage(versionNum),
                                        isDse ? MAX_DSE_VERSION : MAX_OS_VERSION);
        }

        return ret;
    }

    public boolean isDse()
    {
        return isDse(num);
    }

    private static boolean isDse(int num)
    {
        return (num & DSE_VERSION_BIT) == DSE_VERSION_BIT;
    }

    public boolean isBeta()
    {
        return beta;
    }

    public static String invalidVersionMessage(int version)
    {
        return String.format("Invalid or unsupported protocol version (%d); supported versions are (%s)",
                             version, String.join(", ", ProtocolVersion.supportedVersions()));
    }

    public int asInt()
    {
        return num;
    }

    @Override
    public String toString()
    {
        // This format is mandated by the protocl specs for the SUPPORTED message, see OptionsMessage execute().
        return String.format("%d/%s", num, descr);
    }

    public final boolean isGreaterThan(ProtocolVersion other)
    {
        return ordinal() > other.ordinal();
    }

    public final boolean isGreaterOrEqualTo(ProtocolVersion other)
    {
        return ordinal() >= other.ordinal();
    }

    public final boolean isSmallerThan(ProtocolVersion other)
    {
        return ordinal() < other.ordinal();
    }

    public final boolean isSmallerOrEqualTo(ProtocolVersion other)
    {
        return ordinal() <= other.ordinal();
    }
}
