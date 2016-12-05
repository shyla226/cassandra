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
package org.apache.cassandra.net;

public class ProtocolVersion implements Comparable<ProtocolVersion>
{
    public final boolean isDSE;

    /**
     * The "relative" protocol version, where relative refers to the fact that OSS and DSE have overlapping
     * numbering for that number, but we distinguish between OSS and DSE early in the connection using the
     * {@link #rawHeader}.
     */
    public final int version;

    /**
     * The protocol connection header (2nd 4 bytes value we send on any connection, just after the PROTOCOL_MAGIC)
     * corresponding to this version. This is "raw" in that it only contains the version (we add the options,
     * compression and streaming, in {@link #makeProtocolHeader(boolean, boolean)} below).
     */
    private final int rawHeader;

    /**
     * The version number to use when handshaking versions in the protocol.
     * <p>
     * To avoid clash with OSS and make sure DSE version are always greater, we simply shift dse version from 8 bits
     * to the left. As both OSS version and DSE version are limited to 8 bits so far, that's both enough and safe.
     */
    public final int handshakeVersion;

    private ProtocolVersion(boolean isDSE, int version, int rawHeader, int handshakeVersion)
    {
        this.isDSE = isDSE;
        this.version = version;
        this.rawHeader = rawHeader;
        this.handshakeVersion = handshakeVersion;
    }

    static ProtocolVersion oss(int version)
    {
        assert version < 256;
        return new ProtocolVersion(false, version, version << 8, version);
    }

    static ProtocolVersion dse(int version)
    {
        assert version < 256;
        return new ProtocolVersion(true, version, (version << 24) | (0xFF << 8), version << 8);
    }

    /**
     * Creates the 4 byte header that is sent as the header of the protocol connection.
     * <p>
     * On the OSS side, this is defined as:
     * - 2 bits: unused.  used to be "serializer type," which was always Binary
     * - 1 bit: compression
     * - 1 bit: streaming mode
     * - 4 bits: unused
     * - 8 bits: version
     * - 16 bits: unused
     * <p>
     * To handle dse versioning, we rely on the unused bits. Namely, for dse version, we set the 'OSS' version to
     * the max possible (0xFF, see {@link #dse} above), and write our true DSE version in the last 8 bits of the
     * header (we don't use the  full 16 bits so that if for some reason a few bits are needed OSS-side, we can twist
     * people arms to us the bits we don't (we're kind of in trouble otherwise)). The fact that we use 0xFF for the OSS
     * version bits guarantees us that any OSS node always considers us in the future and settles on its own version
     * instead of ours.
     *
     * @param compressionEnabled whether compression is enabled on the connection.
     * @param isStream whether the connection is used for streaming.
     * @return the protocol connection header corresponding to this version.
     */
    int makeProtocolHeader(boolean compressionEnabled, boolean isStream)
    {
        int header = rawHeader;
        if (compressionEnabled)
            header |= 4;
        if (isStream)
            header |= 8;
        return header;
    }

    static ProtocolVersion fromProtocolHeader(int header)
    {
        int version = MessagingService.getBits(header, 15, 8);
        return version == 0xFF ? dse(MessagingService.getBits(header, 31, 8)) : oss(version);
    }

    public static ProtocolVersion fromHandshakeVersion(int handshakeVersion)
    {
        return handshakeVersion < 256 ? oss(handshakeVersion) : dse(handshakeVersion >>> 8);
    }

    public int compareTo(ProtocolVersion protocolVersion)
    {
        return Integer.compare(this.handshakeVersion, protocolVersion.handshakeVersion);
    }

    @Override
    public String toString()
    {
        return isDSE ? String.format("dse(%d)", version) : String.valueOf(version);
    }
}
