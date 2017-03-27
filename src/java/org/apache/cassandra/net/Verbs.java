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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.carrotsearch.hppc.IntObjectOpenHashMap;

import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.WriteVerbs;
import org.apache.cassandra.gms.GossipVerbs;
import org.apache.cassandra.hints.HintsVerbs;
import org.apache.cassandra.repair.messages.RepairVerbs;
import org.apache.cassandra.schema.SchemaVerbs;
import org.apache.cassandra.service.OperationsVerbs;
import org.apache.cassandra.service.paxos.LWTVerbs;

/**
 * Records the {@link VerbGroup} known and used by the messaging service.
 */
public abstract class Verbs
{
    // Not a real class, just a 'namespace' to group all verb groups
    private Verbs() {}

    /**
     * An enum listing the groups we have. The main reason this exists is that it's a bit cleaner and more flexible to
     * refer to a group "by name" using an enum constant rather than, say, a string.
     */
    public enum Group
    {
        READS     (0),
        WRITES    (1),
        LWT       (2),
        HINTS     (3),
        OPERATIONS(4),
        GOSSIP    (5),
        REPAIR    (6),
        SCHEMA    (7);

        private final int code;

        Group(int code)
        {
            this.code = code;
        }

        public int serializationCode()
        {
            return code;
        }
    }

    private final static IntObjectOpenHashMap<VerbGroup> groupsByCode = new IntObjectOpenHashMap<>();
    private static final List<VerbGroup<?>> allGroups = new ArrayList<>();

    public static final ReadVerbs READS            = register(new ReadVerbs(Group.READS));
    public static final WriteVerbs WRITES          = register(new WriteVerbs(Group.WRITES));
    public static final LWTVerbs LWT               = register(new LWTVerbs(Group.LWT));
    public static final HintsVerbs HINTS           = register(new HintsVerbs(Group.HINTS));
    public static final OperationsVerbs OPERATIONS = register(new OperationsVerbs(Group.OPERATIONS));
    public static final GossipVerbs GOSSIP         = register(new GossipVerbs(Group.GOSSIP));
    public static final RepairVerbs REPAIR         = register(new RepairVerbs(Group.REPAIR));
    public static final SchemaVerbs SCHEMA         = register(new SchemaVerbs(Group.SCHEMA));

    private static <G extends VerbGroup> G register(G group)
    {
        allGroups.add(group);
        VerbGroup previous = groupsByCode.put(group.id().serializationCode(), group);
        assert previous == null : "Duplicate code for group " + group + ", already assigned to " + previous;
        return group;
    }

    static Iterable<VerbGroup<?>> allGroups()
    {
        return Collections.unmodifiableList(allGroups);
    }

    static VerbGroup fromSerializationCode(int code)
    {
        VerbGroup group = groupsByCode.get(code);
        if (group == null)
            throw new IllegalArgumentException(String.format("Invalid message group code %d", code));
        return group;
    }
}
