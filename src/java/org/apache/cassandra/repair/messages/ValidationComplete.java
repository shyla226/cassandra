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
package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.messages.RepairVerbs.RepairVersion;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.versioning.Versioned;

/**
 * ValidationComplete message is sent when validation compaction completed successfully.
 *
 * @since 2.0
 */
public class ValidationComplete extends RepairMessage<ValidationComplete>
{
    public static Versioned<RepairVersion, MessageSerializer<ValidationComplete>> serializers = RepairVersion.versioned(v -> new MessageSerializer<ValidationComplete>(v)
    {
        public void serialize(ValidationComplete message, DataOutputPlus out) throws IOException
        {
            RepairJobDesc.serializers.get(version).serialize(message.desc, out);
            out.writeBoolean(message.success());
            if (message.trees != null)
                MerkleTrees.serializers.get(version).serialize(message.trees, out);
        }

        public ValidationComplete deserialize(DataInputPlus in) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializers.get(version).deserialize(in);
            boolean success = in.readBoolean();

            if (success)
            {
                MerkleTrees trees = MerkleTrees.serializers.get(version).deserialize(in);
                return new ValidationComplete(desc, trees);
            }

            return new ValidationComplete(desc);
        }

        public long serializedSize(ValidationComplete message)
        {
            long size = RepairJobDesc.serializers.get(version).serializedSize(message.desc);
            size += TypeSizes.sizeof(message.success());
            if (message.trees != null)
                size += MerkleTrees.serializers.get(version).serializedSize(message.trees);
            return size;
        }
    });

    /** Merkle hash tree response. Null if validation failed. */
    public final MerkleTrees trees;

    public ValidationComplete(RepairJobDesc desc)
    {
        super(desc);
        trees = null;
    }

    public MessageSerializer<ValidationComplete> serializer(RepairVersion version)
    {
        return serializers.get(version);
    }

    public Verb<ValidationComplete, ?> verb()
    {
        return Verbs.REPAIR.VALIDATION_COMPLETE;
    }

    public ValidationComplete(RepairJobDesc desc, MerkleTrees trees)
    {
        super(desc);
        assert trees != null;
        this.trees = trees;
    }

    public boolean success()
    {
        return trees != null;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ValidationComplete))
            return false;

        ValidationComplete other = (ValidationComplete)o;
        return desc.equals(other.desc);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(desc);
    }
}
