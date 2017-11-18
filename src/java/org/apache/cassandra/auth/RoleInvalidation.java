/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.auth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.cassandra.auth.AuthVerbs.AuthVersion;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Versioned;

/**
 * Internode messaging payload to invalidate cached role data.
 */
public class RoleInvalidation
{
    public static Versioned<AuthVersion, Serializer<RoleInvalidation>> serializers =
        AuthVersion.<Serializer<RoleInvalidation>>versioned(v -> new Serializer<RoleInvalidation>()
        // note:     ^^ keep this - eclipse-warnings step requires it
    {
        public void serialize(RoleInvalidation schema, DataOutputPlus out) throws IOException
        {
            out.writeInt(schema.roles.size());
            for (RoleResource role : schema.roles)
                RoleResource.rawSerializers.get(v.encodingVersion).serialize(role, out);
        }

        public RoleInvalidation deserialize(DataInputPlus in) throws IOException
        {
            int count = in.readInt();
            Collection<RoleResource> roles = new ArrayList<>(count);

            for (int i = 0; i < count; i++)
                roles.add(RoleResource.rawSerializers.get(v.encodingVersion).deserialize(in));

            return new RoleInvalidation(roles);
        }

        public long serializedSize(RoleInvalidation schema)
        {
            long size = TypeSizes.sizeof(schema.roles.size());
            for (RoleResource role : schema.roles)
                size += RoleResource.rawSerializers.get(v.encodingVersion).serializedSize(role);
            return size;
        }
    });

    public final Collection<RoleResource> roles;

    RoleInvalidation(Collection<RoleResource> roles)
    {
        this.roles = roles;
    }
}
