/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.auth;

import java.util.function.Function;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.net.Verb.OneWay;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public class AuthVerbs extends VerbGroup<AuthVerbs.AuthVersion>
{
    public final OneWay<RoleInvalidation> INVALIDATE;

    public AuthVerbs(Verbs.Group id)
    {
        super(id, true, AuthVersion.class);

        RegistrationHelper helper = helper().stage(Stage.AUTHZ);

        INVALIDATE = helper.oneWay("INVALIDATE_ROLE", RoleInvalidation.class)
                           .handler((from, invalidation) -> DatabaseDescriptor.getAuthManager()
                                                                              .handleRoleInvalidation(invalidation));
    }

    public enum AuthVersion implements Version<AuthVersion>
    {
        DSE_60(EncodingVersion.OSS_30);

        public final EncodingVersion encodingVersion;

        AuthVersion(EncodingVersion encodingVersion)
        {
            this.encodingVersion = encodingVersion;
        }

        public static <T> Versioned<AuthVersion, T> versioned(Function<AuthVersion, ? extends T> function)
        {
            return new Versioned<>(AuthVersion.class, function);
        }
    }
}
