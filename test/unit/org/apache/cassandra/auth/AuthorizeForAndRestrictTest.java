/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.auth;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.exceptions.UnauthorizedException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.transport.ProtocolVersion;

public class AuthorizeForAndRestrictTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Throwable
    {
        requireAuthentication();
        DatabaseDescriptor.setPermissionsValidity(9999);
        DatabaseDescriptor.setPermissionsUpdateInterval(9999);
        requireNetwork(false);
    }

    @Test
    public void testAuthorizeFor() throws Throwable
    {
        sessionWithUser("cassandra", "cassandra", ProtocolVersion.CURRENT, (session) ->
        {
            session.execute("CREATE USER authfor1 WITH PASSWORD 'pass1'");
            session.execute("CREATE USER authfor2 WITH PASSWORD 'pass2'");
            session.execute("CREATE ROLE authfor_role1");
            session.execute("GRANT authfor_role1 TO authfor1");

            session.execute("CREATE KEYSPACE authfor_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
            session.execute("CREATE TABLE authfor_test.t1 (id int PRIMARY KEY, val text)");
            session.execute("CREATE TABLE authfor_test.t2 (id int PRIMARY KEY, val text)");

            session.execute("GRANT AUTHORIZE FOR SELECT ON KEYSPACE authfor_test TO authfor1");
            session.execute("GRANT MODIFY ON TABLE authfor_test.t1 TO authfor1");
            session.execute("GRANT AUTHORIZE FOR MODIFY ON TABLE authfor_test.t2 TO authfor2");
            session.execute("GRANT MODIFY ON TABLE authfor_test.t2 TO authfor2");
            return null;
        });

        sessionWithUser("authfor1", "pass1", ProtocolVersion.CURRENT, (session) ->
        {
            // SELECT on table t1 must not work, authfor1 only has the privilege to grant the SELECT permission
            assertInvalidThrowMessage(() -> session.execute("SELECT * FROM authfor_test.t1"),
                                      "User authfor1 has no SELECT permission on <table authfor_test.t1> or any of its parents",
                                      UnauthorizedException.class,
                                      "SELECT * FROM authfor_test.t1");

            // authfor1 must not be able to grant the SELECT permission to himself
            assertInvalidThrowMessage(() -> session.execute("GRANT SELECT ON TABLE authfor_test.t1 TO authfor1"),
                                      "User authfor1 has grant privilege for SELECT permission(s) on <table authfor_test.t1> but must not grant/revoke for him/herself",
                                      UnauthorizedException.class,
                                      "GRANT SELECT ON TABLE authfor_test.t1 TO authfor1");

            // authfor1 must not be able to grant the SELECT permission to himself - even via a role
            assertInvalidThrowMessage(() -> session.execute("GRANT SELECT ON TABLE authfor_test.t1 TO authfor_role1"),
                                      "User authfor1 has grant privilege for SELECT permission(s) on <table authfor_test.t1> but must not grant/revoke for him/herself",
                                      UnauthorizedException.class,
                                      "GRANT SELECT ON TABLE authfor_test.t1 TO authfor_role1");

            // authfor1 has MODIFIY permission on t1 but not the privilege to grant the MODIFY permission
            assertInvalidThrowMessage(() -> session.execute("GRANT MODIFY ON TABLE authfor_test.t1 to authfor2"),
                                      "User authfor1 has no AUTHORIZE permission nor AUTHORIZE FOR MODIFY permission on <table authfor_test.t1> or any of its parents",
                                      UnauthorizedException.class,
                                      "GRANT SELECT ON TABLE authfor_test.t1 TO authfor2");

            assertInvalidThrowMessage(() -> session.execute("GRANT AUTHORIZE FOR SELECT ON KEYSPACE authfor_test TO authfor2"),
                                      "User authfor1 must not grant AUTHORIZE FOR AUTHORIZE permission on <keyspace authfor_test>",
                                      UnauthorizedException.class,
                                      "GRANT AUTHORIZE FOR SELECT ON KEYSPACE authfor_test TO authfor2");

            // authfor1 can grant the SELECT privilege - all that must work (although the GRANT on the keyspace is technically sufficient)
            session.execute("GRANT SELECT ON KEYSPACE authfor_test TO authfor2");
            session.execute("GRANT SELECT ON TABLE authfor_test.t1 TO authfor2");
            session.execute("GRANT SELECT ON TABLE authfor_test.t2 TO authfor2");

            // authfor1 has no MODIFIY permission on t2
            assertInvalidThrowMessage(() -> session.execute("INSERT INTO authfor_test.t2 (id, val) VALUES (1, 'foo')"),
                                      "User authfor1 has no MODIFY permission on <table authfor_test.t2> or any of its parents",
                                      UnauthorizedException.class,
                                      "INSERT INTO authfor_test.t2 (id, val) VALUES (1, 'foo')");

            return null;
        });

        sessionWithUser("authfor2", "pass2", ProtocolVersion.CURRENT, (session) ->
        {
            // authfor2 has SELECT permission on t1
            session.execute("SELECT * FROM authfor_test.t1");

            // authfor2 has no MODIFY permission on t1
            assertInvalidThrowMessage(() -> session.execute("INSERT INTO authfor_test.t1 (id, val) VALUES (1, 'foo')"),
                                      "User authfor2 has no MODIFY permission on <table authfor_test.t1> or any of its parents",
                                      UnauthorizedException.class,
                                      "INSERT INTO authfor_test.t1 (id, val) VALUES (1, 'foo')");

            // authfor2 has the privilege to grant the MODIFY permission
            session.execute("GRANT MODIFY ON TABLE authfor_test.t2 TO authfor1");

            // authfor2 has MODIFY permission on t2
            session.execute("INSERT INTO authfor_test.t2 (id, val) VALUES (1, 'foo')");

            // authfor2 has SElECT permission on t2
            assertRowsNet(session.execute("SELECT id, val FROM authfor_test.t2"),
                          new Object[] {1, "foo"});

            return null;
        });

        sessionWithUser("authfor1", "pass1", ProtocolVersion.CURRENT, (session) ->
        {
            // attn: the permission is still cached !

            assertInvalidThrowMessage(() -> session.execute("INSERT INTO authfor_test.t2 (id, val) VALUES (2, 'bar')"),
                                      "User authfor1 has no MODIFY permission on <table authfor_test.t2> or any of its parents",
                                      UnauthorizedException.class,
                                      "INSERT INTO authfor_test.t2 (id, val) VALUES (2, 'bar')");

            // need to invalidate the permissions cache

            invalidateAuthCaches();

            // now the CQL works

            session.execute("INSERT INTO authfor_test.t2 (id, val) VALUES (2, 'bar')");

            return null;
        });

        sessionWithUser("authfor2", "pass2", ProtocolVersion.CURRENT, (session) ->
        {

            assertRowsNet(session.execute("SELECT id, val FROM authfor_test.t2"),
                          new Object[] {1, "foo"},
                          new Object[] {2, "bar"});

            return null;
        });

        sessionWithUser("cassandra", "cassandra", ProtocolVersion.CURRENT, (session) ->
        {
            assertRowsNet(session.execute("LIST PERMISSIONS OF authfor1"),
                          new Object[]{ "authfor1", "authfor1", "<keyspace authfor_test>", "SELECT", false, false, true },
                          new Object[]{ "authfor1", "authfor1", "<table authfor_test.t1>", "MODIFY", true, false, false },
                          new Object[]{ "authfor1", "authfor1", "<table authfor_test.t2>", "MODIFY", true, false, false });
            assertRowsNet(session.execute("LIST PERMISSIONS OF authfor2"),
                          new Object[]{ "authfor2", "authfor2", "<keyspace authfor_test>", "SELECT", true, false, false },
                          new Object[]{ "authfor2", "authfor2", "<table authfor_test.t1>", "SELECT", true, false, false },
                          new Object[]{ "authfor2", "authfor2", "<table authfor_test.t2>", "SELECT", true, false, false },
                          new Object[]{ "authfor2", "authfor2", "<table authfor_test.t2>", "MODIFY", true, false, true }
                          );

            // all permissions and grant options must have been removed
            session.execute("DROP ROLE authfor1");
            session.execute("CREATE USER authfor1 WITH PASSWORD 'pass1'");

            assertRowsNet(session.execute("LIST PERMISSIONS OF authfor1"));
            assertRowsNet(session.execute("LIST PERMISSIONS OF authfor2"),
                          new Object[]{ "authfor2", "authfor2", "<keyspace authfor_test>", "SELECT", true, false, false },
                          new Object[]{ "authfor2", "authfor2", "<table authfor_test.t1>", "SELECT", true, false, false },
                          new Object[]{ "authfor2", "authfor2", "<table authfor_test.t2>", "SELECT", true, false, false },
                          new Object[]{ "authfor2", "authfor2", "<table authfor_test.t2>", "MODIFY", true, false, true }
            );

            // all permissions and grant options must have been removed
            session.execute("DROP ROLE authfor2");
            session.execute("CREATE USER authfor2 WITH PASSWORD 'pass2'");

            assertRowsNet(session.execute("LIST PERMISSIONS OF authfor1"));
            assertRowsNet(session.execute("LIST PERMISSIONS OF authfor2"));

            return null;
        });
    }

    @Test
    public void testRestrict() throws Throwable
    {
        // Test story:
        // - one table
        // - one security admin user (restrict1), which must not gain access to a resource (the table)
        // - one role that has legit access to that table
        // - security admin user is granted the role
        // - security admin user must still not be able to access that table
        // - security admin user must not be able to "unrestrict"

        sessionWithUser("cassandra", "cassandra", ProtocolVersion.CURRENT, (session) ->
        {
            session.execute("CREATE KEYSPACE restrict_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
            session.execute("CREATE TABLE restrict_test.t1 (id int PRIMARY KEY, val text)");

            session.execute("CREATE ROLE role_restrict");
            session.execute("CREATE USER restrict1 WITH PASSWORD 'restrict1'");

            session.execute("GRANT AUTHORIZE FOR SELECT ON KEYSPACE restrict_test TO restrict1");
            return null;
        });

        sessionWithUser("restrict1", "restrict1", ProtocolVersion.CURRENT, (session) ->
        {
            assertInvalidThrowMessage(() -> session.execute("SELECT * FROM restrict_test.t1"),
                                      "User restrict1 has no SELECT permission on <table restrict_test.t1> or any of its parents",
                                      UnauthorizedException.class,
                                      "SELECT * FROM restrict_test.t1");

            session.execute("GRANT SELECT ON KEYSPACE restrict_test TO role_restrict");

            assertInvalidThrowMessage(() -> session.execute("SELECT * FROM restrict_test.t1"),
                                      "User restrict1 has no SELECT permission on <table restrict_test.t1> or any of its parents",
                                      UnauthorizedException.class,
                                      "SELECT * FROM restrict_test.t1");

            return null;
        });

        sessionWithUser("cassandra", "cassandra", ProtocolVersion.CURRENT, (session) ->
        {
            session.execute("GRANT role_restrict TO restrict1");
            return null;
        });

        invalidateAuthCaches();

        sessionWithUser("restrict1", "restrict1", ProtocolVersion.CURRENT, (session) ->
        {
            assertRowsNet(session.execute("SELECT * FROM restrict_test.t1"));
            return null;
        });

        sessionWithUser("cassandra", "cassandra", ProtocolVersion.CURRENT, (session) ->
        {
            assertRowsNet(session.execute("RESTRICT SELECT ON TABLE restrict_test.t1 TO restrict1"));
            return null;
        });

        invalidateAuthCaches();

        sessionWithUser("restrict1", "restrict1", ProtocolVersion.CURRENT, (session) ->
        {
            assertInvalidThrowMessage(() -> session.execute("SELECT * FROM restrict_test.t1"),
                                      "Access for user restrict1 on <table restrict_test.t1> or any of its parents with SELECT permission is restricted",
                                      UnauthorizedException.class,
                                      "SELECT * FROM restrict_test.t1");

            assertInvalidThrowMessage(() -> session.execute("UNRESTRICT SELECT ON TABLE restrict_test.t1 FROM restrict1"),
                                      "Only superusers are allowed to RESTRICT/UNRESTRICT",
                                      UnauthorizedException.class,
                                      "UNRESTRICT SELECT ON TABLE restrict_test.t1 FROM restrict1");

            assertInvalidThrowMessage(() -> session.execute("SELECT * FROM restrict_test.t1"),
                                      "Access for user restrict1 on <table restrict_test.t1> or any of its parents with SELECT permission is restricted",
                                      UnauthorizedException.class,
                                      "SELECT * FROM restrict_test.t1");

            return null;
        });
    }
}
