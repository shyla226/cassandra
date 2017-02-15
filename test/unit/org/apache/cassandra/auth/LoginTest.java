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

package org.apache.cassandra.auth;

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.FailedLoginException;

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.jmx.AuthenticationProxy;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.junit.Assert.*;


public class LoginTest extends CQLTester
{
    private final static String USER1 = "user1";
    private final static String PASSWORD_USER1 = "pass1";

    private final static String INVALID_USER = "abc";
    private final static String INVALID_PASSWORD = "abc";

    @BeforeClass
    public static void setUp()
    {
        requireNetwork();

        // Replace AllowAllAuthenticator with the test authenticator, which will
        // accept only the credentials passed into the constructor
        TestAuthenticator.setUp(ImmutableMap.of(USER1, PASSWORD_USER1));
    }

    @Test
    public void testLoginModuleNullPassword() throws Exception
    {
        testLoginModule(USER1, null, FailedLoginException.class);
    }

    @Test
    public void testLoginModuleNullUser() throws Exception
    {
        testLoginModule(null, PASSWORD_USER1, FailedLoginException.class);
    }

    @Test
    public void testLoginModuleNullUserAndPassword() throws Exception
    {
        testLoginModule(null, null, FailedLoginException.class);
    }

    @Test
    public void testLoginModuleEmptyPassword() throws Exception
    {
        testLoginModule(USER1, "", FailedLoginException.class);
    }

    @Test
    public void testLoginModuleEmptyUser() throws Exception
    {
        testLoginModule("", PASSWORD_USER1, FailedLoginException.class);
    }

    @Test
    public void testLoginModuleEmptyUserAndPassword() throws Exception
    {
        testLoginModule("", "", FailedLoginException.class);
    }

    @Test
    public void testLoginModuleInvalidPassword() throws Exception
    {
        testLoginModule(USER1, INVALID_PASSWORD, FailedLoginException.class);
    }

    @Test
    public void testLoginModuleInvalidUser() throws Exception
    {
        testLoginModule(INVALID_USER, PASSWORD_USER1, FailedLoginException.class);
    }

    @Test
    public void testLoginModuleInvalidUserAndPassword() throws Exception
    {
        testLoginModule(INVALID_USER, INVALID_PASSWORD, FailedLoginException.class);
    }

    @Test
    public void testLoginModuleValidUserAndPassword() throws Exception
    {
        testLoginModule(USER1, PASSWORD_USER1);
    }

    private void testLoginModule(String user, String password) throws Exception
    {
        testLoginModule(user, password, null);
    }

    private void testLoginModule(String user, String password, Class<?> expectedException) throws Exception
    {
        try
        {
            CassandraLoginModule loginModule = new CassandraLoginModule();
            loginModule.initialize(null, new AuthenticationProxy.JMXCallbackHandler(new String[] {user, password}), null, null);
            assertTrue(loginModule.login());
            if (expectedException != null)
                fail("Missing expected exception " + expectedException.getName());
        }
        catch (Exception e)
        {
           checkException(e, expectedException);
        }
    }

    private Map<String, String> makeCredentials(String userName, String password)
    {
        Map<String, String> ret = new HashMap<>();
        ret.put(PasswordAuthenticator.USERNAME_KEY, userName);
        ret.put(PasswordAuthenticator.PASSWORD_KEY, password);
        return ret;
    }

    /** Absorb the exception silently if its class type, or the inner class type, matches the
     * expected exception, else rethrow.
     */
    private static void checkException(Exception e, Class<?> expectedException) throws Exception
    {
        assertNotNull(e);
        if (expectedException == null)
            throw e; // we were not expecting any exception

        if (e.getClass() != expectedException &&
            (e.getCause() == null || e.getCause().getClass() != expectedException))
            throw e; // the exception or its inner exception did not match what we were expecting
    }

    @Test
    public void testSimpleClientNullPassword() throws Exception
    {
        testSimpleClient(USER1, null, AuthenticationException.class);
    }

    @Test
    public void testSimpleClientNullUser() throws Exception
    {
        testSimpleClient(null, PASSWORD_USER1, AuthenticationException.class);
    }

    @Test
    public void testSimpleClientNullUserAndPassword() throws Exception
    {
        testSimpleClient(null, null, AuthenticationException.class);
    }

    @Test
    public void testSimpleClientEmptyPassword() throws Exception
    {
        testSimpleClient(USER1, "", AuthenticationException.class);
    }

    @Test
    public void testSimpleClientEmptyUser() throws Exception
    {
        testSimpleClient("", PASSWORD_USER1, AuthenticationException.class);
    }

    @Test
    public void testSimpleClientEmptyUserAndPassword() throws Exception
    {
        testSimpleClient("", "", AuthenticationException.class);
    }

    @Test
    public void testSimpleClientInvalidPassword() throws Exception
    {
        testSimpleClient(USER1, INVALID_PASSWORD,  AuthenticationException.class);
    }

    @Test
    public void testSimpleClientInvalidUser() throws Exception
    {
        testSimpleClient(INVALID_USER, PASSWORD_USER1,  AuthenticationException.class);
    }

    @Test
    public void testSimpleClientInvalidUserAndPassword() throws Exception
    {
        testSimpleClient(INVALID_USER, INVALID_PASSWORD, AuthenticationException.class);
    }

    @Test
    public void testSimpleClientValidUserAndPassword() throws Exception
    {
        testSimpleClient(USER1, PASSWORD_USER1);
    }

    private void testSimpleClient(String user, String password) throws Exception
    {
        testSimpleClient(user, password, null);
    }

    private void testSimpleClient(String user, String password, Class<?> expectedException) throws Exception
    {
        Map<String, String> credentials = makeCredentials(user, password);
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        for (ProtocolVersion version : PROTOCOL_VERSIONS)
        {
            try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(),
                                                        nativePort,
                                                        version,
                                                        version.isBeta(),
                                                        new EncryptionOptions.ClientEncryptionOptions()))
            {
                client.connect(false);

                client.login(credentials);

                QueryMessage query = new QueryMessage(String.format("SELECT * from %s.%s", KEYSPACE, currentTable()), QueryOptions.DEFAULT);
                Message.Response resp = client.execute(query);

                if (expectedException == null)
                    assertTrue(resp instanceof ResultMessage.Rows);
                else
                    fail("Missing expected exception " + expectedException.getName());
            }
            catch (Exception e)
            {
                checkException(e, expectedException);
            }
        }
    }
}
