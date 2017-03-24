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

import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;

/**
 * A test authenticator that accepts a fixed set of usernames and passwords.
 */
public class TestAuthenticator implements IAuthenticator
{
    private final Map<String, String> credentials;

    public static void setUp(Map<String, String> credentials)
    {
        TestAuthenticator authenticator = new TestAuthenticator(credentials);
        DatabaseDescriptor.setAuthenticator(authenticator);
        DatabaseDescriptor.setRoleManager(authenticator.getRoleManager());
    }

    private TestAuthenticator(Map<String, String> credentials)
    {
        this.credentials = ImmutableMap.copyOf(credentials);
    }

    public boolean requireAuthentication()
    {
        return true;
    }

    public Set<IResource> protectedResources()
    {
        return Collections.emptySet();
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    public void setup()
    {
    }

    public SaslNegotiator newSaslNegotiator(InetAddress clientAddress)
    {
        return new PasswordAuthenticator.PlainTextSaslAuthenticator(this::authenticate);
    }

    public AuthenticatedUser legacyAuthenticate(Map<String, String> credentials)
    {
        PasswordAuthenticator.checkValidCredentials(credentials);
        return authenticate(credentials.get(PasswordAuthenticator.USERNAME_KEY),
                            credentials.get(PasswordAuthenticator.PASSWORD_KEY));
    }

    private AuthenticatedUser authenticate(String username, String password)
    {
        if (!credentials.containsKey(username) || !Objects.equals(credentials.get(username), password))
            throw new AuthenticationException(String.format("Provided username %s and/or password are incorrect", username));

        return new AuthenticatedUser(username);
    }

    private IRoleManager getRoleManager()
    {
        return new IRoleManager()
        {
            public Set<Option> supportedOptions()
            {
                return Collections.emptySet();
            }

            public Set<Option> alterableOptions()
            {
                return null;
            }

            public void createRole(AuthenticatedUser performer,
                                   RoleResource role,
                                   RoleOptions options) throws RequestValidationException, RequestExecutionException
            {

            }

            public void dropRole(AuthenticatedUser performer,
                                 RoleResource role) throws RequestValidationException, RequestExecutionException
            {

            }

            public void alterRole(AuthenticatedUser performer,
                                  RoleResource role,
                                  RoleOptions options) throws RequestValidationException, RequestExecutionException
            {

            }

            public void grantRole(AuthenticatedUser performer,
                                  RoleResource role,
                                  RoleResource grantee) throws RequestValidationException, RequestExecutionException
            {

            }

            public void revokeRole(AuthenticatedUser performer,
                                   RoleResource role,
                                   RoleResource revokee) throws RequestValidationException, RequestExecutionException
            {

            }

            public Set<RoleResource> getRoles(RoleResource grantee,
                                              boolean includeInherited) throws RequestValidationException, RequestExecutionException
            {
                return null;
            }

            public Set<RoleResource> getAllRoles() throws RequestValidationException, RequestExecutionException
            {
                return null;
            }

            public boolean isSuper(RoleResource role)
            {
                return false;
            }

            public boolean canLogin(RoleResource role)
            {
                return true;
            }

            public Map<String, String> getCustomOptions(RoleResource role)
            {
                return Collections.emptyMap();
            }

            public boolean isExistingRole(RoleResource role)
            {
                return false;
            }

            public Set<? extends IResource> protectedResources()
            {
                return null;
            }

            public void validateConfiguration() throws ConfigurationException
            {

            }

            public void setup()
            {

            }
        };
    }
}
