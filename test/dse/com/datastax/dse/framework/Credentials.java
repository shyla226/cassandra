/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.dse.auth.DsePlainTextAuthProvider;
import org.apache.cassandra.auth.PasswordAuthenticator;

public class Credentials
{
    public final String user;
    public final String password;

    public Credentials(String user, String password)
    {
        this.user = user;
        this.password = password;
    }


    public Credentials(Map<String, String> credMap)
    {
        this(credMap.get(PasswordAuthenticator.USERNAME_KEY), credMap.get(PasswordAuthenticator.PASSWORD_KEY));
    }

    public Map<String, String> toMap()
    {
        Map<String, String> credMap = new HashMap<>();

        if (!StringUtils.isEmpty(user))
        {
            credMap.put(PasswordAuthenticator.USERNAME_KEY, user);
        }

        if (!StringUtils.isEmpty(password))
        {
            credMap.put(PasswordAuthenticator.PASSWORD_KEY, password);
        }
        return credMap;
    }

    @Override
    public String toString()
    {
        return String.format("Credentials: authenticate as %s, password %s)", user, password);
    }

    public AuthProvider getPlainTextAuthProvider()
    {
        return new DsePlainTextAuthProvider(user, password);
    }
}