/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.Base64;
import java.util.Enumeration;
import java.util.Optional;
import java.util.Set;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

public class SSLUtil
{
    private final static Logger LOGGER = LoggerFactory.getLogger(SSLUtil.class);

    public final static String[] DEFAULT_CIPHER_SUITES;

    static
    {
        try
        {
            DEFAULT_CIPHER_SUITES = SSLContext.getDefault().getDefaultSSLParameters().getCipherSuites();
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException("Cannot get the default set of cipher suites.", e);
        }
    }

    public enum PemHeader
    {
        CERTIFICATE("CERTIFICATE"),
        PRIVATE_KEY("PRIVATE KEY");

        private final String header;

        PemHeader(String header)
        {
            this.header = header;
        }

        public String getBegin()
        {
            return String.format("-----BEGIN %s-----", header);
        }

        public String getEnd()
        {
            return String.format("-----END %s-----", header);
        }
    }

    /**
     * Creates trust manager factory and loads trust store. Null value is ok for password if it is
     * not used.
     */
    public static TrustManagerFactory initTrustManagerFactory(
                                                             String keystorePath,
                                                             String keystoreType,
                                                             String keystorePassword
    ) throws IOException, GeneralSecurityException
    {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore ts = createKeyStore(keystorePath, keystoreType, keystorePassword);
        tmf.init(ts);
        return tmf;
    }

    /**
     * Creates key manager factory and loads the key store. Null values are ok for passwords if they are
     * not used.
     */
    public static KeyManagerFactory initKeyManagerFactory(
                                                         String keystorePath,
                                                         String keystoreType,
                                                         String keystorePassword,
                                                         String keyPassword) throws IOException, GeneralSecurityException
    {
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        KeyStore ks = createKeyStore(keystorePath, keystoreType, keystorePassword);
        kmf.init(ks, keyPassword != null ? keyPassword.toCharArray() : null);
        return kmf;
    }

    private static KeyStore createKeyStore(String keystorePath, String type, String password)
    throws IOException, GeneralSecurityException
    {
        if (keystorePath == null)
        {
            return null;
        }
        byte[] keystore = FileUtils.readFileToByteArray(new File(keystorePath));
        return createKeyStore(
        ByteBuffer.wrap(keystore),
        type != null ? type : KeyStore.getDefaultType(),
        password != null ? password.toCharArray() : null);
    }

    private static KeyStore createKeyStore(ByteBuffer keystore, String type, char[] password)
    throws IOException, GeneralSecurityException
    {
        KeyStore ks = KeyStore.getInstance(type);
        ks.load(ByteBufferUtil.inputStream(keystore), password);

        return ks;
    }

    /**
     * Creates and initializes SSLContext with given trust manager and key manager factories. Null values
     * for tmf or kmf are ok.
     */
    public static SSLContext initSSLContext(TrustManagerFactory tmf, KeyManagerFactory kmf, String protocol)
    throws GeneralSecurityException
    {
        TrustManager[] tms = tmf != null ? tmf.getTrustManagers() : null;
        KeyManager[] kms = kmf != null ? kmf.getKeyManagers() : null;
        SSLContext ctx = SSLContext.getInstance(protocol);
        ctx.init(kms, tms, new SecureRandom());
        return ctx;
    }

    /**
     * Creates {@link SSLContext} according to the current settings. It supports trusted SSL setups with or without
     * client authentication.
     *
     * @return {@link java.util.Optional} of {@link SSLContext}, which is empty if SSL is not enabled
     */
    public static Optional<SSLContext> createSSLContext(EncryptionOptions.ClientEncryptionOptions options)
    throws IOException, GeneralSecurityException
    {
        if (options.enabled)
        {
            TrustManagerFactory tmf = initTrustManagerFactory(
            options.truststore,
            options.store_type,
            options.truststore_password);

            KeyManagerFactory kmf = null;
            if (options.keystore != null)
            {
                kmf = initKeyManagerFactory(
                options.keystore,
                options.store_type,
                options.keystore_password,
                options.keystore_password);
            }

            return Optional.of(initSSLContext(tmf, kmf, options.protocol));
        }
        else
        {
            return Optional.empty();
        }
    }

    /**
     * Loads the given keystore, retrieves all trusted certificate entries and puts them into a new
     * keystore.
     */
    public static KeyStore makeTrustStoreFromKeyStore(String path, String type, String password)
    throws IOException, GeneralSecurityException
    {
        KeyStore ks = createKeyStore(path, type, password);
        KeyStore exportedKeyStore = KeyStore.getInstance(ks.getType());
        exportedKeyStore.load(null, null);
        Enumeration<String> aliases = ks.aliases();
        while (aliases.hasMoreElements())
        {
            String alias = aliases.nextElement();
            Certificate cert = ks.getCertificate(alias);

            if (cert != null)
            {
                exportedKeyStore.setCertificateEntry(alias, cert);
            }
        }
        return exportedKeyStore;
    }

    /**
     * Loads the given keystore, retrieves all trusted certificate entries and puts them into a new
     * keystore. Keystore is exported without any password to the resulting byte array.
     */
    public static byte[] exportTruststore(String path, String type, String password)
    throws IOException, GeneralSecurityException
    {
        KeyStore exportedKeyStore = makeTrustStoreFromKeyStore(path, type, password);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        exportedKeyStore.store(out, new char[0]);
        return out.toByteArray();
    }

    public static OpenOption[] fileWriteOpts(boolean forceOverwrite)
    {
        if (forceOverwrite)
        {
            return new OpenOption[]{ TRUNCATE_EXISTING, CREATE, WRITE };
        }
        else
        {
            return new OpenOption[]{ CREATE_NEW };
        }
    }

    /**
     * Saves the given truststore data in a truststore file at a given path, with a randomly generated
     * password. The generated password is returned from this method.
     */
    public static String saveTruststore(byte[] truststore, String type, Path path, String password, boolean forceOverwrite)
    {
        try
        {
            String passwordString = password;
            if (passwordString == null)
            {
                byte[] truststorePassword = new byte[16];
                SecureRandom.getInstanceStrong().nextBytes(truststorePassword);
                passwordString = new BigInteger(truststorePassword).toString(36);
            }

            KeyStore ks = KeyStore.getInstance(type);
            ks.load(new ByteArrayInputStream(truststore), new char[0]);

            Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rwx------");
            Files.createFile(path, PosixFilePermissions.asFileAttribute(permissions));
            try (OutputStream out = Files.newOutputStream(path, fileWriteOpts(forceOverwrite)))
            {
                ks.store(out, passwordString.toCharArray());
            }
            return passwordString;
        }
        catch (Exception ex)
        {
            throw new RuntimeException("Failed to save truststore to " + path, ex);
        }
    }

    public static void savePem(byte[] data, Path path, PemHeader pemHeader, boolean forceOverwrite)
    {
        try
        {
            Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rw-------");
            Files.createFile(path, PosixFilePermissions.asFileAttribute(permissions));
            try (OutputStream fout = Files.newOutputStream(path, fileWriteOpts(forceOverwrite)))
            {
                PrintStream out = new PrintStream(fout, true);
                Base64.Encoder encoder = Base64.getMimeEncoder();
                out.println(pemHeader.getBegin());
                out.println(encoder.encodeToString(data));
                out.println(pemHeader.getEnd());
            }
        }
        catch (Exception ex)
        {
            throw new RuntimeException("Failed to save PEM data to " + path, ex);
        }
    }

    public static Optional<byte[]> getCertificate(Path path, String type, String password, Optional<String> alias)
    {
        try (InputStream out = Files.newInputStream(path, READ))
        {
            return getCertificate(out, type, password, alias);
        }
        catch (IOException ex)
        {
            throw new RuntimeException("Cannot load keystore from " + path.toString());
        }
    }

    public static Optional<byte[]> getCertificate(InputStream in, String type, String password, Optional<String> alias)
    {
        final String ksType = type != null ? type : KeyStore.getDefaultType();
        final char[] ksPassword = password != null ? password.toCharArray() : null;

        try
        {
            KeyStore ks = KeyStore.getInstance(ksType);
            ks.load(in, ksPassword);

            Optional<String> resolvedAlias = getAlias(alias, ks.aliases());

            if (resolvedAlias.isPresent())
            {
                Certificate cert = ks.getCertificate(resolvedAlias.get());
                if (cert != null)
                {
                    return Optional.of(cert.getEncoded());
                }
            }
        }
        catch (Exception ex)
        {
            throw new RuntimeException("Failed to retrieve a certificate", ex);
        }

        return Optional.empty();
    }

    public static Optional<byte[]> getKey(Path path, String type, String password, String keyPassword, Optional<String> alias)
    {
        try (InputStream out = Files.newInputStream(path, READ))
        {
            return getKey(out, type, password, keyPassword, alias);
        }
        catch (IOException ex)
        {
            throw new RuntimeException("Cannot load keystore from " + path.toString());
        }
    }

    public static Optional<byte[]> getKey(InputStream in, String type, String password, String keyPassword, Optional<String> alias)
    {
        final String ksType = type != null ? type : KeyStore.getDefaultType();
        final char[] ksPassword = password != null ? password.toCharArray() : null;
        final char[] ksKeyPassword = keyPassword != null ? keyPassword.toCharArray() : null;

        try
        {
            KeyStore ks = KeyStore.getInstance(ksType);
            ks.load(in, ksPassword);

            Optional<String> resolvedAlias = getAlias(alias, ks.aliases());

            if (resolvedAlias.isPresent())
            {
                Key key = ks.getKey(resolvedAlias.get(), ksKeyPassword);
                if (key != null)
                {
                    return Optional.of(key.getEncoded());
                }
            }
        }
        catch (Exception ex)
        {
            throw new RuntimeException("Failed to retrieve a key", ex);
        }

        return Optional.empty();
    }

    private static Optional<String> getAlias(Optional<String> providedAlias, Enumeration<String> aliases)
    {
        while (aliases.hasMoreElements())
        {
            String alias = aliases.nextElement();
            if (providedAlias.isPresent())
            {
                if (providedAlias.get().equals(alias))
                {
                    return providedAlias;
                }
            }
            else
            {
                return Optional.of(alias);
            }
        }

        return Optional.empty();
    }
}
