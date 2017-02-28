/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework;


import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.MessageFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.config.EncryptionOptions;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

public class TestEnvironment
{
    public final Path buildDir = Paths.get(System.getProperty("dse.build.dir", "build")).normalize().toAbsolutePath();
    public final Path buildConfDir = buildDir.resolve("test/conf");
    public final Path ngDir = buildDir.resolve("test/ng");
    public final Path logDir = buildDir.resolve("test/logs");

    public TestEnvironment()
    {
        System.out.println(MessageFormat.format(
        "Initializing test environment\n" +
        "----------------------------------------------\n" +
        "ngDir:                    {0}\n" +
        "buildConfDir:             {1}\n" +
        "buildDir:                 {2}\n", ngDir, buildConfDir, buildDir));
    }

    private final ConcurrentMap<Integer, CassandraYamlBuilder> cassandraYamlBuilders = new ConcurrentHashMap<>();

    private final ConcurrentMap<Integer, String> keystorePaths = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, String> truststorePaths = new ConcurrentHashMap<>();


    public void generateSslKeystore(String host, int node,
                                    Path keystorePath, Path truststorePath,
                                    String keyStorePassword, String trustStorePassword) throws Exception
    {
        if (Files.isRegularFile(keystorePath) || Files.isRegularFile(truststorePath))
        {
            return;
        }

        // Generate our key pair
        KeyPair pair = getKeyPair();

        // Now generate a certificate using the keypair
        X509Certificate certificate = getX509Certificate(host, pair);

        // Generate a keystore for the server using the private key and the certificate
        KeyStore keystore = KeyStore.getInstance("JKS");
        keystore.load(null, null);
        keystore.setKeyEntry("node" + node, pair.getPrivate(), keyStorePassword.toCharArray(), new java.security.cert.Certificate[]{ certificate });
        keystore.store(Files.newOutputStream(keystorePath), keyStorePassword.toCharArray());
        // Generate a truststore for the client and just add the certificate
        KeyStore truststore = KeyStore.getInstance("JKS");
        truststore.load(null, null);
        truststore.setCertificateEntry("node" + node, certificate);
        truststore.store(Files.newOutputStream(truststorePath), trustStorePassword.toCharArray());
    }

    public static KeyPair getKeyPair() throws NoSuchAlgorithmException
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        return generator.generateKeyPair();
    }

    public static X509Certificate getX509Certificate(String host, KeyPair pair) throws OperatorCreationException, CertificateException
    {
        X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
                                                                              new X500Name("CN=" + host + ", OU=None, O=None L=None, C=None"),
                                                                              BigInteger.valueOf(1),
                                                                              new Date(System.currentTimeMillis() - 1000L * 60 * 60 * 24 * 30),
                                                                              new Date(System.currentTimeMillis() + (1000L * 60 * 60 * 24 * 365 * 10)),
                                                                              new X500Name("CN=" + host + ", OU=None, O=None L=None, C=None"),
                                                                              pair.getPublic());
        ContentSigner caSigner = new JcaContentSignerBuilder("SHA512withRSA").build(pair.getPrivate());
        return new JcaX509CertificateConverter().getCertificate(certBuilder.build(caSigner));
    }


    public Path generateCassandraConfig(String host, int node, CassandraYamlBuilder cassandraYamlBuilder, boolean rewrite, boolean clientEncryption)
    throws Exception
    {
        // If the node has been started and stopped within a single test
        // then don't create a new instance for it, just get it's config name
        // unless rewrite is set true then use the same instance but
        // rewrite the config with a different configuration
        if (cassandraYamlBuilders.containsKey(node) && !rewrite)
        {
            cassandraYamlBuilder = cassandraYamlBuilders.get(node);
        }
        else
        {
            // If a builder wasn't passed to us create a new one
            if (cassandraYamlBuilder == null)
            {
                cassandraYamlBuilder = CassandraYamlBuilder.newInstance();
            }
            // If rewrite is true then get the config from the map
            // and get the instance name from it
            if (rewrite && cassandraYamlBuilders.containsKey(node))
            {
                cassandraYamlBuilder.setInstanceId(cassandraYamlBuilders.get(node).getInstanceId());
            }
            // Finally build the yaml and store it in the map
            cassandraYamlBuilder.forNode(node).build();
            cassandraYamlBuilders.put(node, cassandraYamlBuilder);
            if (clientEncryption)
            {
                Path ksPath = cassandraYamlBuilder.getRootDirPath().resolve(cassandraYamlBuilder.clientKeystoreFile);
                Path tsPath = cassandraYamlBuilder.getRootDirPath().resolve(cassandraYamlBuilder.clientTruststoreFile);
                generateSslKeystore(host, node, ksPath, tsPath,
                                    cassandraYamlBuilder.clientEncryptionKeystorePassword,
                                    cassandraYamlBuilder.clientEncryptionTruststorePassword);
                keystorePaths.put(node, ksPath.toString());
                truststorePaths.put(node, tsPath.toString());
            }

            if (Util.toEnum(cassandraYamlBuilder.internodeEncryption, EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.none) != EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.none)
            {
                Path ksPath = cassandraYamlBuilder.getRootDirPath().resolve(cassandraYamlBuilder.serverKeystoreFile);
                Path tsPath = cassandraYamlBuilder.getRootDirPath().resolve(cassandraYamlBuilder.serverTruststoreFile);

                generateSslKeystore(host, node, ksPath, tsPath,
                                    cassandraYamlBuilder.serverEncryptionKeystorePassword,
                                    cassandraYamlBuilder.serverEncryptionTruststorePassword);
            }
        }
        return cassandraYamlBuilder.getConfigFilePath();
    }

    public void clearConfigs()
    {
        cassandraYamlBuilders.clear();
        keystorePaths.clear();
        truststorePaths.clear();
    }

    public CassandraYamlBuilder getCassandraYamlBuilder(int n)
    {
        return cassandraYamlBuilders.get(n);
    }

    public void removeConfig(int n)
    {
        cassandraYamlBuilders.remove(n);
        keystorePaths.remove(n);
        truststorePaths.remove(n);
    }

    public String getTruststorePath(int n)
    {
        return truststorePaths.get(n);
    }

    public String getKeystorePath(int n)
    {
        return keystorePaths.get(n);
    }


    private void setSystemProperties(Map<String, String> props)
    {
        for (String propKey : props.keySet())
        {
            System.setProperty(propKey, props.get(propKey));
        }
    }
}