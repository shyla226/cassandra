/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.dse.framework;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import javax.validation.constraints.NotNull;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.datastax.bdp.db.audit.IAuditWriter;
import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.locator.GossipingPropertyFileSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;

public class CassandraYamlBuilder
{
    static
    {
        Properties p = new Properties();
        p.setProperty("resource.loader", "class");
        p.setProperty("class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
        p.setProperty("runtime.introspector.uberspect", "org.apache.velocity.tools.generic.introspection.PublicFieldUberspect");
        Velocity.init(p);
    }

    private String instanceId = UUID.randomUUID().toString();

    public String getInstanceId()
    {
        return instanceId;
    }

    public void setInstanceId(String instanceId)
    {
        this.instanceId = instanceId;
    }

    public String getConfigFileName()
    {
        return getInstanceName() + ".yaml";
    }

    public String getRootDir()
    {
        return getRootDirPath().toString();
    }

    public Path getRootDirPath()
    {
        return DseTestRunner.testEnvironment.buildDir.resolve("test/ng").resolve(getInstanceId())
                                                     .toAbsolutePath();
    }

    @NotNull
    public Path getConfigFilePath()
    {
        return Paths.get(getRootDir(), getConfigFileName());
    }

    public void build() throws IOException
    {
        if (!new File(getRootDir()).exists() && !new File(getRootDir()).mkdirs())
        {
            throw new RuntimeException("Unable to create " + getRootDir());
        }

        Template template = Velocity.getTemplate(getTemplate());
        VelocityContext context = new VelocityContext();

        context.put("state", this);
        FileWriter writer = new FileWriter(getConfigFilePath().toFile());
        template.merge(context, writer);
        writer.close();
    }

    private static final String[] murmur_tokens = new String[]{ "-7511679497064084122", "-4511679497064084122", "-2511679497064084122", "1", "2511679497064084122" };
    private static final String[] random_tokens = new String[]{ "0", "7511679497064084122", "9511679497064084122" };
    private final static String hostIpTemplate = "127.0.0.Z";

    /**
     * Do not set this attribute directly, use {@link DseTestRunner#setAuthentication(DseTestRunner.Authentication)}
     */
    public String authenticator;

    /**
     * Do not set this attribute directly, use {@link DseTestRunner#setAuthorization(DseTestRunner.Authorization)}
     */
    public String authorizer;

    /**
     * Do not set this attribute directly, is it set automatically
     */
    public String roleManager = CassandraRoleManager.class.getName();

    /**
     * Do not set this attribute directly, use {@link DseTestRunner#setServerEncryption(boolean)}
     */
    public String internodeEncryption = null;

    /**
     * Do not set this attribute directly, use {@link DseTestRunner#setClientEncryption(boolean)}
     */
    public boolean clientEncryption;

    /**
     * Do not set this attribute directly, use {@link DseTestRunner#setRequireClientAuth(boolean)}
     */
    public boolean requireClientAuth = false;

    /**
     * Do not set this attribute directly, use {@link DseTestRunner#setInternodeRequireClientAuth(boolean)}
     */
    public boolean internodeRequireClientAuth = false;

    public int node = 1;
    public int seedNode = 0;
    public String instanceName = "cassandra";
    public String hintedHandoffEnabled = "true";
    public String numberOfTokens;
    public String token;
    public String partitioner = "org.apache.cassandra.dht.Murmur3Partitioner";
    public String autoBootstrap;
    public String snitch = GossipingPropertyFileSnitch.class.getName();
    public String diskFailurePolicy;
    public boolean dynamicSnitch = true;
    public String clientEncryptionKeystorePassword = "secret";
    public String clientEncryptionTruststorePassword = "secret";
    public int readRequestTimeoutInMs = 10000;
    public String serverEncryptionKeystorePassword = "secret";
    public String serverEncryptionTruststorePassword = "secret";
    public final String clientKeystoreFile = "keystore.client";
    public final String clientTruststoreFile = "truststore.client";
    public String serverKeystoreFile = "keystore.server";
    public String serverTruststoreFile = "truststore.server";
    public Integer batchSizeFailThresholdInKb;
    public boolean incrementalBackups = true;
    public boolean useBatchCommitlogSync = true;
    public long commitlogSyncPeriod = 10000;
    public boolean allowUDF = false;
    public String diskAccessMode;
    public String clusterName = "Test Cluster";
    public boolean cdcEnabled = false;
    public String cdcRawDirectory;
    public String auditLogger;
    public String excludedKeyspaces = "";

    public static CassandraYamlBuilder newInstance()
    {
        return new CassandraYamlBuilder();
    }

    protected CassandraYamlBuilder()
    {
    }

    public String getClusterName()
    {
        return clusterName;
    }

    public String getInstanceName()
    {
        return instanceName;
    }

    public String getHintedHandoffEnabled()
    {
        return hintedHandoffEnabled;
    }

    public String getTemplate()
    {
        return "cassandra.yaml.template";
    }

    public CassandraYamlBuilder forNode(int node)
    {
        this.node = node;
        return this;
    }

    public String getHostIp()
    {
        return getHostIp(this.node);
    }

    public String getHostIp(int nodeVal)
    {
        return hostIpTemplate.replaceFirst("Z", "" + nodeVal);
    }

    public CassandraYamlBuilder withNumberOfTokens(String numberOfTokens)
    {
        this.numberOfTokens = numberOfTokens;
        return this;
    }

    public String getToken()
    {
        if (StringUtils.isNotEmpty(token))
        {
            return token;
        }
        else if (partitioner.endsWith("Murmur3Partitioner"))
        {
            return murmur_tokens[node - 1];
        }
        return random_tokens[node - 1];
    }

    public CassandraYamlBuilder withHintedHandoffEnabled(String hintedHandoffEnabled)
    {
        this.hintedHandoffEnabled = hintedHandoffEnabled;
        return this;
    }


    public String getSeeds()
    {
        if (seedNode != 0)
        {
            return getHostIp(seedNode);
        }
        else
        {
            List<String> seeds = Lists.newArrayList("127.0.0.1");
            for (int n = 2; n < node; n++)
            {
                if (DseTestRunner.isNodeRunning(n))
                {
                    seeds.add("127.0.0." + n);
                    // three seeds should be enough
                    if (seeds.size() == 3) break;
                }
            }
            return StringUtils.join(seeds, ',');
        }
    }

    public CassandraYamlBuilder withClusterName(String name)
    {
        clusterName = name;
        return this;
    }

    public CassandraYamlBuilder withToken(String token)
    {
        this.token = token;
        return this;
    }

    public CassandraYamlBuilder withAuthenticator(String authenticator)
    {
        this.authenticator = authenticator;
        return this;
    }

    public CassandraYamlBuilder withAuditLogger(Class<? extends IAuditWriter> klass)
    {
        auditLogger = klass.getName();
        return this;
    }

    /**
     * Disable some keyspaces from being audited (assuming use of the audit logger).
     * @param excludedKeyspaces keyspace names
     */
    public CassandraYamlBuilder withAuditLogExcludedKeyspaces(String... excludedKeyspaces)
    {
        this.excludedKeyspaces = StringUtils.join(excludedKeyspaces, ',');
        return this;
    }


    public CassandraYamlBuilder withPartitioner(String partitioner)
    {
        this.partitioner = partitioner;
        return this;
    }

    public CassandraYamlBuilder withAutoBootstrap(String autoBootstrap)
    {
        this.autoBootstrap = autoBootstrap;
        return this;
    }

    public CassandraYamlBuilder withClientEncryptionKeystorePassword(String password)
    {
        this.clientEncryptionKeystorePassword = password;
        return this;
    }

    public CassandraYamlBuilder withClientEncryptionTruststorePassword(String password)
    {
        this.clientEncryptionTruststorePassword = password;
        return this;
    }

    public CassandraYamlBuilder withSnitch(Class<? extends IEndpointSnitch> snitchClass)
    {
        this.snitch = snitchClass.getName();
        return this;
    }

    public CassandraYamlBuilder withDynamicSnitch(boolean dynamicSnitch)
    {
        this.dynamicSnitch = dynamicSnitch;
        return this;
    }

    public CassandraYamlBuilder withDiskFailurePolicy(String policy)
    {
        this.diskFailurePolicy = policy;
        return this;
    }

    public CassandraYamlBuilder withReadRequestTimeoutInMs(int millis)
    {
        this.readRequestTimeoutInMs = millis;
        return this;
    }

    public CassandraYamlBuilder withSeedNode(int nodeId)
    {
        this.seedNode = nodeId;
        return this;
    }

    @Override
    public boolean equals(Object o)
    {
        return EqualsBuilder.reflectionEquals(this, o, "node", "instanceName", "instanceId");
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode(this, "node", "instanceName", "instanceId");
    }

    public CassandraYamlBuilder withServerEncryptionKeystorePassword(String password)
    {
        this.serverEncryptionKeystorePassword = password;
        return this;
    }

    public CassandraYamlBuilder withServerEncryptionTruststorePassword(String password)
    {
        this.serverEncryptionTruststorePassword = password;
        return this;
    }

    public CassandraYamlBuilder withServerEncryptionKeystore(String path)
    {
        this.serverKeystoreFile = path;
        return this;
    }

    public CassandraYamlBuilder withServerEncryptionTruststore(String path)
    {
        this.serverTruststoreFile = path;
        return this;
    }

    public CassandraYamlBuilder withBatchFailSize(int batchSizeFailThresholdInKb)
    {
        this.batchSizeFailThresholdInKb = batchSizeFailThresholdInKb;
        return this;
    }

    public CassandraYamlBuilder withIncrementalBackups(boolean enableIncrementalBackups)
    {
        this.incrementalBackups = enableIncrementalBackups;
        return this;
    }

    public CassandraYamlBuilder useBatchCommitlogSync(boolean useBatch)
    {
        this.useBatchCommitlogSync = useBatch;
        return this;
    }

    public CassandraYamlBuilder withCommitlogSyncPeriodInMs(long commitlogSyncPeriod)
    {
        assert !useBatchCommitlogSync;
        this.commitlogSyncPeriod = commitlogSyncPeriod;
        return this;
    }

    public CassandraYamlBuilder allowUDF(boolean allowUDF)
    {
        this.allowUDF = allowUDF;
        return this;
    }

    public String getAuthenticator()
    {
        return authenticator;
    }

    public void resetSecurity()
    {
        authenticator = null;
        authorizer = null;
        clientEncryption = false;
    }

    public CassandraYamlBuilder withDiskAccessMode(String diskAccessMode)
    {
        this.diskAccessMode = diskAccessMode;
        return this;
    }

    public CassandraYamlBuilder withCdcEnabled(boolean cdcEnabled)
    {
        this.cdcEnabled = cdcEnabled;
        return this;
    }

    public CassandraYamlBuilder withCdcRawDirectory(String cdcRawDirectory)
    {
        this.cdcRawDirectory = cdcRawDirectory;
        return this;
    }
}
