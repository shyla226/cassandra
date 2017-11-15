/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.config;


/**
 * Note on backward compatibility:<br/>
 * If options are specified in {@code dse.yaml} and not in {@code cassandra.yaml},<br/>
 * {@code DseConfigurainLoader.maybeApplySystemTableEncryptionSettingsFromDseConfig} will copy
 * over the configuration<br/> from {@code DseConfig} to C*'s {@link Config}.
 */
public class SystemTableEncryptionOptions
{
    public boolean enabled = false;
    public String cipher_algorithm = "AES";
    public int secret_key_strength = 128;
    public int chunk_length_kb = 64;
    public String key_name = "system_table_keytab";

    public String key_provider = "com.datastax.bdp.cassandra.crypto.LocalFileSystemKeyProviderFactory";
    // The kmip host config to use from the dse.yaml, this is not an actual hostname.
    public String kmip_host = null;

    public boolean isKmipKeyProvider()
    {
        return key_provider.endsWith("KmipKeyProviderFactory");
    }
}
