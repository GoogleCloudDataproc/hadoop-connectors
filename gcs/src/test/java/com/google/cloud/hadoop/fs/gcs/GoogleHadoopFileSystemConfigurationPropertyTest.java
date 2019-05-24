package com.google.cloud.hadoop.fs.gcs;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.util.EntriesCredentialConfiguration;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collection;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestBase.loadConfig;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

@RunWith(JUnit4.class)
public class GoogleHadoopFileSystemConfigurationPropertyTest {

    @Test
    public void testPropertyCreation() {
        GoogleHadoopFileSystemConfigurationProperty<Integer>
                NEW_KEY_WITH_DEPRECATED_KEY = new GoogleHadoopFileSystemConfigurationProperty<>(
                "actual.key", 0, "deprecated.key");
        assertThat(NEW_KEY_WITH_DEPRECATED_KEY.getKey()).isEqualTo("actual.key");

        GoogleHadoopFileSystemConfigurationProperty<Integer>
                NEW_KEY_WITHOUT_DEPRECATED_KEY = new GoogleHadoopFileSystemConfigurationProperty<>(
                "actual.key", 0,null);
        assertThat(NEW_KEY_WITHOUT_DEPRECATED_KEY.getDefault()).isEqualTo(0);
    }

    @Test
    public void testCollectionTypeProperty() {
        Configuration config = loadConfig("projectId", "serviceAccount", "privateKeyFile");
        GoogleHadoopFileSystemConfigurationProperty<String>
                STRING_KEY = new GoogleHadoopFileSystemConfigurationProperty<>(
                "actual.key", "default-string");
        GoogleHadoopFileSystemConfigurationProperty<Integer>
                INTEGER_KEY = new GoogleHadoopFileSystemConfigurationProperty<>(
                "actual.key", 1);
        GoogleHadoopFileSystemConfigurationProperty<Collection<String>>
                COLLECTION_KEY = new GoogleHadoopFileSystemConfigurationProperty<>(
                        "collection.key", ImmutableList.of("key1", "key2"));
        assertThrows(IllegalStateException.class, () -> STRING_KEY.getStringCollection(config));
        assertThrows(IllegalStateException.class, () -> INTEGER_KEY.getStringCollection(config));
        Collection<String> col = COLLECTION_KEY.getStringCollection(config);
        assertThat(col).isEqualTo(ImmutableList.of("key1", "key2"));

    }

    @Test(expected = IllegalArgumentException.class)
    public void testProxyProperties() {
        GoogleHadoopFileSystemConfigurationProperty<String> GCS_PROXY_USERNAME =
                new GoogleHadoopFileSystemConfigurationProperty<>(
                        EntriesCredentialConfiguration.PROXY_USERNAME_KEY,
                        "proxy-user");
        GoogleHadoopFileSystemConfigurationProperty<String> GCS_PROXY_PASSWORD =
                new GoogleHadoopFileSystemConfigurationProperty<>(
                        EntriesCredentialConfiguration.PROXY_PASSWORD_KEY,
                        "proxy-pass");

        Configuration config = loadConfig("projectId", "serviceAccount", "privateKeyFile");
        config.set(GCS_PROXY_USERNAME.getKey(), GCS_PROXY_USERNAME.getDefault());
        config.set(GCS_PROXY_PASSWORD.getKey(), GCS_PROXY_PASSWORD.getDefault());
        GoogleCloudStorageFileSystemOptions.Builder optionsBuilder =
                GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config);

        /** Proxy properties should fail when no proxy address is specified */

        assertThrows(IllegalArgumentException.class, () -> optionsBuilder.build());

        GoogleHadoopFileSystemConfigurationProperty<String> GCS_PROXY_ADDRESS =
        new GoogleHadoopFileSystemConfigurationProperty<>(
                EntriesCredentialConfiguration.PROXY_ADDRESS_KEY,
                "proxy-address");
        config.set(GCS_PROXY_ADDRESS.getKey(),GCS_PROXY_ADDRESS.getDefault());
        GoogleCloudStorageFileSystemOptions options = optionsBuilder.build();

        assertThat(options.getCloudStorageOptions().getProxyUsername()).isEqualTo("proxy-user");
        assertThat(options.getCloudStorageOptions().getProxyPassword()).isEqualTo("proxy-pass");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeprecatedKeys() {
        GoogleHadoopFileSystemConfigurationProperty<String> GCS_PROXY_ADDRESS =
                new GoogleHadoopFileSystemConfigurationProperty<>(
                        EntriesCredentialConfiguration.PROXY_ADDRESS_KEY,
                        "proxy-address", "fs.gs.proxy.deprecated.address");

        GoogleHadoopFileSystemConfigurationProperty<Integer> GCS_PROXY_USERNAME =
                new GoogleHadoopFileSystemConfigurationProperty<>(
                        EntriesCredentialConfiguration.PROXY_USERNAME_KEY,
                        1234, "fs.gs.proxy.deprecated.user");

        GoogleHadoopFileSystemConfigurationProperty<String> GCS_PROXY_PASSWORD =
                new GoogleHadoopFileSystemConfigurationProperty<>(
                        EntriesCredentialConfiguration.PROXY_PASSWORD_KEY,
                        "proxy-pass", "fs.gs.proxy.deprecated.pass");
        Configuration config = loadConfig("projectId", "serviceAccount", "privateKeyFile");
        config.set(GCS_PROXY_ADDRESS.getKey(), GCS_PROXY_ADDRESS.getDefault());
        config.setInt(GCS_PROXY_USERNAME.getKey(), GCS_PROXY_USERNAME.getDefault());
        config.set("fs.gs.proxy.deprecated.pass", GCS_PROXY_PASSWORD.getDefault());

        /** Verify that we can read password from config when  used key is deprecated. */
        String userpass = GCS_PROXY_PASSWORD.getPassword(config);
        assertThat(userpass).isEqualTo("proxy-pass");

        GoogleCloudStorageFileSystemOptions.Builder optionsBuilder =
                GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config);

        /** Building configuration using deprecated key (in eg. proxy password) should fail. */
        assertThrows(IllegalArgumentException.class, () -> optionsBuilder.build());

        config.set(GCS_PROXY_PASSWORD.getKey(), GCS_PROXY_PASSWORD.getDefault());
        GoogleCloudStorageFileSystemOptions options = optionsBuilder.build();
        assertThat(options.getCloudStorageOptions().getProxyPassword()).isEqualTo("proxy-pass");
    }
}