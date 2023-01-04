/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_ENCRYPTION_ALGORITHM;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_ENCRYPTION_KEY;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_ENCRYPTION_KEY_HASH;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_GRPC_CHECK_INTERVAL_TIMEOUT_MS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_GRPC_DIRECTPATH_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_GRPC_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_GRPC_READ_MESSAGE_TIMEOUT_MS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_GRPC_READ_TIMEOUT_MS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_GRPC_TRAFFICDIRECTOR_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_GRPC_UPLOAD_BUFFERED_REQUESTS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_GRPC_WRITE_MESSAGE_TIMEOUT_MS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_GRPC_WRITE_TIMEOUT_MS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_HTTP_HEADERS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_BUFFER_SIZE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_PIPE_BUFFER_SIZE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_UPLOAD_CACHE_SIZE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_REWRITE_MAX_CHUNK_PER_CALL;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_ROOT_URL;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_SERVICE_PATH;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.PROXY_ADDRESS_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.PROXY_PASSWORD_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.PROXY_USERNAME_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.cloud.hadoop.util.testing.HadoopConfigurationUtils.getDefaultProperties;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem.GcsFileChecksumType;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem.GlobAlgorithm;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.ClientType;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.MetricsSink;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions.PipeType;
import com.google.cloud.hadoop.util.RedactedString;
import com.google.cloud.hadoop.util.RequesterPaysOptions.RequesterPaysMode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleHadoopFileSystemConfigurationTest {

  @SuppressWarnings("DoubleBraceInitialization")
  private static final Map<String, Object> expectedDefaultConfiguration =
      new HashMap<>() {
        {
          put("fs.gs.application.name.suffix", "");
          put("fs.gs.batch.threads", 15);
          put("fs.gs.block.size", 64 * 1024 * 1024L);
          put("fs.gs.bucket.delete.enable", false);
          put("fs.gs.checksum.type", GcsFileChecksumType.NONE);
          put("fs.gs.client.type", ClientType.HTTP_API_CLIENT);
          put("fs.gs.copy.with.rewrite.enable", true);
          put("fs.gs.create.items.conflict.check.enable", true);
          put("fs.gs.delegation.token.binding", null);
          put("fs.gs.encryption.algorithm", null);
          put("fs.gs.encryption.key", null);
          put("fs.gs.encryption.key.hash", null);
          put("fs.gs.glob.algorithm", GlobAlgorithm.CONCURRENT);
          put("fs.gs.grpc.checkinterval.timeout.ms", 1_000L);
          put("fs.gs.grpc.checksums.enable", false);
          put("fs.gs.grpc.directpath.enable", true);
          put("fs.gs.grpc.enable", false);
          put("fs.gs.grpc.read.message.timeout.ms", 3_000L);
          put("fs.gs.grpc.read.timeout.ms", 3600_000L);
          put("fs.gs.grpc.read.zerocopy.enable", true);
          put("fs.gs.grpc.server.address", "storage.googleapis.com");
          put("fs.gs.grpc.trafficdirector.enable", true);
          put("fs.gs.grpc.write.buffered.requests", 20);
          put("fs.gs.grpc.write.message.timeout.ms", 3_000L);
          put("fs.gs.grpc.write.timeout.ms", 10 * 60_000L);
          put("fs.gs.http.connect-timeout", 5_000L);
          put("fs.gs.http.max.retry", 10);
          put("fs.gs.implicit.dir.repair.enable", true);
          put("fs.gs.inputstream.fadvise", Fadvise.AUTO);
          put("fs.gs.inputstream.fast.fail.on.not.found.enable", true);
          put("fs.gs.inputstream.inplace.seek.limit", 8 * 1024 * 1024L);
          put("fs.gs.inputstream.min.range.request.size", 2 * 1024 * 1024L);
          put("fs.gs.inputstream.support.gzip.encoding.enable", false);
          put("fs.gs.io.buffersize.write", 24 * 1024 * 1024L);
          put("fs.gs.javaclient.enable", false);
          put("fs.gs.lazy.init.enable", false);
          put("fs.gs.list.max.items.per.call", 5_000);
          put("fs.gs.marker.file.pattern", null);
          put("fs.gs.max.requests.per.batch", 15);
          put("fs.gs.max.wait.for.empty.object.creation.ms", 3_000L);
          put("fs.gs.metrics.sink", MetricsSink.NONE);
          put("fs.gs.outputstream.buffer.size", 8 * 1024 * 1024L);
          put("fs.gs.outputstream.direct.upload.enable", false);
          put("fs.gs.outputstream.pipe.buffer.size", 1024 * 1024L);
          put("fs.gs.outputstream.pipe.type", PipeType.IO_STREAM_PIPE);
          put("fs.gs.outputstream.sync.min.interval.ms", 0L);
          put("fs.gs.outputstream.upload.cache.size", 0L);
          put("fs.gs.outputstream.upload.chunk.size", 24 * 1024 * 1024L);
          put("fs.gs.performance.cache.enable", false);
          put("fs.gs.performance.cache.max.entry.age.ms", 5_000L);
          put("fs.gs.project.id", null);
          put("fs.gs.reported.permissions", "700");
          put("fs.gs.requester.pays.buckets", ImmutableList.of());
          put("fs.gs.requester.pays.mode", RequesterPaysMode.DISABLED);
          put("fs.gs.requester.pays.project.id", null);
          put("fs.gs.rewrite.max.chunk.per.call", 512 * 1024 * 1024L);
          put("fs.gs.status.parallel.enable", true);
          put("fs.gs.storage.http.headers.", ImmutableMap.of());
          put("fs.gs.storage.root.url", "https://storage.googleapis.com/");
          put("fs.gs.storage.service.path", "storage/v1/");
          put("fs.gs.tracelog.enable", false);
          put("fs.gs.working.dir", "/");
        }
      };

  @Test
  public void testProxyProperties_throwsExceptionWhenMissingProxyAddress() {
    HadoopConfigurationProperty<String> gcsProxyUsername =
        new HadoopConfigurationProperty<>(
            GCS_CONFIG_PREFIX + PROXY_USERNAME_SUFFIX.getKey(), "proxy-user");
    HadoopConfigurationProperty<String> gcsProxyPassword =
        new HadoopConfigurationProperty<>(
            GCS_CONFIG_PREFIX + PROXY_PASSWORD_SUFFIX.getKey(), "proxy-pass");

    Configuration config = new Configuration();
    config.set(gcsProxyUsername.getKey(), gcsProxyUsername.getDefault());
    config.set(gcsProxyPassword.getKey(), gcsProxyPassword.getDefault());
    GoogleCloudStorageOptions.Builder optionsBuilder =
        GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(config);

    assertThrows(IllegalArgumentException.class, optionsBuilder::build);
  }

  @Test
  public void testProxyPropertiesAll() {
    HadoopConfigurationProperty<String> gcsProxyUsername =
        new HadoopConfigurationProperty<>(
            GCS_CONFIG_PREFIX + PROXY_USERNAME_SUFFIX.getKey(), "proxy-user");
    HadoopConfigurationProperty<String> gcsProxyPassword =
        new HadoopConfigurationProperty<>(
            GCS_CONFIG_PREFIX + PROXY_PASSWORD_SUFFIX.getKey(), "proxy-pass");
    HadoopConfigurationProperty<String> gcsProxyAddress =
        new HadoopConfigurationProperty<>(
            GCS_CONFIG_PREFIX + PROXY_ADDRESS_SUFFIX.getKey(), "proxy-address");

    Configuration config = new Configuration();
    config.set(gcsProxyUsername.getKey(), gcsProxyUsername.getDefault());
    config.set(gcsProxyPassword.getKey(), gcsProxyPassword.getDefault());
    config.set(gcsProxyAddress.getKey(), gcsProxyAddress.getDefault());
    GoogleCloudStorageFileSystemOptions options =
        GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config).build();

    assertThat(options.getCloudStorageOptions().getProxyUsername()).isNotNull();
    assertThat(options.getCloudStorageOptions().getProxyUsername().value()).isEqualTo("proxy-user");
    assertThat(options.getCloudStorageOptions().getProxyUsername().toString())
        .isEqualTo("<redacted>");

    assertThat(options.getCloudStorageOptions().getProxyPassword()).isNotNull();
    assertThat(options.getCloudStorageOptions().getProxyPassword().value()).isEqualTo("proxy-pass");
    assertThat(options.getCloudStorageOptions().getProxyPassword().toString())
        .isEqualTo("<redacted>");

    assertThat(options.getCloudStorageOptions().getProxyAddress()).isEqualTo("proxy-address");
  }

  @Test
  public void testDeprecatedKeys_throwsExceptionWhenDeprecatedKeyIsUsed() {
    HadoopConfigurationProperty<String> gcsProxyAddress =
        new HadoopConfigurationProperty<>(
            GCS_CONFIG_PREFIX + PROXY_ADDRESS_SUFFIX.getKey(),
            "proxy-address",
            "fs.gs.proxy.deprecated.address");

    HadoopConfigurationProperty<Integer> gcsProxyUsername =
        new HadoopConfigurationProperty<>(
            GCS_CONFIG_PREFIX + PROXY_USERNAME_SUFFIX.getKey(),
            1234,
            "fs.gs.proxy.deprecated.user");

    HadoopConfigurationProperty<String> gcsProxyPassword =
        new HadoopConfigurationProperty<>(
            GCS_CONFIG_PREFIX + PROXY_PASSWORD_SUFFIX.getKey(),
            "proxy-pass",
            "fs.gs.proxy.deprecated.pass");

    Configuration config = new Configuration();
    config.set(gcsProxyAddress.getKey(), gcsProxyAddress.getDefault());
    config.setInt(gcsProxyUsername.getKey(), gcsProxyUsername.getDefault());
    config.set("fs.gs.proxy.deprecated.pass", gcsProxyPassword.getDefault());

    // Verify that we can read password from config when used key is deprecated.
    RedactedString userPass = gcsProxyPassword.getPassword(config);
    assertThat(userPass.value()).isEqualTo("proxy-pass");

    GoogleCloudStorageOptions.Builder optionsBuilder =
        GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(config);

    // Building configuration using deprecated key (in e.g. proxy password) should fail.
    assertThrows(IllegalArgumentException.class, optionsBuilder::build);
  }

  @Test
  public void testHttpHeadersProperties_singleHeader() {
    Configuration config = new Configuration();
    config.set(GCS_HTTP_HEADERS.getKey() + "header-key", "val=ue");

    GoogleCloudStorageFileSystemOptions options =
        GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config).build();

    assertThat(options.getCloudStorageOptions().getHttpRequestHeaders())
        .containsExactly("header-key", "val=ue");
  }

  @Test
  public void testHttpHeadersProperties_multipleHeaders() {
    Configuration config = new Configuration();
    config.set(GCS_HTTP_HEADERS.getKey() + "test-header", "test-VAL");
    config.set(GCS_HTTP_HEADERS.getKey() + "key-in-header", "+G2Ap33m5NVOgmXznSGTEvG0I=");

    GoogleCloudStorageFileSystemOptions options =
        GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config).build();

    assertThat(options.getCloudStorageOptions().getHttpRequestHeaders())
        .containsExactly("test-header", "test-VAL", "key-in-header", "+G2Ap33m5NVOgmXznSGTEvG0I=");
  }

  @Test
  public void testEncryptionProperties() {
    Configuration config = new Configuration();
    config.set(GCS_ENCRYPTION_ALGORITHM.getKey(), "AES256");
    config.set(GCS_ENCRYPTION_KEY.getKey(), "+G2Ap33m5NVOgmXznSGTEvG0I=");
    config.set(GCS_ENCRYPTION_KEY_HASH.getKey(), "LpH4y6BkG/1B+n3FwORpdoyQ=");

    GoogleCloudStorageFileSystemOptions options =
        GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config).build();
    assertThat(options.getCloudStorageOptions().getEncryptionAlgorithm()).isEqualTo("AES256");

    assertThat(options.getCloudStorageOptions().getEncryptionKey()).isNotNull();
    assertThat(options.getCloudStorageOptions().getEncryptionKey().toString())
        .isEqualTo("<redacted>");
    assertThat(options.getCloudStorageOptions().getEncryptionKey().value())
        .isEqualTo("+G2Ap33m5NVOgmXznSGTEvG0I=");

    assertThat(options.getCloudStorageOptions().getEncryptionKeyHash()).isNotNull();
    assertThat(options.getCloudStorageOptions().getEncryptionKeyHash().toString())
        .isEqualTo("<redacted>");
    assertThat(options.getCloudStorageOptions().getEncryptionKeyHash().value())
        .isEqualTo("LpH4y6BkG/1B+n3FwORpdoyQ=");
  }

  @Test
  public void defaultPropertiesValues() {
    assertThat(getDefaultProperties(GoogleHadoopFileSystemConfiguration.class))
        .containsExactlyEntriesIn(expectedDefaultConfiguration);
  }

  @Test
  public void customPropertiesValues() {
    Configuration config = new Configuration();
    config.set(GCS_ROOT_URL.getKey(), "https://unit-test-storage.googleapis.com/");
    config.set(GCS_SERVICE_PATH.getKey(), "storage/dev_v1/");

    GoogleCloudStorageOptions options =
        GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(config).build();

    assertThat(options.getStorageRootUrl()).isEqualTo("https://unit-test-storage.googleapis.com/");
    assertThat(options.getStorageServicePath()).isEqualTo("storage/dev_v1/");
  }

  @Test
  public void testImpersonationIdentifier() {
    Configuration config = new Configuration();
    config.set(
        GCS_CONFIG_PREFIX + USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey() + "test-user",
        "test-service-account1");
    config.set(
        GCS_CONFIG_PREFIX + GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey() + "test-grp",
        "test-service-account2");

    assertThat(
            USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX
                .withPrefixes(ImmutableList.of(GCS_CONFIG_PREFIX))
                .getPropsWithPrefix(config))
        .containsExactly("test-user", "test-service-account1");
    assertThat(
            GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX
                .withPrefixes(ImmutableList.of(GCS_CONFIG_PREFIX))
                .getPropsWithPrefix(config))
        .containsExactly("test-grp", "test-service-account2");
  }

  @Test
  public void testGrpcConfiguration() {
    Configuration config = new Configuration();
    long grpcCheckIntervalTimeout = 5;
    long grpcReadTimeout = 10;
    long grpcReadMetadataTimeout = 15;
    long grpcReadMessageTimeout = 10;
    long grpcWriteTimeout = 20;
    long grpcWriteMessageTimeout = 25;
    long grpcUploadBufferedRequests = 30;
    boolean directPathEnabled = true;
    boolean trafficDirectorEnabled = true;
    boolean grpcEnabled = true;

    config.set(GCS_GRPC_ENABLE.getKey(), String.valueOf(grpcEnabled));
    config.set(GCS_GRPC_READ_TIMEOUT_MS.getKey(), String.valueOf(grpcReadTimeout));
    config.set(GCS_GRPC_WRITE_TIMEOUT_MS.getKey(), String.valueOf(grpcWriteTimeout));
    config.set(
        GCS_GRPC_UPLOAD_BUFFERED_REQUESTS.getKey(), String.valueOf(grpcUploadBufferedRequests));
    config.set(GCS_GRPC_DIRECTPATH_ENABLE.getKey(), String.valueOf(directPathEnabled));
    config.set(GCS_GRPC_TRAFFICDIRECTOR_ENABLE.getKey(), String.valueOf(trafficDirectorEnabled));
    config.set(
        GCS_GRPC_CHECK_INTERVAL_TIMEOUT_MS.getKey(), String.valueOf(grpcCheckIntervalTimeout));
    config.set(GCS_GRPC_READ_MESSAGE_TIMEOUT_MS.getKey(), String.valueOf(grpcReadMessageTimeout));
    config.set(GCS_GRPC_WRITE_MESSAGE_TIMEOUT_MS.getKey(), String.valueOf(grpcWriteMessageTimeout));

    GoogleCloudStorageOptions options =
        GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(config).build();

    assertThat(options.getGrpcMessageTimeoutCheckInterval()).isEqualTo(Duration.ofMillis(5));
    assertThat(options.getReadChannelOptions().getGrpcReadMessageTimeout())
        .isEqualTo(Duration.ofMillis(10));
    assertThat(options.getReadChannelOptions().getGrpcReadTimeout())
        .isEqualTo(Duration.ofMillis(10));
    assertThat(options.getWriteChannelOptions().getGrpcWriteMessageTimeout())
        .isEqualTo(Duration.ofMillis(25));
    assertThat(options.getWriteChannelOptions().getGrpcWriteTimeout())
        .isEqualTo(Duration.ofMillis(20));
    assertThat(options.getWriteChannelOptions().getNumberOfBufferedRequests()).isEqualTo(30);
    assertThat(options.isDirectPathPreferred()).isTrue();
    assertThat(options.isGrpcEnabled()).isTrue();
    assertThat(options.isTrafficDirectorEnabled()).isTrue();
  }

  @Test
  public void sizeProperties() {
    Configuration config = new Configuration();
    config.set(GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT.getKey(), "2048");
    config.set(GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE.getKey(), "300K");
    config.set(GCS_OUTPUT_STREAM_BUFFER_SIZE.getKey(), "40k");
    config.set(GCS_OUTPUT_STREAM_PIPE_BUFFER_SIZE.getKey(), "256");
    config.set(GCS_OUTPUT_STREAM_UPLOAD_CACHE_SIZE.getKey(), "512M");
    config.set(GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE.getKey(), "16m");
    config.set(GCS_REWRITE_MAX_CHUNK_PER_CALL.getKey(), "2g");

    GoogleCloudStorageOptions options =
        GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(config).build();

    assertThat(options.getReadChannelOptions().getInplaceSeekLimit()).isEqualTo(2048);
    assertThat(options.getReadChannelOptions().getMinRangeRequestSize()).isEqualTo(300 * 1024);
    assertThat(options.getWriteChannelOptions().getBufferSize()).isEqualTo(40 * 1024);
    assertThat(options.getWriteChannelOptions().getPipeBufferSize()).isEqualTo(256);
    assertThat(options.getWriteChannelOptions().getUploadCacheSize()).isEqualTo(512 * 1024 * 1024);
    assertThat(options.getWriteChannelOptions().getUploadChunkSize()).isEqualTo(16 * 1024 * 1024);
    assertThat(options.getMaxBytesRewrittenPerCall()).isEqualTo(2 * 1024 * 1024 * 1024L);
  }
}
