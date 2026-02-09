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
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.cloud.hadoop.util.testing.HadoopConfigurationUtils.getDefaultProperties;
import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem.GcsFileChecksumType;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem.GlobAlgorithm;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.ClientType;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.MetricsSink;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.gcsio.PerformanceCachingGoogleCloudStorageOptions;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions.PartFileCleanupType;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions.PipeType;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions.UploadType;
import com.google.cloud.hadoop.util.RequesterPaysOptions.RequesterPaysMode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
          put("fs.gs.cloud.logging.enable", false);
          put("fs.gs.copy.with.rewrite.enable", true);
          put("fs.gs.create.items.conflict.check.enable", true);
          put("fs.gs.delegation.token.binding", null);
          put("fs.gs.encryption.algorithm", null);
          put("fs.gs.encryption.key", null);
          put("fs.gs.encryption.key.hash", null);
          put("fs.gs.glob.algorithm", GlobAlgorithm.CONCURRENT);
          put("fs.gs.vectored.read.threads", 16);
          put("fs.gs.vectored.read.merged.range.max.size", 8 * 1024 * 1024);
          put("fs.gs.vectored.read.min.range.seek.size", 4 * 1024);
          put("fs.gs.grpc.checkinterval.timeout", 1_000L);
          put("fs.gs.grpc.checksums.enable", false);
          put("fs.gs.grpc.directpath.enable", true);
          put("fs.gs.grpc.enable", false);
          put("fs.gs.grpc.read.message.timeout", 3_000L);
          put("fs.gs.grpc.read.timeout", 3_600_000L);
          put("fs.gs.grpc.read.zerocopy.enable", true);
          put("fs.gs.grpc.trafficdirector.enable", true);
          put("fs.gs.grpc.write.buffered.requests", 20);
          put("fs.gs.grpc.write.message.timeout", 3_000L);
          put("fs.gs.grpc.write.enable", false);
          put("fs.gs.hierarchical.namespace.folders.enable", false);
          put("fs.gs.hierarchical.namespace.folders.optimization.enable", false);
          put("fs.gs.grpc.write.timeout", 600_000L);
          put("fs.gs.http.connect-timeout", 5_000L);
          put("fs.gs.http.max.retry", 10);
          put("fs.gs.implicit.dir.repair.enable", true);
          put("fs.gs.inputstream.fadvise", Fadvise.AUTO);
          put("fs.gs.inputstream.fast.fail.on.not.found.enable", true);
          put("fs.gs.inputstream.inplace.seek.limit", 8 * 1024 * 1024L);
          put("fs.gs.inputstream.min.range.request.size", 2 * 1024 * 1024L);
          put("fs.gs.inputstream.support.gzip.encoding.enable", false);
          put("fs.gs.lazy.init.enable", false);
          put("fs.gs.list.max.items.per.call", 5_000);
          put("fs.gs.marker.file.pattern", null);
          put("fs.gs.max.requests.per.batch", 15);
          put("fs.gs.max.wait.for.empty.object.creation", 3_000L);
          put("fs.gs.metrics.sink", MetricsSink.NONE);
          put("fs.gs.outputstream.buffer.size", 8 * 1024 * 1024L);
          put("fs.gs.outputstream.direct.upload.enable", false);
          put("fs.gs.outputstream.pipe.buffer.size", 1024 * 1024L);
          put("fs.gs.outputstream.pipe.type", PipeType.IO_STREAM_PIPE);
          put("fs.gs.outputstream.sync.min.interval", 0L);
          put("fs.gs.outputstream.upload.cache.size", 0L);
          put("fs.gs.outputstream.upload.chunk.size", 24 * 1024 * 1024L);
          put("fs.gs.performance.cache.enable", false);
          put("fs.gs.performance.cache.max.entry.age", 5_000L);
          put("fs.gs.project.id", null);
          put("fs.gs.reported.permissions", "700");
          put("fs.gs.requester.pays.buckets", ImmutableList.of());
          put("fs.gs.requester.pays.mode", RequesterPaysMode.DISABLED);
          put("fs.gs.requester.pays.project.id", null);
          put("fs.gs.rewrite.max.chunk.size", 512 * 1024 * 1024L);
          put("fs.gs.status.parallel.enable", true);
          put("fs.gs.storage.http.headers.", ImmutableMap.of());
          put("fs.gs.storage.root.url", "https://storage.googleapis.com/");
          put("fs.gs.storage.service.path", "storage/v1/");
          put("fs.gs.tracelog.enable", false);
          put("fs.gs.operation.tracelog.enable", false);
          put("fs.gs.working.dir", "/");
          put("fs.gs.client.upload.type", UploadType.CHUNK_UPLOAD);
          put("fs.gs.write.temporary.dirs", ImmutableSet.of());
          put("fs.gs.write.parallel.composite.upload.buffer.count", 1);
          put("fs.gs.write.parallel.composite.upload.buffer.capacity", 32 * 1024 * 1024L);
          put(
              "fs.gs.write.parallel.composite.upload.part.file.cleanup.type",
              PartFileCleanupType.ALWAYS);
          put("fs.gs.write.parallel.composite.upload.part.file.name.prefix", "");
          put("fs.gs.fadvise.request.track.count", 3);
          put("fs.gs.operation.move.enable", true);
          put("fs.gs.write.rolling.checksum.enable", true);
          put("fs.gs.bidi.enable", false);
          put("fs.gs.bidi.thread.count", 16);
          put("fs.gs.bidi.client.timeout", 30);
          put("fs.gs.bidi.finalize.on.close", false);
          put("fs.gs.storage.client.cache.enable", false);
        }
      };

  @Test
  public void proxyProperties_throwsException_whenMissingProxyAddress() {
    Configuration config = new Configuration();
    config.set("fs.gs.proxy.password", "proxy-pass");
    config.set("fs.gs.proxy.username", "proxy-user");

    GoogleCloudStorageOptions.Builder optionsBuilder =
        GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(config);

    assertThrows(IllegalArgumentException.class, optionsBuilder::build);
  }

  @Test
  public void proxyProperties_throwsException_whenOnlyProxyPasswordIsSet() {
    Configuration config = new Configuration();
    config.set("fs.gs.proxy.address", "proxy-addr");
    config.set("fs.gs.proxy.password", "proxy-pass");

    GoogleCloudStorageOptions.Builder optionsBuilder =
        GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(config);

    assertThrows(IllegalArgumentException.class, optionsBuilder::build);
  }

  @Test
  public void proxyProperties_throwsException_whenOnlyProxyUserIsSet() {
    Configuration config = new Configuration();
    config.set("fs.gs.proxy.address", "proxy-addr");
    config.set("fs.gs.proxy.username", "proxy-user");

    GoogleCloudStorageOptions.Builder optionsBuilder =
        GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(config);

    assertThrows(IllegalArgumentException.class, optionsBuilder::build);
  }

  @Test
  public void proxyProperties() {
    Configuration config = new Configuration();
    config.set("fs.gs.proxy.address", "proxy-addr");
    config.set("fs.gs.proxy.password", "proxy-pass");
    config.set("fs.gs.proxy.username", "proxy-user");

    GoogleCloudStorageFileSystemOptions options =
        GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config).build();

    assertThat(options.getCloudStorageOptions().getProxyAddress()).isEqualTo("proxy-addr");

    assertThat(options.getCloudStorageOptions().getProxyPassword()).isNotNull();
    assertThat(options.getCloudStorageOptions().getProxyPassword().value()).isEqualTo("proxy-pass");
    assertThat(options.getCloudStorageOptions().getProxyPassword().toString())
        .isEqualTo("<redacted>");

    assertThat(options.getCloudStorageOptions().getProxyUsername()).isNotNull();
    assertThat(options.getCloudStorageOptions().getProxyUsername().value()).isEqualTo("proxy-user");
    assertThat(options.getCloudStorageOptions().getProxyUsername().toString())
        .isEqualTo("<redacted>");
  }

  @Test
  public void deprecatedKey_default() {
    HadoopConfigurationProperty<String> prop =
        new HadoopConfigurationProperty<>("prop.key-new", "prop-val-def", "prop.key-deprecated");

    Configuration config = new Configuration();

    assertThat(prop.get(config, config::get)).isEqualTo("prop-val-def");
  }

  @Test
  public void deprecatedKey_canReadValue() {
    HadoopConfigurationProperty<String> prop =
        new HadoopConfigurationProperty<>("prop.key-new", "prop-val-def", "prop.key-deprecated");

    Configuration config = new Configuration();
    config.set("prop.key-deprecated", "prop-val-deprecated");

    assertThat(prop.get(config, config::get)).isEqualTo("prop-val-deprecated");
  }

  @Test
  public void deprecatedKey_canReadOverriddenValue() {
    HadoopConfigurationProperty<String> prop =
        new HadoopConfigurationProperty<>("prop.key-new", "prop-val-def", "prop.key-deprecated");

    Configuration config = new Configuration();
    config.set("prop.key-deprecated", "prop-val-deprecated");
    config.set("prop.key-new", "prop-val-new");

    assertThat(prop.get(config, config::get)).isEqualTo("prop-val-new");
  }

  @Test
  public void httpHeadersProperties_singleHeader() {
    Configuration config = new Configuration();
    config.set("fs.gs.storage.http.headers.header-key", "val=ue");

    GoogleCloudStorageFileSystemOptions options =
        GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config).build();

    assertThat(options.getCloudStorageOptions().getHttpRequestHeaders())
        .containsExactly("header-key", "val=ue");
  }

  @Test
  public void httpHeadersProperties_multipleHeaders() {
    Configuration config = new Configuration();
    config.set("fs.gs.storage.http.headers.test-header", "test-VAL");
    config.set("fs.gs.storage.http.headers.key-in-header", "+G2Ap33m5NVOgmXznSGTEvG0I=");

    GoogleCloudStorageFileSystemOptions options =
        GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config).build();

    assertThat(options.getCloudStorageOptions().getHttpRequestHeaders())
        .containsExactly("test-header", "test-VAL", "key-in-header", "+G2Ap33m5NVOgmXznSGTEvG0I=");
  }

  @Test
  public void encryptionProperties() {
    Configuration config = new Configuration();
    config.set("fs.gs.encryption.algorithm", "AES256");
    config.set("fs.gs.encryption.key", "+G2Ap33m5NVOgmXznSGTEvG0I=");
    config.set("fs.gs.encryption.key.hash", "LpH4y6BkG/1B+n3FwORpdoyQ=");

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
    config.set("fs.gs.storage.root.url", "https://unit-test-storage.googleapis.com/");
    config.set("fs.gs.storage.service.path", "storage/dev_v1/");

    GoogleCloudStorageOptions options =
        GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(config).build();

    assertThat(options.getStorageRootUrl()).isEqualTo("https://unit-test-storage.googleapis.com/");
    assertThat(options.getStorageServicePath()).isEqualTo("storage/dev_v1/");
  }

  @Test
  public void saImpersonationProperties() {
    Configuration config = new Configuration();
    config.set(
        "fs.gs.auth.impersonation.service.account.for.user.test-user", "test-service-account1");
    config.set(
        "fs.gs.auth.impersonation.service.account.for.group.test-grp", "test-service-account2");

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
  public void grpcConfiguration() {
    Configuration config = new Configuration();
    config.setBoolean("fs.gs.grpc.directpath.enable", true);
    config.setBoolean("fs.gs.grpc.enable", true);
    config.setBoolean("fs.gs.grpc.trafficdirector.enable", true);
    config.setInt("fs.gs.grpc.write.buffered.requests", 30);
    config.setTimeDuration("fs.gs.grpc.checkinterval.timeout", 5, MILLISECONDS);
    config.setTimeDuration("fs.gs.grpc.read.message.timeout", 10, MILLISECONDS);
    config.setTimeDuration("fs.gs.grpc.read.timeout", 7, MILLISECONDS);
    config.setTimeDuration("fs.gs.grpc.write.message.timeout", 25, MILLISECONDS);
    config.setTimeDuration("fs.gs.grpc.write.timeout", 20, MILLISECONDS);

    GoogleCloudStorageOptions options =
        GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(config).build();

    assertThat(options.isDirectPathPreferred()).isTrue();
    assertThat(options.isGrpcEnabled()).isTrue();
    assertThat(options.isTrafficDirectorEnabled()).isTrue();
    assertThat(options.getWriteChannelOptions().getNumberOfBufferedRequests()).isEqualTo(30);
    assertThat(options.getGrpcMessageTimeoutCheckInterval()).isEqualTo(Duration.ofMillis(5));
    assertThat(options.getReadChannelOptions().getGrpcReadMessageTimeout())
        .isEqualTo(Duration.ofMillis(10));
    assertThat(options.getReadChannelOptions().getGrpcReadTimeout())
        .isEqualTo(Duration.ofMillis(7));
    assertThat(options.getWriteChannelOptions().getGrpcWriteMessageTimeout())
        .isEqualTo(Duration.ofMillis(25));
    assertThat(options.getWriteChannelOptions().getGrpcWriteTimeout())
        .isEqualTo(Duration.ofMillis(20));
  }

  @Test
  public void readChannelOptions() {
    Configuration config = new Configuration();
    config.set("fs.gs.inputstream.fadvise", "RANDOM");
    config.setBoolean("fs.gs.inputstream.fast.fail.on.not.found.enable", false);
    config.setBoolean("fs.gs.grpc.checksums.enable", true);
    config.setTimeDuration("fs.gs.grpc.read.message.timeout", 1, MILLISECONDS);
    config.setTimeDuration("fs.gs.grpc.read.timeout", 2, MILLISECONDS);
    config.setBoolean("fs.gs.grpc.read.zerocopy.enable", false);
    config.setBoolean("fs.gs.inputstream.support.gzip.encoding.enable", true);
    config.setLong("fs.gs.inputstream.inplace.seek.limit", 3L);
    config.setLong("fs.gs.inputstream.min.range.request.size", 4L);
    config.setInt("fs.gs.bidi.thread.count", 5);
    config.setBoolean("fs.gs.bidi.enable", true);
    config.setInt("fs.gs.bidi.client.timeout", 6);

    GoogleCloudStorageReadOptions options =
        GoogleHadoopFileSystemConfiguration.getReadChannelOptions(config);

    assertThat(options.getFadvise()).isEqualTo(Fadvise.RANDOM);
    assertThat(options.isFastFailOnNotFoundEnabled()).isFalse();
    assertThat(options.isGrpcChecksumsEnabled()).isTrue();
    assertThat(options.getGrpcReadMessageTimeout()).isEqualTo(Duration.ofMillis(1));
    assertThat(options.getGrpcReadTimeout()).isEqualTo(Duration.ofMillis(2));
    assertThat(options.isGrpcReadZeroCopyEnabled()).isFalse();
    assertThat(options.isGzipEncodingSupportEnabled()).isTrue();
    assertThat(options.getInplaceSeekLimit()).isEqualTo(3L);
    assertThat(options.getMinRangeRequestSize()).isEqualTo(4L);
    assertThat(options.getBidiThreadCount()).isEqualTo(5);
    assertThat(options.getBidiClientTimeout()).isEqualTo(6);
  }

  @Test
  public void writeChannelOptions() {
    Configuration config = new Configuration();
    config.setLong("fs.gs.outputstream.buffer.size", 1L);
    config.setBoolean("fs.gs.outputstream.direct.upload.enable", true);
    config.setBoolean("fs.gs.grpc.checksums.enable", true);
    config.setTimeDuration("fs.gs.grpc.write.message.timeout", 2, MILLISECONDS);
    config.setTimeDuration("fs.gs.grpc.write.timeout", 3, MILLISECONDS);
    config.setInt("fs.gs.grpc.write.buffered.requests", 4);
    config.setLong("fs.gs.outputstream.pipe.buffer.size", 5L);
    config.set("fs.gs.outputstream.pipe.type", "NIO_CHANNEL_PIPE");
    config.setLong("fs.gs.outputstream.upload.cache.size", 6L);
    config.setLong("fs.gs.outputstream.upload.chunk.size", 7 * 8 * 1024 * 1024);
    config.set("fs.gs.client.upload.type", "JOURNALING");
    config.set("fs.gs.write.temporary.dirs", "foo,bar");
    config.setInt("fs.gs.write.parallel.composite.upload.buffer.count", 8);
    config.setLong("fs.gs.write.parallel.composite.upload.buffer.capacity", 9L);
    config.set("fs.gs.write.parallel.composite.upload.part.file.cleanup.type", "NEVER");
    config.set("fs.gs.write.parallel.composite.upload.part.file.name.prefix", "baz");
    config.setBoolean("fs.gs.write.rolling.checksum.enable", true);

    AsyncWriteChannelOptions options =
        GoogleHadoopFileSystemConfiguration.getWriteChannelOptions(config);

    assertThat(options.getBufferSize()).isEqualTo(1L);
    assertThat(options.isDirectUploadEnabled()).isTrue();
    assertThat(options.isGrpcChecksumsEnabled()).isTrue();
    assertThat(options.getGrpcWriteMessageTimeout()).isEqualTo(Duration.ofMillis(2));
    assertThat(options.getGrpcWriteTimeout()).isEqualTo(Duration.ofMillis(3));
    assertThat(options.getNumberOfBufferedRequests()).isEqualTo(4);
    assertThat(options.getPipeBufferSize()).isEqualTo(5L);
    assertThat(options.getPipeType()).isEqualTo(PipeType.NIO_CHANNEL_PIPE);
    assertThat(options.getUploadCacheSize()).isEqualTo(6L);
    assertThat(options.getUploadChunkSize()).isEqualTo(7 * 8 * 1024 * 1024);
    assertThat(options.getUploadType()).isEqualTo(UploadType.JOURNALING);
    assertThat(options.getTemporaryPaths()).isEqualTo(ImmutableSet.of("foo", "bar"));
    assertThat(options.getPCUBufferCount()).isEqualTo(8);
    assertThat(options.getPCUBufferCapacity()).isEqualTo(9L);
    assertThat(options.getPartFileCleanupType()).isEqualTo(PartFileCleanupType.NEVER);
    assertThat(options.getPartFileNamePrefix()).isEqualTo("baz");
    assertThat(options.isRollingChecksumEnabled()).isTrue();
  }

  @Test
  public void sizeProperties_sizeSuffixes() {
    Configuration config = new Configuration();
    config.set("fs.gs.inputstream.inplace.seek.limit", "2048");
    config.set("fs.gs.inputstream.min.range.request.size", "300K");
    config.set("fs.gs.outputstream.buffer.size", "40k");
    config.set("fs.gs.outputstream.pipe.buffer.size", "256");
    config.set("fs.gs.outputstream.upload.cache.size", "512M");
    config.set("fs.gs.outputstream.upload.chunk.size", "16m");
    config.set("fs.gs.rewrite.max.chunk.size", "2g");

    GoogleCloudStorageOptions options =
        GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(config).build();

    assertThat(options.getReadChannelOptions().getInplaceSeekLimit()).isEqualTo(2048);
    assertThat(options.getReadChannelOptions().getMinRangeRequestSize()).isEqualTo(300 * 1024);
    assertThat(options.getWriteChannelOptions().getBufferSize()).isEqualTo(40 * 1024);
    assertThat(options.getWriteChannelOptions().getPipeBufferSize()).isEqualTo(256);
    assertThat(options.getWriteChannelOptions().getUploadCacheSize()).isEqualTo(512 * 1024 * 1024);
    assertThat(options.getWriteChannelOptions().getUploadChunkSize()).isEqualTo(16 * 1024 * 1024);
    assertThat(options.getMaxRewriteChunkSize()).isEqualTo(2 * 1024 * 1024 * 1024L);
  }

  @Test
  public void timeProperties_timeDurationSuffixes() {
    Configuration config = new Configuration();
    config.set("fs.gs.grpc.checkinterval.timeout", "1m");
    config.set("fs.gs.grpc.read.message.timeout", "0");
    config.set("fs.gs.grpc.read.timeout", "100");
    config.set("fs.gs.grpc.write.message.timeout", "3s");
    config.set("fs.gs.grpc.write.timeout", "1h");
    config.set("fs.gs.http.connect-timeout", "2m");
    config.set("fs.gs.http.read-timeout", "2000ms");
    config.set("fs.gs.max.wait.for.empty.object.creation", "90s");
    config.set("fs.gs.performance.cache.max.entry.age", "4s");

    GoogleCloudStorageOptions options =
        GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(config).build();
    PerformanceCachingGoogleCloudStorageOptions perfCacheOptions =
        GoogleHadoopFileSystemConfiguration.getPerformanceCachingOptions(config);

    assertThat(options.getGrpcMessageTimeoutCheckInterval()).isEqualTo(Duration.ofMinutes(1));
    assertThat(options.getReadChannelOptions().getGrpcReadMessageTimeout())
        .isEqualTo(Duration.ZERO);
    assertThat(options.getReadChannelOptions().getGrpcReadTimeout())
        .isEqualTo(Duration.ofMillis(100));
    assertThat(options.getWriteChannelOptions().getGrpcWriteMessageTimeout())
        .isEqualTo(Duration.ofSeconds(3));
    assertThat(options.getWriteChannelOptions().getGrpcWriteTimeout())
        .isEqualTo(Duration.ofHours(1));
    assertThat(options.getHttpRequestConnectTimeout()).isEqualTo(Duration.ofMinutes(2));
    assertThat(options.getHttpRequestReadTimeout()).isEqualTo(Duration.ofSeconds(2));
    assertThat(options.getMaxWaitTimeForEmptyObjectCreation()).isEqualTo(Duration.ofSeconds(90));
    assertThat(perfCacheOptions.getMaxEntryAge()).isEqualTo(Duration.ofSeconds(4));
  }
}
