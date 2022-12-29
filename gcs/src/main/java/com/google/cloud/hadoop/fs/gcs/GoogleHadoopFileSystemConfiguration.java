/*
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.BATCH_THREADS_DEFAULT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.COPY_WITH_REWRITE_DEFAULT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.DEFAULT_DIRECT_PATH_PREFERRED;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.DEFAULT_GRPC_ENABLED;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.DEFAULT_GRPC_MESSAGE_TIMEOUT_CHECK_INTERVAL_MILLIS;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.DEFAULT_TRAFFIC_DIRECTOR_ENABLED;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.HTTP_REQUEST_CONNECT_TIMEOUT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.MAX_BYTES_REWRITTEN_PER_CALL_DEFAULT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.MAX_HTTP_REQUEST_RETRIES;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.MAX_REQUESTS_PER_BATCH_DEFAULT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.MAX_WAIT_MILLIS_FOR_EMPTY_OBJECT_CREATION;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.DEFAULT_FADVISE;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.DEFAULT_FAST_FAIL_ON_NOT_FOUND_ENABLED;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.DEFAULT_GRPC_CHECKSUMS_ENABLED;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.DEFAULT_GRPC_READ_MESSAGE_TIMEOUT_MILLIS;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.DEFAULT_GRPC_READ_TIMEOUT_MILLIS;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.DEFAULT_GRPC_READ_ZEROCOPY_ENABLED;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.DEFAULT_GZIP_ENCODING_SUPPORT_ENABLED;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.DEFAULT_INPLACE_SEEK_LIMIT;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.PROXY_ADDRESS_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.PROXY_PASSWORD_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.PROXY_USERNAME_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.READ_TIMEOUT_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.getConfigKeyPrefixes;
import static com.google.common.base.Strings.nullToEmpty;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem.GcsFileChecksumType;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem.GlobAlgorithm;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.FilesystemAPI;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.MetricsSink;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.gcsio.PerformanceCachingGoogleCloudStorageOptions;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions.PipeType;
import com.google.cloud.hadoop.util.RedactedString;
import com.google.cloud.hadoop.util.RequesterPaysOptions;
import com.google.cloud.hadoop.util.RequesterPaysOptions.RequesterPaysMode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.GoogleLogger;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;

/** This class provides a configuration for the {@link GoogleHadoopFileSystem} implementations. */
public class GoogleHadoopFileSystemConfiguration {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static final String GCS_CONFIG_PREFIX = "fs.gs";

  public static final List<String> CONFIG_KEY_PREFIXES =
      ImmutableList.copyOf(getConfigKeyPrefixes(GCS_CONFIG_PREFIX));

  // -----------------------------------------------------------------
  // Configuration settings.
  // -----------------------------------------------------------------

  /** Configuration key for the Cloud Storage API endpoint root URL. */
  public static final HadoopConfigurationProperty<String> GCS_ROOT_URL =
      new HadoopConfigurationProperty<>(
          "fs.gs.storage.root.url", GoogleCloudStorageOptions.STORAGE_ROOT_URL_DEFAULT);

  /** Configuration key for the Cloud Storage API endpoint service path. */
  public static final HadoopConfigurationProperty<String> GCS_SERVICE_PATH =
      new HadoopConfigurationProperty<>(
          "fs.gs.storage.service.path", GoogleCloudStorageOptions.STORAGE_SERVICE_PATH_DEFAULT);

  /**
   * Key for the permissions that we report a file or directory to have. Can either be octal or
   * symbolic mode accepted by {@link FsPermission#FsPermission(String)}
   *
   * <p>Default value for the permissions that we report a file or directory to have. Note: We do
   * not really support file/dir permissions but we need to report some permission value when Hadoop
   * calls getFileStatus(). A MapReduce job fails if we report permissions more relaxed than the
   * value below and this is the default File System.
   */
  public static final HadoopConfigurationProperty<String> PERMISSIONS_TO_REPORT =
      new HadoopConfigurationProperty<>("fs.gs.reported.permissions", "700");

  /**
   * Configuration key for default block size of a file.
   *
   * <p>Note that this is the size that is reported to Hadoop FS clients. It does not modify the
   * actual block size of an underlying GCS object, because GCS JSON API does not allow modifying or
   * querying the value. Modifying this value allows one to control how many mappers are used to
   * process a given file.
   */
  public static final HadoopConfigurationProperty<Long> BLOCK_SIZE =
      new HadoopConfigurationProperty<>("fs.gs.block.size", 64 * 1024 * 1024L);

  /** Configuration key for Delegation Token binding class. Default value: none */
  public static final HadoopConfigurationProperty<String> DELEGATION_TOKEN_BINDING_CLASS =
      new HadoopConfigurationProperty<>("fs.gs.delegation.token.binding");

  /** Configuration key for GCS project ID. Default value: none */
  public static final HadoopConfigurationProperty<String> GCS_PROJECT_ID =
      new HadoopConfigurationProperty<>("fs.gs.project.id");

  /** Configuration key for initial working directory of a GHFS instance. Default value: '/' */
  public static final HadoopConfigurationProperty<String> GCS_WORKING_DIRECTORY =
      new HadoopConfigurationProperty<>("fs.gs.working.dir", "/");

  /**
   * If true, recursive delete on a path that refers to a GCS bucket itself ('/' for any
   * bucket-rooted GoogleHadoopFileSystem) or delete on that path when it's empty will result in
   * fully deleting the GCS bucket. If false, any operation that normally would have deleted the
   * bucket will be ignored instead. Setting to 'false' preserves the typical behavior of "rm -rf /"
   * which translates to deleting everything inside of root, but without clobbering the filesystem
   * authority corresponding to that root path in the process.
   */
  public static final HadoopConfigurationProperty<Boolean> GCE_BUCKET_DELETE_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.bucket.delete.enable", false);

  /** Configuration key for GCS project ID. Default value: "DISABLED" */
  public static final HadoopConfigurationProperty<RequesterPaysMode> GCS_REQUESTER_PAYS_MODE =
      new HadoopConfigurationProperty<>(
          "fs.gs.requester.pays.mode", RequesterPaysOptions.REQUESTER_PAYS_MODE_DEFAULT);

  /** Configuration key for GCS Requester Pays Project ID. Default value: none */
  public static final HadoopConfigurationProperty<String> GCS_REQUESTER_PAYS_PROJECT_ID =
      new HadoopConfigurationProperty<>("fs.gs.requester.pays.project.id");

  /** Configuration key for GCS Requester Pays Buckets. Default value: none */
  public static final HadoopConfigurationProperty<Collection<String>> GCS_REQUESTER_PAYS_BUCKETS =
      new HadoopConfigurationProperty<>("fs.gs.requester.pays.buckets", ImmutableList.of());

  /**
   * Configuration key for which type of FileChecksum to return; if a particular file doesn't
   * support the requested type, then getFileChecksum() will return null for that file.
   */
  public static final HadoopConfigurationProperty<GcsFileChecksumType> GCS_FILE_CHECKSUM_TYPE =
      new HadoopConfigurationProperty<>("fs.gs.checksum.type", GcsFileChecksumType.NONE);

  /**
   * Configuration key for using a local item cache to supplement GCS API "getFile" results. This
   * provides faster access to recently queried data. Because the data is cached, modifications made
   * outside of this instance may not be immediately reflected. The performance cache can be used in
   * conjunction with other caching options.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_PERFORMANCE_CACHE_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.performance.cache.enable", false);

  /**
   * Configuration key for maximum number of milliseconds a GoogleCloudStorageItemInfo will remain
   * "valid" in the performance cache before it's invalidated.
   */
  public static final HadoopConfigurationProperty<Long> GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE_MILLIS =
      new HadoopConfigurationProperty<>(
          "fs.gs.performance.cache.max.entry.age.ms",
          PerformanceCachingGoogleCloudStorageOptions.MAX_ENTRY_AGE_MILLIS_DEFAULT);

  /**
   * If true, executes GCS requests in {@code listStatus} and {@code getFileStatus} methods in
   * parallel to reduce latency.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_STATUS_PARALLEL_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.status.parallel.enable", true);

  /** Configuration key for enabling lazy initialization of GCS FS instance. */
  public static final HadoopConfigurationProperty<Boolean> GCS_LAZY_INITIALIZATION_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.lazy.init.enable", false);

  /**
   * Configuration key for enabling automatic repair of implicit directories whenever detected
   * inside delete and rename calls.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.implicit.dir.repair.enable",
          GoogleCloudStorageOptions.AUTO_REPAIR_IMPLICIT_DIRECTORIES_DEFAULT);

  /**
   * Configuration key for enabling check to ensure that conflicting directories do not exist when
   * creating files and conflicting files do not exist when creating directories.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_CREATE_ITEMS_CONFLICT_CHECK_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.create.items.conflict.check.enable", true);

  /** Configuration key for customizing glob search algorithm. */
  public static final HadoopConfigurationProperty<GlobAlgorithm> GCS_GLOB_ALGORITHM =
      new HadoopConfigurationProperty<>("fs.gs.glob.algorithm", GlobAlgorithm.CONCURRENT);

  /** Configuration key for marker file pattern. Default value: none */
  public static final HadoopConfigurationProperty<String> GCS_MARKER_FILE_PATTERN =
      new HadoopConfigurationProperty<>("fs.gs.marker.file.pattern");

  /** Configuration key for a max number of GCS RPCs in batch request. */
  public static final HadoopConfigurationProperty<Long> GCS_MAX_REQUESTS_PER_BATCH =
      new HadoopConfigurationProperty<>(
          "fs.gs.max.requests.per.batch", MAX_REQUESTS_PER_BATCH_DEFAULT);

  /** Configuration key for a number of threads to execute batch requests. */
  public static final HadoopConfigurationProperty<Integer> GCS_BATCH_THREADS =
      new HadoopConfigurationProperty<>("fs.gs.batch.threads", BATCH_THREADS_DEFAULT);

  /**
   * Configuration key for enabling the use of Rewrite requests for copy operations. Rewrite request
   * has the same effect as Copy request, but it can handle moving large objects that may
   * potentially timeout a Copy request.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_COPY_WITH_REWRITE_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.copy.with.rewrite.enable", COPY_WITH_REWRITE_DEFAULT);

  /**
   * Configuration key for specifying max number of bytes rewritten in a single rewrite request when
   * fs.gs.copy.with.rewrite.enable is set to 'true'.
   */
  public static final HadoopConfigurationProperty<Long> GCS_REWRITE_MAX_BYTES_PER_CALL =
      new HadoopConfigurationProperty<>(
          "fs.gs.rewrite.max.bytes.per.call", MAX_BYTES_REWRITTEN_PER_CALL_DEFAULT);

  /** Configuration key for number of items to return per call to the list* GCS RPCs. */
  public static final HadoopConfigurationProperty<Long> GCS_MAX_LIST_ITEMS_PER_CALL =
      new HadoopConfigurationProperty<>(
          "fs.gs.list.max.items.per.call", MAX_LIST_ITEMS_PER_CALL_DEFAULT);

  /**
   * Configuration key for the max number of retries for failed HTTP request to GCS. Note that the
   * connector will retry *up to* the number of times as specified, using a default
   * ExponentialBackOff strategy.
   *
   * <p>Also, note that this number will only control the number of retries in the low level HTTP
   * request implementation.
   */
  public static final HadoopConfigurationProperty<Integer> GCS_HTTP_MAX_RETRY =
      new HadoopConfigurationProperty<>("fs.gs.http.max.retry", MAX_HTTP_REQUEST_RETRIES);

  /** Configuration key for the connect timeout (in millisecond) for HTTP request to GCS. */
  public static final HadoopConfigurationProperty<Integer> GCS_HTTP_CONNECT_TIMEOUT =
      new HadoopConfigurationProperty<>("fs.gs.http.connect-timeout", HTTP_REQUEST_CONNECT_TIMEOUT);

  /** Configuration key for adding a suffix to the GHFS application name sent to GCS. */
  public static final HadoopConfigurationProperty<String> GCS_APPLICATION_NAME_SUFFIX =
      new HadoopConfigurationProperty<>("fs.gs.application.name.suffix", "");

  /**
   * Configuration key for modifying the maximum amount of time to wait for empty object creation.
   */
  public static final HadoopConfigurationProperty<Integer> GCS_MAX_WAIT_MILLIS_EMPTY_OBJECT_CREATE =
      new HadoopConfigurationProperty<>(
          "fs.gs.max.wait.for.empty.object.creation.ms", MAX_WAIT_MILLIS_FOR_EMPTY_OBJECT_CREATION);

  /** Configuration key for setting write buffer size. */
  public static final HadoopConfigurationProperty<Integer> GCS_OUTPUT_STREAM_BUFFER_SIZE =
      new HadoopConfigurationProperty<>("fs.gs.outputstream.buffer.size", 8 * 1024 * 1024);

  /** Configuration key for setting pipe buffer size. */
  public static final HadoopConfigurationProperty<Integer> GCS_OUTPUT_STREAM_PIPE_BUFFER_SIZE =
      new HadoopConfigurationProperty<>("fs.gs.outputstream.pipe.buffer.size", 1024 * 1024);

  /** Configuration key for setting pipe type. */
  public static final HadoopConfigurationProperty<PipeType> GCS_OUTPUT_STREAM_PIPE_TYPE =
      new HadoopConfigurationProperty<>("fs.gs.outputstream.pipe.type", PipeType.IO_STREAM_PIPE);

  /** Configuration key for setting GCS upload chunk size. */
  // chunk size etc. Get the following value from GCSWC class in a better way. For now, we hard code
  // it to a known good value.
  public static final HadoopConfigurationProperty<Integer> GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE =
      new HadoopConfigurationProperty<>(
          "fs.gs.outputstream.upload.chunk.size", 64 * 1024 * 1024, "fs.gs.io.buffersize.write");

  /** Configuration for setting GCS upload cache size. */
  public static final HadoopConfigurationProperty<Integer> GCS_OUTPUT_STREAM_UPLOAD_CACHE_SIZE =
      new HadoopConfigurationProperty<>("fs.gs.outputstream.upload.cache.size", 0);

  /** Configuration key for enabling GCS direct upload. */
  public static final HadoopConfigurationProperty<Boolean> GCS_OUTPUT_STREAM_DIRECT_UPLOAD_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.outputstream.direct.upload.enable", false);

  /**
   * Configuration key for the minimal time interval between consecutive sync/hsync/hflush calls.
   */
  public static final HadoopConfigurationProperty<Integer> GCS_OUTPUT_STREAM_SYNC_MIN_INTERVAL_MS =
      new HadoopConfigurationProperty<>("fs.gs.outputstream.sync.min.interval.ms", 0);

  /**
   * If {@code true}, on opening a file we will proactively perform a metadata {@code GET} to check
   * whether the object exists, even though the underlying channel will not open a data stream until
   * {@code read()} is actually called. This is necessary to technically match the expected behavior
   * of Hadoop filesystems, but incurs an extra latency overhead on {@code open()}. If the calling
   * code can handle late failures on not-found errors, or has independently already ensured that a
   * file exists before calling {@code open()}, then you can set this to {@code false} for more
   * efficient reads.
   *
   * <p>Note, this is known to not work with YARN {@code CommonNodeLabelsManager} and potentially
   * other Hadoop components. That's why it's not recommended to set this property to {@code false}
   * cluster-wide, instead set it for a specific job/application that is compatible with it.
   */
  public static final HadoopConfigurationProperty<Boolean>
      GCS_INPUT_STREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE =
          new HadoopConfigurationProperty<>(
              "fs.gs.inputstream.fast.fail.on.not.found.enable",
              DEFAULT_FAST_FAIL_ON_NOT_FOUND_ENABLED);

  /**
   * If false, reading a file with GZIP content encoding (HTTP header "Content-Encoding: gzip") will
   * result in failure (IOException is thrown).
   */
  public static final HadoopConfigurationProperty<Boolean>
      GCS_INPUT_STREAM_SUPPORT_GZIP_ENCODING_ENABLE =
          new HadoopConfigurationProperty<>(
              "fs.gs.inputstream.support.gzip.encoding.enable",
              DEFAULT_GZIP_ENCODING_SUPPORT_ENABLED);

  /**
   * If forward seeks are within this many bytes of the current position, seeks are performed by
   * reading and discarding bytes in-place rather than opening a new underlying stream.
   */
  public static final HadoopConfigurationProperty<Long> GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT =
      new HadoopConfigurationProperty<>(
          "fs.gs.inputstream.inplace.seek.limit", DEFAULT_INPLACE_SEEK_LIMIT);

  /** Tunes reading objects behavior to optimize HTTP GET requests for various use cases. */
  public static final HadoopConfigurationProperty<Fadvise> GCS_INPUT_STREAM_FADVISE =
      new HadoopConfigurationProperty<>("fs.gs.inputstream.fadvise", DEFAULT_FADVISE);

  /**
   * Minimum size in bytes of the HTTP Range header set in GCS request when opening new stream to
   * read an object.
   */
  public static final HadoopConfigurationProperty<Long> GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE =
      new HadoopConfigurationProperty<>(
          "fs.gs.inputstream.min.range.request.size",
          GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE);

  /** Configuration key for enabling use of the gRPC API for read/write. */
  public static final HadoopConfigurationProperty<Boolean> GCS_GRPC_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.grpc.enable", DEFAULT_GRPC_ENABLED);

  /** Configuration key for enabling checksum validation for the gRPC API. */
  public static final HadoopConfigurationProperty<Boolean> GCS_GRPC_CHECKSUMS_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.checksums.enable", DEFAULT_GRPC_CHECKSUMS_ENABLED);

  /** Configuration key for the Cloud Storage gRPC server address. */
  public static final HadoopConfigurationProperty<String> GCS_GRPC_SERVER_ADDRESS =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.server.address", GoogleCloudStorageOptions.DEFAULT_GCS_GRPC_SERVER_ADDRESS);

  /** Configuration key for check interval (in millisecond) for gRPC request timeout to GCS. */
  public static final HadoopConfigurationProperty<Long> GCS_GRPC_CHECK_INTERVAL_TIMEOUT_MS =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.checkinterval.timeout.ms",
          DEFAULT_GRPC_MESSAGE_TIMEOUT_CHECK_INTERVAL_MILLIS);

  /**
   * Configuration key for the connection timeout (in millisecond) for gRPC read requests to GCS.
   */
  public static final HadoopConfigurationProperty<Long> GCS_GRPC_READ_TIMEOUT_MS =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.read.timeout.ms", DEFAULT_GRPC_READ_TIMEOUT_MILLIS);

  /** Configuration key for the message timeout (in millisecond) for gRPC read requests to GCS. */
  public static final HadoopConfigurationProperty<Long> GCS_GRPC_READ_MESSAGE_TIMEOUT_MS =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.read.message.timeout.ms", DEFAULT_GRPC_READ_MESSAGE_TIMEOUT_MILLIS);

  /** Configuration key for enabling the zero-copy deserializer for the gRPC API. */
  public static final HadoopConfigurationProperty<Boolean> GCS_GRPC_READ_ZEROCOPY_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.read.zerocopy.enable", DEFAULT_GRPC_READ_ZEROCOPY_ENABLED);

  /** Configuration key for the number of requests to be buffered for uploads to GCS. */
  public static final HadoopConfigurationProperty<Long> GCS_GRPC_UPLOAD_BUFFERED_REQUESTS =
      new HadoopConfigurationProperty<>("fs.gs.grpc.write.buffered.requests", 20L);

  /** Configuration key for the connect timeout (in millisecond) for gRPC write requests to GCS. */
  public static final HadoopConfigurationProperty<Long> GCS_GRPC_WRITE_TIMEOUT_MS =
      new HadoopConfigurationProperty<>("fs.gs.grpc.write.timeout.ms", 10 * 60 * 1000L);

  /** Configuration key for the message timeout (in millisecond) for gRPC write requests to GCS. */
  public static final HadoopConfigurationProperty<Long> GCS_GRPC_WRITE_MESSAGE_TIMEOUT_MS =
      new HadoopConfigurationProperty<>("fs.gs.grpc.write.message.timeout.ms", 3 * 1_000L);

  /** Configuration key for enabling use of directpath gRPC API for read/write. */
  public static final HadoopConfigurationProperty<Boolean> GCS_GRPC_DIRECTPATH_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.directpath.enable", DEFAULT_DIRECT_PATH_PREFERRED);

  /** Configuration key for enabling use of traffic director gRPC API for read/write. */
  public static final HadoopConfigurationProperty<Boolean> GCS_GRPC_TRAFFICDIRECTOR_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.trafficdirector.enable", DEFAULT_TRAFFIC_DIRECTOR_ENABLED);

  /** Configuration key for the headers for HTTP request to GCS. */
  public static final HadoopConfigurationProperty<Map<String, String>> GCS_HTTP_HEADERS =
      new HadoopConfigurationProperty<>("fs.gs.storage.http.headers.", ImmutableMap.of());

  /** Configuration key for the CSEK encryption algorithm. */
  public static final HadoopConfigurationProperty<String> GCS_ENCRYPTION_ALGORITHM =
      new HadoopConfigurationProperty<>("fs.gs.encryption.algorithm");

  /** Configuration key for the CSEK encryption key. */
  public static final HadoopConfigurationProperty<RedactedString> GCS_ENCRYPTION_KEY =
      new HadoopConfigurationProperty<>("fs.gs.encryption.key");

  /** Configuration key for sha256 hash of the CSEK encryption key. */
  public static final HadoopConfigurationProperty<RedactedString> GCS_ENCRYPTION_KEY_HASH =
      new HadoopConfigurationProperty<>("fs.gs.encryption.key.hash");

  /** Configuration key to enable publishing metrics to Google cloud monitoring. */
  public static final HadoopConfigurationProperty<MetricsSink> GCS_METRICS_SINK =
      new HadoopConfigurationProperty<>("fs.gs.metrics.sink", MetricsSink.NONE);

  /** Configuration key to enable publishing metrics to Google cloud monitoring. */
  public static final HadoopConfigurationProperty<FilesystemAPI> GCS_FILESYSTEM_API =
      new HadoopConfigurationProperty<>("fs.gs.filesystem.api", FilesystemAPI.OBJECT);

  /** Configuration key to enable logging of additional trace details. */
  public static final HadoopConfigurationProperty<Boolean> GCS_TRACE_LOG_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.tracelog.enable", false);

  /** Configuration key to enable using google-cloud-storage client for connecting with GCS */
  public static final HadoopConfigurationProperty<Boolean> GCS_JAVA_CLIENT_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.javaclient.enable", false);

  // TODO(b/120887495): This @VisibleForTesting annotation was being ignored by prod code.
  // Please check that removing it is correct, and remove this comment along with it.
  // @VisibleForTesting
  static GoogleCloudStorageFileSystemOptions.Builder getGcsFsOptionsBuilder(Configuration config) {
    return GoogleCloudStorageFileSystemOptions.builder()
        .setCloudStorageOptions(getGcsOptionsBuilder(config).build())
        .setBucketDeleteEnabled(GCE_BUCKET_DELETE_ENABLE.get(config, config::getBoolean))
        .setEnsureNoConflictingItems(
            GCS_CREATE_ITEMS_CONFLICT_CHECK_ENABLE.get(config, config::getBoolean))
        .setMarkerFilePattern(GCS_MARKER_FILE_PATTERN.get(config, config::get))
        .setPerformanceCacheEnabled(GCS_PERFORMANCE_CACHE_ENABLE.get(config, config::getBoolean))
        .setPerformanceCacheOptions(getPerformanceCachingOptions(config))
        .setStatusParallelEnabled(GCS_STATUS_PARALLEL_ENABLE.get(config, config::getBoolean))
        .setJavaClientEnabled(GCS_JAVA_CLIENT_ENABLE.get(config, config::getBoolean))
        .setFilesystemApi(GCS_FILESYSTEM_API.get(config, config::getEnum));
  }

  @VisibleForTesting
  static GoogleCloudStorageOptions.Builder getGcsOptionsBuilder(Configuration config) {
    String projectId = GCS_PROJECT_ID.get(config, config::get);
    return GoogleCloudStorageOptions.builder()
        .setStorageRootUrl(GCS_ROOT_URL.get(config, config::get))
        .setStorageServicePath(GCS_SERVICE_PATH.get(config, config::get))
        .setAutoRepairImplicitDirectoriesEnabled(
            GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE.get(config, config::getBoolean))
        .setCopyWithRewriteEnabled(GCS_COPY_WITH_REWRITE_ENABLE.get(config, config::getBoolean))
        .setMaxBytesRewrittenPerCall(GCS_REWRITE_MAX_BYTES_PER_CALL.get(config, config::getLong))
        .setProxyAddress(
            PROXY_ADDRESS_SUFFIX.withPrefixes(CONFIG_KEY_PREFIXES).get(config, config::get))
        .setProxyUsername(
            PROXY_USERNAME_SUFFIX.withPrefixes(CONFIG_KEY_PREFIXES).getPassword(config))
        .setProxyPassword(
            PROXY_PASSWORD_SUFFIX.withPrefixes(CONFIG_KEY_PREFIXES).getPassword(config))
        .setProjectId(projectId)
        .setMaxListItemsPerCall(GCS_MAX_LIST_ITEMS_PER_CALL.get(config, config::getLong))
        .setMaxRequestsPerBatch(GCS_MAX_REQUESTS_PER_BATCH.get(config, config::getLong))
        .setBatchThreads(GCS_BATCH_THREADS.get(config, config::getInt))
        .setMaxHttpRequestRetries(GCS_HTTP_MAX_RETRY.get(config, config::getInt))
        .setHttpRequestConnectTimeout(GCS_HTTP_CONNECT_TIMEOUT.get(config, config::getInt))
        .setHttpRequestReadTimeout(
            READ_TIMEOUT_SUFFIX.withPrefixes(CONFIG_KEY_PREFIXES).get(config, config::getInt))
        .setAppName(getApplicationName(config))
        .setMaxWaitMillisForEmptyObjectCreation(
            GCS_MAX_WAIT_MILLIS_EMPTY_OBJECT_CREATE.get(config, config::getInt))
        .setReadChannelOptions(getReadChannelOptions(config))
        .setWriteChannelOptions(getWriteChannelOptions(config))
        .setRequesterPaysOptions(getRequesterPaysOptions(config, projectId))
        .setHttpRequestHeaders(GCS_HTTP_HEADERS.getPropsWithPrefix(config))
        .setEncryptionAlgorithm(GCS_ENCRYPTION_ALGORITHM.get(config, config::get))
        .setEncryptionKey(GCS_ENCRYPTION_KEY.getPassword(config))
        .setEncryptionKeyHash(GCS_ENCRYPTION_KEY_HASH.getPassword(config))
        .setGrpcEnabled(GCS_GRPC_ENABLE.get(config, config::getBoolean))
        .setGrpcServerAddress(GCS_GRPC_SERVER_ADDRESS.get(config, config::get))
        .setGrpcMessageTimeoutCheckInterval(
            GCS_GRPC_CHECK_INTERVAL_TIMEOUT_MS.get(config, config::getLong))
        .setDirectPathPreferred(GCS_GRPC_DIRECTPATH_ENABLE.get(config, config::getBoolean))
        .setTrafficDirectorEnabled(GCS_GRPC_TRAFFICDIRECTOR_ENABLE.get(config, config::getBoolean))
        .setMetricsSink(GCS_METRICS_SINK.get(config, config::getEnum))
        .setTraceLogEnabled(GCS_TRACE_LOG_ENABLE.get(config, config::getBoolean));
  }

  private static PerformanceCachingGoogleCloudStorageOptions getPerformanceCachingOptions(
      Configuration config) {
    return PerformanceCachingGoogleCloudStorageOptions.builder()
        .setMaxEntryAgeMillis(
            GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE_MILLIS.get(config, config::getLong))
        .build();
  }

  private static String getApplicationName(Configuration config) {
    String appNameSuffix = nullToEmpty(GCS_APPLICATION_NAME_SUFFIX.get(config, config::get));
    String applicationName = GoogleHadoopFileSystem.GHFS_ID + appNameSuffix;
    logger.atFiner().log("getApplicationName(config: %s): %s", config, applicationName);
    return applicationName;
  }

  private static GoogleCloudStorageReadOptions getReadChannelOptions(Configuration config) {
    return GoogleCloudStorageReadOptions.builder()
        .setFastFailOnNotFoundEnabled(
            GCS_INPUT_STREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE.get(config, config::getBoolean))
        .setGzipEncodingSupportEnabled(
            GCS_INPUT_STREAM_SUPPORT_GZIP_ENCODING_ENABLE.get(config, config::getBoolean))
        .setInplaceSeekLimit(GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT.get(config, config::getLong))
        .setFadvise(GCS_INPUT_STREAM_FADVISE.get(config, config::getEnum))
        .setMinRangeRequestSize(
            GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE.get(config, config::getLongBytes))
        .setGrpcChecksumsEnabled(GCS_GRPC_CHECKSUMS_ENABLE.get(config, config::getBoolean))
        .setGrpcReadTimeoutMillis(GCS_GRPC_READ_TIMEOUT_MS.get(config, config::getLong))
        .setGrpcReadMessageTimeoutMillis(
            GCS_GRPC_READ_MESSAGE_TIMEOUT_MS.get(config, config::getLong))
        .setGrpcReadZeroCopyEnabled(GCS_GRPC_READ_ZEROCOPY_ENABLE.get(config, config::getBoolean))
        .build();
  }

  private static AsyncWriteChannelOptions getWriteChannelOptions(Configuration config) {
    return AsyncWriteChannelOptions.builder()
        .setBufferSize(GCS_OUTPUT_STREAM_BUFFER_SIZE.get(config, config::getInt))
        .setPipeBufferSize(GCS_OUTPUT_STREAM_PIPE_BUFFER_SIZE.get(config, config::getInt))
        .setPipeType(GCS_OUTPUT_STREAM_PIPE_TYPE.get(config, config::getEnum))
        .setUploadChunkSize(GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE.get(config, config::getInt))
        .setUploadCacheSize(GCS_OUTPUT_STREAM_UPLOAD_CACHE_SIZE.get(config, config::getInt))
        .setDirectUploadEnabled(
            GCS_OUTPUT_STREAM_DIRECT_UPLOAD_ENABLE.get(config, config::getBoolean))
        .setGrpcChecksumsEnabled(GCS_GRPC_CHECKSUMS_ENABLE.get(config, config::getBoolean))
        .setGrpcWriteTimeout(GCS_GRPC_WRITE_TIMEOUT_MS.get(config, config::getLong))
        .setGrpcWriteMessageTimeoutMillis(
            GCS_GRPC_WRITE_MESSAGE_TIMEOUT_MS.get(config, config::getLong))
        .setNumberOfBufferedRequests(GCS_GRPC_UPLOAD_BUFFERED_REQUESTS.get(config, config::getLong))
        .build();
  }

  private static RequesterPaysOptions getRequesterPaysOptions(
      Configuration config, String projectId) {
    String requesterPaysProjectId = GCS_REQUESTER_PAYS_PROJECT_ID.get(config, config::get);
    return RequesterPaysOptions.builder()
        .setMode(GCS_REQUESTER_PAYS_MODE.get(config, config::getEnum))
        .setProjectId(requesterPaysProjectId == null ? projectId : requesterPaysProjectId)
        .setBuckets(GCS_REQUESTER_PAYS_BUCKETS.getStringCollection(config))
        .build();
  }
}
