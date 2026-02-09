/*
 * Copyright 2013 Google Inc.
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

import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.PROXY_ADDRESS_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.PROXY_PASSWORD_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.PROXY_USERNAME_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.READ_TIMEOUT_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.getConfigKeyPrefixes;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.Math.toIntExact;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem.GcsFileChecksumType;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem.GlobAlgorithm;
import com.google.cloud.hadoop.gcsio.CreateFileOptions;
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
import com.google.cloud.hadoop.util.RedactedString;
import com.google.cloud.hadoop.util.RequesterPaysOptions;
import com.google.cloud.hadoop.util.RequesterPaysOptions.RequesterPaysMode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
          "fs.gs.storage.root.url", GoogleCloudStorageOptions.DEFAULT.getStorageRootUrl());

  /** Configuration key for the Cloud Storage API endpoint service path. */
  public static final HadoopConfigurationProperty<String> GCS_SERVICE_PATH =
      new HadoopConfigurationProperty<>(
          "fs.gs.storage.service.path", GoogleCloudStorageOptions.DEFAULT.getStorageServicePath());

  /**
   * Key for the permissions that we report a file or directory to have. Can either be octal or
   * symbolic mode accepted by {@link FsPermission#FsPermission(String)}
   *
   * <p>Default value for the permissions that we report a file or directory to have. Note: We do
   * not really support file/dir permissions, but we need to report some permission value when
   * Hadoop calls getFileStatus(). A MapReduce job fails if we report permissions more relaxed than
   * the value below and this is the default File System.
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

  /**
   * Configuration key for enabling hierarchical namespace buckets.
   *
   * <p>If this is enabled, rename folder operation on a Hierarchical namespace enabled bucket will
   * be performed by calling the rename API.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_HIERARCHICAL_NAMESPACE_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.hierarchical.namespace.folders.enable", false);

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
      new HadoopConfigurationProperty<>(
          "fs.gs.bucket.delete.enable",
          GoogleCloudStorageFileSystemOptions.DEFAULT.isBucketDeleteEnabled());

  /** Configuration key for GCS project ID. Default value: "DISABLED" */
  public static final HadoopConfigurationProperty<RequesterPaysMode> GCS_REQUESTER_PAYS_MODE =
      new HadoopConfigurationProperty<>(
          "fs.gs.requester.pays.mode", RequesterPaysOptions.DEFAULT.getMode());

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
      new HadoopConfigurationProperty<>(
          "fs.gs.performance.cache.enable",
          GoogleCloudStorageFileSystemOptions.DEFAULT.isPerformanceCacheEnabled());

  /**
   * Configuration key for maximum time a GoogleCloudStorageItemInfo will remain "valid" in the
   * performance cache before it's invalidated.
   */
  public static final HadoopConfigurationProperty<Long> GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE =
      new HadoopConfigurationProperty<>(
          "fs.gs.performance.cache.max.entry.age",
          PerformanceCachingGoogleCloudStorageOptions.DEFAULT.getMaxEntryAge().toMillis());

  /**
   * If true, executes GCS requests in {@code listStatus} and {@code getFileStatus} methods in
   * parallel to reduce latency.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_STATUS_PARALLEL_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.status.parallel.enable",
          GoogleCloudStorageFileSystemOptions.DEFAULT.isStatusParallelEnabled());

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
          GoogleCloudStorageOptions.DEFAULT.isAutoRepairImplicitDirectoriesEnabled());

  /**
   * Configuration key for enabling check to ensure that conflicting directories do not exist when
   * creating files and conflicting files do not exist when creating directories.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_CREATE_ITEMS_CONFLICT_CHECK_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.create.items.conflict.check.enable",
          GoogleCloudStorageFileSystemOptions.DEFAULT.isEnsureNoConflictingItems());

  /** Configuration key for customizing glob search algorithm. */
  public static final HadoopConfigurationProperty<GlobAlgorithm> GCS_GLOB_ALGORITHM =
      new HadoopConfigurationProperty<>("fs.gs.glob.algorithm", GlobAlgorithm.CONCURRENT);

  /** Configuration key for marker file pattern. Default value: none */
  public static final HadoopConfigurationProperty<String> GCS_MARKER_FILE_PATTERN =
      new HadoopConfigurationProperty<>("fs.gs.marker.file.pattern");

  /** Configuration key for a max number of GCS RPCs in batch request. */
  public static final HadoopConfigurationProperty<Integer> GCS_MAX_REQUESTS_PER_BATCH =
      new HadoopConfigurationProperty<>(
          "fs.gs.max.requests.per.batch",
          GoogleCloudStorageOptions.DEFAULT.getMaxRequestsPerBatch());

  /** Configuration key for a number of threads to execute batch requests. */
  public static final HadoopConfigurationProperty<Integer> GCS_BATCH_THREADS =
      new HadoopConfigurationProperty<>(
          "fs.gs.batch.threads", GoogleCloudStorageOptions.DEFAULT.getBatchThreads());

  /**
   * Configuration key for number of request to track for adapting the access pattern i.e. fadvise:
   * AUTO & AUTO_RANDOM.
   */
  public static final HadoopConfigurationProperty<Integer> GCS_FADVISE_REQUEST_TRACK_COUNT =
      new HadoopConfigurationProperty<>("fs.gs.fadvise.request.track.count", 3);

  /**
   * Configuration key for enabling the use of Rewrite requests for copy operations. Rewrite request
   * has the same effect as Copy request, but it can handle moving large objects that may
   * potentially time out a Copy request.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_COPY_WITH_REWRITE_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.copy.with.rewrite.enable",
          GoogleCloudStorageOptions.DEFAULT.isCopyWithRewriteEnabled());

  /**
   * Configuration key for specifying max number of bytes rewritten in a single rewrite request when
   * fs.gs.copy.with.rewrite.enable is set to 'true'.
   */
  public static final HadoopConfigurationProperty<Long> GCS_REWRITE_MAX_CHUNK_SIZE =
      new HadoopConfigurationProperty<>(
          "fs.gs.rewrite.max.chunk.size",
          GoogleCloudStorageOptions.DEFAULT.getMaxRewriteChunkSize());

  /** Configuration key for number of items to return per call to the list* GCS RPCs. */
  public static final HadoopConfigurationProperty<Integer> GCS_MAX_LIST_ITEMS_PER_CALL =
      new HadoopConfigurationProperty<>(
          "fs.gs.list.max.items.per.call",
          GoogleCloudStorageOptions.DEFAULT.getMaxListItemsPerCall());

  /**
   * Configuration key for the max number of retries for failed HTTP request to GCS. Note that the
   * connector will retry *up to* the number of times as specified, using a default
   * ExponentialBackOff strategy.
   *
   * <p>Also, note that this number will only control the number of retries in the low level HTTP
   * request implementation.
   */
  public static final HadoopConfigurationProperty<Integer> GCS_HTTP_MAX_RETRY =
      new HadoopConfigurationProperty<>(
          "fs.gs.http.max.retry", GoogleCloudStorageOptions.DEFAULT.getMaxHttpRequestRetries());

  /** Configuration key for the connect timeout for HTTP request to GCS. */
  public static final HadoopConfigurationProperty<Long> GCS_HTTP_CONNECT_TIMEOUT =
      new HadoopConfigurationProperty<>(
          "fs.gs.http.connect-timeout",
          GoogleCloudStorageOptions.DEFAULT.getHttpRequestConnectTimeout().toMillis());

  /** Configuration key for adding a suffix to the GHFS application name sent to GCS. */
  public static final HadoopConfigurationProperty<String> GCS_APPLICATION_NAME_SUFFIX =
      new HadoopConfigurationProperty<>("fs.gs.application.name.suffix", "");

  /**
   * Configuration key for modifying the maximum amount of time to wait for empty object creation.
   */
  public static final HadoopConfigurationProperty<Long> GCS_MAX_WAIT_TIME_EMPTY_OBJECT_CREATE =
      new HadoopConfigurationProperty<>(
          "fs.gs.max.wait.for.empty.object.creation",
          GoogleCloudStorageOptions.DEFAULT.getMaxWaitTimeForEmptyObjectCreation().toMillis());

  /** Configuration key for setting write buffer size. */
  public static final HadoopConfigurationProperty<Long> GCS_OUTPUT_STREAM_BUFFER_SIZE =
      new HadoopConfigurationProperty<>(
          "fs.gs.outputstream.buffer.size",
          (long) AsyncWriteChannelOptions.DEFAULT.getBufferSize());

  /** Configuration key for setting pipe buffer size. */
  public static final HadoopConfigurationProperty<Long> GCS_OUTPUT_STREAM_PIPE_BUFFER_SIZE =
      new HadoopConfigurationProperty<>(
          "fs.gs.outputstream.pipe.buffer.size",
          (long) AsyncWriteChannelOptions.DEFAULT.getPipeBufferSize());

  /** Configuration key for setting pipe type. */
  public static final HadoopConfigurationProperty<PipeType> GCS_OUTPUT_STREAM_PIPE_TYPE =
      new HadoopConfigurationProperty<>(
          "fs.gs.outputstream.pipe.type", AsyncWriteChannelOptions.DEFAULT.getPipeType());

  /** Configuration key for setting GCS upload chunk size. */
  // chunk size etc. Get the following value from GCSWC class in a better way. For now, we hard code
  // it to a known good value.
  public static final HadoopConfigurationProperty<Long> GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE =
      new HadoopConfigurationProperty<>(
          "fs.gs.outputstream.upload.chunk.size",
          (long) AsyncWriteChannelOptions.DEFAULT.getUploadChunkSize());

  /** Configuration for setting GCS upload cache size. */
  public static final HadoopConfigurationProperty<Long> GCS_OUTPUT_STREAM_UPLOAD_CACHE_SIZE =
      new HadoopConfigurationProperty<>(
          "fs.gs.outputstream.upload.cache.size",
          (long) AsyncWriteChannelOptions.DEFAULT.getUploadCacheSize());

  /** Configuration key for enabling GCS direct upload. */
  public static final HadoopConfigurationProperty<Boolean> GCS_OUTPUT_STREAM_DIRECT_UPLOAD_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.outputstream.direct.upload.enable",
          AsyncWriteChannelOptions.DEFAULT.isDirectUploadEnabled());

  /**
   * Configuration key for the minimal time interval between consecutive sync/hsync/hflush calls.
   */
  public static final HadoopConfigurationProperty<Long> GCS_OUTPUT_STREAM_SYNC_MIN_INTERVAL =
      new HadoopConfigurationProperty<>(
          "fs.gs.outputstream.sync.min.interval",
          CreateFileOptions.DEFAULT.getMinSyncInterval().toMillis());

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
              GoogleCloudStorageReadOptions.DEFAULT.isFastFailOnNotFoundEnabled());

  /**
   * If false, reading a file with GZIP content encoding (HTTP header "Content-Encoding: gzip") will
   * result in failure (IOException is thrown).
   */
  public static final HadoopConfigurationProperty<Boolean>
      GCS_INPUT_STREAM_SUPPORT_GZIP_ENCODING_ENABLE =
          new HadoopConfigurationProperty<>(
              "fs.gs.inputstream.support.gzip.encoding.enable",
              GoogleCloudStorageReadOptions.DEFAULT.isGzipEncodingSupportEnabled());

  /**
   * If forward seeks are within this many bytes of the current position, seeks are performed by
   * reading and discarding bytes in-place rather than opening a new underlying stream.
   */
  public static final HadoopConfigurationProperty<Long> GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT =
      new HadoopConfigurationProperty<>(
          "fs.gs.inputstream.inplace.seek.limit",
          GoogleCloudStorageReadOptions.DEFAULT.getInplaceSeekLimit());

  /** Tunes reading objects behavior to optimize HTTP GET requests for various use cases. */
  public static final HadoopConfigurationProperty<Fadvise> GCS_INPUT_STREAM_FADVISE =
      new HadoopConfigurationProperty<>(
          "fs.gs.inputstream.fadvise", GoogleCloudStorageReadOptions.DEFAULT.getFadvise());

  /**
   * Minimum size in bytes of the HTTP Range header set in GCS request when opening new stream to
   * read an object.
   */
  public static final HadoopConfigurationProperty<Long> GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE =
      new HadoopConfigurationProperty<>(
          "fs.gs.inputstream.min.range.request.size",
          GoogleCloudStorageReadOptions.DEFAULT.getMinRangeRequestSize());

  /** Minimum distance that will be seeked without merging the ranges together. */
  public static final HadoopConfigurationProperty<Integer> GCS_VECTORED_READ_RANGE_MIN_SEEK =
      new HadoopConfigurationProperty<>(
          "fs.gs.vectored.read.min.range.seek.size",
          VectoredReadOptions.DEFAULT.getMinSeekVectoredReadSize());

  /** Maximum size allowed for a merged range request. */
  public static final HadoopConfigurationProperty<Integer> GCS_VECTORED_READ_MERGED_RANGE_MAX_SIZE =
      new HadoopConfigurationProperty<>(
          "fs.gs.vectored.read.merged.range.max.size",
          VectoredReadOptions.DEFAULT.getMergeRangeMaxSize());

  /** Maximum threads to process individual FileRange requests */
  public static final HadoopConfigurationProperty<Integer> GCS_VECTORED_READ_THREADS =
      new HadoopConfigurationProperty<>(
          "fs.gs.vectored.read.threads", VectoredReadOptions.DEFAULT.getReadThreads());

  /** Configuration key for enabling use of the gRPC API for read/write. */
  public static final HadoopConfigurationProperty<Boolean> GCS_GRPC_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.enable", GoogleCloudStorageOptions.DEFAULT.isGrpcEnabled());

  /** Configuration key for enabling checksum validation for the gRPC API. */
  public static final HadoopConfigurationProperty<Boolean> GCS_GRPC_CHECKSUMS_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.checksums.enable",
          GoogleCloudStorageReadOptions.DEFAULT.isGrpcChecksumsEnabled());

  /** Configuration key for check interval for gRPC request timeout to GCS. */
  public static final HadoopConfigurationProperty<Long> GCS_GRPC_CHECK_INTERVAL_TIMEOUT =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.checkinterval.timeout",
          GoogleCloudStorageOptions.DEFAULT.getGrpcMessageTimeoutCheckInterval().toMillis());

  /** Configuration key for the connection timeout for gRPC read requests to GCS. */
  public static final HadoopConfigurationProperty<Long> GCS_GRPC_READ_TIMEOUT =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.read.timeout",
          GoogleCloudStorageReadOptions.DEFAULT.getGrpcReadTimeout().toMillis());

  /** Configuration key for the message timeout for gRPC read requests to GCS. */
  public static final HadoopConfigurationProperty<Long> GCS_GRPC_READ_MESSAGE_TIMEOUT =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.read.message.timeout",
          GoogleCloudStorageReadOptions.DEFAULT.getGrpcReadMessageTimeout().toMillis());

  /** Configuration key for enabling the zero-copy deserializer for the gRPC API. */
  public static final HadoopConfigurationProperty<Boolean> GCS_GRPC_READ_ZEROCOPY_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.read.zerocopy.enable",
          GoogleCloudStorageReadOptions.DEFAULT.isGrpcReadZeroCopyEnabled());

  /** Configuration key for the number of requests to be buffered for uploads to GCS. */
  public static final HadoopConfigurationProperty<Integer> GCS_GRPC_UPLOAD_BUFFERED_REQUESTS =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.write.buffered.requests",
          AsyncWriteChannelOptions.DEFAULT.getNumberOfBufferedRequests());

  /** Configuration key for the connect timeout for gRPC write requests to GCS. */
  public static final HadoopConfigurationProperty<Long> GCS_GRPC_WRITE_TIMEOUT =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.write.timeout",
          AsyncWriteChannelOptions.DEFAULT.getGrpcWriteTimeout().toMillis());

  /** Configuration key for the message timeout for gRPC write requests to GCS. */
  public static final HadoopConfigurationProperty<Long> GCS_GRPC_WRITE_MESSAGE_TIMEOUT =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.write.message.timeout",
          AsyncWriteChannelOptions.DEFAULT.getGrpcWriteMessageTimeout().toMillis());

  /** Configuration key for enabling use of directpath gRPC API for read/write. */
  public static final HadoopConfigurationProperty<Boolean> GCS_GRPC_DIRECTPATH_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.directpath.enable",
          GoogleCloudStorageOptions.DEFAULT.isDirectPathPreferred());

  /** Configuration key for enabling use of traffic director gRPC API for read/write. */
  public static final HadoopConfigurationProperty<Boolean> GCS_GRPC_TRAFFICDIRECTOR_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.trafficdirector.enable",
          GoogleCloudStorageOptions.DEFAULT.isTrafficDirectorEnabled());

  /** Configuration key for the headers for HTTP request to GCS. */
  public static final HadoopConfigurationProperty<Map<String, String>> GCS_HTTP_HEADERS =
      new HadoopConfigurationProperty<>(
          "fs.gs.storage.http.headers.", GoogleCloudStorageOptions.DEFAULT.getHttpRequestHeaders());

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
      new HadoopConfigurationProperty<>(
          "fs.gs.metrics.sink", GoogleCloudStorageOptions.DEFAULT.getMetricsSink());

  /** Configuration key to enable logging of additional trace details. */
  public static final HadoopConfigurationProperty<Boolean> GCS_TRACE_LOG_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.tracelog.enable", GoogleCloudStorageOptions.DEFAULT.isTraceLogEnabled());

  /** Configuration key to enable operational level traces */
  public static final HadoopConfigurationProperty<Boolean> GCS_OPERATION_TRACE_LOG_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.operation.tracelog.enable", GoogleCloudStorageOptions.DEFAULT.isTraceLogEnabled());

  /** Configuration key to export logs to Google cloud logging. */
  public static final HadoopConfigurationProperty<Boolean> GCS_CLOUD_LOGGING_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.cloud.logging.enable", false);

  /** Configuration key to configure client to use for GCS access. */
  public static final HadoopConfigurationProperty<ClientType> GCS_CLIENT_TYPE =
      new HadoopConfigurationProperty<>(
          "fs.gs.client.type", GoogleCloudStorageFileSystemOptions.DEFAULT.getClientType());

  /** Configuration key to configure client to use for GCS access. */
  public static final HadoopConfigurationProperty<Boolean> GCS_GRPC_WRITE_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.grpc.write.enable", GoogleCloudStorageOptions.DEFAULT.isGrpcWriteEnabled());

  /**
   * Configuration key to configure the properties to optimize gcs-write. This config will be
   * effective only if fs.gs.client.type is set to STORAGE_CLIENT.
   */
  public static final HadoopConfigurationProperty<UploadType> GCS_CLIENT_UPLOAD_TYPE =
      new HadoopConfigurationProperty<>("fs.gs.client.upload.type", UploadType.CHUNK_UPLOAD);

  /**
   * Configuration key to enable JAVA_STORAGE client caching across FS objects. This config will be
   * effective only if fs.gs.client.type is set to STORAGE_CLIENT.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_STORAGE_CLIENT_CACHING =
      new HadoopConfigurationProperty<>(
          "fs.gs.storage.client.cache.enable",
          GoogleCloudStorageOptions.DEFAULT.isStorageClientCachingEnabled());

  /**
   * Configuration key to configure the Path where uploads will be parked on disk. If not set then
   * uploads will be parked at default location pointed by java-storage client. This will only be
   * effective if fs.gs.client.upload.type is set to non-default value.
   */
  public static final HadoopConfigurationProperty<Collection<String>>
      GCS_WRITE_TEMPORARY_FILES_PATH =
          new HadoopConfigurationProperty<>("fs.gs.write.temporary.dirs", ImmutableSet.of());

  /**
   * Configuration key to configure the Buffers for UploadType.PARALLEL_COMPOSITE_UPLOAD. It is in
   * alignment with configuration of java-storage client
   * https://cloud.google.com/java/docs/reference/google-cloud-storage/latest/com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig.BufferAllocationStrategy#com_google_cloud_storage_ParallelCompositeUploadBlobWriteSessionConfig_BufferAllocationStrategy_fixedPool_int_int_
   */
  public static final HadoopConfigurationProperty<Integer> GCS_PCU_BUFFER_COUNT =
      new HadoopConfigurationProperty<>(
          "fs.gs.write.parallel.composite.upload.buffer.count",
          AsyncWriteChannelOptions.DEFAULT.getPCUBufferCount());

  /**
   * Configuration key to configure the buffer capacity for UploadType.PARALLEL_COMPOSITE_UPLOAD. It
   * is in alignment with configuration of java-storage client
   * https://cloud.google.com/java/docs/reference/google-cloud-storage/latest/com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig.BufferAllocationStrategy#com_google_cloud_storage_ParallelCompositeUploadBlobWriteSessionConfig_BufferAllocationStrategy_fixedPool_int_int_
   */
  public static final HadoopConfigurationProperty<Long> GCS_PCU_BUFFER_CAPACITY =
      new HadoopConfigurationProperty<>(
          "fs.gs.write.parallel.composite.upload.buffer.capacity",
          (long) AsyncWriteChannelOptions.DEFAULT.getPCUBufferCapacity());

  /**
   * Configuration key to clean up strategy of part files created via
   * UploadType.PARALLEL_COMPOSITE_UPLOAD. It is in alignment with configuration of java-storage
   * client
   * https://cloud.google.com/java/docs/reference/google-cloud-storage/latest/com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig.PartCleanupStrategy
   */
  public static final HadoopConfigurationProperty<PartFileCleanupType>
      GCS_PCU_PART_FILE_CLEANUP_TYPE =
          new HadoopConfigurationProperty<>(
              "fs.gs.write.parallel.composite.upload.part.file.cleanup.type",
              AsyncWriteChannelOptions.DEFAULT.getPartFileCleanupType());

  /**
   * Configuration key to set up the naming strategy of part files created via
   * UploadType.PARALLEL_COMPOSITE_UPLOAD. It is in alignment with configuration of java-storage
   * client
   * https://cloud.google.com/java/docs/reference/google-cloud-storage/latest/com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig.PartNamingStrategy
   */
  public static final HadoopConfigurationProperty<String> GCS_PCU_PART_FILE_NAME_PREFIX =
      new HadoopConfigurationProperty<>(
          "fs.gs.write.parallel.composite.upload.part.file.name.prefix",
          AsyncWriteChannelOptions.DEFAULT.getPartFileNamePrefix());

  /**
   * Configuration key for rolling checksum on writes.
   *
   * <p>If this is enabled, write channel will calculate rolling crc32c checksum and compare it from
   * server response.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_WRITE_ROLLING_CHECKSUM_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.write.rolling.checksum.enable",
          AsyncWriteChannelOptions.DEFAULT.isRollingChecksumEnabled());

  /** Configuration key to enable Hierarchical Namespace (HNS) optimizations. */
  public static final HadoopConfigurationProperty<Boolean> GCS_HNS_OPTIMIZATION_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.hierarchical.namespace.folders.optimization.enable",
          GoogleCloudStorageOptions.DEFAULT.isHnOptimizationEnabled());

  /** Configuration key for enabling move operation in gcs instead of copy+delete. */
  public static final HadoopConfigurationProperty<Boolean> GCS_OPERATION_MOVE_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.operation.move.enable",
          GoogleCloudStorageOptions.DEFAULT.isMoveOperationEnabled());

  /** Flag to enable the bidirectional Rapid Storage API. */
  public static final HadoopConfigurationProperty<Boolean> GCS_OPERATION_BIDI_API_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.bidi.enable", GoogleCloudStorageOptions.DEFAULT.isBidiEnabled());

  /**
   * Number of threads used by ThreadPoolExecutor in bidi channel. This executor is used to read
   * individual range and populate the buffer.
   */
  public static final HadoopConfigurationProperty<Integer> GCS_BIDI_THREAD_COUNT =
      new HadoopConfigurationProperty<>(
          "fs.gs.bidi.thread.count", GoogleCloudStorageReadOptions.DEFAULT.getBidiThreadCount());

  /** Sets the total amount of time, we would wait for bidi client initialization. */
  public static final HadoopConfigurationProperty<Integer> GCS_BIDI_CLIENT_INITIALIZATION_TIMEOUT =
      new HadoopConfigurationProperty<>(
          "fs.gs.bidi.client.timeout",
          GoogleCloudStorageReadOptions.DEFAULT.getBidiClientTimeout());

  /** Flag to enable finalizing object before closing bidi write channel. */
  public static final HadoopConfigurationProperty<Boolean>
      GCS_APPENDABLE_OBJECTS_FINALIZE_BEFORE_CLOSE =
          new HadoopConfigurationProperty<>(
              "fs.gs.bidi.finalize.on.close",
              GoogleCloudStorageOptions.DEFAULT.isFinalizeBeforeClose());

  static GoogleCloudStorageFileSystemOptions.Builder getGcsFsOptionsBuilder(Configuration config) {
    return GoogleCloudStorageFileSystemOptions.builder()
        .setBucketDeleteEnabled(GCE_BUCKET_DELETE_ENABLE.get(config, config::getBoolean))
        .setClientType(GCS_CLIENT_TYPE.get(config, config::getEnum))
        .setCloudStorageOptions(getGcsOptionsBuilder(config).build())
        .setEnsureNoConflictingItems(
            GCS_CREATE_ITEMS_CONFLICT_CHECK_ENABLE.get(config, config::getBoolean))
        .setMarkerFilePattern(GCS_MARKER_FILE_PATTERN.get(config, config::get))
        .setPerformanceCacheEnabled(GCS_PERFORMANCE_CACHE_ENABLE.get(config, config::getBoolean))
        .setPerformanceCacheOptions(getPerformanceCachingOptions(config))
        .setStatusParallelEnabled(GCS_STATUS_PARALLEL_ENABLE.get(config, config::getBoolean))
        .setCloudLoggingEnabled(GCS_CLOUD_LOGGING_ENABLE.get(config, config::getBoolean));
  }

  static VectoredReadOptions.Builder getVectoredReadOptionBuilder(Configuration config) {
    return VectoredReadOptions.builder()
        .setMinSeekVectoredReadSize(GCS_VECTORED_READ_RANGE_MIN_SEEK.get(config, config::getInt))
        .setMergeRangeMaxSize(GCS_VECTORED_READ_MERGED_RANGE_MAX_SIZE.get(config, config::getInt))
        .setReadThreads(GCS_VECTORED_READ_THREADS.get(config, config::getInt));
  }

  @VisibleForTesting
  static GoogleCloudStorageOptions.Builder getGcsOptionsBuilder(Configuration config) {
    String projectId = GCS_PROJECT_ID.get(config, config::get);
    return GoogleCloudStorageOptions.builder()
        .setAppName(getApplicationName(config))
        .setGrpcWriteEnabled(GCS_GRPC_WRITE_ENABLE.get(config, config::getBoolean))
        .setAutoRepairImplicitDirectoriesEnabled(
            GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE.get(config, config::getBoolean))
        .setBatchThreads(GCS_BATCH_THREADS.get(config, config::getInt))
        .setCopyWithRewriteEnabled(GCS_COPY_WITH_REWRITE_ENABLE.get(config, config::getBoolean))
        .setDirectPathPreferred(GCS_GRPC_DIRECTPATH_ENABLE.get(config, config::getBoolean))
        .setEncryptionAlgorithm(GCS_ENCRYPTION_ALGORITHM.get(config, config::get))
        .setEncryptionKey(GCS_ENCRYPTION_KEY.getPassword(config))
        .setEncryptionKeyHash(GCS_ENCRYPTION_KEY_HASH.getPassword(config))
        .setGrpcEnabled(GCS_GRPC_ENABLE.get(config, config::getBoolean))
        .setHnBucketRenameEnabled(GCS_HIERARCHICAL_NAMESPACE_ENABLE.get(config, config::getBoolean))
        .setGrpcMessageTimeoutCheckInterval(GCS_GRPC_CHECK_INTERVAL_TIMEOUT.getTimeDuration(config))
        .setHttpRequestConnectTimeout(GCS_HTTP_CONNECT_TIMEOUT.getTimeDuration(config))
        .setHttpRequestHeaders(GCS_HTTP_HEADERS.getPropsWithPrefix(config))
        .setHttpRequestReadTimeout(
            READ_TIMEOUT_SUFFIX.withPrefixes(CONFIG_KEY_PREFIXES).getTimeDuration(config))
        .setMaxHttpRequestRetries(GCS_HTTP_MAX_RETRY.get(config, config::getInt))
        .setMaxListItemsPerCall(GCS_MAX_LIST_ITEMS_PER_CALL.get(config, config::getInt))
        .setMaxRequestsPerBatch(GCS_MAX_REQUESTS_PER_BATCH.get(config, config::getInt))
        .setMaxRewriteChunkSize(GCS_REWRITE_MAX_CHUNK_SIZE.get(config, config::getLongBytes))
        .setMaxWaitTimeForEmptyObjectCreation(
            GCS_MAX_WAIT_TIME_EMPTY_OBJECT_CREATE.getTimeDuration(config))
        .setMetricsSink(GCS_METRICS_SINK.get(config, config::getEnum))
        .setProjectId(projectId)
        .setProxyAddress(
            PROXY_ADDRESS_SUFFIX.withPrefixes(CONFIG_KEY_PREFIXES).get(config, config::get))
        .setProxyPassword(
            PROXY_PASSWORD_SUFFIX.withPrefixes(CONFIG_KEY_PREFIXES).getPassword(config))
        .setProxyUsername(
            PROXY_USERNAME_SUFFIX.withPrefixes(CONFIG_KEY_PREFIXES).getPassword(config))
        .setReadChannelOptions(getReadChannelOptions(config))
        .setRequesterPaysOptions(getRequesterPaysOptions(config, projectId))
        .setStorageRootUrl(GCS_ROOT_URL.get(config, config::get))
        .setStorageServicePath(GCS_SERVICE_PATH.get(config, config::get))
        .setTraceLogEnabled(GCS_TRACE_LOG_ENABLE.get(config, config::getBoolean))
        .setOperationTraceLogEnabled(GCS_OPERATION_TRACE_LOG_ENABLE.get(config, config::getBoolean))
        .setTrafficDirectorEnabled(GCS_GRPC_TRAFFICDIRECTOR_ENABLE.get(config, config::getBoolean))
        .setWriteChannelOptions(getWriteChannelOptions(config))
        .setMoveOperationEnabled(GCS_OPERATION_MOVE_ENABLE.get(config, config::getBoolean))
        .setStorageClientCachingEnabled(GCS_STORAGE_CLIENT_CACHING.get(config, config::getBoolean))
        .setBidiEnabled(GCS_OPERATION_BIDI_API_ENABLE.get(config, config::getBoolean))
        .setFinalizeBeforeClose(
            GCS_APPENDABLE_OBJECTS_FINALIZE_BEFORE_CLOSE.get(config, config::getBoolean))
        .setHnOptimizationEnabled(GCS_HNS_OPTIMIZATION_ENABLE.get(config, config::getBoolean));
  }

  @VisibleForTesting
  static PerformanceCachingGoogleCloudStorageOptions getPerformanceCachingOptions(
      Configuration config) {
    return PerformanceCachingGoogleCloudStorageOptions.builder()
        .setMaxEntryAge(GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE.getTimeDuration(config))
        .build();
  }

  private static String getApplicationName(Configuration config) {
    String appNameSuffix = nullToEmpty(GCS_APPLICATION_NAME_SUFFIX.get(config, config::get));
    String applicationName = GoogleHadoopFileSystem.GHFS_ID + appNameSuffix;
    logger.atFiner().log("getApplicationName(config: %s): %s", config, applicationName);
    return applicationName;
  }

  /**
   * Parses GCS read options from the supplied Hadoop configuration object, using the property keys
   * documented in
   * https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md
   *
   * @param config the Hadoop Configuration object to parse
   */
  public static GoogleCloudStorageReadOptions getReadChannelOptions(Configuration config) {
    return GoogleCloudStorageReadOptions.builder()
        .setFadvise(GCS_INPUT_STREAM_FADVISE.get(config, config::getEnum))
        .setFastFailOnNotFoundEnabled(
            GCS_INPUT_STREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE.get(config, config::getBoolean))
        .setGrpcChecksumsEnabled(GCS_GRPC_CHECKSUMS_ENABLE.get(config, config::getBoolean))
        .setGrpcReadMessageTimeout(GCS_GRPC_READ_MESSAGE_TIMEOUT.getTimeDuration(config))
        .setGrpcReadTimeout(GCS_GRPC_READ_TIMEOUT.getTimeDuration(config))
        .setGrpcReadZeroCopyEnabled(GCS_GRPC_READ_ZEROCOPY_ENABLE.get(config, config::getBoolean))
        .setGzipEncodingSupportEnabled(
            GCS_INPUT_STREAM_SUPPORT_GZIP_ENCODING_ENABLE.get(config, config::getBoolean))
        .setInplaceSeekLimit(GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT.get(config, config::getLongBytes))
        .setMinRangeRequestSize(
            GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE.get(config, config::getLongBytes))
        .setBlockSize(BLOCK_SIZE.get(config, config::getLong))
        .setFadviseRequestTrackCount(GCS_FADVISE_REQUEST_TRACK_COUNT.get(config, config::getInt))
        .setBidiThreadCount(GCS_BIDI_THREAD_COUNT.get(config, config::getInt))
        .setBidiClientTimeout(GCS_BIDI_CLIENT_INITIALIZATION_TIMEOUT.get(config, config::getInt))
        .build();
  }

  /**
   * Parses GCS write options from the supplied Hadoop configuration object, using the property keys
   * documented in
   * https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md
   *
   * @param config the Hadoop Configuration object to parse
   */
  public static AsyncWriteChannelOptions getWriteChannelOptions(Configuration config) {
    return AsyncWriteChannelOptions.builder()
        .setBufferSize(toIntExact(GCS_OUTPUT_STREAM_BUFFER_SIZE.get(config, config::getLongBytes)))
        .setDirectUploadEnabled(
            GCS_OUTPUT_STREAM_DIRECT_UPLOAD_ENABLE.get(config, config::getBoolean))
        .setGrpcChecksumsEnabled(GCS_GRPC_CHECKSUMS_ENABLE.get(config, config::getBoolean))
        .setGrpcWriteMessageTimeout(GCS_GRPC_WRITE_MESSAGE_TIMEOUT.getTimeDuration(config))
        .setGrpcWriteTimeout(GCS_GRPC_WRITE_TIMEOUT.getTimeDuration(config))
        .setNumberOfBufferedRequests(GCS_GRPC_UPLOAD_BUFFERED_REQUESTS.get(config, config::getInt))
        .setPipeBufferSize(
            toIntExact(GCS_OUTPUT_STREAM_PIPE_BUFFER_SIZE.get(config, config::getLongBytes)))
        .setPipeType(GCS_OUTPUT_STREAM_PIPE_TYPE.get(config, config::getEnum))
        .setUploadCacheSize(
            toIntExact(GCS_OUTPUT_STREAM_UPLOAD_CACHE_SIZE.get(config, config::getLongBytes)))
        .setUploadChunkSize(
            toIntExact(GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE.get(config, config::getLongBytes)))
        .setUploadType(GCS_CLIENT_UPLOAD_TYPE.get(config, config::getEnum))
        .setTemporaryPaths(
            ImmutableSet.copyOf(GCS_WRITE_TEMPORARY_FILES_PATH.getStringCollection(config)))
        .setPCUBufferCount(GCS_PCU_BUFFER_COUNT.get(config, config::getInt))
        .setPCUBufferCapacity(toIntExact(GCS_PCU_BUFFER_CAPACITY.get(config, config::getLongBytes)))
        .setPartFileCleanupType(GCS_PCU_PART_FILE_CLEANUP_TYPE.get(config, config::getEnum))
        .setPartFileNamePrefix(GCS_PCU_PART_FILE_NAME_PREFIX.get(config, config::get))
        .setRollingChecksumEnabled(
            GCS_WRITE_ROLLING_CHECKSUM_ENABLE.get(config, config::getBoolean))
        .build();
  }

  private static RequesterPaysOptions getRequesterPaysOptions(
      Configuration config, String projectId) {
    String requesterPaysProjectId = GCS_REQUESTER_PAYS_PROJECT_ID.get(config, config::get);
    return RequesterPaysOptions.builder()
        .setBuckets(GCS_REQUESTER_PAYS_BUCKETS.getStringCollection(config))
        .setMode(GCS_REQUESTER_PAYS_MODE.get(config, config::getEnum))
        .setProjectId(requesterPaysProjectId == null ? projectId : requesterPaysProjectId)
        .build();
  }
}
