/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.storage.preprod.Storage;
import com.google.api.services.storage.preprod.Storage.Directories.Insert;
import com.google.api.services.storage.preprod.model.Directory;
import com.google.auth.Credentials;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage.ListPage;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.cloud.hadoop.util.CheckedFunction;
import com.google.cloud.hadoop.util.HttpTransportFactory;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/** File system implementation for GCS based on Directory APIs */
public class GoogleCloudStorageNativeFileSystem implements GoogleCloudStorageFileSystem {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // GCS access instance.
  private final GoogleCloudStorage gcs;

  // Storage instance with directory support
  private final Storage nativeStorage;

  // FS options
  private final GoogleCloudStorageFileSystemOptions options;

  // Instance of legacy FS impl, as delegate in the initial implementation
  private final GoogleCloudStorageFileSystem legacyGcsFs;

  /**
   * Constructs an instance of GoogleCloudStorageFileSystem.
   *
   * @param credentials OAuth2 credentials that allows access to GCS.
   * @param options Options for how this filesystem should operate and configure its underlying
   *     storage.
   */
  public GoogleCloudStorageNativeFileSystem(
      Credentials credentials, GoogleCloudStorageFileSystemOptions options) throws IOException {
    this(
        credentials,
        new GoogleCloudStorageImpl(
            checkNotNull(options, "options must not be null").getCloudStorageOptions(),
            credentials),
        options);
    logger.atFiner().log("GoogleCloudStorageFileSystem(options: %s)", options);
  }

  /**
   * Constructs an instance of GoogleCloudStorageFileSystem.
   *
   * @param credentials OAuth2 credentials that allows access to GCS.
   * @param downscopedAccessTokenFn Function that generates downscoped access token.
   * @param options Options for how this filesystem should operate and configure its underlying
   *     storage.
   */
  public GoogleCloudStorageNativeFileSystem(
      Credentials credentials,
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn,
      GoogleCloudStorageFileSystemOptions options)
      throws IOException {
    this(
        credentials,
        new GoogleCloudStorageImpl(
            checkNotNull(options, "options must not be null").getCloudStorageOptions(),
            credentials,
            downscopedAccessTokenFn),
        options);
    logger.atFiner().log("GoogleCloudStorageFileSystem(options: %s)", options);
  }

  /**
   * Constructs a GoogleCloudStorageFilesystem based on an already-configured underlying
   * GoogleCloudStorage {@code gcs}. Any options pertaining to GCS creation will be ignored.
   */
  @VisibleForTesting
  public GoogleCloudStorageNativeFileSystem(
      Credentials credentials,
      CheckedFunction<GoogleCloudStorageOptions, GoogleCloudStorage, IOException> gcsFn,
      GoogleCloudStorageFileSystemOptions options)
      throws IOException {
    this(credentials, gcsFn.apply(options.getCloudStorageOptions()), options);
  }

  @VisibleForTesting
  public GoogleCloudStorageNativeFileSystem(
      Credentials credentials, GoogleCloudStorage gcs, GoogleCloudStorageFileSystemOptions options)
      throws IOException {
    checkArgument(
        gcs.getOptions() == options.getCloudStorageOptions(),
        "gcs and gcsfs should use the same options");
    options.throwIfNotValid();
    this.gcs =
        options.isPerformanceCacheEnabled()
            ? new PerformanceCachingGoogleCloudStorage(gcs, options.getPerformanceCacheOptions())
            : gcs;
    this.options = options;
    HttpRequestInitializer retryHttpInitializer =
        new ChainingHttpRequestInitializer(
            new StatisticsTrackingHttpRequestInitializer(statistics),
            new RetryHttpInitializer(
                credentials, options.getCloudStorageOptions().toRetryHttpInitializerOptions()));
    this.nativeStorage = createStorage(options.getCloudStorageOptions(), retryHttpInitializer);
    this.legacyGcsFs = new GoogleCloudStorageFileSystemImpl(gcs, options);
  }

  private final ImmutableMap<GoogleCloudStorageStatistics, AtomicLong> statistics =
      ImmutableMap.copyOf(
          stream(GoogleCloudStorageStatistics.values())
              .collect(
                  toMap(
                      identity(),
                      k -> new AtomicLong(0),
                      (u, v) -> {
                        throw new IllegalStateException(
                            String.format(
                                "Duplicate key (attempted merging values %s and %s)", u, u));
                      },
                      () -> new EnumMap<>(GoogleCloudStorageStatistics.class))));

  private static Storage createStorage(
      GoogleCloudStorageOptions options, HttpRequestInitializer httpRequestInitializer)
      throws IOException {
    HttpTransport httpTransport =
        HttpTransportFactory.createHttpTransport(
            options.getProxyAddress(), options.getProxyUsername(), options.getProxyPassword());
    return new Storage.Builder(
            httpTransport, GsonFactory.getDefaultInstance(), httpRequestInitializer)
        .setRootUrl(options.getStorageRootUrl())
        .setServicePath(options.getStorageServicePath())
        .setApplicationName(options.getAppName())
        .build();
  }

  @Override
  public WritableByteChannel create(URI path, CreateFileOptions createOptions) throws IOException {
    return legacyGcsFs.create(path, createOptions);
  }

  @Override
  public SeekableByteChannel open(URI path, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    return legacyGcsFs.open(path, readOptions);
  }

  @Override
  public SeekableByteChannel open(FileInfo fileInfo, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    return legacyGcsFs.open(fileInfo, readOptions);
  }

  @Override
  public void delete(URI path, boolean recursive) throws IOException {
    legacyGcsFs.delete(path, recursive);
  }

  @Override
  public boolean exists(URI path) throws IOException {
    return legacyGcsFs.exists(path);
  }

  @Override
  public void mkdirs(URI path) throws IOException {
    legacyGcsFs.mkdirs(path);
  }

  @Override
  public void rename(URI src, URI dst) throws IOException {
    legacyGcsFs.rename(src, dst);
  }

  @Override
  public void compose(List<URI> sources, URI destination, String contentType) throws IOException {
    legacyGcsFs.compose(sources, destination, contentType);
  }

  @Override
  public List<FileInfo> listFileInfoForPrefix(URI prefix, ListFileOptions listOptions)
      throws IOException {
    return legacyGcsFs.listFileInfoForPrefix(prefix, listOptions);
  }

  @Override
  public ListPage<FileInfo> listFileInfoForPrefixPage(
      URI prefix, ListFileOptions listOptions, String pageToken) throws IOException {
    return legacyGcsFs.listFileInfoForPrefixPage(prefix, listOptions, pageToken);
  }

  @Override
  public List<FileInfo> listFileInfo(URI path, ListFileOptions listOptions) throws IOException {
    return legacyGcsFs.listFileInfo(path, listOptions);
  }

  @Override
  public FileInfo getFileInfo(URI path) throws IOException {
    return legacyGcsFs.getFileInfo(path);
  }

  @Override
  public List<FileInfo> getFileInfos(List<URI> paths) throws IOException {
    return legacyGcsFs.getFileInfos(paths);
  }

  @Override
  public void close() {
    legacyGcsFs.close();
  }

  @Override
  public void mkdir(URI path) throws IOException {
    checkNotNull(path);
    logger.atFiner().log("mkdir(path: %s)", path);
    checkArgument(!path.equals(GCS_ROOT), "Cannot create root directory.");

    StorageResourceId resourceId = StorageResourceId.fromUriPath(path, true);

    // If this is a top level directory, create the corresponding bucket.
    if (resourceId.isBucket()) {
      gcs.createBucket(resourceId.getBucketName());
      return;
    }

    // Ensure that the path looks like a directory path.
    resourceId = resourceId.toDirectoryId();

    Insert insertDir =
        nativeStorage
            .directories()
            .insert(
                resourceId.getBucketName(), new Directory().setName(resourceId.getObjectName()));
    try {
      insertDir.execute();
    } catch (IOException e) {
      if (ApiErrorExtractor.INSTANCE.itemAlreadyExists(e)) {
        throw (FileAlreadyExistsException)
            new FileAlreadyExistsException(String.format("Object '%s' already exists.", resourceId))
                .initCause(e);
      }
      throw e;
    }
  }

  @Override
  public GoogleCloudStorage getGcs() {
    return gcs;
  }

  @Override
  public GoogleCloudStorageFileSystemOptions getOptions() {
    return options;
  }
}
