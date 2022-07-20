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

import com.google.auth.Credentials;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage.ListPage;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.CheckedFunction;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.function.Function;

/** File system implementation for GCS based on Directory APIs */
public class GoogleCloudStorageNativeFileSystem implements GoogleCloudStorageFileSystem {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // GCS access instance.
  private final GoogleCloudStorage gcs;

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
      CheckedFunction<GoogleCloudStorageOptions, GoogleCloudStorage, IOException> gcsFn,
      GoogleCloudStorageFileSystemOptions options)
      throws IOException {
    this(gcsFn.apply(options.getCloudStorageOptions()), options);
  }

  @VisibleForTesting
  public GoogleCloudStorageNativeFileSystem(
      GoogleCloudStorage gcs, GoogleCloudStorageFileSystemOptions options) {
    checkArgument(
        gcs.getOptions() == options.getCloudStorageOptions(),
        "gcs and gcsfs should use the same options");
    options.throwIfNotValid();
    this.gcs =
        options.isPerformanceCacheEnabled()
            ? new PerformanceCachingGoogleCloudStorage(gcs, options.getPerformanceCacheOptions())
            : gcs;
    this.options = options;
    this.legacyGcsFs = new GoogleCloudStorageFileSystemImpl(gcs, options);
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
    legacyGcsFs.mkdir(path);
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
