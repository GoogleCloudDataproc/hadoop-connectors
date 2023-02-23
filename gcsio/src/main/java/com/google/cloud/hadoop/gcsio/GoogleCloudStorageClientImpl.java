/*
 * Copyright 2022 Google LLC
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

package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.Credentials;
import com.google.auto.value.AutoBuilder;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * Provides read/write access to Google Cloud Storage (GCS), using Java nio channel semantics. This
 * is a basic implementation of the GoogleCloudStorage interface that mostly delegates through to
 * the appropriate API call(s) google-cloud-storage client.
 */
@VisibleForTesting
public class GoogleCloudStorageClientImpl extends ForwardingGoogleCloudStorage {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final GoogleCloudStorageOptions storageOptions;
  private final Storage storage;

  // Thread-pool used for background tasks.
  private ExecutorService backgroundTasksThreadPool =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setNameFormat("gcsio-storage-client-write-channel-pool-%d")
              .setDaemon(true)
              .build());
  /**
   * Having an instance of gscImpl to redirect calls to Json client while new client implementation
   * is in WIP.
   */
  GoogleCloudStorageClientImpl(
      GoogleCloudStorageOptions options,
      @Nullable Storage clientLibraryStorage,
      @Nullable Credentials credentials,
      @Nullable HttpTransport httpTransport,
      @Nullable HttpRequestInitializer httpRequestInitializer,
      @Nullable Function<List<AccessBoundary>, String> downscopedAccessTokenFn)
      throws IOException {
    super(
        GoogleCloudStorageImpl.builder()
            .setOptions(options)
            .setCredentials(credentials)
            .setHttpTransport(httpTransport)
            .setHttpRequestInitializer(httpRequestInitializer)
            .setDownscopedAccessTokenFn(downscopedAccessTokenFn)
            .build());
    this.storageOptions = options;
    this.storage =
        clientLibraryStorage == null ? createStorage(credentials, options) : clientLibraryStorage;
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    logger.atFiner().log("create(%s)", resourceId);
    checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);
    // Update resourceId if generationId is missing
    StorageResourceId resourceIdWithGeneration = resourceId;
    if (!resourceId.hasGenerationId()) {
      resourceIdWithGeneration =
          new StorageResourceId(
              resourceId.getBucketName(),
              resourceId.getObjectName(),
              getWriteGeneration(resourceId, options.isOverwriteExisting()));
    }

    GoogleCloudStorageClientWriteChannel channel =
        new GoogleCloudStorageClientWriteChannel(
            storage, storageOptions, resourceIdWithGeneration, options, backgroundTasksThreadPool);
    channel.initialize();
    return channel;
  }

  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions) throws IOException {
    logger.atFiner().log("open(%s, %s)", resourceId, readOptions);
    return open(resourceId, /* itemInfo= */ null, readOptions);
  }

  @Override
  public SeekableByteChannel open(
      GoogleCloudStorageItemInfo itemInfo, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    logger.atFiner().log("open(%s, %s)", itemInfo, readOptions);
    checkNotNull(itemInfo, "itemInfo should not be null");

    StorageResourceId resourceId = itemInfo.getResourceId();
    checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);

    return open(resourceId, itemInfo, readOptions);
  }

  private SeekableByteChannel open(
      StorageResourceId resourceId,
      GoogleCloudStorageItemInfo itemInfo,
      GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    return new GoogleCloudStorageClientReadChannel(
        storage, itemInfo == null ? getItemInfo(resourceId) : itemInfo, readOptions);
  }

  @Override
  public void close() {
    try {
      try {
        super.close();
      } finally {
        backgroundTasksThreadPool.shutdown();
      }
    } finally {
      backgroundTasksThreadPool = null;
    }
  }

  /**
   * Gets the object generation for a write operation
   *
   * <p>making getItemInfo call even if overwrite is disabled to fail fast in case file is existing.
   *
   * @param resourceId object for which generation info is requested
   * @param overwrite whether existing object should be overwritten
   * @return the generation of the object
   * @throws IOException if the object already exists and cannot be overwritten
   */
  private long getWriteGeneration(StorageResourceId resourceId, boolean overwrite)
      throws IOException {
    logger.atFiner().log("getWriteGeneration(%s, %s)", resourceId, overwrite);
    GoogleCloudStorageItemInfo info = getItemInfo(resourceId);
    if (!info.exists()) {
      return 0L;
    }
    if (info.exists() && overwrite) {
      long generation = info.getContentGeneration();
      checkState(generation != 0, "Generation should not be 0 for an existing item");
      return generation;
    }
    throw new FileAlreadyExistsException(String.format("Object %s already exists.", resourceId));
  }

  private static Storage createStorage(
      Credentials credentials, GoogleCloudStorageOptions storageOptions) {
    return StorageOptions.grpc()
        .setAttemptDirectPath(storageOptions.isDirectPathPreferred())
        .setHeaderProvider(populateHeaderValues(storageOptions))
        .setCredentials(credentials)
        .build()
        .getService();
  }

  private static HeaderProvider populateHeaderValues(GoogleCloudStorageOptions storageOptions) {
    return () ->
        ImmutableMap.<String, String>builder()
            .putAll(getEncryptionHeaders(storageOptions))
            .putAll(getDecryptionHeaders(storageOptions))
            .putAll(storageOptions.getHttpRequestHeaders())
            .build();
  }

  private static Map<String, String> getEncryptionHeaders(
      GoogleCloudStorageOptions storageOptions) {
    if (storageOptions.getEncryptionKey() == null) {
      return Collections.EMPTY_MAP;
    }
    return ImmutableMap.<String, String>builder()
        .put(
            "x-goog-encryption-algorithm",
            checkNotNull(
                storageOptions.getEncryptionAlgorithm(), "encryption algorithm must not be null"))
        .put(
            "x-goog-encryption-key",
            checkNotNull(storageOptions.getEncryptionKey(), "encryption key must not be null")
                .value())
        .put(
            "x-goog-encryption-key-sha256",
            checkNotNull(
                    storageOptions.getEncryptionKeyHash(), "encryption key hash must not be null")
                .value())
        .build();
  }

  private static Map<String, String> getDecryptionHeaders(
      GoogleCloudStorageOptions storageOptions) {
    if (storageOptions.getEncryptionKey() == null) {
      return Collections.EMPTY_MAP;
    }
    return ImmutableMap.<String, String>builder()
        .put(
            "x-goog-copy-source-encryption-algorithm",
            checkNotNull(
                storageOptions.getEncryptionAlgorithm(), "encryption algorithm must not be null"))
        .put(
            "x-goog-copy-source-encryption-key",
            checkNotNull(storageOptions.getEncryptionKey(), "encryption key must not be null")
                .value())
        .put(
            "x-goog-copy-source-encryption-key-sha256",
            checkNotNull(
                    storageOptions.getEncryptionKeyHash(), "encryption key hash must not be null")
                .value())
        .build();
  }

  public static Builder builder() {
    return new AutoBuilder_GoogleCloudStorageClientImpl_Builder();
  }

  @AutoBuilder(ofClass = GoogleCloudStorageClientImpl.class)
  public abstract static class Builder {

    public abstract Builder setOptions(GoogleCloudStorageOptions options);

    public abstract Builder setHttpTransport(@Nullable HttpTransport httpTransport);

    public abstract Builder setCredentials(@Nullable Credentials credentials);

    @VisibleForTesting
    public abstract Builder setHttpRequestInitializer(
        @Nullable HttpRequestInitializer httpRequestInitializer);

    public abstract Builder setDownscopedAccessTokenFn(
        @Nullable Function<List<AccessBoundary>, String> downscopedAccessTokenFn);

    @VisibleForTesting
    public abstract Builder setClientLibraryStorage(@Nullable Storage clientLibraryStorage);

    public abstract GoogleCloudStorageClientImpl build() throws IOException;
  }
}
