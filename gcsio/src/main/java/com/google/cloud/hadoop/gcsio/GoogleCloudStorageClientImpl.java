/*
 * Copyright 2023 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.auto.value.AutoBuilder;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.List;
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
  /**
   * Having an instance of gscImpl to redirect calls to Json client while new client implementation
   * is in WIP.
   */
  private GoogleCloudStorageOptions storageOptions;

  private Credential credential;

  GoogleCloudStorageClientImpl(
      GoogleCloudStorageOptions storageOptions,
      @Nullable Credential credential,
      @Nullable com.google.api.services.storage.Storage apiaryClientStorage,
      @Nullable HttpRequestInitializer httpRequestInitializer,
      @Nullable Function<List<AccessBoundary>, String> downscopedAccessTokenFn)
      throws IOException {
    super(
        getDelegate(
            httpRequestInitializer,
            apiaryClientStorage,
            storageOptions,
            credential,
            downscopedAccessTokenFn));
    this.storageOptions = checkNotNull(storageOptions, "options must not be null");
    this.credential = checkNotNull(credential, "credentials must not be null");
  }

  private static GoogleCloudStorage getDelegate(
      HttpRequestInitializer httpRequestInitializer,
      com.google.api.services.storage.Storage storage,
      GoogleCloudStorageOptions storageOptions,
      Credential credential,
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn)
      throws IOException {
    if (httpRequestInitializer != null) {
      logger.atWarning().log(
          "Overriding httpRequestInitializer. ALERT: Should not be hit in production");
      return new GoogleCloudStorageImpl(
          storageOptions, httpRequestInitializer, downscopedAccessTokenFn);
    } else if (storage != null) {
      logger.atWarning().log("Overriding storage. ALERT: Should not be hit in production");
      return new GoogleCloudStorageImpl(storageOptions, storage);
    }

    return new GoogleCloudStorageImpl(storageOptions, credential, downscopedAccessTokenFn);
  }

  public static Builder builder() {
    return new AutoBuilder_GoogleCloudStorageClientImpl_Builder();
  }

  @AutoBuilder(ofClass = GoogleCloudStorageClientImpl.class)
  public abstract static class Builder {

    public abstract Builder setStorageOptions(GoogleCloudStorageOptions storageOptions);

    public abstract Builder setCredential(@Nullable Credential credential);

    public abstract Builder setApiaryClientStorage(
        @Nullable com.google.api.services.storage.Storage apiaryClientStorage);

    @VisibleForTesting
    public abstract Builder setHttpRequestInitializer(
        @Nullable HttpRequestInitializer httpRequestInitializer);

    public abstract Builder setDownscopedAccessTokenFn(
        @Nullable Function<List<AccessBoundary>, String> downscopedAccessTokenFn);

    public abstract GoogleCloudStorageClientImpl build() throws IOException;
  }
}
