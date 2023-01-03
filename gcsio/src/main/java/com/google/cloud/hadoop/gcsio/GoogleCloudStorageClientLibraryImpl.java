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

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.auth.Credentials;
import com.google.auto.value.AutoBuilder;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.common.annotations.VisibleForTesting;
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
public class GoogleCloudStorageClientLibraryImpl extends ForwardingGoogleCloudStorage {

  /**
   * Having an instance of gscImpl to redirect calls to Json client while new client implementation
   * is in WIP.
   */
  GoogleCloudStorageClientLibraryImpl(
      GoogleCloudStorageOptions options,
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
  }

  public static Builder builder() {
    return new AutoBuilder_GoogleCloudStorageClientLibraryImpl_Builder();
  }

  @AutoBuilder(ofClass = GoogleCloudStorageClientLibraryImpl.class)
  public abstract static class Builder {

    public abstract Builder setOptions(GoogleCloudStorageOptions options);

    public abstract Builder setHttpTransport(@Nullable HttpTransport httpTransport);

    public abstract Builder setCredentials(@Nullable Credentials credentials);

    @VisibleForTesting
    public abstract Builder setHttpRequestInitializer(
        @Nullable HttpRequestInitializer httpRequestInitializer);

    public abstract Builder setDownscopedAccessTokenFn(
        @Nullable Function<List<AccessBoundary>, String> downscopedAccessTokenFn);

    public abstract GoogleCloudStorageClientLibraryImpl build() throws IOException;
  }
}
