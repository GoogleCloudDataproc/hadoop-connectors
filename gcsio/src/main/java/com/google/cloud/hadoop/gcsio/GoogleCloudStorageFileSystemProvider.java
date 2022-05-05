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

import com.google.auth.Credentials;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.FilesystemAPI;
import com.google.cloud.hadoop.util.AccessBoundary;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/** Provides an instance of {@link GoogleCloudStorageFileSystem} based on the FS configurations */
public class GoogleCloudStorageFileSystemProvider {

  public static GoogleCloudStorageFileSystem newInstance(
      Credentials credentials,
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn,
      GoogleCloudStorageFileSystemOptions options)
      throws IOException {
    return options.getFilesystemApi() == FilesystemAPI.DIRECTORY
        ? new GoogleCloudStorageNativeFileSystem(credentials, downscopedAccessTokenFn, options)
        : new GoogleCloudStorageFileSystemImpl(credentials, downscopedAccessTokenFn, options);
  }
}
