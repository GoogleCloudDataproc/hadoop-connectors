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

import com.google.cloud.hadoop.gcsio.GoogleCloudStorage.ListPage;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

public interface GoogleCloudStorageFileSystem {

  // URI scheme for GCS.
  String SCHEME = "gs";

  // URI of the root path.
  URI GCS_ROOT = URI.create(SCHEME + ":/");

  WritableByteChannel create(URI path) throws IOException;

  WritableByteChannel create(URI path, CreateFileOptions createOptions) throws IOException;

  SeekableByteChannel open(URI path) throws IOException;

  SeekableByteChannel open(URI path, GoogleCloudStorageReadOptions readOptions) throws IOException;

  SeekableByteChannel open(FileInfo fileInfo, GoogleCloudStorageReadOptions readOptions)
      throws IOException;

  void delete(URI path, boolean recursive) throws IOException;

  boolean exists(URI path) throws IOException;

  void mkdirs(URI path) throws IOException;

  void rename(URI src, URI dst) throws IOException;

  void compose(List<URI> sources, URI destination, String contentType) throws IOException;

  List<FileInfo> listFileInfoForPrefix(URI prefix) throws IOException;

  List<FileInfo> listFileInfoForPrefix(URI prefix, ListFileOptions listOptions) throws IOException;

  ListPage<FileInfo> listFileInfoForPrefixPage(URI prefix, String pageToken) throws IOException;

  ListPage<FileInfo> listFileInfoForPrefixPage(
      URI prefix, ListFileOptions listOptions, String pageToken) throws IOException;

  List<FileInfo> listFileInfo(URI path) throws IOException;

  List<FileInfo> listFileInfo(URI path, ListFileOptions listOptions) throws IOException;

  FileInfo getFileInfo(URI path) throws IOException;

  List<FileInfo> getFileInfos(List<URI> paths) throws IOException;

  void close();

  @VisibleForTesting
  void mkdir(URI path) throws IOException;

  GoogleCloudStorage getGcs();

  public GoogleCloudStorageFileSystemOptions getOptions();
}
