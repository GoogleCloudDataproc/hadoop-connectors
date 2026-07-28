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

import com.google.cloud.hadoop.gcsio.GoogleCloudStorage.ListPage;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 * Provides a POSIX like file system layered on top of Google Cloud Storage (GCS).
 *
 * <p>All file system aspects (eg, path) are encapsulated in this class, they are not exposed to the
 * underlying layer. That is, all interactions with the underlying layer are strictly in terms of
 * buckets and objects.
 *
 * @see <a
 *     href="https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/index.html">Hadoop
 *     FileSystem specification.</a>
 */
public interface GoogleCloudStorageFileSystem {

  // URI scheme for GCS.
  String SCHEME = "gs";

  // URI of the root path.
  URI GCS_ROOT = URI.create(SCHEME + ":/");

  /**
   * Creates and opens an object for writing. If the object already exists, it is overwritten.
   *
   * @param path Object full path of the form gs://bucket/object-path.
   * @return A channel for writing to the given object.
   */
  default WritableByteChannel create(URI path) throws IOException {
    return create(
        path,
        CreateFileOptions.builder().setWriteMode(CreateFileOptions.WriteMode.OVERWRITE).build());
  }

  /**
   * Creates and opens an object for writing.
   *
   * @param path Object full path of the form gs://bucket/object-path.
   * @return A channel for writing to the given object.
   */
  WritableByteChannel create(URI path, CreateFileOptions createOptions) throws IOException;

  /**
   * Opens an object for reading.
   *
   * @param path Object full path of the form gs://bucket/object-path.
   * @return A channel for reading from the given object.
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException if object exists but cannot be opened.
   */
  default SeekableByteChannel open(URI path) throws IOException {
    return open(path, GoogleCloudStorageReadOptions.DEFAULT);
  }

  /**
   * Opens an object for reading.
   *
   * @param path Object full path of the form gs://bucket/object-path.
   * @param readOptions Fine-grained options for behaviors of retries, buffering, etc.
   * @return A channel for reading from the given object.
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException if object exists but cannot be opened.
   */
  SeekableByteChannel open(URI path, GoogleCloudStorageReadOptions readOptions) throws IOException;

  /**
   * Opens an object for reading using {@link FileInfo}.
   *
   * @param fileInfo contains metadata information about the file
   * @param readOptions readOptions fine-grained options specifying things like retry settings,
   *     buffering, etc.
   * @return Seekable Byte Channel to enable file open
   * @throws IOException IOException on IO Error
   */
  SeekableByteChannel open(FileInfo fileInfo, GoogleCloudStorageReadOptions readOptions)
      throws IOException;

  /**
   * Deletes one or more items indicated by the given path.
   *
   * <p>If path points to a directory:
   *
   * <ul>
   *   <li>if recursive is true, all items under that path are recursively deleted followed by
   *       deletion of the directory.
   *   <li>else,
   *       <ul>
   *         <li>the directory is deleted if it is empty,
   *         <li>else, an IOException is thrown.
   *       </ul>
   * </ul>
   *
   * <p>The recursive parameter is ignored for a file.
   *
   * @param path Path of the item to delete.
   * @param recursive If true, all sub-items are also deleted.
   * @throws FileNotFoundException if the given path does not exist.
   */
  void delete(URI path, boolean recursive) throws IOException;

  /**
   * Indicates whether the given item exists.
   *
   * @param path Path of the item to check.
   * @return true if the given item exists, false otherwise.
   */
  boolean exists(URI path) throws IOException;

  /**
   * Creates a directory at the specified path. Also creates any parent directories as necessary.
   * Similar to 'mkdir -p' command.
   *
   * @param path Path of the directory to create.
   */
  void mkdirs(URI path) throws IOException;

  /**
   * Renames the given item's path.
   *
   * <p>The operation is disallowed if any of the following is true:
   *
   * <ul>
   *   <li>src equals GCS_ROOT
   *   <li>src is a file and dst equals GCS_ROOT
   *   <li>src does not exist
   *   <li>dst is a file that already exists
   *   <li>parent of the destination does not exist
   * </ul>
   *
   * <p>Otherwise, the expected behavior is as follows:
   *
   * <ul>
   *   <li>if src is a directory:
   *       <ul>
   *         <li>if dst is an existing file then disallowed
   *         <li>if dst is a directory then rename the directory
   *       </ul>
   *       <p>
   *   <li>if src is a file:
   *       <ul>
   *         <li>if dst is a file then rename the file.
   *         <li>if dst is a directory then similar to the previous case after appending src
   *             file-name to dst
   *       </ul>
   * </ul>
   *
   * <p>Note: This function is very expensive to call for directories that have many sub-items.
   *
   * @param src Path of the item to rename.
   * @param dst New path of the item.
   * @throws FileNotFoundException if src does not exist.
   */
  void rename(URI src, URI dst) throws IOException;

  /**
   * Composes inputs into a single GCS object. This performs a GCS Compose. Objects will be composed
   * according to the order they appear in the input. The destination object, if already present,
   * will be overwritten. Sources and destination are assumed to be in the same bucket.
   *
   * @param sources the list of URIs to be composed
   * @param destination the resulting URI with composed sources
   * @param contentType content-type of the composed object
   * @throws IOException if the Compose operation was unsuccessful
   */
  void compose(List<URI> sources, URI destination, String contentType) throws IOException;

  /**
   * Equivalent to a recursive listing of {@code prefix}, except that {@code prefix} doesn't have to
   * represent an actual object but can just be a partial prefix string. The 'authority' component
   * of the {@code prefix} <b>must</b> be the complete authority, however; we can only list prefixes
   * of <b>objects</b>, not buckets.
   *
   * @param prefix the prefix to use to list all matching objects.
   */
  default List<FileInfo> listFileInfoForPrefix(URI prefix) throws IOException {
    return listFileInfoForPrefix(prefix, ListFileOptions.DEFAULT);
  }

  /**
   * Equivalent to a recursive listing of {@code prefix}, except that {@code prefix} doesn't have to
   * represent an actual object but can just be a partial prefix string. The 'authority' component
   * of the {@code prefix} <b>must</b> be the complete authority, however; we can only list prefixes
   * of <b>objects</b>, not buckets.
   *
   * @param prefix the prefix to use to list all matching objects.
   */
  List<FileInfo> listFileInfoForPrefix(URI prefix, ListFileOptions listOptions) throws IOException;

  /**
   * Equivalent to {@link #listFileInfoForPrefix} but returns {@link FileInfo}s listed by single
   * request (1 page).
   *
   * @param prefix the prefix to use to list all matching objects.
   * @param pageToken the page token to list
   */
  default ListPage<FileInfo> listFileInfoForPrefixPage(URI prefix, String pageToken)
      throws IOException {
    return listFileInfoForPrefixPage(prefix, ListFileOptions.DEFAULT, pageToken);
  }

  /**
   * Equivalent to {@link #listFileInfoForPrefix} but returns {@link FileInfo}s listed by single
   * request (1 page).
   *
   * @param prefix the prefix to use to list all matching objects.
   * @param pageToken the page token to list
   */
  ListPage<FileInfo> listFileInfoForPrefixPage(
      URI prefix, ListFileOptions listOptions, String pageToken) throws IOException;

  /**
   * If the given path points to a directory then the information about its children is returned,
   * otherwise information about the given file is returned.
   *
   * @param path Given path.
   * @return Information about a file or children of a directory.
   * @throws FileNotFoundException if the given path does not exist.
   */
  default List<FileInfo> listFileInfo(URI path) throws IOException {
    return listFileInfo(path, ListFileOptions.DEFAULT);
  }

  /**
   * Return all the files which are lexicographically equal or greater than the provided path. This
   * is an experimental API and can change without notice.
   *
   * @param startsFrom Given path.
   * @return Information about all the files which satisfies the criteria.
   */
  default List<FileInfo> listFileInfoStartingFrom(URI startsFrom) throws IOException {
    return listFileInfoStartingFrom(startsFrom, ListFileOptions.DEFAULT);
  }

  /**
   * If the given path points to a directory then the information about its children is returned,
   * otherwise information about the given file is returned.
   *
   * @param path Given path.
   * @return Information about a file or children of a directory.
   * @throws FileNotFoundException if the given path does not exist.
   */
  List<FileInfo> listFileInfo(URI path, ListFileOptions listOptions) throws IOException;

  /**
   * Return all the files which are lexicographically equal or greater than the provided path. This
   * is an experimental API and can change without notice.
   *
   * @param startsFrom Given path.
   * @return Information about all the files which satisfies the criteria.
   */
  List<FileInfo> listFileInfoStartingFrom(URI startsFrom, ListFileOptions listOptions)
      throws IOException;

  /**
   * Returns the list of folder resources in the prefix. It lists all the folder resources
   *
   * @param prefix the prefix to use to list all matching folder resources.
   * @param pageToken the page token to list
   * @param listFolderOptions the page token to list
   */
  ListPage<FolderInfo> listFoldersInfoForPrefixPage(
      URI prefix, ListFolderOptions listFolderOptions, String pageToken) throws IOException;

  /**
   * Gets information about the given path item.
   *
   * @param path The path we want information about.
   * @return Information about the given path item.
   */
  FileInfo getFileInfo(URI path) throws IOException;

  /**
   * Gets information about the given path item with hint providing the path type (file vs
   * directory).
   *
   * @param path The path we want information about.
   * @return Information about the given path item.
   */
  FileInfo getFileInfoWithHint(URI path, GoogleCloudStorageFileSystemImpl.PathTypeHint pathTypeHint)
      throws IOException;

  /**
   * Gets information about the given path item. Here path should be pointing to a gcs object and
   * can't be a directory
   *
   * @param path The path we want information about.
   * @return Information about the given path item.
   */
  FileInfo getFileInfoObject(URI path) throws IOException;

  /**
   * Gets information about each path in the given list; more efficient than calling getFileInfo()
   * on each path individually in a loop.
   *
   * @param paths List of paths.
   * @return Information about each path in the given list.
   */
  List<FileInfo> getFileInfos(List<URI> paths) throws IOException;

  /** Releases resources used by this instance. */
  void close();

  /**
   * Creates a directory at the specified path.
   *
   * <p>There are two conventions for using objects as directories in GCS. 1. An object of zero size
   * with name ending in / 2. An object of zero size with name ending in _$folder$
   *
   * <p>#1 is the recommended convention by the GCS team. We use it when creating a directory.
   *
   * <p>However, some old tools still use #2. We will decide based on customer use cases whether to
   * support #2 as well. For now, we only support #1.
   *
   * <p>Note that a bucket is always considered a directory. Doesn't create parent directories;
   * normal use cases should only call mkdirs().
   */
  void mkdir(URI path) throws IOException;

  /** Retrieve our internal gcs. */
  GoogleCloudStorage getGcs();

  /** Retrieve the options that were used to create this GoogleCloudStorageFileSystem. */
  GoogleCloudStorageFileSystemOptions getOptions();
}
