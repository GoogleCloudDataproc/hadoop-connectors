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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.PATH_DELIMITER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.lang.Math.min;
import static java.util.Comparator.comparing;
import static java.util.concurrent.Executors.newFixedThreadPool;

import com.google.auth.Credentials;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage.ListPage;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.CheckedFunction;
import com.google.cloud.hadoop.util.LazyExecutorService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Provides FS semantics over GCS based on Objects API
 */
public class GoogleCloudStorageFileSystemImpl implements GoogleCloudStorageFileSystem {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final ThreadFactory DAEMON_THREAD_FACTORY =
      new ThreadFactoryBuilder().setNameFormat("gcsfs-thread-%d").setDaemon(true).build();

  private static final ListObjectOptions GET_FILE_INFO_LIST_OPTIONS =
      ListObjectOptions.DEFAULT.toBuilder().setIncludePrefix(true).setMaxResults(1).build();

  private static final ListObjectOptions LIST_FILE_INFO_LIST_OPTIONS =
      ListObjectOptions.DEFAULT.toBuilder().setIncludePrefix(true).build();

  public static final ListFileOptions DELETE_RENAME_LIST_OPTIONS =
      ListFileOptions.DEFAULT.toBuilder().setFields("bucket,name,generation").build();

  // GCS access instance.
  private GoogleCloudStorage gcs;

  // FS options
  private final GoogleCloudStorageFileSystemOptions options;

  /** Cached executor for asynchronous tasks. */
  private ExecutorService cachedExecutor = createCachedExecutor();

  /** Cached executor for synchronous tasks. */
  private ExecutorService lazyExecutor = new LazyExecutorService();

  // Comparator used for sorting paths.
  //
  // For some bulk operations, we need to operate on parent directories before
  // we operate on their children. To achieve this, we sort paths such that
  // shorter paths appear before longer paths. Also, we sort lexicographically
  // within paths of the same length (this is not strictly required but helps when
  // debugging/testing).
  @VisibleForTesting
  static final Comparator<URI> PATH_COMPARATOR =
      comparing(
          URI::toString,
          (as, bs) ->
              (as.length() == bs.length())
                  ? as.compareTo(bs)
                  : Integer.compare(as.length(), bs.length()));

  // Comparator used for sorting a collection of FileInfo items based on path comparison.
  @VisibleForTesting
  static final Comparator<FileInfo> FILE_INFO_PATH_COMPARATOR =
      comparing(FileInfo::getPath, PATH_COMPARATOR);

  /**
   * Constructs an instance of GoogleCloudStorageFileSystem.
   *
   * @param credentials OAuth2 credentials that allows access to GCS.
   * @param options Options for how this filesystem should operate and configure its underlying
   *     storage.
   */
  public GoogleCloudStorageFileSystemImpl(
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
  public GoogleCloudStorageFileSystemImpl(
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
  public GoogleCloudStorageFileSystemImpl(
      CheckedFunction<GoogleCloudStorageOptions, GoogleCloudStorage, IOException> gcsFn,
      GoogleCloudStorageFileSystemOptions options)
      throws IOException {
    this(gcsFn.apply(options.getCloudStorageOptions()), options);
  }

  @VisibleForTesting
  public GoogleCloudStorageFileSystemImpl(
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
  }

  private static ExecutorService createCachedExecutor() {
    ThreadPoolExecutor service =
        new ThreadPoolExecutor(
            /* corePoolSize= */ 2,
            /* maximumPoolSize= */ Integer.MAX_VALUE,
            /* keepAliveTime= */ 30,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("gcsfs-misc-%d").setDaemon(true).build());
    service.allowCoreThreadTimeOut(true);
    // allowCoreThreadTimeOut needs to be enabled for cases where the encapsulating class does not
    return service;
  }

  public GoogleCloudStorageFileSystemOptions getOptions() {
    return options;
  }

  /** Convert {@code CreateFileOptions} to {@code CreateObjectOptions}. */
  public static CreateObjectOptions objectOptionsFromFileOptions(CreateFileOptions options) {
    return CreateObjectOptions.builder()
        .setContentType(options.getContentType())
        .setMetadata(options.getAttributes())
        .setOverwriteExisting(options.isOverwriteExisting())
        .build();
  }

  @Override
  public WritableByteChannel create(URI path) throws IOException {
    logger.atFiner().log("create(path: %s)", path);
    return create(path, CreateFileOptions.DEFAULT_OVERWRITE);
  }

  @Override
  public WritableByteChannel create(URI path, CreateFileOptions createOptions) throws IOException {
    logger.atFiner().log("create(path: %s, createOptions: %s)", path, createOptions);
    checkNotNull(path, "path could not be null");
    StorageResourceId resourceId =
        StorageResourceId.fromUriPath(path, /* allowEmptyObjectName=*/ true);

    if (resourceId.isDirectory()) {
      throw new IOException(
          String.format(
              "Cannot create a file whose name looks like a directory: '%s'", resourceId));
    }

    // Because create call should create parent directories too, before creating an actual file
    // we need to check if there are no conflicting items in the directory tree:
    // - if there are no conflicting files with the same name as any parent subdirectory
    // - if there are no conflicting directory with the name as a file
    //
    // For example, for a new `gs://bucket/c/d/f` file:
    // - files `gs://bucket/c` and `gs://bucket/c/d` should not exist
    // - directory `gs://bucket/c/d/f/` should not exist
    if (options.isEnsureNoConflictingItems()) {
      // Asynchronously check if a directory with the same name exists.
      StorageResourceId dirId = resourceId.toDirectoryId();
      Future<Boolean> conflictingDirExist =
          createOptions.isEnsureNoDirectoryConflict()
              ? cachedExecutor.submit(
                  () -> getFileInfoInternal(dirId, /* inferImplicitDirectories */ true).exists())
              : immediateFuture(false);

      checkNoFilesConflictingWithDirs(resourceId);

      // Check if a directory with the same name exists.
      if (getFromFuture(conflictingDirExist)) {
        throw new FileAlreadyExistsException("A directory with that name exists: " + path);
      }
    }

    if (createOptions.getOverwriteGenerationId() != StorageResourceId.UNKNOWN_GENERATION_ID) {
      resourceId =
          new StorageResourceId(
              resourceId.getBucketName(),
              resourceId.getObjectName(),
              createOptions.getOverwriteGenerationId());
    }

    return gcs.create(resourceId, objectOptionsFromFileOptions(createOptions));
  }

  @Override
  public SeekableByteChannel open(URI path) throws IOException {
    return open(path, GoogleCloudStorageReadOptions.DEFAULT);
  }

  @Override
  public SeekableByteChannel open(URI path, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    logger.atFiner().log("open(path: %s, readOptions: %s)", path, readOptions);
    checkNotNull(path, "path should not be null");
    StorageResourceId resourceId =
        StorageResourceId.fromUriPath(path, /* allowEmptyObjectName= */ false);
    checkArgument(!resourceId.isDirectory(), "Cannot open a directory for reading: %s", path);

    return gcs.open(resourceId, readOptions);
  }

  @Override
  public SeekableByteChannel open(FileInfo fileInfo, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    logger.atFiner().log("open(fileInfo : %s, readOptions: %s)", fileInfo, readOptions);
    checkNotNull(fileInfo, "fileInfo should not be null");
    checkArgument(
        !fileInfo.isDirectory(), "Cannot open a directory for reading: %s", fileInfo.getPath());

    return gcs.open(fileInfo.getItemInfo(), readOptions);
  }

  @Override
  public void delete(URI path, boolean recursive) throws IOException {
    checkNotNull(path, "path should not be null");
    checkArgument(!path.equals(GCS_ROOT), "Cannot delete root path (%s)", path);
    logger.atFiner().log("delete(path: %s, recursive: %b)", path, recursive);

    FileInfo fileInfo = getFileInfo(path);
    if (!fileInfo.exists()) {
      throw new FileNotFoundException("Item not found: " + path);
    }

    Future<GoogleCloudStorageItemInfo> parentInfoFuture = null;
    if (options.getCloudStorageOptions().isAutoRepairImplicitDirectoriesEnabled()) {
      StorageResourceId parentId =
          StorageResourceId.fromUriPath(UriPaths.getParentPath(path), true);
      parentInfoFuture =
          cachedExecutor.submit(
              () -> getFileInfoInternal(parentId, /* inferImplicitDirectories= */ false));
    }

    List<FileInfo> itemsToDelete;
    // Delete sub-items if it is a directory.
    if (fileInfo.isDirectory()) {
      itemsToDelete =
          recursive
              ? listFileInfoForPrefix(fileInfo.getPath(), DELETE_RENAME_LIST_OPTIONS)
              // TODO: optimize by listing just one object instead of whole page
              //  (up to 1024 objects now)
              : listFileInfoForPrefixPage(
                      fileInfo.getPath(), DELETE_RENAME_LIST_OPTIONS, /* pageToken= */ null)
                  .getItems();
      if (!itemsToDelete.isEmpty() && !recursive) {
        throw new DirectoryNotEmptyException("Cannot delete a non-empty directory.");
      }
    } else {
      itemsToDelete = new ArrayList<>();
    }

    List<FileInfo> bucketsToDelete = new ArrayList<>();
    (fileInfo.getItemInfo().isBucket() ? bucketsToDelete : itemsToDelete).add(fileInfo);

    deleteInternal(itemsToDelete, bucketsToDelete);

    repairImplicitDirectory(parentInfoFuture);
  }

  /** Deletes all items in the given path list followed by all bucket items. */
  private void deleteInternal(List<FileInfo> itemsToDelete, List<FileInfo> bucketsToDelete)
      throws IOException {
    // TODO(user): We might need to separate out children into separate batches from parents to
    // avoid deleting a parent before somehow failing to delete a child.

    // Delete children before their parents.
    //
    // Note: we modify the input list, which is ok for current usage.
    // We should make a copy in case that changes in future.
    itemsToDelete.sort(FILE_INFO_PATH_COMPARATOR.reversed());

    if (!itemsToDelete.isEmpty()) {
      List<StorageResourceId> objectsToDelete = new ArrayList<>(itemsToDelete.size());
      for (FileInfo fileInfo : itemsToDelete) {
        // TODO(b/110833109): populate generation ID in StorageResourceId when listing infos?
        if (!fileInfo.isInferredDirectory()) {
          objectsToDelete.add(
              new StorageResourceId(
                  fileInfo.getItemInfo().getBucketName(),
                  fileInfo.getItemInfo().getObjectName(),
                  fileInfo.getItemInfo().getContentGeneration()));
        }
      }
      gcs.deleteObjects(objectsToDelete);
    }

    if (!bucketsToDelete.isEmpty()) {
      List<String> bucketNames = new ArrayList<>(bucketsToDelete.size());
      for (FileInfo bucketInfo : bucketsToDelete) {
        bucketNames.add(bucketInfo.getItemInfo().getResourceId().getBucketName());
      }
      if (options.isBucketDeleteEnabled()) {
        gcs.deleteBuckets(bucketNames);
      } else {
        logger.atInfo().log(
            "Skipping deletion of buckets because enableBucketDelete is false: %s", bucketNames);
      }
    }
  }

  @Override
  public boolean exists(URI path) throws IOException {
    logger.atFiner().log("exists(path: %s)", path);
    return getFileInfo(path).exists();
  }

  @Override
  public void mkdirs(URI path) throws IOException {
    logger.atFiner().log("mkdirs(path: %s)", path);
    checkNotNull(path, "path should not be null");

    mkdirsInternal(StorageResourceId.fromUriPath(path, /* allowEmptyObjectName= */ true));
  }

  private void mkdirsInternal(StorageResourceId resourceId) throws IOException {
    if (resourceId.isRoot()) {
      // GCS_ROOT directory always exists, no need to go through the rest of the method.
      return;
    }

    // In case path is a bucket we just attempt to create it without additional checks
    if (resourceId.isBucket()) {
      try {
        gcs.createBucket(resourceId.getBucketName());
      } catch (FileAlreadyExistsException e) {
        // This means that bucket already exist, and we do not need to do anything.
        logger.atFiner().withCause(e).log(
            "mkdirs: %s already exists, ignoring creation failure", resourceId);
      }
      return;
    }

    resourceId = resourceId.toDirectoryId();

    // Before creating a leaf directory we need to check if there are no conflicting files
    // with the same name as any subdirectory
    if (options.isEnsureNoConflictingItems()) {
      checkNoFilesConflictingWithDirs(resourceId);
    }

    // Create only a leaf directory because subdirectories will be inferred
    // if leaf directory exists
    try {
      gcs.createEmptyObject(resourceId);
    } catch (FileAlreadyExistsException e) {
      // This means that directory object already exist and we do not need to do anything.
      logger.atFiner().withCause(e).log(
          "mkdirs: %s already exists, ignoring creation failure", resourceId);
    }
  }

  @Override
  public void rename(URI src, URI dst) throws IOException {
    logger.atFiner().log("rename(src: %s, dst: %s)", src, dst);
    checkNotNull(src);
    checkNotNull(dst);
    checkArgument(!src.equals(GCS_ROOT), "Root path cannot be renamed.");

    // Parent of the destination path.
    URI dstParent = UriPaths.getParentPath(dst);

    // Obtain info on source, destination and destination-parent.
    List<URI> paths = new ArrayList<>();
    paths.add(src);
    paths.add(dst);
    if (dstParent != null) {
      // dstParent is null if dst is GCS_ROOT.
      paths.add(dstParent);
    }
    List<FileInfo> fileInfos = getFileInfos(paths);
    FileInfo srcInfo = fileInfos.get(0);
    FileInfo dstInfo = fileInfos.get(1);
    FileInfo dstParentInfo = dstParent == null ? null : fileInfos.get(2);

    // Throw if the source file does not exist.
    if (!srcInfo.exists()) {
      throw new FileNotFoundException("Item not found: " + src);
    }

    // Make sure paths match what getFileInfo() returned (it can add / at the end).
    src = srcInfo.getPath();
    dst = getDstUri(srcInfo, dstInfo, dstParentInfo);

    // if src and dst are equal then do nothing
    if (src.equals(dst)) {
      return;
    }

    Future<GoogleCloudStorageItemInfo> srcParentInfoFuture = null;
    if (options.getCloudStorageOptions().isAutoRepairImplicitDirectoriesEnabled()) {
      StorageResourceId srcParentId =
          StorageResourceId.fromUriPath(
              UriPaths.getParentPath(src), /* allowEmptyObjectName= */ true);
      srcParentInfoFuture =
          cachedExecutor.submit(
              () -> getFileInfoInternal(srcParentId, /* inferImplicitDirectories= */ false));
    }

    if (srcInfo.isDirectory()) {
      renameDirectoryInternal(srcInfo, dst);
    } else {
      StorageResourceId srcResourceId =
          StorageResourceId.fromUriPath(src, /* allowEmptyObjectName= */ true);
      StorageResourceId dstResourceId =
          StorageResourceId.fromUriPath(
              dst, /* allowEmptyObjectName= */ true, /* generationId= */ 0L);

      gcs.copy(ImmutableMap.of(srcResourceId, dstResourceId));

      gcs.deleteObjects(
          ImmutableList.of(
              new StorageResourceId(
                  srcInfo.getItemInfo().getBucketName(),
                  srcInfo.getItemInfo().getObjectName(),
                  srcInfo.getItemInfo().getContentGeneration())));
    }

    repairImplicitDirectory(srcParentInfoFuture);
  }

  private URI getDstUri(FileInfo srcInfo, FileInfo dstInfo, @Nullable FileInfo dstParentInfo)
      throws IOException {
    URI src = srcInfo.getPath();
    URI dst = dstInfo.getPath();

    // Throw if src is a file and dst == GCS_ROOT
    if (!srcInfo.isDirectory() && dst.equals(GCS_ROOT)) {
      throw new IOException("A file cannot be created in root.");
    }

    // Throw if the destination is a file that already exists, and it's not a source file.
    if (dstInfo.exists() && !dstInfo.isDirectory() && (srcInfo.isDirectory() || !dst.equals(src))) {
      throw new IOException("Cannot overwrite an existing file: " + dst);
    }

    // Rename operation cannot be completed if parent of destination does not exist.
    if (dstParentInfo != null && !dstParentInfo.exists()) {
      throw new IOException(
          "Cannot rename because path does not exist: " + dstParentInfo.getPath());
    }

    // Leaf item of the source path.
    String srcItemName = getItemName(src);

    // Having taken care of the initial checks, apply the regular rules.
    // After applying the rules, we will be left with 2 paths such that:
    // -- either both are files or both are directories
    // -- src exists and dst leaf does not exist
    if (srcInfo.isDirectory()) {
      // -- if src is a directory
      //    -- dst is an existing file => disallowed
      //    -- dst is a directory => rename the directory.

      // The first case (dst is an existing file) is already checked earlier.
      // If the destination path looks like a file, make it look like a
      // directory path. This is because users often type 'mv foo bar'
      // rather than 'mv foo bar/'.
      if (!dstInfo.isDirectory()) {
        dst = UriPaths.toDirectory(dst);
      }

      // Throw if renaming directory to self - this is forbidden
      if (src.equals(dst)) {
        throw new IOException("Rename dir to self is forbidden");
      }

      URI dstRelativeToSrc = src.relativize(dst);
      // Throw if dst URI relative to src is not equal to dst,
      // because this means that src is a parent directory of dst
      // and src cannot be "renamed" to its subdirectory
      if (!dstRelativeToSrc.equals(dst)) {
        throw new IOException("Rename to subdir is forbidden");
      }

      if (dstInfo.exists()) {
        if (dst.equals(GCS_ROOT)) {
          dst =
              UriPaths.fromStringPathComponents(
                  srcItemName, /* objectName= */ null, /* allowEmptyObjectName= */ true);
        } else {
          dst = UriPaths.toDirectory(dst.resolve(srcItemName));
        }
      }
    } else {
      // -- src is a file
      //    -- dst is a file => rename the file.
      //    -- dst is a directory => similar to the previous case after
      //                             appending src file-name to dst

      if (dstInfo.isDirectory()) {
        if (!dstInfo.exists()) {
          throw new IOException("Cannot rename because path does not exist: " + dstInfo.getPath());
        } else {
          dst = dst.resolve(srcItemName);
        }
      }
    }

    return dst;
  }

  @Override
  public void compose(List<URI> sources, URI destination, String contentType) throws IOException {
    StorageResourceId destResource = StorageResourceId.fromStringPath(destination.toString());
    List<String> sourceObjects =
        sources.stream()
            .map(uri -> StorageResourceId.fromStringPath(uri.toString()).getObjectName())
            .collect(Collectors.toList());
    gcs.compose(
        destResource.getBucketName(), sourceObjects, destResource.getObjectName(), contentType);
  }

  /**
   * Renames given directory without checking any parameters.
   *
   * <p>GCS does not support atomic renames therefore a rename is implemented as copying source
   * metadata to destination and then deleting source metadata. Note that only the metadata is
   * copied and not the content of any file.
   */
  private void renameDirectoryInternal(FileInfo srcInfo, URI dst) throws IOException {
    checkArgument(srcInfo.isDirectory(), "'%s' should be a directory", srcInfo);
    checkArgument(dst.toString().endsWith(PATH_DELIMITER), "'%s' should be a directory", dst);

    URI src = srcInfo.getPath();

    // Mapping from each src to its respective dst.
    // Sort src items so that parent directories appear before their children.
    // That allows us to copy parent directories before we copy their children.
    Map<FileInfo, URI> srcToDstItemNames = new TreeMap<>(FILE_INFO_PATH_COMPARATOR);
    Map<FileInfo, URI> srcToDstMarkerItemNames = new TreeMap<>(FILE_INFO_PATH_COMPARATOR);

    // List of individual paths to rename;
    // we will try to carry out the copies in this list's order.
    List<FileInfo> srcItemInfos = listFileInfoForPrefix(src, DELETE_RENAME_LIST_OPTIONS);

    // Create a list of sub-items to copy.
    Pattern markerFilePattern = options.getMarkerFilePattern();
    String prefix = src.toString();
    for (FileInfo srcItemInfo : srcItemInfos) {
      String relativeItemName = srcItemInfo.getPath().toString().substring(prefix.length());
      URI dstItemName = dst.resolve(relativeItemName);
      if (markerFilePattern != null && markerFilePattern.matcher(relativeItemName).matches()) {
        srcToDstMarkerItemNames.put(srcItemInfo, dstItemName);
      } else {
        srcToDstItemNames.put(srcItemInfo, dstItemName);
      }
    }

    // First, copy all items except marker items
    copyInternal(srcToDstItemNames);
    // Finally, copy marker items (if any) to mark rename operation success
    copyInternal(srcToDstMarkerItemNames);

    List<FileInfo> bucketsToDelete = new ArrayList<>(1);
    List<FileInfo> srcItemsToDelete = new ArrayList<>(srcToDstItemNames.size() + 1);
    srcItemsToDelete.addAll(srcToDstItemNames.keySet());
    if (srcInfo.getItemInfo().isBucket()) {
      bucketsToDelete.add(srcInfo);
    } else {
      // If src is a directory then srcItemInfos does not contain its own name,
      // therefore add it to the list before we delete items in the list.
      srcItemsToDelete.add(srcInfo);
    }

    // First delete marker files from the src
    deleteInternal(new ArrayList<>(srcToDstMarkerItemNames.keySet()), new ArrayList<>());
    // Then delete rest of the items that we successfully copied.
    deleteInternal(srcItemsToDelete, bucketsToDelete);
  }

  /** Copies items in given map that maps source items to destination items. */
  private void copyInternal(Map<FileInfo, URI> srcToDstItemNames) throws IOException {
    if (srcToDstItemNames.isEmpty()) {
      return;
    }

    String srcBucketName = null;
    String dstBucketName = null;
    List<String> srcObjectNames = new ArrayList<>(srcToDstItemNames.size());
    List<String> dstObjectNames = new ArrayList<>(srcToDstItemNames.size());

    // Prepare list of items to copy.
    for (Map.Entry<FileInfo, URI> srcToDstItemName : srcToDstItemNames.entrySet()) {
      StorageResourceId srcResourceId = srcToDstItemName.getKey().getItemInfo().getResourceId();
      srcBucketName = srcResourceId.getBucketName();
      String srcObjectName = srcResourceId.getObjectName();
      srcObjectNames.add(srcObjectName);

      StorageResourceId dstResourceId =
          StorageResourceId.fromUriPath(srcToDstItemName.getValue(), true);
      dstBucketName = dstResourceId.getBucketName();
      String dstObjectName = dstResourceId.getObjectName();
      dstObjectNames.add(dstObjectName);
    }

    // Perform copy.
    gcs.copy(srcBucketName, srcObjectNames, dstBucketName, dstObjectNames);
  }

  /**
   * Attempts to create the directory object explicitly for provided {@code infoFuture} if it
   * doesn't already exist as a directory object.
   */
  private void repairImplicitDirectory(Future<GoogleCloudStorageItemInfo> infoFuture)
      throws IOException {
    if (infoFuture == null) {
      return;
    }

    checkState(
        options.getCloudStorageOptions().isAutoRepairImplicitDirectoriesEnabled(),
        "implicit directories auto repair should be enabled");

    GoogleCloudStorageItemInfo info = getFromFuture(infoFuture);
    StorageResourceId resourceId = info.getResourceId();
    logger.atFiner().log("repairImplicitDirectory(resourceId: %s)", resourceId);

    if (info.exists()
        || resourceId.isRoot()
        || resourceId.isBucket()
        || PATH_DELIMITER.equals(resourceId.getObjectName())) {
      return;
    }

    checkState(resourceId.isDirectory(), "'%s' should be a directory", resourceId);

    try {
      gcs.createEmptyObject(resourceId);
      logger.atInfo().log("Successfully repaired '%s' directory.", resourceId);
    } catch (IOException e) {
      logger.atWarning().withCause(e).log("Failed to repair '%s' directory", resourceId);
    }
  }

  static <T> T getFromFuture(Future<T> future) throws IOException {
    try {
      return future.get();
    } catch (ExecutionException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IOException(
          String.format(
              "Failed to get result: %s", e instanceof ExecutionException ? e.getCause() : e),
          e);
    }
  }

  @Override
  public List<FileInfo> listFileInfoForPrefix(URI prefix, ListFileOptions listOptions)
      throws IOException {
    logger.atFiner().log("listAllFileInfoForPrefix(prefix: %s)", prefix);
    StorageResourceId prefixId = getPrefixId(prefix);
    List<GoogleCloudStorageItemInfo> itemInfos =
        gcs.listObjectInfo(
            prefixId.getBucketName(),
            prefixId.getObjectName(),
            updateListObjectOptions(ListObjectOptions.DEFAULT_FLAT_LIST, listOptions));
    List<FileInfo> fileInfos = FileInfo.fromItemInfos(itemInfos);
    fileInfos.sort(FILE_INFO_PATH_COMPARATOR);
    return fileInfos;
  }

  @Override
  public ListPage<FileInfo> listFileInfoForPrefixPage(
      URI prefix, ListFileOptions listOptions, String pageToken) throws IOException {
    logger.atFiner().log(
        "listAllFileInfoForPrefixPage(prefix: %s, pageToken:%s)", prefix, pageToken);
    StorageResourceId prefixId = getPrefixId(prefix);
    ListPage<GoogleCloudStorageItemInfo> itemInfosPage =
        gcs.listObjectInfoPage(
            prefixId.getBucketName(),
            prefixId.getObjectName(),
            updateListObjectOptions(ListObjectOptions.DEFAULT_FLAT_LIST, listOptions),
            pageToken);
    List<FileInfo> fileInfosPage = FileInfo.fromItemInfos(itemInfosPage.getItems());
    fileInfosPage.sort(FILE_INFO_PATH_COMPARATOR);
    return new ListPage<>(fileInfosPage, itemInfosPage.getNextPageToken());
  }

  private StorageResourceId getPrefixId(URI prefix) {
    checkNotNull(prefix, "prefix could not be null");

    StorageResourceId prefixId = StorageResourceId.fromUriPath(prefix, true);
    checkArgument(!prefixId.isRoot(), "prefix must not be global root, got '%s'", prefix);

    return prefixId;
  }

  @Override
  public List<FileInfo> listFileInfo(URI path, ListFileOptions listOptions) throws IOException {
    checkNotNull(path, "path can not be null");
    logger.atFiner().log("listFileInfo(path: %s)", path);

    StorageResourceId pathId =
        StorageResourceId.fromUriPath(path, /* allowEmptyObjectName= */ true);
    StorageResourceId dirId = pathId.toDirectoryId();

    Future<List<GoogleCloudStorageItemInfo>> dirItemInfosFuture =
        (options.isStatusParallelEnabled() ? cachedExecutor : lazyExecutor)
            .submit(
                () ->
                    dirId.isRoot()
                        ? gcs.listBucketInfo()
                        : gcs.listObjectInfo(
                            dirId.getBucketName(),
                            dirId.getObjectName(),
                            updateListObjectOptions(LIST_FILE_INFO_LIST_OPTIONS, listOptions)));

    if (!pathId.isDirectory()) {
      try {
        GoogleCloudStorageItemInfo pathInfo = gcs.getItemInfo(pathId);
        if (pathInfo.exists()) {
          List<FileInfo> listedInfo = new ArrayList<>();
          listedInfo.add(FileInfo.fromItemInfo(pathInfo));
          dirItemInfosFuture.cancel(/* mayInterruptIfRunning= */ true);
          return listedInfo;
        }
      } catch (Exception e) {
        dirItemInfosFuture.cancel(/* mayInterruptIfRunning= */ true);
        throw e;
      }
    }

    List<GoogleCloudStorageItemInfo> dirItemInfos = getFromFuture(dirItemInfosFuture);
    if (pathId.isStorageObject() && dirItemInfos.isEmpty()) {
      throw new FileNotFoundException("Item not found: " + path);
    }

    if (!dirItemInfos.isEmpty() && Objects.equals(dirItemInfos.get(0).getResourceId(), dirId)) {
      dirItemInfos.remove(0);
    }

    List<FileInfo> fileInfos = FileInfo.fromItemInfos(dirItemInfos);
    fileInfos.sort(FILE_INFO_PATH_COMPARATOR);
    return fileInfos;
  }

  @Override
  public FileInfo getFileInfo(URI path) throws IOException {
    checkArgument(path != null, "path must not be null");
    // Validate the given path. true == allow empty object name.
    // One should be able to get info about top level directory (== bucket),
    // therefore we allow object name to be empty.
    StorageResourceId resourceId = StorageResourceId.fromUriPath(path, true);
    FileInfo fileInfo =
        FileInfo.fromItemInfo(
            getFileInfoInternal(resourceId, /* inferImplicitDirectories= */ true));
    logger.atFiner().log("getFileInfo(path: %s): %s", path, fileInfo);
    return fileInfo;
  }

  /** @see #getFileInfo(URI) */
  private GoogleCloudStorageItemInfo getFileInfoInternal(
      StorageResourceId resourceId, boolean inferImplicitDirectories) throws IOException {
    if (resourceId.isRoot() || resourceId.isBucket()) {
      return gcs.getItemInfo(resourceId);
    }
    StorageResourceId dirId = resourceId.toDirectoryId();

    Future<List<GoogleCloudStorageItemInfo>> listDirFuture =
        (options.isStatusParallelEnabled() ? cachedExecutor : lazyExecutor)
            .submit(
                () ->
                    inferImplicitDirectories
                        ? gcs.listObjectInfo(
                            dirId.getBucketName(),
                            dirId.getObjectName(),
                            GET_FILE_INFO_LIST_OPTIONS)
                        : ImmutableList.of(gcs.getItemInfo(dirId)));

    if (!resourceId.isDirectory()) {
      try {
        GoogleCloudStorageItemInfo itemInfo = gcs.getItemInfo(resourceId);
        if (itemInfo.exists()) {
          listDirFuture.cancel(/* mayInterruptIfRunning= */ true);
          return itemInfo;
        }
      } catch (Exception e) {
        listDirFuture.cancel(/* mayInterruptIfRunning= */ true);
        throw e;
      }
    }

    List<GoogleCloudStorageItemInfo> listDirInfo = getFromFuture(listDirFuture);
    if (listDirInfo.isEmpty()) {
      return GoogleCloudStorageItemInfo.createNotFound(resourceId);
    }
    checkState(listDirInfo.size() <= 2, "listed more than 2 objects: '%s'", listDirInfo);
    GoogleCloudStorageItemInfo dirInfo = Iterables.get(listDirInfo, /* position= */ 0);
    checkState(
        dirInfo.getResourceId().equals(dirId) || !inferImplicitDirectories,
        "listed wrong object '%s', but should be '%s'",
        dirInfo.getResourceId(),
        resourceId);
    return dirInfo.getResourceId().equals(dirId) && dirInfo.exists()
        ? dirInfo
        : GoogleCloudStorageItemInfo.createNotFound(resourceId);
  }

  @Override
  public List<FileInfo> getFileInfos(List<URI> paths) throws IOException {
    checkArgument(paths != null, "paths must not be null");
    logger.atFiner().log("getFileInfos(paths: %s)", paths);

    if (paths.size() == 1) {
      return new ArrayList<>(Collections.singleton(getFileInfo(paths.get(0))));
    }

    int maxThreads = gcs.getOptions().getBatchThreads();
    ExecutorService fileInfoExecutor =
        maxThreads == 0
            ? newDirectExecutorService()
            : newFixedThreadPool(min(maxThreads, paths.size()), DAEMON_THREAD_FACTORY);
    try {
      List<Future<FileInfo>> infoFutures = new ArrayList<>(paths.size());
      for (URI path : paths) {
        infoFutures.add(fileInfoExecutor.submit(() -> getFileInfo(path)));
      }
      fileInfoExecutor.shutdown();

      List<FileInfo> infos = new ArrayList<>(paths.size());
      for (Future<FileInfo> infoFuture : infoFutures) {
        infos.add(getFromFuture(infoFuture));
      }
      return infos;
    } finally {
      fileInfoExecutor.shutdownNow();
    }
  }

  @Override
  public void close() {
    if (gcs == null) {
      return;
    }
    logger.atFiner().log("close()");
    try {
      cachedExecutor.shutdown();
      lazyExecutor.shutdown();
      gcs.close();
    } finally {
      cachedExecutor = null;
      lazyExecutor = null;
      gcs = null;
    }
  }

  @Override
  @VisibleForTesting
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

    // Not a top-level directory, create 0 sized object.
    gcs.createEmptyObject(resourceId);
  }

  private void checkNoFilesConflictingWithDirs(StorageResourceId resourceId) throws IOException {
    // Create a list of all files that can conflict with intermediate/subdirectory paths.
    // For example: gs://foo/bar/zoo/ => (gs://foo/bar, gs://foo/bar/zoo)
    List<StorageResourceId> fileIds =
        getDirs(resourceId.getObjectName()).stream()
            .filter(subdir -> !isNullOrEmpty(subdir))
            .map(
                subdir ->
                    new StorageResourceId(
                        resourceId.getBucketName(), StringPaths.toFilePath(subdir)))
            .collect(toImmutableList());

    // Each intermediate path must ensure that corresponding file does not exist
    //
    // If for any of the intermediate paths file already exists then bail out early.
    // It is possible that the status of intermediate paths can change after
    // we make this check therefore this is a good faith effort and not a guarantee.
    for (GoogleCloudStorageItemInfo fileInfo : gcs.getItemInfos(fileIds)) {
      if (fileInfo.exists()) {
        throw new FileAlreadyExistsException(
            "Cannot create directories because of existing file: " + fileInfo.getResourceId());
      }
    }
  }

  /**
   * For objects whose name looks like a path (foo/bar/zoo), returns all directory paths.
   *
   * <p>For example:
   *
   * <ul>
   *   <li>foo/bar/zoo => returns: (foo/, foo/bar/)
   *   <li>foo/bar/zoo/ => returns: (foo/, foo/bar/, foo/bar/zoo/)
   *   <li>foo => returns: ()
   * </ul>
   *
   * @param objectName Name of an object.
   * @return List of sub-directory like paths.
   */
  static List<String> getDirs(String objectName) {
    if (isNullOrEmpty(objectName)) {
      return ImmutableList.of();
    }
    List<String> dirs = new ArrayList<>();
    int index = 0;
    while ((index = objectName.indexOf(PATH_DELIMITER, index)) >= 0) {
      index = index + PATH_DELIMITER.length();
      dirs.add(objectName.substring(0, index));
    }
    return dirs;
  }

  /** Gets the leaf item of the given path. */
  static String getItemName(URI path) {
    checkNotNull(path, "path can not be null");

    // There is no leaf item for the root path.
    if (path.equals(GCS_ROOT)) {
      return null;
    }

    StorageResourceId resourceId = StorageResourceId.fromUriPath(path, true);

    if (resourceId.isBucket()) {
      return resourceId.getBucketName();
    }

    String objectName = resourceId.getObjectName();
    int index =
        StringPaths.isDirectoryPath(objectName)
            ? objectName.lastIndexOf(PATH_DELIMITER, objectName.length() - 2)
            : objectName.lastIndexOf(PATH_DELIMITER);
    return index < 0 ? objectName : objectName.substring(index + 1);
  }

  private static ListObjectOptions updateListObjectOptions(
      ListObjectOptions listObjectOptions, ListFileOptions listFileOptions) {
    return listObjectOptions.toBuilder().setFields(listFileOptions.getFields()).build();
  }

  @Override
  public GoogleCloudStorage getGcs() {
    return gcs;
  }
}
