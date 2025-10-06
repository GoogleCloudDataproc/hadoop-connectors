/*
 * Copyright 2014 Google Inc.
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

package com.google.cloud.hadoop.gcsio.testing;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageExceptions.createFileNotFoundException;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.client.util.Clock;
import com.google.cloud.hadoop.gcsio.CreateBucketOptions;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.FolderInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageExceptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageStrings;
import com.google.cloud.hadoop.gcsio.ListFolderOptions;
import com.google.cloud.hadoop.gcsio.ListObjectOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.UpdatableItemInfo;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * InMemoryGoogleCloudStorage overrides the public methods of GoogleCloudStorage by implementing all
 * the equivalent bucket/object semantics with local in-memory storage.
 */
public class InMemoryGoogleCloudStorage implements GoogleCloudStorage {

  private static final CreateObjectOptions EMPTY_OBJECT_CREATE_OPTIONS =
      CreateObjectOptions.DEFAULT_OVERWRITE.toBuilder()
          .setEnsureEmptyObjectsMetadataMatch(false)
          .build();

  // Mapping from bucketName to structs representing a bucket.
  private final Map<String, InMemoryBucketEntry> bucketLookup = new TreeMap<>();
  private final GoogleCloudStorageOptions storageOptions;
  private final Clock clock;

  public InMemoryGoogleCloudStorage() {
    this(getInMemoryGoogleCloudStorageOptions());
  }

  public InMemoryGoogleCloudStorage(GoogleCloudStorageOptions storageOptions) {
    this(storageOptions, Clock.SYSTEM);
  }

  public InMemoryGoogleCloudStorage(GoogleCloudStorageOptions storageOptions, Clock clock) {
    this.storageOptions = storageOptions;
    this.clock = clock;
  }

  public static GoogleCloudStorageOptions getInMemoryGoogleCloudStorageOptions() {
    return GoogleCloudStorageOptions.builder().setAppName("GHFS/in-memory").build();
  }

  @Override
  public GoogleCloudStorageOptions getOptions() {
    return storageOptions;
  }

  private boolean validateBucketName(String bucketName) {
    // Validation as per https://developers.google.com/storage/docs/bucketnaming
    if (isNullOrEmpty(bucketName)) {
      return false;
    }

    if (bucketName.length() < 3) {
      return false;
    }

    if (!bucketName.matches("^[a-z0-9][a-z0-9_.-]*[a-z0-9]$")) {
      return false;
    }

    return bucketName.length() <= 63;

    // TODO(user): Handle dots and names longer than 63, but less than 222.
  }

  private boolean validateObjectName(String objectName) {
    // Validation as per https://developers.google.com/storage/docs/bucketnaming
    // Object names must be less than 1024 bytes and may not contain
    // CR or LF characters.
    return !(objectName.length() > 1024
        || objectName.indexOf((char) 0x0A) > -1
        || objectName.indexOf((char) 0x0D) > -1);
  }

  @Override
  public synchronized WritableByteChannel create(
      StorageResourceId resourceId, CreateObjectOptions options) throws IOException {
    if (!bucketLookup.containsKey(resourceId.getBucketName())) {
      throw new IOException(
          String.format(
              "Tried to insert object '%s' into nonexistent bucket '%s'",
              resourceId.getObjectName(), resourceId.getBucketName()));
    }
    if (!validateObjectName(resourceId.getObjectName())) {
      throw new IOException("Error creating object. Invalid name: " + resourceId.getObjectName());
    }
    if (resourceId.hasGenerationId() && resourceId.getGenerationId() != 0L) {
      GoogleCloudStorageItemInfo itemInfo = getItemInfo(resourceId);
      if (itemInfo.getContentGeneration() != resourceId.getGenerationId()) {
        throw new IOException(
            String.format(
                "Required generationId '%d' doesn't match existing '%d' for '%s'",
                resourceId.getGenerationId(), itemInfo.getContentGeneration(), resourceId));
      }
    }
    if (!options.isOverwriteExisting() || resourceId.getGenerationId() == 0L) {
      if (getItemInfo(resourceId).exists()) {
        throw new FileAlreadyExistsException(String.format("%s exists.", resourceId));
      }
    }
    InMemoryObjectEntry entry =
        new InMemoryObjectEntry(
            resourceId.getBucketName(),
            resourceId.getObjectName(),
            clock.currentTimeMillis(),
            clock.currentTimeMillis(),
            options.getContentType(),
            options.getContentEncoding(),
            options.getMetadata());
    bucketLookup.get(resourceId.getBucketName()).add(entry);
    return entry.getWriteChannel();
  }

  @Override
  public synchronized void createBucket(String bucketName, CreateBucketOptions options)
      throws IOException {
    if (!validateBucketName(bucketName)) {
      throw new IOException("Error creating bucket. Invalid name: " + bucketName);
    }
    if (bucketLookup.containsKey(bucketName)) {
      throw new FileAlreadyExistsException("Bucket '" + bucketName + "' already exists");
    }
    bucketLookup.put(
        bucketName,
        new InMemoryBucketEntry(
            bucketName, clock.currentTimeMillis(), clock.currentTimeMillis(), options));
  }

  @Override
  public synchronized void createEmptyObject(StorageResourceId resourceId) throws IOException {
    createEmptyObject(resourceId, EMPTY_OBJECT_CREATE_OPTIONS);
  }

  @Override
  public synchronized void createEmptyObject(
      StorageResourceId resourceId, CreateObjectOptions options) throws IOException {
    // TODO(user): Since this class is not performance-tuned, we'll just delegate to the
    // write-channel version of the method.
    create(resourceId, options).close();
  }

  @Override
  public synchronized void createEmptyObjects(List<StorageResourceId> resourceIds)
      throws IOException {
    createEmptyObjects(resourceIds, EMPTY_OBJECT_CREATE_OPTIONS);
  }

  @Override
  public synchronized void createEmptyObjects(
      List<StorageResourceId> resourceIds, CreateObjectOptions options) throws IOException {
    for (StorageResourceId resourceId : resourceIds) {
      createEmptyObject(resourceId, options);
    }
  }

  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions) throws IOException {
    return open(getItemInfo(resourceId), readOptions);
  }

  @Override
  public SeekableByteChannel open(
      GoogleCloudStorageItemInfo itemInfo, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    if (!itemInfo.exists()) {
      IOException notFoundException =
          createFileNotFoundException(
              itemInfo.getBucketName(), itemInfo.getObjectName(), /* cause= */ null);

      if (readOptions.isFastFailOnNotFoundEnabled()) {
        throw notFoundException;
      }

      // We'll need to simulate a lazy-evaluating byte channel which only detects nonexistence
      // on size() and read(ByteBuffer) calls.
      return new SeekableByteChannel() {
        private long position = 0;
        private boolean isOpen = true;

        @Override
        public long position() {
          return position;
        }

        @CanIgnoreReturnValue
        @Override
        public SeekableByteChannel position(long newPosition) {
          position = newPosition;
          return this;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
          throw notFoundException;
        }

        @Override
        public long size() throws IOException {
          throw notFoundException;
        }

        @Override
        public SeekableByteChannel truncate(long size) {
          throw new UnsupportedOperationException("Cannot mutate read-only channel");
        }

        @Override
        public int write(ByteBuffer src) {
          throw new UnsupportedOperationException("Cannot mutate read-only channel");
        }

        @Override
        public void close() {
          isOpen = false;
        }

        @Override
        public boolean isOpen() {
          return isOpen;
        }
      };
    }

    return bucketLookup
        .get(itemInfo.getBucketName())
        .get(itemInfo.getObjectName())
        .getReadChannel(itemInfo.getBucketName(), itemInfo.getObjectName(), readOptions);
  }

  @Override
  public synchronized void deleteBuckets(List<String> bucketNames) throws IOException {
    boolean hasError = false;
    for (String bucketName : bucketNames) {
      // TODO(user): Enforcement of not being able to delete non-empty buckets should probably
      // also
      // be in here, but gcsfs handles it explicitly when it calls listObjectInfo.
      if (bucketLookup.containsKey(bucketName)) {
        bucketLookup.remove(bucketName);
      } else {
        hasError = true;
      }
      hasError = hasError || !validateBucketName(bucketName);
    }

    if (hasError) {
      throw new IOException("Error deleting");
    }
  }

  @Override
  public synchronized void deleteObjects(List<StorageResourceId> fullObjectNames)
      throws IOException {

    for (StorageResourceId resourceId : fullObjectNames) {
      if (!validateObjectName(resourceId.getObjectName())) {
        throw new IOException("Error deleting object. Invalid name: " + resourceId.getObjectName());
      }
    }

    for (StorageResourceId fullObjectName : fullObjectNames) {
      String bucketName = fullObjectName.getBucketName();
      String objectName = fullObjectName.getObjectName();
      if (fullObjectName.hasGenerationId()) {
        GoogleCloudStorageItemInfo existingInfo = getItemInfo(fullObjectName);
        if (existingInfo.getContentGeneration() != fullObjectName.getGenerationId()) {
          throw new IOException(
              String.format(
                  "Required generationId '%d' doesn't match existing '%d' for '%s'",
                  fullObjectName.getGenerationId(),
                  existingInfo.getContentGeneration(),
                  fullObjectName));
        }
      }
      bucketLookup.get(bucketName).remove(objectName);
    }
  }

  @Override
  public void deleteFolders(List<FolderInfo> folders) throws IOException {
    throw new IOException("Not implemented");
  }

  @Override
  public void createFolder(StorageResourceId resourceId, boolean recursive) throws IOException {
    String bucketName = resourceId.getBucketName();
    if (!validateBucketName(bucketName)) {
      throw new IOException("Error creating folder. Invalid bucket name: " + bucketName);
    }
    if (!bucketLookup.containsKey(bucketName)) {
      throw new IOException("Bucket does not exist: " + bucketName);
    }
    // Check for any conflicting resource (file, placeholder, or native folder).
    if (getItemInfo(resourceId).exists()) {
      throw new FileAlreadyExistsException("Folder or object '" + resourceId + "' already exists");
    }

    // Simulate the Folder object that the real API would return.
    long now = clock.currentTimeMillis();
    Timestamp timestamp = Timestamps.fromMillis(now);

    com.google.storage.control.v2.Folder fakeApiFolder =
        com.google.storage.control.v2.Folder.newBuilder()
            .setName(
                String.format(
                    "projects/_/buckets/%s/folders/%s",
                    resourceId.getBucketName(), resourceId.getObjectName()))
            .setMetageneration(1L)
            .setCreateTime(timestamp)
            .setUpdateTime(timestamp)
            .build();

    GoogleCloudStorageItemInfo folderItemInfo =
        GoogleCloudStorageItemInfo.createFolder(resourceId, fakeApiFolder);

    InMemoryObjectEntry folderEntry = new InMemoryObjectEntry(folderItemInfo);

    // Add it to in-memory store
    bucketLookup.get(resourceId.getBucketName()).add(folderEntry);
  }

  @Override
  public GoogleCloudStorageItemInfo getFolderInfo(StorageResourceId resourceId) throws IOException {
    GoogleCloudStorageItemInfo itemInfo = getItemInfo(resourceId);

    if (itemInfo.exists() && itemInfo.isNativeHNSFolder()) {
      return itemInfo;
    }

    // If it doesn't exist or isn't a native folder, return not found.
    return GoogleCloudStorageItemInfo.createNotFound(resourceId);
  }

  @Override
  public synchronized void move(
      Map<StorageResourceId, StorageResourceId> sourceToDestinationObjectsMap) throws IOException {
    if (sourceToDestinationObjectsMap == null) {
      throw new IllegalArgumentException("sourceToDestinationObjectsMap must not be null");
    }

    if (sourceToDestinationObjectsMap.isEmpty()) {
      return;
    }

    // Gather exceptions
    List<IOException> innerExceptions = new ArrayList<>();

    for (Map.Entry<StorageResourceId, StorageResourceId> entry :
        sourceToDestinationObjectsMap.entrySet()) {
      StorageResourceId srcObject = entry.getKey();
      StorageResourceId dstObject = entry.getValue();

      if (!validateObjectName(srcObject.getObjectName())
          || !validateObjectName(dstObject.getObjectName())) {
        innerExceptions.add(
            new IOException(
                String.format(
                    "Invalid object name for move source '%s' or destination '%s'",
                    srcObject, dstObject)));
        continue;
      }

      try {
        GoogleCloudStorageItemInfo srcInfo = getItemInfo(srcObject);
        if (!srcInfo.exists()) {
          // If the source is not found, add an error to the list and continue.
          innerExceptions.add(
              createFileNotFoundException(
                  srcObject.getBucketName(), srcObject.getObjectName(), /* cause= */ null));
          continue;
        }

        // Simulate copy part of the move.
        InMemoryBucketEntry srcBucketEntry = bucketLookup.get(srcObject.getBucketName());
        InMemoryObjectEntry srcEntry = srcBucketEntry.get(srcObject.getObjectName());

        bucketLookup
            .get(dstObject.getBucketName())
            .add(srcEntry.getShallowCopy(dstObject.getBucketName(), dstObject.getObjectName()));

        // simulate delete
        srcBucketEntry.remove(srcObject.getObjectName());
      } catch (IOException e) {
        innerExceptions.add(e);
      }
    }

    if (!innerExceptions.isEmpty()) {
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
  }

  @Override
  public synchronized void copy(
      String srcBucketName,
      List<String> srcObjectNames,
      String dstBucketName,
      List<String> dstObjectNames)
      throws IOException {
    GoogleCloudStorageImpl.validateCopyArguments(
        srcBucketName, srcObjectNames, dstBucketName, dstObjectNames, this);

    // Gather FileNotFoundExceptions for individual objects, but only throw a single combined
    // exception at the end.
    // TODO(user): Add a unittest for this entire class to test for the behavior of partial
    // failures; there is no way to do so in GCSFSIT because it only indirectly calls GCS.copy.
    List<IOException> innerExceptions = new ArrayList<>();

    // Perform the copy operations.
    for (int i = 0; i < srcObjectNames.size(); i++) {
      // Due to the metadata-copy semantics of GCS, we copy the object container, but not the
      // byte[]
      // contents; the write-once constraint means this behavior is indistinguishable from a
      // deep
      // copy, but the behavior might have to become complicated if GCS ever supports appends.
      if (!getItemInfo(new StorageResourceId(srcBucketName, srcObjectNames.get(i))).exists()) {
        innerExceptions.add(
            createFileNotFoundException(srcBucketName, srcObjectNames.get(i), /* cause= */ null));
        continue;
      }

      InMemoryObjectEntry srcObject = bucketLookup.get(srcBucketName).get(srcObjectNames.get(i));
      bucketLookup
          .get(dstBucketName)
          .add(srcObject.getShallowCopy(dstBucketName, dstObjectNames.get(i)));
    }

    if (!innerExceptions.isEmpty()) {
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
  }

  @Override
  public synchronized List<String> listBucketNames() {
    return new ArrayList<>(bucketLookup.keySet());
  }

  @Override
  public synchronized List<GoogleCloudStorageItemInfo> listBucketInfo() {
    List<GoogleCloudStorageItemInfo> bucketInfos = new ArrayList<>();
    for (InMemoryBucketEntry entry : bucketLookup.values()) {
      bucketInfos.add(entry.getInfo());
    }
    return bucketInfos;
  }

  private synchronized List<String> listObjectNames(
      String bucketName, String objectNamePrefix, ListObjectOptions listOptions) {
    InMemoryBucketEntry bucketEntry = bucketLookup.get(bucketName);
    if (bucketEntry == null) {
      return new ArrayList<>();
    }
    Set<String> uniqueNames = new TreeSet<>();
    for (String objectName : bucketEntry.getObjectNames()) {
      String processedName =
          GoogleCloudStorageStrings.matchListPrefix(objectNamePrefix, objectName, listOptions);
      if (processedName != null) {
        uniqueNames.add(processedName);
      }
      if (listOptions.getMaxResults() > 0 && uniqueNames.size() >= listOptions.getMaxResults()) {
        break;
      }
    }
    if (listOptions.isIncludePrefix() && !uniqueNames.isEmpty() && objectNamePrefix != null) {
      uniqueNames.add(objectNamePrefix);
    }
    return new ArrayList<>(uniqueNames);
  }

  private synchronized List<String> listObjectNamesStartingFrom(
      String bucketName, String startOffset, ListObjectOptions listOptions) {
    InMemoryBucketEntry bucketEntry = bucketLookup.get(bucketName);
    if (bucketEntry == null) {
      return new ArrayList<>();
    }

    Set<String> uniqueNames = new TreeSet<>();
    for (String objectName : bucketEntry.getObjectNames()) {
      if (objectName.compareTo(startOffset) >= 0) {
        uniqueNames.add(objectName);
      }
      if (listOptions.getMaxResults() > 0 && uniqueNames.size() >= listOptions.getMaxResults()) {
        break;
      }
    }
    return uniqueNames.stream().sorted().collect(Collectors.toList());
  }

  @Override
  public ListPage<GoogleCloudStorageItemInfo> listObjectInfoPage(
      String bucketName, String objectNamePrefix, ListObjectOptions listOptions, String pageToken)
      throws IOException {
    // TODO: implement pagination
    return new ListPage<>(
        listObjectInfo(bucketName, objectNamePrefix, listOptions), /* nextPageToken= */ null);
  }

  @Override
  public ListPage<FolderInfo> listFolderInfoForPrefixPage(
      String bucketName,
      String objectNamePrefix,
      ListFolderOptions listFolderOptions,
      String pageToken)
      throws IOException {
    throw new IOException("Not implemented");
  }

  @Override
  public synchronized List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, ListObjectOptions listOptions)
      throws IOException {
    // Since we're just in memory, we can do the naive implementation of just listing names and
    // then calling getItemInfo for each.
    List<String> listedNames =
        listObjectNames(
            bucketName,
            objectNamePrefix,
            listOptions.toBuilder().setMaxResults(MAX_RESULTS_UNLIMITED).build());
    return convertToItemInfo(bucketName, listedNames, listOptions);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfoStartingFrom(
      String bucketName, String startOffset, ListObjectOptions listOptions) throws IOException {
    // Since we're just in memory, we can do the naive implementation of just listing names and
    // then calling getItemInfo for each.
    List<String> listObjectNamesStartingFrom =
        listObjectNamesStartingFrom(bucketName, startOffset, listOptions);
    return convertToItemInfo(bucketName, listObjectNamesStartingFrom, listOptions);
  }

  public void renameHnFolder(URI src, URI dst) throws IOException {
    throw new IOException("Not implemented");
  }

  @Override
  public boolean isHnBucket(URI src) throws IOException {
    return false;
  }

  @Override
  public synchronized GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId)
      throws IOException {
    if (resourceId.isRoot()) {
      return GoogleCloudStorageItemInfo.ROOT_INFO;
    }

    if (resourceId.isBucket()) {
      if (bucketLookup.containsKey(resourceId.getBucketName())) {
        return bucketLookup.get(resourceId.getBucketName()).getInfo();
      }
    } else {
      if (!validateObjectName(resourceId.getObjectName())) {
        throw new IOException(
            String.format("Invalid object name: '%s'", resourceId.getObjectName()));
      }
      if (bucketLookup.containsKey(resourceId.getBucketName())
          && bucketLookup.get(resourceId.getBucketName()).get(resourceId.getObjectName()) != null) {
        return bucketLookup
            .get(resourceId.getBucketName())
            .get(resourceId.getObjectName())
            .getInfo();
      }
    }
    return GoogleCloudStorageItemInfo.createNotFound(resourceId);
  }

  @Override
  public synchronized List<GoogleCloudStorageItemInfo> getItemInfos(
      List<StorageResourceId> resourceIds) throws IOException {
    List<GoogleCloudStorageItemInfo> itemInfos = new ArrayList<>();
    for (StorageResourceId resourceId : resourceIds) {
      try {
        itemInfos.add(getItemInfo(resourceId));
      } catch (IOException ioe) {
        throw new IOException("Error getting StorageObject", ioe);
      }
    }
    return itemInfos;
  }

  @Override
  public List<GoogleCloudStorageItemInfo> updateItems(List<UpdatableItemInfo> itemInfoList)
      throws IOException {
    List<GoogleCloudStorageItemInfo> itemInfos = new ArrayList<>();
    for (UpdatableItemInfo updatableItemInfo : itemInfoList) {
      StorageResourceId resourceId = updatableItemInfo.getStorageResourceId();
      checkArgument(
          !resourceId.isRoot() && !resourceId.isBucket(),
          "Can't update item on GCS Root or bucket resources");
      if (!validateObjectName(resourceId.getObjectName())) {
        throw new IOException("Error accessing");
      }
      if (bucketLookup.containsKey(resourceId.getBucketName())
          && bucketLookup.get(resourceId.getBucketName()).get(resourceId.getObjectName()) != null) {
        InMemoryObjectEntry objectEntry =
            bucketLookup.get(resourceId.getBucketName()).get(resourceId.getObjectName());
        objectEntry.patchMetadata(updatableItemInfo.getMetadata());
        itemInfos.add(getItemInfo(resourceId));
      } else {
        throw new IOException(String.format("Error getting StorageObject %s", resourceId));
      }
    }
    return itemInfos;
  }

  @Override
  public void close() {}

  @Override
  public void compose(
      String bucketName, List<String> sources, String destination, String contentType)
      throws IOException {
    List<StorageResourceId> sourceResourcesIds =
        sources.stream()
            .map(s -> new StorageResourceId(bucketName, s))
            .collect(Collectors.toList());
    StorageResourceId destinationId = new StorageResourceId(bucketName, destination);
    CreateObjectOptions options =
        CreateObjectOptions.DEFAULT_OVERWRITE.toBuilder().setContentType(contentType).build();
    composeObjects(sourceResourcesIds, destinationId, options);
  }

  @Override
  public GoogleCloudStorageItemInfo composeObjects(
      List<StorageResourceId> sources, StorageResourceId destination, CreateObjectOptions options)
      throws IOException {
    checkArgument(
        sources.size() <= MAX_COMPOSE_OBJECTS,
        "Can not compose more than %s sources",
        MAX_COMPOSE_OBJECTS);
    ByteArrayOutputStream tempOutput = new ByteArrayOutputStream();
    for (StorageResourceId sourceId : sources) {
      // TODO(user): If we change to also set generationIds for source objects in the base
      // GoogleCloudStorageImpl, make sure to also add a generationId check here.
      try (SeekableByteChannel sourceChannel = open(sourceId)) {
        byte[] bufferArray = new byte[4 * 1024 * 1024];
        int bytesRead;
        do {
          ByteBuffer buffer = ByteBuffer.wrap(bufferArray);
          bytesRead = sourceChannel.read(buffer);
          tempOutput.write(bufferArray, 0, buffer.position());
        } while (bytesRead >= 0);
      }
    }

    // If destination.hasGenerationId(), it'll automatically get enforced here by the create()
    // implementation.
    try (WritableByteChannel destChannel = create(destination, options)) {
      destChannel.write(ByteBuffer.wrap(tempOutput.toByteArray()));
    }
    return getItemInfo(destination);
  }

  @Override
  public Map<String, Long> getStatistics() {
    throw new UnsupportedOperationException("not implemented");
  }

  private List<GoogleCloudStorageItemInfo> convertToItemInfo(
      String bucketName, final List<String> listedNames, ListObjectOptions listOptions)
      throws IOException {
    List<GoogleCloudStorageItemInfo> listedInfo = new ArrayList<>();
    for (String objectName : listedNames) {
      GoogleCloudStorageItemInfo itemInfo =
          getItemInfo(new StorageResourceId(bucketName, objectName));
      if (itemInfo.exists()) {
        listedInfo.add(itemInfo);
      } else if (itemInfo.getResourceId().isStorageObject()) {
        listedInfo.add(
            GoogleCloudStorageItemInfo.createInferredDirectory(itemInfo.getResourceId()));
      }
      if (listOptions.getMaxResults() > 0 && listedInfo.size() >= listOptions.getMaxResults()) {
        break;
      }
    }
    return listedInfo;
  }
}
