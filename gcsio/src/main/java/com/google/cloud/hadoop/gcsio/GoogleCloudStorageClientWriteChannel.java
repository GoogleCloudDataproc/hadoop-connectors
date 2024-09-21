/*
 * Copyright 2023 Google LLC
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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.encodeMetadata;

import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** Implements WritableByteChannel to provide write access to GCS via java-storage client */
class GoogleCloudStorageClientWriteChannel implements WritableByteChannel {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final StorageResourceId resourceId;
  private WritableByteChannel writableByteChannel;

  // TODO: not supported as of now
  // private final String requesterPaysProject;

  public GoogleCloudStorageClientWriteChannel(
      Storage storage,
      GoogleCloudStorageOptions storageOptions,
      StorageResourceId resourceId,
      CreateObjectOptions createOptions,
      Function<String, Boolean> requesterShouldPay)
      throws IOException {
    this.resourceId = resourceId;
    BlobWriteSession blobWriteSession =
        getBlobWriteSession(storage, resourceId, createOptions, storageOptions, requesterShouldPay);
    this.writableByteChannel = blobWriteSession.open();
  }

  private static BlobInfo getBlobInfo(
      StorageResourceId resourceId, CreateObjectOptions createOptions) {
    BlobInfo blobInfo =
        BlobInfo.newBuilder(
                BlobId.of(
                    resourceId.getBucketName(),
                    resourceId.getObjectName(),
                    resourceId.getGenerationId()))
            .setContentType(createOptions.getContentType())
            .setContentEncoding(createOptions.getContentEncoding())
            .setMetadata(encodeMetadata(createOptions.getMetadata()))
            .build();
    return blobInfo;
  }

  private static BlobWriteSession getBlobWriteSession(
      Storage storage,
      StorageResourceId resourceId,
      CreateObjectOptions createOptions,
      GoogleCloudStorageOptions storageOptions,
      Function<String, Boolean> requesterShouldPay) {
    return storage.blobWriteSession(
        getBlobInfo(resourceId, createOptions),
        generateWriteOptions(resourceId, createOptions, storageOptions, requesterShouldPay));
  }

  private static BlobWriteOption[] generateWriteOptions(
      StorageResourceId resourceId,
      CreateObjectOptions createOptions,
      GoogleCloudStorageOptions storageOptions,
      Function<String, Boolean> requesterShouldPay) {
    List<BlobWriteOption> blobWriteOptions = new ArrayList<>();
    blobWriteOptions.add(BlobWriteOption.disableGzipContent());
    blobWriteOptions.add(BlobWriteOption.generationMatch());
    if (createOptions.getKmsKeyName() != null) {
      blobWriteOptions.add(BlobWriteOption.kmsKeyName(createOptions.getKmsKeyName()));
    }
    if (storageOptions.getWriteChannelOptions().isGrpcChecksumsEnabled()) {
      blobWriteOptions.add(BlobWriteOption.crc32cMatch());
    }
    if (storageOptions.getEncryptionKey() != null) {
      blobWriteOptions.add(
          BlobWriteOption.encryptionKey(storageOptions.getEncryptionKey().value()));
    }
    if (requesterShouldPay.apply(resourceId.getBucketName())) {
      blobWriteOptions.add(
          BlobWriteOption.userProject(storageOptions.getRequesterPaysOptions().getProjectId()));
    }
    return blobWriteOptions.toArray(new BlobWriteOption[blobWriteOptions.size()]);
  }

  @Override
  public boolean isOpen() {
    return writableByteChannel != null && writableByteChannel.isOpen();
  }

  @Override
  public void close() throws IOException {
    try {
      if (!isOpen()) {
        return;
      }

      // WriteChannel close is overloaded with
      // 1. object closable
      // 2. finalizing gcs-object
      // TODO: what if we want to close the object and free up the resources but not call finalize
      // the gcs-object.
      writableByteChannel.close();
    } catch (Exception e) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(
          String.format("Upload failed for '%s'. reason=%s", resourceId, e.getMessage()), e);
    } finally {
      writableByteChannel = null;
    }
  }

  private int writeInternal(ByteBuffer byteBuffer) throws IOException {
    int bytesWritten = writableByteChannel.write(byteBuffer);
    logger.atFinest().log(
        "%d bytes were written out of provided buffer of capacity %d, for resourceId %s",
        bytesWritten, byteBuffer.limit(), resourceId);
    return bytesWritten;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return writeInternal(src);
  }
}
