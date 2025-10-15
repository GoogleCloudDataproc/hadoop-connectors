/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.encodeMetadata;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.storage.*;
import com.google.cloud.storage.BlobAppendableUpload.AppendableUploadWriteableByteChannel;
import com.google.cloud.storage.StorageChannelUtils;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;

@VisibleForTesting
public class GoogleCloudStorageBidiWriteChannel implements FinalizableWritableByteChannel {
  private final AppendableUploadWriteableByteChannel gcsAppendChannel;
  private boolean open = true;

  public GoogleCloudStorageBidiWriteChannel(
      Storage storage,
      GoogleCloudStorageOptions storageOptions,
      StorageResourceId resourceId,
      CreateObjectOptions createOptions)
      throws IOException {

    checkNotNull(storage, "storage cannot be null");
    checkNotNull(resourceId, "resourceId cannot be null");

    BlobAppendableUpload appendUploadSession =
        getBlobAppendableUploadSession(storage, resourceId, createOptions, storageOptions);

    try {
      this.gcsAppendChannel = appendUploadSession.open();
    } catch (StorageException e) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException("Failed to initialize appendable upload session for: " + resourceId, e);
    }
  }

  private static BlobAppendableUpload getBlobAppendableUploadSession(
      Storage storage,
      StorageResourceId resourceId,
      CreateObjectOptions createOptions,
      GoogleCloudStorageOptions storageOptions) {
    BlobAppendableUploadConfig.CloseAction closeAction =
        storageOptions.isFinalizeBeforeClose()
            ? BlobAppendableUploadConfig.CloseAction.FINALIZE_WHEN_CLOSING
            : BlobAppendableUploadConfig.CloseAction.CLOSE_WITHOUT_FINALIZING;
    return storage.blobAppendableUpload(
        getBlobInfo(resourceId, createOptions),
        BlobAppendableUploadConfig.of().withCloseAction(closeAction),
        generateWriteOptions(createOptions, storageOptions));
  }

  private static BlobInfo getBlobInfo(
      StorageResourceId resourceId, CreateObjectOptions createOptions) {
    return BlobInfo.newBuilder(BlobId.of(resourceId.getBucketName(), resourceId.getObjectName()))
        .setContentType(createOptions.getContentType())
        .setContentEncoding(createOptions.getContentEncoding())
        .setMetadata(encodeMetadata(createOptions.getMetadata()))
        .build();
  }

  private static Storage.BlobWriteOption[] generateWriteOptions(
      CreateObjectOptions createOptions, GoogleCloudStorageOptions storageOptions) {
    List<Storage.BlobWriteOption> blobWriteOptions = new ArrayList<>();

    blobWriteOptions.add(Storage.BlobWriteOption.disableGzipContent());
    if (createOptions.getKmsKeyName() != null) {
      blobWriteOptions.add(Storage.BlobWriteOption.kmsKeyName(createOptions.getKmsKeyName()));
    }
    if (storageOptions.getWriteChannelOptions().isGrpcChecksumsEnabled()) {
      blobWriteOptions.add(Storage.BlobWriteOption.crc32cMatch());
    }
    if (storageOptions.getEncryptionKey() != null) {
      blobWriteOptions.add(
          Storage.BlobWriteOption.encryptionKey(storageOptions.getEncryptionKey().value()));
    }
    return blobWriteOptions.toArray(new Storage.BlobWriteOption[blobWriteOptions.size()]);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    if (!open) throw new ClosedChannelException();
    checkNotNull(src, "Source ByteBuffer (src) cannot be null");
    return StorageChannelUtils.blockingEmptyTo(src, gcsAppendChannel);
  }

  @Override
  public void close() throws IOException {
    if (!open) {
      return;
    }
    open = false;

    if (gcsAppendChannel == null) {
      return;
    }

    gcsAppendChannel.close();
  }

  @Override
  public void finalizeAndClose() throws IOException {
    if (!open) {
      return;
    }
    open = false;

    if (gcsAppendChannel == null) {
      return;
    }

    gcsAppendChannel.finalizeAndClose();
  }
}
