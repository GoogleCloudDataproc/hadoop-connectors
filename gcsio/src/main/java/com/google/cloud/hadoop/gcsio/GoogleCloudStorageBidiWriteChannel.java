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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@VisibleForTesting
public class GoogleCloudStorageBidiWriteChannel implements FinalizableWritableByteChannel {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final AppendableUploadWriteableByteChannel gcsAppendChannel;
  private boolean open = true;
  private final StorageResourceId resourceId;

  public GoogleCloudStorageBidiWriteChannel(
      Storage storage,
      GoogleCloudStorageOptions storageOptions,
      StorageResourceId resourceId,
      CreateObjectOptions createOptions)
      throws IOException {

    checkNotNull(storage, "storage cannot be null");
    checkNotNull(resourceId, "resourceId cannot be null");
    this.resourceId = resourceId;

    logger.atSevere().log("GCSBidiWriteChannel: Constructor called for resourceId: %s", resourceId);
    logger.atSevere().log("GCSBidiWriteChannel: storageOptions: %s", storageOptions);
    logger.atSevere().log("GCSBidiWriteChannel: createOptions: %s", createOptions);

    BlobAppendableUpload appendUploadSession =
        getBlobAppendableUploadSession(storage, resourceId, createOptions, storageOptions);

    logger.atSevere().log(
        "GCSBidiWriteChannel: BlobAppendableUpload session created for: %s", resourceId);

    try {
      this.gcsAppendChannel = appendUploadSession.open();
      logger.atSevere().log(
          "GCSBidiWriteChannel: AppendableUploadWriteableByteChannel opened successfully for: %s",
          resourceId);
    } catch (StorageException e) {
      GoogleCloudStorageEventBus.postOnException();
      logger.atSevere().withCause(e).log(
          "GCSBidiWriteChannel: StorageException while opening append session for: %s", resourceId);
      throw new IOException("Failed to initialize appendable upload session for: " + resourceId, e);
    } catch (Exception e) {
      logger.atSevere().withCause(e).log(
          "GCSBidiWriteChannel: Unexpected Exception while opening append session for: %s",
          resourceId);
      throw new IOException("Failed to initialize appendable upload session for: " + resourceId, e);
    }
  }

  private static BlobAppendableUpload getBlobAppendableUploadSession(
      Storage storage,
      StorageResourceId resourceId,
      CreateObjectOptions createOptions,
      GoogleCloudStorageOptions storageOptions) {
    BlobAppendableUploadConfig.CloseAction closeAction =
        BlobAppendableUploadConfig.CloseAction.FINALIZE_WHEN_CLOSING;

    BlobInfo blobInfo = getBlobInfo(resourceId, createOptions);
    BlobAppendableUploadConfig appendConfig =
        BlobAppendableUploadConfig.of().withCloseAction(closeAction);
    Storage.BlobWriteOption[] writeOptions = generateWriteOptions(createOptions, storageOptions);

    logger.atSevere().log(
        "GCSBidiWriteChannel: getBlobAppendableUploadSession for resourceId: %s", resourceId);
    logger.atSevere().log("GCSBidiWriteChannel: BlobInfo: %s", blobInfo);
    logger.atSevere().log("GCSBidiWriteChannel: BlobAppendableUploadConfig: %s", appendConfig);
    logger.atSevere().log(
        "GCSBidiWriteChannel: BlobWriteOptions: %s", Arrays.toString(writeOptions));

    return storage.blobAppendableUpload(blobInfo, appendConfig, writeOptions);
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
      blobWriteOptions.add(Storage.BlobWriteOption.crc3cMatch());
    }
    if (storageOptions.getEncryptionKey() != null) {
      blobWriteOptions.add(
          Storage.BlobWriteOption.encryptionKey(storageOptions.getEncryptionKey().value()));
    }

    // Log the options being generated
    String optionsString =
        blobWriteOptions.stream().map(Object::toString).collect(Collectors.joining(", "));
    logger.atSevere().log("GCSBidiWriteChannel: Generated WriteOptions: [%s]", optionsString);

    return blobWriteOptions.toArray(new Storage.BlobWriteOption[0]);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    if (!open) {
      logger.atSevere().log(
          "GCSBidiWriteChannel: Write attempted on closed channel for: %s", resourceId);
      throw new ClosedChannelException();
    }
    checkNotNull(src, "Source ByteBuffer (src) cannot be null");

    int remaining = src.remaining();
    logger.atSevere().log("GCSBidiWriteChannel: Writing %d bytes to: %s", remaining, resourceId);

    try {
      int written = blockingEmptyTo(src, gcsAppendChannel);
      logger.atSevere().log(
          "GCSBidiWriteChannel: Successfully wrote %d bytes to: %s", written, resourceId);
      return written;
    } catch (IOException e) {
      logger.atSevere().withCause(e).log(
          "GCSBidiWriteChannel: IOException during write to: %s", resourceId);
      throw e;
    } catch (Exception e) {
      logger.atSevere().withCause(e).log(
          "GCSBidiWriteChannel: Unexpected Exception during write to: %s", resourceId);
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    logger.atSevere().log("GCSBidiWriteChannel: close() called for: %s", resourceId);
    if (!open) {
      logger.atSevere().log("GCSBidiWriteChannel: Channel already closed for: %s", resourceId);
      return;
    }
    open = false;

    if (gcsAppendChannel == null) {
      logger.atSevere().log(
          "GCSBidiWriteChannel: gcsAppendChannel is null on close for: %s", resourceId);
      return;
    }

    try {
      gcsAppendChannel.close();
      logger.atSevere().log("GCSBidiWriteChannel: gcsAppendChannel closed for: %s", resourceId);
    } catch (IOException e) {
      logger.atSevere().withCause(e).log(
          "GCSBidiWriteChannel: IOException during gcsAppendChannel.close() for: %s", resourceId);
      throw e;
    } catch (Exception e) {
      logger.atSevere().withCause(e).log(
          "GCSBidiWriteChannel: Unexpected Exception during gcsAppendChannel.close() for: %s",
          resourceId);
      throw new IOException(e);
    }
  }

  @Override
  public void finalizeAndClose() throws IOException {
    logger.atSevere().log("GCSBidiWriteChannel: finalizeAndClose() called for: %s", resourceId);
    if (!open) {
      logger.atSevere().log(
          "GCSBidiWriteChannel: Channel already closed (finalizeAndClose) for: %s", resourceId);
      return;
    }
    open = false;

    if (gcsAppendChannel == null) {
      logger.atSevere().log(
          "GCSBidiWriteChannel: gcsAppendChannel is null on finalizeAndClose for: %s", resourceId);
      return;
    }

    try {
      gcsAppendChannel.finalizeAndClose();
      logger.atSevere().log(
          "GCSBidiWriteChannel: gcsAppendChannel finalized and closed for: %s", resourceId);
    } catch (IOException e) {
      logger.atSevere().withCause(e).log(
          "GCSBidiWriteChannel: IOException during gcsAppendChannel.finalizeAndClose() for: %s",
          resourceId);
      throw e;
    } catch (Exception e) {
      logger.atSevere().withCause(e).log(
          "GCSBidiWriteChannel: Unexpected Exception during gcsAppendChannel.finalizeAndClose() for: %s",
          resourceId);
      throw new IOException(e);
    }
  }

  private static int blockingEmptyTo(ByteBuffer buf, WritableByteChannel c) throws IOException {
    int total = 0;
    int loops = 0;
    while (buf.hasRemaining()) {
      loops++;
      int written = c.write(buf);
      if (written > 0) {
        total += written;
      } else if (written < 0) {
        // Should not happen for most WritableByteChannel implementations
        logger.atSevere().log(
            "GCSBidiWriteChannel: WritableByteChannel.write() returned %d", written);
        throw new IOException("Channel write returned negative value: " + written);
      }
      if (loops % 100 == 0) { // Log progress on large writes
        logger.atSevere().log(
            "GCSBidiWriteChannel: blockingEmptyTo in loop %d, total written: %d", loops, total);
      }
    }
    logger.atSevere().log(
        "GCSBidiWriteChannel: blockingEmptyTo completed, total written: %d in %d loops",
        total, loops);
    return total;
  }
}
