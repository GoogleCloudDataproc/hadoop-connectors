package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.encodeMetadata;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.storage.*;
import com.google.cloud.storage.BlobAppendableUpload.AppendableUploadWriteableByteChannel;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

@VisibleForTesting
public class GoogleCloudStorageBidiWriteChannel implements WritableByteChannel {
  private final AppendableUploadWriteableByteChannel gcsAppendChannel;
  private boolean open = true;

  private final boolean finalizeBeforeClose;

  public GoogleCloudStorageBidiWriteChannel(
      Storage storage,
      GoogleCloudStorageOptions storageOptions,
      StorageResourceId resourceId,
      CreateObjectOptions createOptions)
      throws IOException {

    checkNotNull(storage, "storage cannot be null");
    checkNotNull(resourceId, "resourceId cannot be null");

    finalizeBeforeClose = storageOptions.isFinalizeBeforeClose();

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
    return storage.blobAppendableUpload(
        getBlobInfo(resourceId, createOptions),
        BlobAppendableUploadConfig.of(),
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
    return gcsAppendChannel.write(src);
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

    if (finalizeBeforeClose) {
      gcsAppendChannel.finalizeAndClose();
    } else {
      gcsAppendChannel.closeWithoutFinalizing();
    }
  }
}
