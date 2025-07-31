package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.encodeMetadata;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.core.ApiFuture;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUpload.AppendableUploadWriteableByteChannel; // Specific channel type
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel; // We still implement the generic interface
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@VisibleForTesting
public class GoogleCloudStorageBidiWriteChannel implements WritableByteChannel {
  private static final long DEFAULT_RESULT_TIMEOUT_SECONDS = 60;
  private static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";

  private final Storage storage;
  private final StorageResourceId resourceId;
  private final BlobInfo
      blobInfoForSession; // Stored for getResult() if needed, or use session object

  private boolean open = true;
  // This is the specialized channel from the SDK for appendable uploads
  private AppendableUploadWriteableByteChannel gcsAppendChannel;
  private BlobAppendableUpload appendUploadSession; // The session object itself
  private BlobInfo finalizedBlobInfo;

  public GoogleCloudStorageBidiWriteChannel(
      Storage storage,
      GoogleCloudStorageOptions
          storageOptions, // Kept if it provides essential configs like timeouts
      StorageResourceId resourceId,
      CreateObjectOptions createOptions)
      throws IOException {

    System.out.println("Bidi Write is called");
    this.storage = checkNotNull(storage, "storage cannot be null");
    this.resourceId = checkNotNull(resourceId, "resourceId cannot be null");

    this.blobInfoForSession = getBlobInfoForSession(this.resourceId, createOptions);

    try {
      this.appendUploadSession =
          storage.blobAppendableUpload(this.blobInfoForSession, BlobAppendableUploadConfig.of());
      this.gcsAppendChannel = this.appendUploadSession.open();

    } catch (StorageException e) {
      throw new IOException(
          "Failed to initialize appendable upload session for: " + this.resourceId, e);
    }
  }

  private static BlobInfo getBlobInfoForSession(
      StorageResourceId resourceId, CreateObjectOptions createOptions) {
    BlobInfo.Builder blobInfoBuilder =
        BlobInfo.newBuilder(BlobId.of(resourceId.getBucketName(), resourceId.getObjectName()));

    blobInfoBuilder.setContentType(
        !Strings.isNullOrEmpty(createOptions.getContentType())
            ? createOptions.getContentType()
            : DEFAULT_CONTENT_TYPE);

    if (!Strings.isNullOrEmpty(createOptions.getContentEncoding())) {
      blobInfoBuilder.setContentEncoding(createOptions.getContentEncoding());
    }
    if (createOptions.getMetadata() != null && !createOptions.getMetadata().isEmpty()) {
      blobInfoBuilder.setMetadata(encodeMetadata(createOptions.getMetadata()));
    }
    return blobInfoBuilder.build();
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    if (!open) throw new ClosedChannelException();
    checkNotNull(src, "Source ByteBuffer (src) cannot be null");
    try {
      // Delegate directly to the specialized AppendableUploadWriteableByteChannel
      return gcsAppendChannel.write(src);
    } catch (IOException e) { // Includes StorageException
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    if (!open) {
      return;
    }
    open = false;

    IOException L_Exception = null; // Local exception variable

    try {
      if (gcsAppendChannel != null) {
        gcsAppendChannel.finalizeAndClose();
      }

      // Get the result from the BlobAppendableUpload session object
      ApiFuture<BlobInfo> resultFuture = appendUploadSession.getResult();
      long resultTimeout = DEFAULT_RESULT_TIMEOUT_SECONDS;

      this.finalizedBlobInfo = resultFuture.get(resultTimeout, TimeUnit.SECONDS);

      if (this.finalizedBlobInfo == null) {
        L_Exception =
            new IOException(
                "Append session for "
                    + resourceId
                    + " finalized, but no BlobInfo result obtained.");
      }
    } catch (IOException e) { // Includes StorageException from finalizeAndClose or getResult
      L_Exception = e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      L_Exception = new IOException("Interrupted closing append session for: " + resourceId, e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      L_Exception =
          new IOException(
              "Failed to get result of append session for: "
                  + resourceId
                  + (cause != null ? ": " + cause.getMessage() : ""),
              cause != null ? cause : e);
    } catch (TimeoutException e) {
      L_Exception =
          new IOException("Timeout getting result of append session for: " + resourceId, e);
    }

    if (L_Exception != null) {
      throw L_Exception;
    }
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  public BlobInfo getFinalizedBlobInfo() {
    if (open) {
      return null;
    }
    return finalizedBlobInfo;
  }
}
