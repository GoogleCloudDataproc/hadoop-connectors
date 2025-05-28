/* Copyright 2023 Google LLC
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

import static com.google.common.base.Preconditions.*;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.Math.*;

import com.google.api.core.ApiFuture;
import com.google.cloud.hadoop.util.ErrorTypeExtractor;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.ReadAsSeekableChannel;
import com.google.cloud.storage.ReadProjectionConfigs;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/** Provides seekable read access to GCS via java-storage library. */
@VisibleForTesting
class GoogleCloudStorageBidiReadChannel implements SeekableByteChannel {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final String GZIP_ENCODING = "gzip";

  private final StorageResourceId resourceId;
  private final GoogleCloudStorageReadOptions readOptions;
  private final GoogleCloudStorageOptions storageOptions;
  private final Storage storage;
  private final BlobId blobId;
  // The size of this object generation, in bytes.
  private long objectSize;
  private final ErrorTypeExtractor errorExtractor;
  private BlobReadSession blobReadSession;
  private SeekableByteChannel contentReadChannel;
  private boolean gzipEncoded = false;
  private boolean open = true;

  // Current position in this channel, it could be different from contentChannelCurrentPosition if
  // position(long) method calls were made without calls to read(ByteBuffer) method.
  private long currentPosition = 0;

  /**
   * @param storage The GCS Storage client to use for operations.
   * @param itemInfo Used to get the info of the item in consideration. The resourceId and intern
   *     the bloId are extracted with the help of Item Info
   * @param readOptions Advanced options for reading cloud storage objects.
   * @param errorExtractor Translated exceptions from API calls into Error Types
   * @param storageOptions Configuration options for the google cloud storage class
   */
  public GoogleCloudStorageBidiReadChannel(
      Storage storage,
      GoogleCloudStorageItemInfo itemInfo,
      GoogleCloudStorageReadOptions readOptions,
      ErrorTypeExtractor errorExtractor,
      GoogleCloudStorageOptions storageOptions)
      throws IOException {
    //    System.out.println("Bidi Read is called !");
    validate(itemInfo);
    this.storage = storage;
    this.errorExtractor = errorExtractor;
    this.resourceId =
        new StorageResourceId(
            itemInfo.getBucketName(), itemInfo.getObjectName(), itemInfo.getContentGeneration());
    this.blobId =
        BlobId.of(
            resourceId.getBucketName(), resourceId.getObjectName(), resourceId.getGenerationId());
    this.readOptions = readOptions;
    this.storageOptions = storageOptions;
    initMetadata(itemInfo.getContentEncoding(), itemInfo.getSize());
    intializeReadSession(0);
  }

  private void intializeReadSession(long position) throws IOException {
    if (blobReadSession != null) {
      try {
        blobReadSession.close(); // Close the previous session if it exists
      } catch (Exception e) {
        GoogleCloudStorageEventBus.postOnException();
        throw new IOException(
            String.format("Exception occurred while closing channel '%s'", resourceId), e);
      }
    }

    ApiFuture<BlobReadSession> futureBlobReadSession = storage.blobReadSession(blobId);
    try {
      this.blobReadSession = futureBlobReadSession.get(30, TimeUnit.SECONDS);

      ReadAsSeekableChannel seekableChannelConfig = ReadProjectionConfigs.asSeekableChannel();
      this.contentReadChannel = blobReadSession.readAs(seekableChannelConfig);
      this.currentPosition = position;
      if (position > 0) {
        this.contentReadChannel.position(position);
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IOException("Failed to establish BlobReadSession: " + e.getMessage());
    }
  }

  private static void validate(GoogleCloudStorageItemInfo itemInfo) throws IOException {
    checkNotNull(itemInfo, "itemInfo cannot be null");
    StorageResourceId resourceId = itemInfo.getResourceId();
    checkArgument(
        resourceId.isStorageObject(), "Can not open a non-file object for read: %s", resourceId);
    if (!itemInfo.exists()) {
      GoogleCloudStorageEventBus.postOnException();
      throw new FileNotFoundException(String.format("Item not found: %s", resourceId));
    }
  }

  protected void initMetadata(@Nullable String encoding, long sizeFromMetadata) throws IOException {
    // Not handling gzipEncoding for now.
    gzipEncoded = nullToEmpty(encoding).contains(GZIP_ENCODING);
    if (gzipEncoded) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(
          "Cannot read GZIP encoded files - content encoding support is disabled.");
    }
    objectSize = sizeFromMetadata;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    GoogleCloudStorageEventBus.postOnException();
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public long size() throws IOException {
    if (!open) {
      throw new IllegalStateException("Channel is not open");
    }
    return objectSize;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    throwIfNotOpen();

    if (dst.remaining() == 0) {
      return 0;
    }
    int bytesRead = contentReadChannel.read(dst);
    if (currentPosition == objectSize) {
      return -1;
    }
    if (bytesRead > 0) {
      currentPosition += bytesRead;
    }
    //    System.out.print("Bytes Read: ");
    //    System.out.println(bytesRead);
    //    dst.rewind();
    //    System.out.println("Buffer dst: ");
    //    Charset charset = StandardCharsets.UTF_8; // Or any other appropriate charset
    //    String decodedString1 = charset.decode(dst).toString();
    //    System.out.println("Decoded String (using Charset.decode()): " + decodedString1);
    return bytesRead;
  }

  @Override
  public long position() throws IOException {
    return currentPosition;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    throwIfNotOpen();

    if (newPosition < 0) {
      throw new IllegalArgumentException("Position cannot be negative");
    }

    if (objectSize >= 0 && newPosition >= objectSize) {
      GoogleCloudStorageEventBus.postOnException();
      throw new EOFException(
              String.format(
                      "Invalid seek offset: position value (%d) must be between 0 and %d for '%s'",
                      newPosition, objectSize, resourceId));
    }

    if (newPosition != currentPosition) {
      contentReadChannel.position(newPosition);
      this.currentPosition = newPosition;
    }
    return this;
  }

  @Override
  public void close() throws IOException {
    if (open) {
      open = false;
      if (contentReadChannel != null) {
        try {
          contentReadChannel.close();
        } catch (IOException e) {
          throw new IOException("Error in Closing channel: " + e.getMessage());
        }
      }
      if (blobReadSession != null) {
        try {
          blobReadSession.close();
        } catch (IOException e) {
          throw new IOException("Error closing BlobReadSession: " + e.getMessage());
        }
      }
    }
  }
}

private void throwIfNotOpen() throws IOException {
    if (!isOpen()) {
      GoogleCloudStorageEventBus.postOnException();
      throw new ClosedChannelException();
    }
  }
}
