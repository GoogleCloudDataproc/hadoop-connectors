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
import static com.google.storage.v2.ServiceConstants.Values.MAX_WRITE_CHUNK_BYTES;

import com.google.cloud.WriteChannel;
import com.google.cloud.hadoop.util.AbstractGoogleAsyncWriteChannel;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.common.flogger.GoogleLogger;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/** Implements WritableByteChannel to provide write access to GCS via java-storage client */
class GoogleCloudStorageClientWriteChannel extends AbstractGoogleAsyncWriteChannel<Boolean> {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final StorageResourceId resourceId;
  private WriteChannel writeChannel;
  private boolean uploadSucceeded = false;
  // TODO: not supported as of now
  // private final String requesterPaysProject;

  public GoogleCloudStorageClientWriteChannel(
      Storage storage,
      GoogleCloudStorageOptions storageOptions,
      StorageResourceId resourceId,
      CreateObjectOptions createOptions,
      ExecutorService uploadThreadPool) {
    super(uploadThreadPool, storageOptions.getWriteChannelOptions());
    this.resourceId = resourceId;
    this.writeChannel = getClientWriteChannel(storage, resourceId, createOptions, storageOptions);
  }

  @Override
  public void startUpload(InputStream pipeSource) throws IOException {
    // Given that the two ends of the pipe must operate asynchronous relative
    // to each other, we need to start the upload operation on a separate thread.
    try {
      uploadOperation = threadPool.submit(new UploadOperation(pipeSource, this.resourceId));
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to start upload for '%s'", resourceId), e);
    }
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

  private static WriteChannel getClientWriteChannel(
      Storage storage,
      StorageResourceId resourceId,
      CreateObjectOptions createOptions,
      GoogleCloudStorageOptions storageOptions) {
    AsyncWriteChannelOptions channelOptions = storageOptions.getWriteChannelOptions();
    WriteChannel writeChannel =
        storage.writer(
            getBlobInfo(resourceId, createOptions),
            generateWriteOptions(createOptions, channelOptions));
    writeChannel.setChunkSize(channelOptions.getUploadChunkSize());

    return writeChannel;
  }

  private class UploadOperation implements Callable<Boolean> {

    // Read end of the pipe.
    private final InputStream pipeSource;
    private final StorageResourceId resourceId;
    private final int MAX_BYTES_PER_MESSAGE = MAX_WRITE_CHUNK_BYTES.getNumber();

    UploadOperation(InputStream pipeSource, StorageResourceId resourceId) {
      this.resourceId = resourceId;
      this.pipeSource = pipeSource;
    }

    @Override
    public Boolean call() throws Exception {
      // Try-with-resource will close this end of the pipe so that
      // the writer at the other end will not hang indefinitely.
      try (pipeSource) {
        boolean lastChunk = false;
        ByteBuffer byteBuffer = ByteBuffer.allocate(MAX_BYTES_PER_MESSAGE);
        while (!lastChunk) {
          int remainingCapacity = byteBuffer.remaining();
          ByteString data =
              ByteString.readFrom(
                  ByteStreams.limit(pipeSource, remainingCapacity), remainingCapacity);
          if (data.size() < remainingCapacity) {
            lastChunk = true;
          }
          byteBuffer.put(data.toByteArray());
          // switch to read mode
          byteBuffer.flip();
          // this could result into partial write
          writeInternal(byteBuffer);
          if (!lastChunk) {
            // compact buffer for further writing
            byteBuffer.compact();
          }
        }
        // last chunk could be partially written
        // uploading all bytes of last chunk
        if (lastChunk && byteBuffer.hasRemaining()) {
          while (byteBuffer.hasRemaining()) {
            writeInternal(byteBuffer);
          }
        }
        return true;
      } catch (Exception e) {
        throw new IOException(
            String.format("Error occurred while uploading resource %s", resourceId), e);
      }
    }
  }

  private static BlobWriteOption[] generateWriteOptions(
      CreateObjectOptions createOptions, AsyncWriteChannelOptions channelOptions) {
    List<BlobWriteOption> writeOptions = new ArrayList<>();

    writeOptions.add(BlobWriteOption.disableGzipContent());
    writeOptions.add(BlobWriteOption.generationMatch());
    if (createOptions.getKmsKeyName() != null) {
      writeOptions.add(BlobWriteOption.kmsKeyName(createOptions.getKmsKeyName()));
    }
    if (channelOptions.isGrpcChecksumsEnabled()) {
      writeOptions.add(BlobWriteOption.crc32cMatch());
    }
    return writeOptions.toArray(new BlobWriteOption[writeOptions.size()]);
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
      writeChannel.close();
    } catch (Exception e) {
      throw new IOException(String.format("Upload failed for '%s'", resourceId), e);
    }
  }

  @Override
  public void handleResponse(Boolean response) {
    this.uploadSucceeded = response;
  }

  @Override
  protected String getResourceString() {
    return resourceId.toString();
  }

  public boolean isUploadSuccessful() {
    return uploadSucceeded;
  }

  private int writeInternal(ByteBuffer byteBuffer) throws IOException {
    int bytesWritten = writeChannel.write(byteBuffer);
    logger.atFinest().log("Bytes written %d for resource %s", bytesWritten, resourceId);
    return bytesWritten;
  }
}
