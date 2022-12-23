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
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

class GCSJavaClientWriteChannel extends AbstractGoogleAsyncWriteChannel<Boolean> {

  protected static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final GoogleCloudStorageOptions storageOptions;
  private final StorageResourceId resourceId;
  private final AsyncWriteChannelOptions channelOptions;
  private final Storage storage;
  private WriteChannel writeChannel;
  private final CreateObjectOptions createOptions;
  private Boolean uploadSuccess = false;
  // TODO: not supported as of now
  // private final String requesterPaysProject;

  public GCSJavaClientWriteChannel(
      Storage storage,
      GoogleCloudStorageOptions storageOptions,
      StorageResourceId resourceId,
      CreateObjectOptions createOptions,
      ExecutorService uploadThreadPool) {
    super(uploadThreadPool, storageOptions.getWriteChannelOptions());
    this.storage = storage;
    this.storageOptions = storageOptions;
    this.resourceId = resourceId;
    this.createOptions = createOptions;
    this.channelOptions = storageOptions.getWriteChannelOptions();
  }

  public void initialize() throws IOException {
    final BlobId blobId =
        BlobId.of(
            resourceId.getBucketName(), resourceId.getObjectName(), resourceId.getGenerationId());
    BlobInfo blobInfo =
        BlobInfo.newBuilder(blobId)
            .setContentType(createOptions.getContentType())
            .setContentEncoding(createOptions.getContentEncoding())
            .setMetadata(encodeMetadata(createOptions.getMetadata()))
            .build();
    this.writeChannel = storage.writer(blobInfo, generateWriteOptions());
    this.writeChannel.setChunkSize(
        (int) channelOptions.getNumberOfBufferedRequests() * MAX_WRITE_CHUNK_BYTES.getNumber());
    super.initialize();
  }

  @Override
  public void startUpload(InputStream pipeSource) throws IOException {
    // Given that the two ends of the pipe must operate asynchronous relative
    // to each other, we need to start the upload operation on a separate thread.
    try {
      uploadOperation = threadPool.submit(new UploadOperation(pipeSource, this.resourceId));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to start upload for '%s'", resourceId.toString()), e);
    }
  }

  private class UploadOperation implements Callable<Boolean> {

    // Read end of the pipe.
    private final BufferedInputStream pipeSource;
    private final StorageResourceId resourceId;
    private final int MAX_BYTES_PER_MESSAGE = MAX_WRITE_CHUNK_BYTES.getNumber();

    UploadOperation(InputStream pipeSource, StorageResourceId resourceId) {
      this.resourceId = resourceId;
      this.pipeSource = new BufferedInputStream(pipeSource, MAX_BYTES_PER_MESSAGE);
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
          writeToGcs(byteBuffer);
          if (!lastChunk) {
            // compact buffer for further writing
            byteBuffer.compact();
          }
        }
        // last chunk could be partially written
        // uploading all bytes of last chunk
        if (lastChunk && byteBuffer.hasRemaining()) {
          while (byteBuffer.hasRemaining()) {
            writeToGcs(byteBuffer);
          }
        }
        return true;
      } catch (Exception e) {
        logger.atSevere().withCause(e).log("Exception while writing to channel");
        throw new IOException(
            String.format("Error occurred while uploading resource %s", resourceId.toString()), e);
      }
    }
  }

  private BlobWriteOption[] generateWriteOptions() {
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
      logger.atSevere().withCause(e).log(
          "Error occurred while closing write channel for resource %s", resourceId.toString());
      throw new IOException(String.format("Upload failed for '%s'", getResourceString()), e);
    }
  }

  @Override
  public void handleResponse(Boolean response) {
    this.uploadSuccess = response;
  }

  @Override
  protected String getResourceString() {
    return resourceId.toString();
  }

  public boolean isUploadSuccessful() {
    return uploadSuccess;
  }

  private int writeToGcs(ByteBuffer byteBuffer) throws IOException {
    // TODO: add metrics to capture write time
    int bytesWritten = writeChannel.write(byteBuffer);
    logger.atFinest().log("Bytes written %d", bytesWritten);
    return bytesWritten;
  }
}
