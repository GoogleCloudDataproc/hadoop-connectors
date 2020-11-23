/*
 * Copyright 2020 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.gcsio;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.flogger.GoogleLogger;
import com.google.common.hash.Hashing;
import com.google.google.storage.v1.GetObjectMediaRequest;
import com.google.google.storage.v1.GetObjectMediaResponse;
import com.google.google.storage.v1.GetObjectRequest;
import com.google.google.storage.v1.StorageGrpc.StorageBlockingStub;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Context.CancellableContext;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.time.Duration;
import java.util.Iterator;
import javax.annotation.Nullable;

public class GoogleCloudStorageGrpcReadChannel implements SeekableByteChannel {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final Duration READ_STREAM_TIMEOUT = Duration.ofMinutes(20);
  private static final Duration READ_OBJECT_METADATA_TIMEOUT = Duration.ofMinutes(1);
  private static final int READ_RETRIES = 5;

  // Context of the request that returned resIterator.
  @Nullable CancellableContext requestContext;
  Fadvise readStrategy;
  // GCS gRPC stub.
  private final StorageBlockingStub stub;
  // Name of the bucket containing the object being read.
  private final String bucketName;
  // Name of the object being read.
  private final String objectName;
  // We read from a specific generation, to maintain consistency between read() calls.
  private final long objectGeneration;
  // The size of this object generation, in bytes.
  private final long objectSize;
  // True if this channel is open, false otherwise.
  private boolean channelIsOpen = true;
  // Current position in the object.
  private long position = 0;
  // If a user seeks forwards by a configurably small amount, we continue reading from where
  // we are instead of starting a new connection. The user's intended read position is
  // position + bytesToSkipBeforeReading.
  private long bytesToSkipBeforeReading = 0;
  // The user may have read less data than we received from the server. If that's the case, we keep
  // the most recently received content and a reference to how much of it we've returned so far.
  @Nullable private ByteString bufferedContent = null;
  private int bufferedContentReadOffset = 0;
  // The streaming read operation. If null, there is not an in-flight read in progress.
  @Nullable private Iterator<GetObjectMediaResponse> resIterator = null;
  // Fine-grained options.
  private final GoogleCloudStorageReadOptions readOptions;

  private GoogleCloudStorageGrpcReadChannel(
      StorageBlockingStub gcsGrpcBlockingStub,
      String bucketName,
      String objectName,
      long objectGeneration,
      long objectSize,
      GoogleCloudStorageReadOptions readOptions) {
    this.stub = gcsGrpcBlockingStub;
    this.bucketName = bucketName;
    this.objectName = objectName;
    this.objectGeneration = objectGeneration;
    this.objectSize = objectSize;
    this.readOptions = readOptions;
    this.readStrategy = readOptions.getFadvise();
  }

  public static GoogleCloudStorageGrpcReadChannel open(
      StorageBlockingStub stub,
      String bucketName,
      String objectName,
      GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    // The gRPC API's GetObjectMedia call does not provide a generation number, so to ensure
    // consistent reads, we need to begin by checking the current generation number with a separate
    // call.
    // TODO(b/135138893): We can avoid this call by adding metadata to a read request.
    //      That will save about 40ms per read.
    // TODO(b/136088557): If we add metadata to a read, we can also use that first call to
    //      implement footer prefetch.
    Retryer<GoogleCloudStorageGrpcReadChannel> retryer = getRetryer();
    try {
      return retryer.call(
          () -> {
            com.google.google.storage.v1.Object getObjectResult;
            try {
              // TODO(b/151184800): Implement per-message timeout, in addition to stream timeout.
              getObjectResult =
                  stub.withDeadlineAfter(READ_OBJECT_METADATA_TIMEOUT.toMillis(), MILLISECONDS)
                      .getObject(
                          GetObjectRequest.newBuilder()
                              .setBucket(bucketName)
                              .setObject(objectName)
                              .build());
            } catch (StatusRuntimeException e) {
              throw convertError(e, bucketName, objectName);
            }
            // The non-gRPC read channel has special support for gzip. This channel doesn't
            // decompress gzip-encoded objects on the fly, so best to fail fast rather than return
            // gibberish unexpectedly.
            if (getObjectResult.getContentEncoding().contains("gzip")) {
              throw new IOException(
                  "Can't read GZIP encoded files - content encoding support is disabled.");
            }

            return new GoogleCloudStorageGrpcReadChannel(
                stub,
                bucketName,
                objectName,
                getObjectResult.getGeneration(),
                getObjectResult.getSize(),
                readOptions);
          });
    } catch (Exception e) {
      throw new IOException(
          String.format("Error reading '%s'", StringPaths.fromComponents(bucketName, objectName)),
          e);
    }
  }

  private static IOException convertError(
      StatusRuntimeException error, String bucketName, String objectName) {
    String msg =
        String.format("Error reading '%s'", StringPaths.fromComponents(bucketName, objectName));
    switch (Status.fromThrowable(error).getCode()) {
      case NOT_FOUND:
        return GoogleCloudStorageExceptions.createFileNotFoundException(
            bucketName, objectName, new IOException(msg, error));
      case OUT_OF_RANGE:
        return (IOException) new EOFException(msg).initCause(error);
      default:
        return new IOException(msg, error);
    }
  }

  /** Writes part of a ByteString into a ByteBuffer with as little copying as possible */
  private static void put(ByteString source, int offset, int size, ByteBuffer dest) {
    ByteString croppedSource = source.substring(offset, offset + size);
    for (ByteBuffer sourcePiece : croppedSource.asReadOnlyByteBufferList()) {
      dest.put(sourcePiece);
    }
  }

  private int readBufferedContentInto(ByteBuffer byteBuffer) {
    // Handle skipping forward through the buffer for a seek.
    long bufferSkip =
        min(bufferedContent.size() - bufferedContentReadOffset, bytesToSkipBeforeReading);
    bufferSkip = max(0, bufferSkip);
    bufferedContentReadOffset += bufferSkip;
    bytesToSkipBeforeReading -= bufferSkip;
    int remainingBufferedBytes = bufferedContent.size() - bufferedContentReadOffset;

    boolean remainingBufferedContentLargerThanByteBuffer =
        remainingBufferedBytes > byteBuffer.remaining();
    int bytesToWrite =
        remainingBufferedContentLargerThanByteBuffer
            ? byteBuffer.remaining()
            : remainingBufferedBytes;
    put(bufferedContent, bufferedContentReadOffset, bytesToWrite, byteBuffer);
    position += bytesToWrite;

    if (remainingBufferedContentLargerThanByteBuffer) {
      bufferedContentReadOffset += bytesToWrite;
    } else {
      bufferedContent = null;
      bufferedContentReadOffset = 0;
    }

    return bytesToWrite;
  }

  @Override
  public int read(ByteBuffer byteBuffer) throws IOException {
    logger.atFine().log(
        "GCS gRPC read request for up to %d bytes at offset %d from object %s",
        byteBuffer.remaining(), position(), objectName);

    if (!isOpen()) {
      throw new ClosedChannelException();
    }

    int bytesRead = 0;

    // The server responds in 2MB chunks, but the client can ask for less than that. We store the
    // remainder in bufferedContent and return pieces of that on the next read call (and flush
    // that buffer if there is a seek).
    if (bufferedContent != null) {
      bytesRead += readBufferedContentInto(byteBuffer);
    }
    if (!byteBuffer.hasRemaining()) {
      return bytesRead;
    }
    if (position == objectSize) {
      return bytesRead > 0 ? bytesRead : -1;
    }
    if (resIterator == null) {
      Integer bytesToRead =
          readStrategy == Fadvise.RANDOM
              ? max(byteBuffer.remaining(), readOptions.getMinRangeRequestSize())
              : null;
      requestObjectMedia(bytesToRead);
    }
    while (moreServerContent() && byteBuffer.hasRemaining()) {
      GetObjectMediaResponse res = resIterator.next();

      ByteString content = res.getChecksummedData().getContent();
      if (bytesToSkipBeforeReading >= 0 && bytesToSkipBeforeReading < content.size()) {
        content = res.getChecksummedData().getContent().substring((int) bytesToSkipBeforeReading);
        bytesToSkipBeforeReading = 0;
      } else if (bytesToSkipBeforeReading >= content.size()) {
        bytesToSkipBeforeReading -= content.size();
        continue;
      }

      if (readOptions.isGrpcChecksumsEnabled() && res.getChecksummedData().hasCrc32C()) {
        // TODO: Concatenate all these hashes together and compare the result at the end.
        int calculatedChecksum =
            Hashing.crc32c().hashBytes(res.getChecksummedData().getContent().toByteArray()).asInt();
        int expectedChecksum = res.getChecksummedData().getCrc32C().getValue();
        if (calculatedChecksum != expectedChecksum) {
          throw new IOException(
              String.format(
                  "Message checksum (%s) didn't match expected checksum (%s) for '%s'",
                  expectedChecksum, calculatedChecksum, resourceIdString()));
        }
      }

      boolean responseSizeLargerThanRemainingBuffer = content.size() > byteBuffer.remaining();
      int bytesToWrite =
          responseSizeLargerThanRemainingBuffer ? byteBuffer.remaining() : content.size();
      put(content, 0, bytesToWrite, byteBuffer);
      bytesRead += bytesToWrite;
      position += bytesToWrite;

      if (responseSizeLargerThanRemainingBuffer) {
        bufferedContent = content;
        bufferedContentReadOffset = bytesToWrite;
      }
    }
    return bytesRead;
  }

  private void requestObjectMedia(@Nullable Integer bytesToRead) throws IOException {
    GetObjectMediaRequest.Builder requestBuilder =
        GetObjectMediaRequest.newBuilder()
            .setBucket(bucketName)
            .setObject(objectName)
            .setGeneration(objectGeneration)
            .setReadOffset(position);
    if (bytesToRead != null) {
      requestBuilder.setReadLimit(bytesToRead);
    }
    GetObjectMediaRequest request = requestBuilder.build();
    Retryer<Boolean> retryer = getRetryer();
    try {
      retryer.call(
          () -> {
            try {
              requestContext = Context.current().withCancellation();
              Context toReattach = requestContext.attach();
              try {
                resIterator =
                    stub.withDeadlineAfter(READ_STREAM_TIMEOUT.toMillis(), MILLISECONDS)
                        .getObjectMedia(request);
              } finally {
                requestContext.detach(toReattach);
              }
            } catch (StatusRuntimeException e) {
              throw convertError(e, bucketName, objectName);
            }
            return null;
          });
    } catch (Exception e) {
      throw new IOException(String.format("Error reading '%s'", resourceIdString()), e);
    }
  }

  private void cancelCurrentRequest() {
    if (requestContext != null) {
      requestContext.close();
      requestContext = null;
    }
    if (resIterator != null) {
      resIterator = null;
    }
  }

  /**
   * Waits until more data is available from the server, or returns false if read is done.
   *
   * @return true if more data is available with .next()
   * @throws IOException of the appropriate type if there was an I/O error.
   */
  private boolean moreServerContent() throws IOException {
    if (resIterator == null || requestContext == null || requestContext.isCancelled()) {
      return false;
    }

    Retryer<Boolean> retryer = getRetryer();
    try {
      return retryer.call(
          () -> {
            try {
              boolean moreDataAvailable = resIterator.hasNext();
              if (!moreDataAvailable) {
                cancelCurrentRequest();
              }

              return moreDataAvailable;
            } catch (StatusRuntimeException e) {
              throw convertError(e, bucketName, objectName);
            }
          });
    } catch (Exception e) {
      cancelCurrentRequest();
      throw new IOException(String.format("Error reading '%s'", resourceIdString()), e);
    }
  }

  private static <T> Retryer<T> getRetryer() {
    return RetryerBuilder.<T>newBuilder()
        .retryIfExceptionOfType(IOException.class)
        .withWaitStrategy(WaitStrategies.exponentialWait(2, 20, SECONDS))
        .withStopStrategy(StopStrategies.stopAfterAttempt(READ_RETRIES))
        .build();
  }

  @Override
  public int write(ByteBuffer byteBuffer) {
    throw new UnsupportedOperationException("Cannot mutate read-only channel: " + this);
  }

  @Override
  public long position() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    // Our real position is tracked in "position," but if the user is skipping forwards a bit, we
    // pretend we're at the new position already.
    return position + bytesToSkipBeforeReading;
  }

  private String resourceIdString() {
    return StringPaths.fromComponents(bucketName, objectName);
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    Preconditions.checkArgument(
        newPosition >= 0, "Read position must be non-negative, but was %s", newPosition);
    Preconditions.checkArgument(
        newPosition < size(),
        "Read position must be before end of file (%s), but was %s",
        size(),
        newPosition);
    if (newPosition == position) {
      return this;
    }

    long seekDistance = newPosition - position;

    if (seekDistance >= 0 && seekDistance <= readOptions.getInplaceSeekLimit()) {
      bytesToSkipBeforeReading = seekDistance;
      return this;
    }

    if (readStrategy == Fadvise.AUTO) {
      if (seekDistance < 0 || seekDistance > readOptions.getInplaceSeekLimit()) {
        readStrategy = Fadvise.RANDOM;
      }
    }

    // Reset any ongoing read operations or local data caches.
    cancelCurrentRequest();
    bufferedContent = null;
    bufferedContentReadOffset = 0;
    bytesToSkipBeforeReading = 0;

    position = newPosition;
    return this;
  }

  @Override
  public long size() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }

    return objectSize;
  }

  @Override
  public SeekableByteChannel truncate(long l) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public boolean isOpen() {
    return channelIsOpen;
  }

  @Override
  public void close() {
    cancelCurrentRequest();
    channelIsOpen = false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("bucketName", bucketName)
        .add("objectName", objectName)
        .add("generation", objectGeneration)
        .toString();
  }
}
