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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageExceptions.createFileNotFoundException;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.api.client.util.Sleeper;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.Storage.Objects.Get;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.BackOffFactory;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ResilientOperation;
import com.google.cloud.hadoop.util.RetryDeterminer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.flogger.GoogleLogger;
import com.google.common.hash.Hashing;
import com.google.google.storage.v1.GetObjectMediaRequest;
import com.google.google.storage.v1.GetObjectMediaResponse;
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
import java.util.Iterator;
import javax.annotation.Nullable;

public class GoogleCloudStorageGrpcReadChannel implements SeekableByteChannel {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  protected static final String METADATA_FIELDS = "contentEncoding,generation,size";

  private volatile StorageBlockingStub stub;

  private final StorageStubProvider stubProvider;

  private final StorageResourceId resourceId;

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

  private final BackOffFactory backOffFactory;

  // Context of the request that returned resIterator.
  @Nullable
  CancellableContext requestContext;

  Fadvise readStrategy;

  @Nullable
  private final ByteString footerContent;

  private final long footerStartOffsetInBytes;

  public static GoogleCloudStorageGrpcReadChannel open(
      StorageStubProvider stubProvider,
      Storage storage,
      ApiErrorExtractor errorExtractor,
      StorageResourceId resourceId,
      GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    return open(stubProvider, storage, errorExtractor, resourceId, readOptions,
        BackOffFactory.DEFAULT);
  }

  @VisibleForTesting
  static GoogleCloudStorageGrpcReadChannel open(
      StorageStubProvider stubProvider,
      Storage storage,
      ApiErrorExtractor errorExtractor,
      StorageResourceId resourceId,
      GoogleCloudStorageReadOptions readOptions,
      BackOffFactory backOffFactory)
      throws IOException {
    // The gRPC API's GetObjectMedia call does not provide a generation number, so to ensure
    // consistent reads, we need to begin by checking the current generation number with a separate
    // call.
    try {
      return ResilientOperation.retry(
          () -> openChannel(stubProvider, storage, errorExtractor, resourceId, readOptions,
              backOffFactory),
          backOffFactory.newBackOff(),
          RetryDeterminer.ALL_ERRORS,
          IOException.class);
    } catch (Exception e) {
      throw new IOException(String.format("Error reading '%s'", resourceId), e);
    }
  }

  private static GoogleCloudStorageGrpcReadChannel openChannel(
      StorageStubProvider stubProvider,
      Storage storage,
      ApiErrorExtractor errorExtractor,
      StorageResourceId resourceId,
      GoogleCloudStorageReadOptions readOptions,
      BackOffFactory backOffFactory) throws IOException {
    StorageBlockingStub stub = stubProvider.newBlockingStub();
    // TODO(b/135138893): We can avoid this call by adding metadata to a read request.
    //      That will save about 40ms per read.
    Preconditions.checkArgument(storage != null, "GCS json client cannot be null");
    GoogleCloudStorageItemInfo itemInfo = getObjectMetadata(resourceId,
        errorExtractor, backOffFactory, storage);
    Preconditions.checkArgument(itemInfo != null, "object metadata cannot be null");
    // The non-gRPC read channel has special support for gzip. This channel doesn't
    // decompress gzip-encoded objects on the fly, so best to fail fast rather than return
    // gibberish unexpectedly.
    String contentEncoding = itemInfo.getContentEncoding();
    if (contentEncoding != null && contentEncoding.contains("gzip")) {
      throw new IOException(
          "Cannot read GZIP encoded files - content encoding support is disabled.");
    }

    int prefetchSizeInBytes = readOptions.getMinRangeRequestSize() / 2;
    long footerOffsetInBytes = Math.max(0, (itemInfo.getSize() - prefetchSizeInBytes));

    ByteString footerContent = getFooterContent(resourceId, readOptions, stub, footerOffsetInBytes);

    return new GoogleCloudStorageGrpcReadChannel(
        stub,
        stubProvider,
        resourceId,
        itemInfo.getContentGeneration(),
        itemInfo.getSize(),
        footerOffsetInBytes,
        footerContent,
        readOptions,
        backOffFactory);
  }

  private static GoogleCloudStorageItemInfo getObjectMetadata(
      StorageResourceId resourceId,
      ApiErrorExtractor errorExtractor,
      BackOffFactory backOffFactory,
      Storage gcs) throws IOException {
    StorageObject object;
    try {
      // Request only fields that are used for metadata initialization
      Get metadataRequest = getMetadataRequest(gcs, resourceId).setFields(METADATA_FIELDS);
      object =
          ResilientOperation.retry(
              metadataRequest::execute,
              backOffFactory.newBackOff(),
              RetryDeterminer.SOCKET_ERRORS,
              IOException.class,
              Sleeper.DEFAULT);
    } catch (IOException e) {
      throw errorExtractor.itemNotFound(e)
          ? createFileNotFoundException(resourceId, e)
          : new IOException("Error reading " + resourceId, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Thread interrupt received.", e);
    }
    return GoogleCloudStorageItemInfo.createObject(
        resourceId,
        /* creationTime= */ 0,
        /* modificationTime= */ 0,
        checkNotNull(object.getSize(), "size can not be null for '%s'", resourceId).longValue(),
        /* contentType= */ null,
        object.getContentEncoding(),
        /* metadata= */ null,
        checkNotNull(object.getGeneration(), "generation can not be null for '%s'", resourceId),
        /* metaGeneration= */ 0,
        /* verificationAttributes= */ null);
  }

  private static Get getMetadataRequest(Storage gcs, StorageResourceId resourceId)
      throws IOException {
    Get getObject = gcs.objects().get(resourceId.getBucketName(), resourceId.getObjectName());
    if (resourceId.hasGenerationId()) {
      getObject.setGeneration(resourceId.getGenerationId());
    }
    return getObject;
  }

  private static ByteString getFooterContent(StorageResourceId resourceId,
      GoogleCloudStorageReadOptions readOptions, StorageBlockingStub stub, long footerOffset)
      throws IOException {
    try {
      Iterator<GetObjectMediaResponse> footerContentResponse = stub
          .withDeadlineAfter(readOptions.getGrpcReadTimeoutMillis(), MILLISECONDS)
          .getObjectMedia(GetObjectMediaRequest.newBuilder()
              .setReadOffset(footerOffset)
              .setBucket(resourceId.getBucketName())
              .setObject(resourceId.getObjectName())
              .build());

      ByteString footerContent = null;
      while (footerContentResponse.hasNext()) {
        GetObjectMediaResponse objectMediaResponse = footerContentResponse.next();
        if (objectMediaResponse.hasChecksummedData()) {
          ByteString content = objectMediaResponse.getChecksummedData().getContent();
          if (footerContent == null) {
            footerContent = content;
          } else {
            footerContent = footerContent.concat(content);
          }
        }
      }
      if (footerContent == null) {
        logger.atFiner()
            .log("Prefetched footer content is null for resource '%s'", resourceId);
      } else {
        logger.atFiner()
            .log("Prefetched %s bytes footer for '%s'", footerContent.size(), resourceId);
      }
      return footerContent;
    } catch (StatusRuntimeException e) {
      throw convertError(e, resourceId);
    }
  }

  private GoogleCloudStorageGrpcReadChannel(
      StorageBlockingStub gcsGrpcBlockingStub,
      StorageStubProvider stubProvider,
      StorageResourceId resourceId,
      long objectGeneration,
      long objectSize,
      long footerStartOffsetInBytes,
      ByteString footerContent,
      GoogleCloudStorageReadOptions readOptions,
      BackOffFactory backOffFactory) {
    this.stub = gcsGrpcBlockingStub;
    this.stubProvider = stubProvider;
    this.resourceId = resourceId;
    this.objectGeneration = objectGeneration;
    this.objectSize = objectSize;
    this.readOptions = readOptions;
    this.backOffFactory = backOffFactory;
    this.readStrategy = readOptions.getFadvise();
    this.footerStartOffsetInBytes = footerStartOffsetInBytes;
    this.footerContent = footerContent;
  }

  private static IOException convertError(
      StatusRuntimeException error, StorageResourceId resourceId) {
    String msg = String.format("Error reading '%s'", resourceId);
    switch (Status.fromThrowable(error).getCode()) {
      case NOT_FOUND:
        return GoogleCloudStorageExceptions.createFileNotFoundException(
            resourceId.getBucketName(), resourceId.getObjectName(), new IOException(msg, error));
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
    logger.atFiner().log(
        "GCS gRPC read request for up to %d bytes at offset %d from object '%s'",
        byteBuffer.remaining(), position(), resourceId);

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

    // read request content overlaps with cached footer data
    long effectivePosition = position + bytesToSkipBeforeReading;
    if ((footerContent != null) && (effectivePosition >= footerStartOffsetInBytes)) {
      logger.atFiner()
          .log("Read request responded with footer content at position '%s'",
              effectivePosition);
      bytesRead += readFooterContentIntoBuffer(byteBuffer);
      return bytesRead;
    }

    if (resIterator == null) {
      long bytesToRead = getBytesToRead(byteBuffer);
      requestObjectMedia(bytesToRead);
    }

    while (moreServerContent() && byteBuffer.hasRemaining()) {
      GetObjectMediaResponse res = resIterator.next();

      ByteString content = res.getChecksummedData().getContent();
      if (bytesToSkipBeforeReading >= 0 && bytesToSkipBeforeReading < content.size()) {
        content = res.getChecksummedData().getContent().substring((int) bytesToSkipBeforeReading);
        position += bytesToSkipBeforeReading;
        bytesToSkipBeforeReading = 0;
      } else if (bytesToSkipBeforeReading >= content.size()) {
        position += content.size();
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
                  expectedChecksum, calculatedChecksum, resourceId));
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

    if (hasMoreFooterContentToRead(byteBuffer)) {
      int bytesToWrite = min(byteBuffer.remaining(), footerContent.size());
      int bytesToSkipInFooter = (int) (position - footerStartOffsetInBytes);
      put(footerContent, bytesToSkipInFooter, bytesToWrite, byteBuffer);
      position += bytesToWrite;
      bytesRead += bytesToWrite;
    }

    return bytesRead;
  }

  private boolean hasMoreFooterContentToRead(ByteBuffer byteBuffer) {
    return footerContent != null &&
        position >= footerStartOffsetInBytes &&
        byteBuffer.hasRemaining();
  }

  private Long getBytesToRead(ByteBuffer byteBuffer) {
    long bytesToRead = 0L; // bytesToRead = 0 indicates read till EOF
    if (readStrategy == Fadvise.RANDOM) {
      bytesToRead = max(byteBuffer.remaining(), readOptions.getMinRangeRequestSize());
    }

    if (footerContent == null) {
      return bytesToRead;
    }

    long bytesToFooterOffset = footerStartOffsetInBytes - position;
    return bytesToRead == 0 ? bytesToFooterOffset
        : min(bytesToRead, bytesToFooterOffset);
  }

  private int readFooterContentIntoBuffer(ByteBuffer byteBuffer) {
    position += bytesToSkipBeforeReading;
    bytesToSkipBeforeReading = 0;
    long bytesToSkipFromFooter = position - footerStartOffsetInBytes;
    long bytesToWriteFromFooter = footerContent.size() - bytesToSkipFromFooter;
    int bytesToWrite = Math.toIntExact(min(byteBuffer.remaining(), bytesToWriteFromFooter));
    put(footerContent, Math.toIntExact(bytesToSkipFromFooter), bytesToWrite, byteBuffer);
    position += bytesToWrite;
    return bytesToWrite;
  }

  private void requestObjectMedia(long bytesToRead) throws IOException {
    GetObjectMediaRequest.Builder requestBuilder =
        GetObjectMediaRequest.newBuilder()
            .setBucket(resourceId.getBucketName())
            .setObject(resourceId.getObjectName())
            .setGeneration(objectGeneration)
            .setReadOffset(position);
    if (bytesToRead != 0) {
      requestBuilder.setReadLimit(bytesToRead);
    }
    GetObjectMediaRequest request = requestBuilder.build();
    try {
      ResilientOperation.retry(
          () -> {
            try {
              requestContext = Context.current().withCancellation();
              Context toReattach = requestContext.attach();
              try {
                resIterator =
                    stub.withDeadlineAfter(readOptions.getGrpcReadTimeoutMillis(), MILLISECONDS)
                        .getObjectMedia(request);
              } finally {
                requestContext.detach(toReattach);
              }
            } catch (StatusRuntimeException e) {
              recreateStub(e);
              throw convertError(e, resourceId);
            }
            return null;
          },
          backOffFactory.newBackOff(),
          RetryDeterminer.ALL_ERRORS,
          IOException.class);
    } catch (Exception e) {
      throw new IOException(String.format("Error reading '%s'", resourceId), e);
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

    try {
      return ResilientOperation.retry(
          () -> {
            try {
              boolean moreDataAvailable = resIterator.hasNext();
              if (!moreDataAvailable) {
                cancelCurrentRequest();
              }
              return moreDataAvailable;
            } catch (StatusRuntimeException e) {
              recreateStub(e);
              throw convertError(e, resourceId);
            }
          },
          backOffFactory.newBackOff(),
          RetryDeterminer.ALL_ERRORS,
          IOException.class);
    } catch (Exception e) {
      cancelCurrentRequest();
      throw new IOException(String.format("Error reading '%s'", resourceId), e);
    }
  }

  private void recreateStub(StatusRuntimeException e) {
    if (stubProvider.isStubBroken(Status.fromThrowable(e).getCode())) {
      stub = stubProvider.newBlockingStub();
    }
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
        .add("resourceId", resourceId)
        .add("generation", objectGeneration)
        .toString();
  }
}
