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
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.api.client.util.BackOff;
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
import com.google.common.flogger.GoogleLogger;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import com.google.storage.v2.ReadObjectRequest;
import com.google.storage.v2.ReadObjectResponse;
import com.google.storage.v2.StorageGrpc;
import com.google.storage.v2.StorageGrpc.StorageBlockingStub;
import io.grpc.Context;
import io.grpc.Context.CancellableContext;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import javax.annotation.Nullable;

public class GoogleCloudStorageGrpcReadChannel implements SeekableByteChannel {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  protected static final String METADATA_FIELDS = "contentEncoding,generation,size";

  // ZeroCopy version of GetObjectMedia Method
  private final ZeroCopyMessageMarshaller getObjectMediaResponseMarshaller =
      new ZeroCopyMessageMarshaller(ReadObjectResponse.getDefaultInstance());
  private final MethodDescriptor<ReadObjectRequest, ReadObjectResponse> getObjectMediaMethod =
      StorageGrpc.getReadObjectMethod()
          .toBuilder()
          .setResponseMarshaller(getObjectMediaResponseMarshaller)
          .build();
  private final boolean useZeroCopyMarshaller;

  private volatile StorageBlockingStub stub;

  private final StorageResourceId resourceId;

  // We read from a specific generation, to maintain consistency between read() calls.
  private final long objectGeneration;

  // The size of this object generation, in bytes.
  private final long objectSize;

  // True if this channel is open, false otherwise.
  private boolean channelIsOpen = true;

  // Current position in the object.
  private long positionInGrpcStream = 0;

  // If a user seeks forwards by a configurably small amount, we continue reading from where
  // we are instead of starting a new connection. The user's intended read position is
  // position + bytesToSkipBeforeReading.
  private long bytesToSkipBeforeReading = 0;

  // The user may have read less data than we received from the server. If that's the case, we
  // keep
  // the most recently received content and a reference to how much of it we've returned so far.
  @Nullable private ByteString bufferedContent = null;

  private int bufferedContentReadOffset = 0;

  // InputStream that backs bufferedContent. This needs to be closed when bufferedContent is no
  // longer needed.
  @Nullable private InputStream streamForBufferedContent = null;

  // The streaming read operation. If null, there is not an in-flight read in progress.
  @Nullable private Iterator<ReadObjectResponse> resIterator = null;

  // Fine-grained options.
  private final GoogleCloudStorageReadOptions readOptions;

  private final BackOffFactory backOffFactory;

  // Context of the request that returned resIterator.
  @Nullable CancellableContext requestContext;

  Fadvise readStrategy;

  @Nullable private final ByteString footerContent;

  private final long footerStartOffsetInBytes;

  // Offset in the object for the end of the range-requests
  private long contentChannelEndOffset = -1;

  private long readTimeout;

  private final Watchdog watchdog;

  private final long gRPCReadMessageTimeout;

  /**
   * Used to open given file using item info
   *
   * @param stubProvider gRPC stub for accessing the Storage gRPC API
   * @param storage store and retrieve data object
   * @param errorExtractor ApiErrorExtractor instance to convert downstream error into appropriate
   *     fs exception
   * @param resourceId Identifier for the file to be opened
   * @param watchdog Watchdog instance to monitor open streams
   * @param readOptions readOptions fine-grained options specifying things like retry settings,
   *     buffering, etc.
   * @return gRPC read channel
   * @throws IOException IO Error
   */
  public static GoogleCloudStorageGrpcReadChannel open(
      StorageStubProvider stubProvider,
      Storage storage,
      ApiErrorExtractor errorExtractor,
      StorageResourceId resourceId,
      Watchdog watchdog,
      GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    return open(
        stubProvider,
        storage,
        errorExtractor,
        resourceId,
        watchdog,
        readOptions,
        BackOffFactory.DEFAULT);
  }

  /**
   * Used to open given file using item info
   *
   * @param stubProvider gRPC stub for accessing the Storage gRPC API
   * @param storage store and retrieve data object
   * @param itemInfo contains metadata information about the file
   * @param readOptions readOptions fine-grained options specifying things like retry settings,
   *     buffering, etc.
   * @return gRPC read channel
   * @throws IOException IOException on IO Error
   */
  static GoogleCloudStorageGrpcReadChannel open(
      StorageStubProvider stubProvider,
      Storage storage,
      GoogleCloudStorageItemInfo itemInfo,
      Watchdog watchdog,
      GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    return open(stubProvider, storage, itemInfo, watchdog, readOptions, BackOffFactory.DEFAULT);
  }

  @VisibleForTesting
  static GoogleCloudStorageGrpcReadChannel open(
      StorageStubProvider stubProvider,
      Storage storage,
      ApiErrorExtractor errorExtractor,
      StorageResourceId resourceId,
      Watchdog watchdog,
      GoogleCloudStorageReadOptions readOptions,
      BackOffFactory backOffFactory)
      throws IOException {
    // The gRPC API's GetObjectMedia call does not provide a generation number, so to ensure
    // consistent reads, we need to begin by checking the current generation number with a
    // separate
    // call.
    try {
      return ResilientOperation.retry(
          () ->
              openChannel(
                  stubProvider,
                  storage,
                  errorExtractor,
                  resourceId,
                  watchdog,
                  readOptions,
                  backOffFactory),
          backOffFactory.newBackOff(),
          RetryDeterminer.ALL_ERRORS,
          IOException.class);
    } catch (Exception e) {
      throw new IOException(String.format("Error reading '%s'", resourceId), e);
    }
  }

  /**
   * The gRPC API's GetObjectMedia call does not provide a generation number, so to ensure
   * consistent reads, we need to begin by checking the current generation number with a separate
   * call.
   *
   * @param stubProvider gRPC stub for accessing the Storage gRPC API
   * @param storage store and retrieve data object
   * @param itemInfo contains metadata information about the file
   * @param watchdog monitors read channel for Idle time
   * @param readOptions readOptions fine-grained options specifying things like retry settings,
   *     buffering, etc.
   * @param backOffFactory
   * @return gRPC read channel
   * @throws IOException IOException on IO Error
   */
  static GoogleCloudStorageGrpcReadChannel open(
      StorageStubProvider stubProvider,
      Storage storage,
      GoogleCloudStorageItemInfo itemInfo,
      Watchdog watchdog,
      GoogleCloudStorageReadOptions readOptions,
      BackOffFactory backOffFactory)
      throws IOException {

    try {
      return ResilientOperation.retry(
          () -> openChannel(stubProvider, storage, itemInfo, watchdog, readOptions, backOffFactory),
          backOffFactory.newBackOff(),
          RetryDeterminer.ALL_ERRORS,
          IOException.class);
    } catch (Exception e) {
      throw new IOException(String.format("Error reading '%s'", itemInfo.getResourceId()), e);
    }
  }

  private static GoogleCloudStorageGrpcReadChannel openChannel(
      StorageStubProvider stubProvider,
      Storage storage,
      ApiErrorExtractor errorExtractor,
      StorageResourceId resourceId,
      Watchdog watchdog,
      GoogleCloudStorageReadOptions readOptions,
      BackOffFactory backOffFactory)
      throws IOException {
    // TODO(b/135138893): We can avoid this call by adding metadata to a read request.
    //      That will save about 40ms per read.
    checkArgument(storage != null, "GCS json client cannot be null");
    GoogleCloudStorageItemInfo itemInfo =
        getObjectMetadata(resourceId, errorExtractor, backOffFactory, storage);
    checkArgument(itemInfo != null, "object metadata cannot be null");
    return openChannel(stubProvider, storage, itemInfo, watchdog, readOptions, backOffFactory);
  }

  /**
   * Overloaded implementation of openChannel with item info to reduce an object metadata call
   *
   * @param stubProvider gRPC stub for accessing the Storage gRPC API
   * @param storage store and retrieve data object
   * @param itemInfo contains metadata information about the file
   * @param readOptions readOptions fine-grained options specifying things like retry settings,
   *     buffering, etc.
   * @param backOffFactory
   * @return gRPC read channel
   * @throws IOException IOException on IO Error
   */
  private static GoogleCloudStorageGrpcReadChannel openChannel(
      StorageStubProvider stubProvider,
      Storage storage,
      GoogleCloudStorageItemInfo itemInfo,
      Watchdog watchdog,
      GoogleCloudStorageReadOptions readOptions,
      BackOffFactory backOffFactory)
      throws IOException {
    StorageBlockingStub stub = stubProvider.newBlockingStub();
    checkArgument(storage != null, "GCS json client cannot be null");
    checkArgument(itemInfo != null, "object metadata cannot be null");
    // The non-gRPC read channel has special support for gzip. This channel doesn't
    // decompress gzip-encoded objects on the fly, so best to fail fast rather than return
    // gibberish unexpectedly.
    StorageResourceId resourceId = itemInfo.getResourceId();
    if (!itemInfo.exists()) {
      throw new FileNotFoundException(
          String.format(
              "%s not found: %s", itemInfo.isDirectory() ? "Directory" : "File", resourceId));
    }
    String contentEncoding = itemInfo.getContentEncoding();
    if (contentEncoding != null && contentEncoding.contains("gzip")) {
      throw new IOException(
          "Cannot read GZIP encoded files - content encoding support is disabled.");
    }

    int prefetchSizeInBytes = readOptions.getMinRangeRequestSize() / 2;
    long objectSize = itemInfo.getSize();
    long footerOffsetInBytes = Math.max(0, (objectSize - prefetchSizeInBytes));

    long startTime = System.currentTimeMillis();
    ByteString footerContent = getFooterContent(resourceId, readOptions, stub, footerOffsetInBytes);
    long endTime = System.currentTimeMillis();
    if (footerContent == null) {
      logger.atFiner().log(
          "Prefetched footer content is null for resource '%s', spent %d milliseconds",
          resourceId, (endTime - startTime));
    } else {
      logger.atFiner().log(
          "prefetched footer, resource:%s, time:%d, size:%d",
          resourceId, (endTime - startTime), footerContent.size());
    }

    return new GoogleCloudStorageGrpcReadChannel(
        stub,
        resourceId,
        itemInfo.getContentGeneration(),
        itemInfo.getSize(),
        footerOffsetInBytes,
        footerContent,
        watchdog,
        readOptions,
        backOffFactory);
  }

  private static GoogleCloudStorageItemInfo getObjectMetadata(
      StorageResourceId resourceId,
      ApiErrorExtractor errorExtractor,
      BackOffFactory backOffFactory,
      Storage gcs)
      throws IOException {
    StorageObject object;
    long startTime = System.currentTimeMillis();
    try {
      // TODO(b/190617054) : Migrate to gRPC requests for metadata when available
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
    } finally {
      long endTime = System.currentTimeMillis();
      logger.atFinest().log(
          "fetched metadata, resource:%s, time:%d", resourceId, (endTime - startTime));
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

  private static ByteString getFooterContent(
      StorageResourceId resourceId,
      GoogleCloudStorageReadOptions readOptions,
      StorageBlockingStub stub,
      long footerOffset)
      throws IOException {
    try {
      Iterator<ReadObjectResponse> footerContentResponse =
          stub.withDeadlineAfter(readOptions.getGrpcReadTimeoutMillis(), MILLISECONDS)
              .readObject(
                  ReadObjectRequest.newBuilder()
                      .setReadOffset(footerOffset)
                      .setBucket(GrpcChannelUtils.toV2BucketName(resourceId.getBucketName()))
                      .setObject(resourceId.getObjectName())
                      .build());

      ByteString footerContent = null;
      while (footerContentResponse.hasNext()) {
        ReadObjectResponse readObjectResponse = footerContentResponse.next();
        if (readObjectResponse.hasChecksummedData()) {
          ByteString content = readObjectResponse.getChecksummedData().getContent();
          if (footerContent == null) {
            footerContent = content;
          } else {
            footerContent = footerContent.concat(content);
          }
        }
      }
      return footerContent;
    } catch (StatusRuntimeException e) {
      throw convertError(e, resourceId);
    }
  }

  private GoogleCloudStorageGrpcReadChannel(
      StorageBlockingStub gcsGrpcBlockingStub,
      StorageResourceId resourceId,
      long objectGeneration,
      long objectSize,
      long footerStartOffsetInBytes,
      ByteString footerContent,
      Watchdog watchdog,
      GoogleCloudStorageReadOptions readOptions,
      BackOffFactory backOffFactory) {
    this.useZeroCopyMarshaller =
        ZeroCopyReadinessChecker.isReady() && readOptions.isGrpcReadZeroCopyEnabled();
    this.stub = gcsGrpcBlockingStub;
    this.resourceId = resourceId;
    this.objectGeneration = objectGeneration;
    this.objectSize = objectSize;
    this.watchdog = watchdog;
    this.readOptions = readOptions;
    this.backOffFactory = backOffFactory;
    this.readStrategy = readOptions.getFadvise();
    this.footerStartOffsetInBytes = footerStartOffsetInBytes;
    this.footerContent = footerContent;
    this.gRPCReadMessageTimeout = readOptions.getGrpcReadMessageTimeoutMillis();
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
    positionInGrpcStream += bufferSkip;
    int remainingBufferedBytes = bufferedContent.size() - bufferedContentReadOffset;

    boolean remainingBufferedContentLargerThanByteBuffer =
        remainingBufferedBytes > byteBuffer.remaining();
    int bytesToWrite =
        remainingBufferedContentLargerThanByteBuffer
            ? byteBuffer.remaining()
            : remainingBufferedBytes;
    put(bufferedContent, bufferedContentReadOffset, bytesToWrite, byteBuffer);
    positionInGrpcStream += bytesToWrite;

    if (remainingBufferedContentLargerThanByteBuffer) {
      bufferedContentReadOffset += bytesToWrite;
    } else {
      invalidateBufferedContent();
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
    long startTime = System.currentTimeMillis();
    try {
      int bytesRead = 0;

      if (resIterator != null && isByteBufferBeyondCurrentRequestRange(byteBuffer)) {
        positionInGrpcStream += bytesToSkipBeforeReading;
        cancelCurrentRequest();
        invalidateBufferedContent();
        bytesToSkipBeforeReading = 0;
      }

      // The server responds in 2MB chunks, but the client can ask for less than that. We
      // store the remainder in bufferedContent and return pieces of that on the next read call (and
      // flush that buffer if there is a seek).
      if (bufferedContent != null) {
        bytesRead += readBufferedContentInto(byteBuffer);
      }
      if (!byteBuffer.hasRemaining()) {
        return bytesRead;
      }
      if (positionInGrpcStream == objectSize) {
        return bytesRead > 0 ? bytesRead : -1;
      }

      // read request content overlaps with cached footer data
      long effectivePosition = positionInGrpcStream + bytesToSkipBeforeReading;
      if ((footerContent != null) && (effectivePosition >= footerStartOffsetInBytes)) {
        logger.atFiner().log(
            "Read request responded with footer content at position '%s'", effectivePosition);
        bytesRead += readFooterContentIntoBuffer(byteBuffer);
        return bytesRead;
      }

      bytesRead += readFromGCS(byteBuffer);

      if (hasMoreFooterContentToRead(byteBuffer)) {
        int bytesToWrite = min(byteBuffer.remaining(), footerContent.size());
        int bytesToSkipInFooter = (int) (positionInGrpcStream - footerStartOffsetInBytes);
        put(footerContent, bytesToSkipInFooter, bytesToWrite, byteBuffer);
        positionInGrpcStream += bytesToWrite;
        bytesRead += bytesToWrite;
      }

      return bytesRead;
    } catch (InterruptedException e) {
      cancelCurrentRequest();
      long endTime = System.currentTimeMillis();
      logger.atFinest().log(
          "read data errored, resource:%s, time:%d, offset:%d, remaining:%d",
          resourceId, (endTime - startTime), positionInGrpcStream, byteBuffer.remaining());
      throw new IOException(e);
    } finally {
      long endTime = System.currentTimeMillis();
      logger.atFinest().log(
          "read data, resource:%s, time:%d, offset:%d, remaining:%d",
          resourceId, (endTime - startTime), positionInGrpcStream, byteBuffer.remaining());
    }
  }

  /**
   * Reads data from GCS over network, with retries
   *
   * @param byteBuffer Buffer to be filled with data from GCS
   * @return number of bytes read into the buffer
   * @throws IOException In case of data errors or network errors
   * @throws InterruptedException In case of thread interrupt while retrying
   */
  private int readFromGCS(ByteBuffer byteBuffer) throws IOException, InterruptedException {
    int read = 0;
    StatusRuntimeException statusRuntimeException;
    BackOff backoff = backOffFactory.newBackOff();
    Sleeper sleeper = Sleeper.DEFAULT;
    do {
      try {
        if (resIterator == null) {
          OptionalLong bytesToRead = getBytesToRead(byteBuffer);
          positionInGrpcStream += bytesToSkipBeforeReading;
          bytesToSkipBeforeReading = 0;
          requestObjectMedia(bytesToRead);
          if (bytesToRead.isPresent()) {
            contentChannelEndOffset = positionInGrpcStream + bytesToRead.getAsLong();
          }
        }
        while (byteBuffer.hasRemaining() && moreServerContent()) {
          read += readObjectContentFromGCS(byteBuffer);
        }
        return read;
      } catch (StatusRuntimeException e) {
        cancelCurrentRequest();
        statusRuntimeException = e;
      }
    } while (ResilientOperation.nextSleep(backoff, sleeper, statusRuntimeException));
    throw convertError(statusRuntimeException, resourceId);
  }

  private boolean isByteBufferBeyondCurrentRequestRange(ByteBuffer byteBuffer) {
    long effectivePosition = positionInGrpcStream + bytesToSkipBeforeReading;
    // current request does not have a range or this is the first request
    if (contentChannelEndOffset == -1) {
      return false;
    }
    return (effectivePosition + byteBuffer.remaining()) > (contentChannelEndOffset);
  }

  private int readObjectContentFromGCS(ByteBuffer byteBuffer) throws IOException {
    int bytesRead = 0;
    ReadObjectResponse res = resIterator.next();

    // When zero-copy marshaller is used, the stream that backs GetObjectMediaResponse
    // should be closed when the message is no longed needed so that all buffers in the
    // stream can be reclaimed. If zero-copy is not used, stream will be null.
    InputStream stream = getObjectMediaResponseMarshaller.popStream(res);
    try {
      ByteString content = res.getChecksummedData().getContent();
      if (bytesToSkipBeforeReading >= 0 && bytesToSkipBeforeReading < content.size()) {
        content = content.substring((int) bytesToSkipBeforeReading);
        positionInGrpcStream += bytesToSkipBeforeReading;
        bytesToSkipBeforeReading = 0;
      } else if (bytesToSkipBeforeReading >= content.size()) {
        positionInGrpcStream += content.size();
        bytesToSkipBeforeReading -= content.size();
        return bytesRead;
      }

      if (readOptions.isGrpcChecksumsEnabled() && res.getChecksummedData().hasCrc32C()) {
        validateChecksum(res);
      }

      boolean responseSizeLargerThanRemainingBuffer = content.size() > byteBuffer.remaining();
      int bytesToWrite =
          responseSizeLargerThanRemainingBuffer ? byteBuffer.remaining() : content.size();
      put(content, 0, bytesToWrite, byteBuffer);
      bytesRead += bytesToWrite;
      positionInGrpcStream += bytesToWrite;

      if (responseSizeLargerThanRemainingBuffer) {
        invalidateBufferedContent();
        bufferedContent = content;
        bufferedContentReadOffset = bytesToWrite;
        // This is to keep the stream alive for the message backed by this.
        streamForBufferedContent = stream;
        stream = null;
      }
    } finally {
      if (stream != null) {
        stream.close();
      }
    }
    return bytesRead;
  }

  private void validateChecksum(ReadObjectResponse res) throws IOException {
    // TODO: Concatenate all these hashes together and compare the result at the end.
    int calculatedChecksum =
        Hashing.crc32c().hashBytes(res.getChecksummedData().getContent().toByteArray()).asInt();
    int expectedChecksum = res.getChecksummedData().getCrc32C();
    if (calculatedChecksum != expectedChecksum) {
      throw new IOException(
          String.format(
              "Message checksum (%s) didn't match expected checksum (%s) for '%s'",
              expectedChecksum, calculatedChecksum, this.resourceId));
    }
  }

  private boolean hasMoreFooterContentToRead(ByteBuffer byteBuffer) {
    return footerContent != null
        && positionInGrpcStream >= footerStartOffsetInBytes
        && byteBuffer.hasRemaining();
  }

  private OptionalLong getBytesToRead(ByteBuffer byteBuffer) {
    OptionalLong optionalBytesToRead = OptionalLong.empty();
    if (readStrategy == Fadvise.RANDOM) {
      long rangeRequestSize =
          Math.max(readOptions.getInplaceSeekLimit(), readOptions.getMinRangeRequestSize());
      optionalBytesToRead = OptionalLong.of(max((long) byteBuffer.remaining(), rangeRequestSize));
    }

    if (footerContent == null) {
      return optionalBytesToRead;
    }

    long bytesToFooterOffset = footerStartOffsetInBytes - positionInGrpcStream;
    if (optionalBytesToRead.isPresent()) {
      return OptionalLong.of(min(optionalBytesToRead.getAsLong(), bytesToFooterOffset));
    }
    return OptionalLong.of(bytesToFooterOffset);
  }

  private int readFooterContentIntoBuffer(ByteBuffer byteBuffer) {
    positionInGrpcStream += bytesToSkipBeforeReading;
    bytesToSkipBeforeReading = 0;
    long bytesToSkipFromFooter = positionInGrpcStream - footerStartOffsetInBytes;
    long bytesToWriteFromFooter = footerContent.size() - bytesToSkipFromFooter;
    int bytesToWrite = Math.toIntExact(min(byteBuffer.remaining(), bytesToWriteFromFooter));
    put(footerContent, Math.toIntExact(bytesToSkipFromFooter), bytesToWrite, byteBuffer);
    positionInGrpcStream += bytesToWrite;
    return bytesToWrite;
  }

  private void requestObjectMedia(OptionalLong bytesToRead) throws StatusRuntimeException {
    ReadObjectRequest.Builder requestBuilder =
        ReadObjectRequest.newBuilder()
            .setBucket(GrpcChannelUtils.toV2BucketName(resourceId.getBucketName()))
            .setObject(resourceId.getObjectName())
            .setGeneration(objectGeneration)
            .setReadOffset(positionInGrpcStream);
    bytesToRead.ifPresent(requestBuilder::setReadLimit);
    ReadObjectRequest request = requestBuilder.build();

    requestContext = Context.current().withCancellation();
    Context toReattach = requestContext.attach();
    StorageBlockingStub blockingStub =
        stub.withDeadlineAfter(readOptions.getGrpcReadTimeoutMillis(), MILLISECONDS);
    try {
      if (useZeroCopyMarshaller) {
        Iterator<ReadObjectResponse> responseIterator =
            ClientCalls.blockingServerStreamingCall(
                blockingStub.getChannel(),
                getObjectMediaMethod,
                blockingStub.getCallOptions(),
                request);
        resIterator =
            watchdog.watch(
                requestContext, responseIterator, Duration.ofMillis(this.gRPCReadMessageTimeout));
      } else {
        resIterator =
            watchdog.watch(
                requestContext,
                blockingStub.readObject(request),
                Duration.ofMillis(this.gRPCReadMessageTimeout));
      }
    } finally {
      requestContext.detach(toReattach);
    }
  }

  private void cancelCurrentRequest() {
    if (requestContext != null) {
      requestContext.close();
      requestContext = null;
    }
    // gRPC read calls use blocking server streaming api. On cancellation, iterator can be leaked.
    // To avoid oom, we drain the iterator ref b/210660938
    drainIterator();
    resIterator = null;
    List<InputStream> unclosedStreams = getObjectMediaResponseMarshaller.popAllStreams();
    for (InputStream stream : unclosedStreams) {
      try {
        stream.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    contentChannelEndOffset = -1;
  }

  /** Drains the iterator */
  private void drainIterator() {
    if (resIterator == null) {
      return;
    }
    try {
      while (resIterator.hasNext()) {
        resIterator.next();
      }
    } catch (Exception e) {
      logger.atFiner().withCause(e).log("Exception while draining the iteration on cancellation");
    }
  }

  /**
   * Waits until more data is available from the server, or returns false if read is done.
   *
   * @return true if more data is available with .next()
   */
  private boolean moreServerContent() {
    if (resIterator == null || requestContext == null || requestContext.isCancelled()) {
      return false;
    }
    boolean moreDataAvailable = resIterator.hasNext();
    if (!moreDataAvailable) {
      cancelCurrentRequest();
    }
    return moreDataAvailable;
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
    // Our real position is tracked in "positionInGrpcStream," but if the user is skipping
    // forwards a bit, we pretend we're at the new position already.
    return positionInGrpcStream + bytesToSkipBeforeReading;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    checkArgument(newPosition >= 0, "Read position must be non-negative, but was %s", newPosition);
    checkArgument(
        newPosition < size(),
        "Read position must be before end of file (%s), but was %s",
        size(),
        newPosition);
    if (newPosition == positionInGrpcStream) {
      return this;
    }

    long seekDistance = newPosition - positionInGrpcStream;

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
    invalidateBufferedContent();

    positionInGrpcStream = newPosition;
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
    invalidateBufferedContent();
    channelIsOpen = false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("resourceId", resourceId)
        .add("generation", objectGeneration)
        .toString();
  }

  private void invalidateBufferedContent() {
    bufferedContent = null;
    bufferedContentReadOffset = 0;
    if (streamForBufferedContent != null) {
      try {
        streamForBufferedContent.close();
        streamForBufferedContent = null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
