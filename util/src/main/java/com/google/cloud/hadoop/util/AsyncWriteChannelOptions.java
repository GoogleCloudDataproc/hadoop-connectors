/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.auto.value.AutoValue;
import com.google.common.flogger.GoogleLogger;

/** Options for the {@link AbstractGoogleAsyncWriteChannel}. */
@AutoValue
public abstract class AsyncWriteChannelOptions {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /** Pipe used for output stream. */
  public enum PipeType {
    NIO_CHANNEL_PIPE,
    IO_STREAM_PIPE,
  }

  /** Default upload buffer size. */
  public static final int BUFFER_SIZE_DEFAULT = 8 * 1024 * 1024;

  /** Default pipe buffer size. */
  public static final int PIPE_BUFFER_SIZE_DEFAULT = 1024 * 1024;

  /** Upload chunk size granularity */
  public static final int UPLOAD_CHUNK_SIZE_GRANULARITY = 8 * 1024 * 1024;

  /** Default upload chunk size. */
  public static final int UPLOAD_CHUNK_SIZE_DEFAULT =
      Runtime.getRuntime().maxMemory() < 512 * 1024 * 1024
          ? UPLOAD_CHUNK_SIZE_GRANULARITY
          : 8 * UPLOAD_CHUNK_SIZE_GRANULARITY;

  /** Default upload cache size. */
  public static final int UPLOAD_CACHE_SIZE_DEFAULT = 0;

  /** Default of whether to use direct upload. */
  public static final boolean DIRECT_UPLOAD_ENABLED_DEFAULT = false;

  /** Default of whether to enabled checksums for gRPC. */
  public static final boolean GRPC_CHECKSUMS_ENABLED_DEFAULT = false;

  /** Default timeout for grpc write stream. */
  public static final long DEFAULT_GRPC_WRITE_TIMEOUT = 10 * 60 * 1000;

  /** Default number of insert requests to retain, in case we need to rewind and resume an upload */
  public static final long DEFAULT_NUM_REQUESTS_BUFFERED_GRPC = 20;

  public static final long DEFAULT_GRPC_WRITE_MESSAGE_TIMEOUT_MILLIS = 3 * 1000;

  public static final PipeType PIPE_TYPE_DEFAULT = PipeType.IO_STREAM_PIPE;

  public static final AsyncWriteChannelOptions DEFAULT = builder().build();

  public static Builder builder() {
    return new AutoValue_AsyncWriteChannelOptions.Builder()
        .setBufferSize(BUFFER_SIZE_DEFAULT)
        .setPipeBufferSize(PIPE_BUFFER_SIZE_DEFAULT)
        .setPipeType(PIPE_TYPE_DEFAULT)
        .setUploadChunkSize(UPLOAD_CHUNK_SIZE_DEFAULT)
        .setUploadCacheSize(UPLOAD_CACHE_SIZE_DEFAULT)
        .setDirectUploadEnabled(DIRECT_UPLOAD_ENABLED_DEFAULT)
        .setGrpcChecksumsEnabled(GRPC_CHECKSUMS_ENABLED_DEFAULT)
        .setGrpcWriteTimeout(DEFAULT_GRPC_WRITE_TIMEOUT)
        .setNumberOfBufferedRequests(DEFAULT_NUM_REQUESTS_BUFFERED_GRPC)
        .setGrpcWriteMessageTimeoutMillis(DEFAULT_GRPC_WRITE_MESSAGE_TIMEOUT_MILLIS);
  }

  public abstract Builder toBuilder();

  public abstract int getBufferSize();

  public abstract int getPipeBufferSize();

  public abstract PipeType getPipeType();

  public abstract int getUploadChunkSize();

  public abstract int getUploadCacheSize();

  public abstract boolean isDirectUploadEnabled();

  public abstract boolean isGrpcChecksumsEnabled();

  public abstract long getGrpcWriteTimeout();

  public abstract long getNumberOfBufferedRequests();

  public abstract long getGrpcWriteMessageTimeoutMillis();

  /** Mutable builder for the GoogleCloudStorageWriteChannelOptions class. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setBufferSize(int bufferSize);

    public abstract Builder setPipeBufferSize(int pipeBufferSize);

    public abstract Builder setPipeType(PipeType pipeType);

    public abstract Builder setUploadChunkSize(int uploadChunkSize);

    public abstract Builder setUploadCacheSize(int uploadCacheSize);

    public abstract Builder setDirectUploadEnabled(boolean directUploadEnabled);

    public abstract Builder setGrpcWriteTimeout(long grpcWriteTimeout);

    public abstract Builder setNumberOfBufferedRequests(long numberOfBufferedRequests);

    /**
     * Enable gRPC checksumming. On by default. It is strongly recommended to leave this enabled, to
     * protect against possible data corruption caused by software bugs.
     */
    public abstract Builder setGrpcChecksumsEnabled(boolean grpcChecksumsEnabled);

    public abstract Builder setGrpcWriteMessageTimeoutMillis(long grpcWriteMessageTimeoutMillis);

    abstract AsyncWriteChannelOptions autoBuild();

    public AsyncWriteChannelOptions build() {
      AsyncWriteChannelOptions options = autoBuild();
      checkUploadChunkSize(options.getUploadChunkSize());
      return options;
    }

    private static void checkUploadChunkSize(int chunkSize) {
      checkArgument(
          chunkSize > 0, "Upload chunk size must be great than 0, but was $%s", chunkSize);
      checkArgument(
          chunkSize % MediaHttpUploader.MINIMUM_CHUNK_SIZE == 0,
          "Upload chunk size must be a multiple of %s",
          MediaHttpUploader.MINIMUM_CHUNK_SIZE);
      if (chunkSize > UPLOAD_CHUNK_SIZE_GRANULARITY
          && chunkSize % UPLOAD_CHUNK_SIZE_GRANULARITY != 0) {
        logger.atWarning().log(
            "Upload chunk size should be a multiple of %s for the best performance, got %s",
            UPLOAD_CHUNK_SIZE_GRANULARITY, chunkSize);
      }
    }
  }
}
