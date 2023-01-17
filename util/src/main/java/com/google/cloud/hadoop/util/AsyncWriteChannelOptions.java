/*
 * Copyright 2014 Google Inc.
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

package com.google.cloud.hadoop.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.auto.value.AutoValue;
import com.google.common.flogger.GoogleLogger;
import java.time.Duration;

/** Options for the {@link AbstractGoogleAsyncWriteChannel}. */
@AutoValue
public abstract class AsyncWriteChannelOptions {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /** Pipe used for output stream. */
  public enum PipeType {
    IO_STREAM_PIPE,
    NIO_CHANNEL_PIPE,
  }

  /** Upload chunk size granularity */
  private static final int UPLOAD_CHUNK_SIZE_GRANULARITY = 8 * 1024 * 1024;

  /** Default upload chunk size. */
  private static final int DEFAULT_UPLOAD_CHUNK_SIZE =
      Runtime.getRuntime().maxMemory() < 512 * 1024 * 1024
          ? UPLOAD_CHUNK_SIZE_GRANULARITY
          : 3 * UPLOAD_CHUNK_SIZE_GRANULARITY;

  public static final AsyncWriteChannelOptions DEFAULT = builder().build();

  public static Builder builder() {
    return new AutoValue_AsyncWriteChannelOptions.Builder()
        .setBufferSize(8 * 1024 * 1024)
        .setDirectUploadEnabled(false)
        .setGrpcChecksumsEnabled(false)
        .setGrpcWriteMessageTimeout(Duration.ofSeconds(3))
        .setGrpcWriteTimeout(Duration.ofMinutes(10))
        .setNumberOfBufferedRequests(20)
        .setPipeBufferSize(1024 * 1024)
        .setPipeType(PipeType.IO_STREAM_PIPE)
        .setUploadCacheSize(0)
        .setUploadChunkSize(DEFAULT_UPLOAD_CHUNK_SIZE);
  }

  public abstract Builder toBuilder();

  public abstract int getBufferSize();

  public abstract int getPipeBufferSize();

  public abstract PipeType getPipeType();

  public abstract int getUploadChunkSize();

  public abstract int getUploadCacheSize();

  public abstract boolean isDirectUploadEnabled();

  public abstract boolean isGrpcChecksumsEnabled();

  public abstract Duration getGrpcWriteTimeout();

  public abstract int getNumberOfBufferedRequests();

  public abstract Duration getGrpcWriteMessageTimeout();

  /** Mutable builder for the GoogleCloudStorageWriteChannelOptions class. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setBufferSize(int bufferSize);

    public abstract Builder setPipeBufferSize(int pipeBufferSize);

    public abstract Builder setPipeType(PipeType pipeType);

    public abstract Builder setUploadChunkSize(int uploadChunkSize);

    public abstract Builder setUploadCacheSize(int uploadCacheSize);

    public abstract Builder setDirectUploadEnabled(boolean directUploadEnabled);

    public abstract Builder setGrpcWriteTimeout(Duration grpcWriteTimeout);

    public abstract Builder setNumberOfBufferedRequests(int numberOfBufferedRequests);

    /**
     * Enable gRPC checksumming. On by default. It is strongly recommended to leave this enabled, to
     * protect against possible data corruption caused by software bugs.
     */
    public abstract Builder setGrpcChecksumsEnabled(boolean grpcChecksumsEnabled);

    public abstract Builder setGrpcWriteMessageTimeout(Duration grpcWriteMessageTimeout);

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
