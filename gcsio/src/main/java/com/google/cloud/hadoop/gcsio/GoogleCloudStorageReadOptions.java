/*
 * Copyright 2016 Google Inc.
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

import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.time.Duration;

/**
 * Advanced options for reading GoogleCloudStorage objects. Immutable; callers must use the inner
 * {@link Builder} class to construct instances.
 */
@AutoValue
public abstract class GoogleCloudStorageReadOptions {

  /** Operational modes of fadvise feature. */
  public enum Fadvise {
    AUTO,
    RANDOM,
    SEQUENTIAL,
    AUTO_RANDOM
  }

  // Default builder should be initialized after default values,
  // otherwise it will access not initialized default values.
  public static final GoogleCloudStorageReadOptions DEFAULT = builder().build();

  public static Builder builder() {
    return new AutoValue_GoogleCloudStorageReadOptions.Builder()
        .setBackoffInitialInterval(Duration.ofMillis(200))
        .setBackoffMaxElapsedTime(Duration.ofMinutes(2))
        .setBackoffMaxInterval(Duration.ofSeconds(10))
        .setBackoffMultiplier(1.5)
        .setBackoffRandomizationFactor(0.5)
        .setFadvise(Fadvise.AUTO)
        .setFastFailOnNotFoundEnabled(true)
        .setGrpcChecksumsEnabled(false)
        .setGrpcReadMessageTimeout(Duration.ofSeconds(3))
        .setGrpcReadTimeout(Duration.ofHours(1))
        .setGrpcReadZeroCopyEnabled(true)
        .setGzipEncodingSupportEnabled(false)
        .setInplaceSeekLimit(8 * 1024 * 1024)
        .setMinRangeRequestSize(2 * 1024 * 1024)
        .setBlockSize(64 * 1024 * 1024)
        .setFadviseRequestTrackCount(3)
        .setReadExactRequestedBytesEnabled(false)
        .setBidiThreadCount(16)
        .setBidiClientTimeout(30);
  }

  public abstract Builder toBuilder();

  /** See {@link Builder#setBackoffInitialInterval}. */
  public abstract Duration getBackoffInitialInterval();

  /** See {@link Builder#setBackoffRandomizationFactor}. */
  public abstract double getBackoffRandomizationFactor();

  /** See {@link Builder#setBackoffMultiplier}. */
  public abstract double getBackoffMultiplier();

  /** See {@link Builder#setBackoffMaxInterval}. */
  public abstract Duration getBackoffMaxInterval();

  /** See {@link Builder#setBackoffMaxElapsedTime}. */
  public abstract Duration getBackoffMaxElapsedTime();

  /** See {@link Builder#setFastFailOnNotFoundEnabled}. */
  public abstract boolean isFastFailOnNotFoundEnabled();

  /** See {@link Builder#setGzipEncodingSupportEnabled}. */
  public abstract boolean isGzipEncodingSupportEnabled();

  /** See {@link Builder#setInplaceSeekLimit}. */
  public abstract long getInplaceSeekLimit();

  /** See {@link Builder#setFadvise}. */
  public abstract Fadvise getFadvise();

  /** See {@link Builder#setMinRangeRequestSize}. */
  public abstract long getMinRangeRequestSize();

  /** See {@link Builder#setGrpcChecksumsEnabled}. */
  public abstract boolean isGrpcChecksumsEnabled();

  /** See {@link Builder#setReadExactRequestedBytesEnabled}. */
  public abstract boolean isReadExactRequestedBytesEnabled();

  /** See {@link Builder#setGrpcReadTimeout}. */
  public abstract Duration getGrpcReadTimeout();

  /** See {@link Builder#setGrpcReadZeroCopyEnabled}. */
  public abstract boolean isGrpcReadZeroCopyEnabled();

  /** See {@link Builder#setGrpcReadTimeout(Duration)}. */
  public abstract Duration getGrpcReadMessageTimeout();

  public abstract long getBlockSize();

  public abstract int getFadviseRequestTrackCount();

  /** See {@link Builder#setBidiThreadCount(int)}. */
  public abstract int getBidiThreadCount();

  /** See {@link Builder#setBidiClientTimeout(int)}. */
  public abstract int getBidiClientTimeout();

  /** Mutable builder for GoogleCloudStorageReadOptions. */
  @AutoValue.Builder
  public abstract static class Builder {
    /**
     * On exponential back-off, the initial delay before the first retry; subsequent retries then
     * grow as an exponential function of the current delay interval.
     */
    public abstract Builder setBackoffInitialInterval(Duration backoffInitialInterval);

    /**
     * The amount of jitter introduced when computing the next retry sleep interval so that when
     * many clients are retrying, they don't all retry at the same time.
     */
    public abstract Builder setBackoffRandomizationFactor(double backoffRandomizationFactor);

    /**
     * The base of the exponent used for exponential back-off; each subsequent sleep interval is
     * roughly this many times the previous interval.
     */
    public abstract Builder setBackoffMultiplier(double backoffMultiplier);

    /**
     * The maximum amount of sleep between retries; at this point, there will be no further
     * exponential back-off. This prevents intervals from growing unreasonably large.
     */
    public abstract Builder setBackoffMaxInterval(Duration backoffMaxInterval);

    /**
     * The maximum total time elapsed since the first retry over the course of a series of retries.
     * This makes it easier to bound the maximum time it takes to respond to a permanent failure
     * without having to calculate the summation of a series of exponentiated intervals while
     * accounting for the randomization of back-off intervals.
     */
    public abstract Builder setBackoffMaxElapsedTime(Duration backoffMaxElapsedTime);

    /**
     * True if attempts to open a new channel on a nonexistent object are required to immediately
     * throw an IOException. If false, then channels may not throw exceptions for such cases until
     * attempting to call read(). Performance can be improved if this is set to false and the caller
     * is equipped to deal with delayed failures for not-found objects. Or if the caller is already
     * sure the object being opened exists, it is recommended to set this to `false` to avoid doing
     * extraneous checks on open().
     */
    public abstract Builder setFastFailOnNotFoundEnabled(boolean fastFailOnNotFound);

    /**
     * If false then reading a file with GZIP content encoding (HTTP header "Content-Encoding:
     * gzip") will result in failure (IOException is thrown). If true then GZIP-encoded files will
     * be read successfully.
     */
    public abstract Builder setGzipEncodingSupportEnabled(boolean gzipEncodingSupportEnabled);

    /**
     * If seeking to a new position which is within this number of bytes in front of the current
     * position, then we will skip forward by reading and discarding the necessary amount of bytes
     * rather than trying to open a brand-new underlying stream.
     */
    public abstract Builder setInplaceSeekLimit(long inplaceSeekLimit);

    /**
     * Sets fadvise mode that tunes behavior to optimize HTTP GET requests for various use cases.
     *
     * <p>Supported modes:
     *
     * <ul>
     *   <li>{@code AUTO} - automatically switches to {@code RANDOM} mode if backward read or
     *       forward read for more than {@link #setInplaceSeekLimit} bytes is detected.
     *   <li>{@code RANDOM} - sends HTTP requests with {@code Range} header set to greater of
     *       provided reade buffer by user.
     *   <li>{@code SEQUENTIAL} - sends HTTP requests with unbounded {@code Range} header.
     * </ul>
     */
    public abstract Builder setFadvise(Fadvise fadvise);

    /**
     * Sets the minimum size of the HTTP Range header that could be set in GCS request when opening
     * new stream to read an object.
     */
    public abstract Builder setMinRangeRequestSize(long size);

    /**
     * Sets whether to validate checksums when doing gRPC reads. If enabled, for sequential reads of
     * a whole object, the object checksums will be validated.
     *
     * <p>TODO(b/134521856): Update this to discuss per-request checksums once the server supplies
     * them and we're validating them.
     */
    public abstract Builder setGrpcChecksumsEnabled(boolean grpcChecksumsEnabled);

    public abstract Builder setReadExactRequestedBytesEnabled(
        boolean readExactRequestedBytesEnabled);

    /** Sets the property to override the default GCS gRPC read stream timeout. */
    public abstract Builder setGrpcReadTimeout(Duration grpcReadTimeout);

    /** Sets the property to use the zero-copy deserializer for gRPC read. */
    public abstract Builder setGrpcReadZeroCopyEnabled(boolean grpcReadZeroCopyEnabled);

    /** Sets the property for gRPC read message timeout in milliseconds. */
    public abstract Builder setGrpcReadMessageTimeout(Duration grpcMessageTimeout);

    public abstract Builder setBlockSize(long blockSize);

    public abstract Builder setFadviseRequestTrackCount(int requestTrackCount);

    /**
     * Sets the number of threads used by ThreadPoolExecutor in bidi channel. This executor is used
     * to read individual range and populate the buffer.
     */
    public abstract Builder setBidiThreadCount(int bidiThreadCount);

    /** Sets the total amount of time, we would wait for bidi client initialization. */
    public abstract Builder setBidiClientTimeout(int bidiClientTimeout);

    abstract GoogleCloudStorageReadOptions autoBuild();

    public GoogleCloudStorageReadOptions build() {
      GoogleCloudStorageReadOptions options = autoBuild();
      checkState(
          options.getInplaceSeekLimit() >= 0,
          "inplaceSeekLimit must be non-negative! Got %s",
          options.getInplaceSeekLimit());
      return options;
    }
  }
}
