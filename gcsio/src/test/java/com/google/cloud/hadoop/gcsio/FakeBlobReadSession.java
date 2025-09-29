/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadAsFutureByteString;
import com.google.cloud.storage.ReadProjectionConfig;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.ZeroCopySupport.DisposableByteString;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class FakeBlobReadSession implements BlobReadSession {

  public static final String TEST_STRING =
      "Lorem ipsum dolor sit amet. Qui esse voluptatum qui tempora quia quo maiores galisum. Et officia cum";

  // The following are substring used in Test cases following the format SUBSTRING_{Offset}_{Length}
  public static final String SUBSTRING_0_10 = TEST_STRING.substring(0, 10);
  public static final String SUBSTRING_10_10 = TEST_STRING.substring(10, 20);
  public static final String SUBSTRING_20_10 = TEST_STRING.substring(20, 30);
  public static final String SUBSTRING_50_7 = TEST_STRING.substring(50, 57);
  public static final String SUBSTRING_65_17 = TEST_STRING.substring(65, 82);

  public enum Behavior {
    DEFAULT,
    READ_ZERO_BYTES,
    FAIL_FUTURE,
    TIMEOUT_FUTURE,
    IO_EXCEPTION,
    READ_PARTIAL_BYTES
  }

  private final Behavior behavior;
  private final Iterator<Behavior> behaviorIterator;
  private final SettableApiFuture<DisposableByteString> neverCompleteFuture;

  public FakeBlobReadSession() {
    this(Behavior.DEFAULT, null);
  }

  public FakeBlobReadSession(Behavior behavior) {
    this(behavior, SettableApiFuture.create());
  }

  public FakeBlobReadSession(List<Behavior> behaviors) {
    this(behaviors, SettableApiFuture.create());
  }

  public FakeBlobReadSession(
      Behavior behavior, SettableApiFuture<DisposableByteString> neverCompleteFuture) {
    this.behavior = behavior;
    this.behaviorIterator = null;
    this.neverCompleteFuture = neverCompleteFuture;
  }

  public FakeBlobReadSession(
      List<Behavior> behaviors, SettableApiFuture<DisposableByteString> neverCompleteFuture) {
    this.behavior = null;
    this.behaviorIterator = behaviors.iterator();
    this.neverCompleteFuture = neverCompleteFuture;
  }

  @Override
  public BlobInfo getBlobInfo() {
    return null;
  }

  @Override
  public <Projection> Projection readAs(ReadProjectionConfig<Projection> readProjectionConfig) {
    Behavior currentBehavior = behavior;
    if (behaviorIterator != null && behaviorIterator.hasNext()) {
      currentBehavior = behaviorIterator.next();
    }

    if (currentBehavior == null) {
      currentBehavior = Behavior.DEFAULT;
    }
    RangeSpec range = ((ReadAsFutureByteString) readProjectionConfig).getRange();

    switch (currentBehavior) {
      case READ_ZERO_BYTES:
        return (Projection)
            ApiFutures.immediateFuture(
                new DisposableByteString() {
                  @Override
                  public ByteString byteString() {
                    return ByteString.EMPTY;
                  }

                  @Override
                  public void close() {}
                });
      case READ_PARTIAL_BYTES:
        return (Projection)
            ApiFutures.immediateFuture(
                new DisposableByteString() {
                  @Override
                  public ByteString byteString() {
                    return ByteString.copyFrom(
                        getPartialSubstring(range).getBytes(StandardCharsets.UTF_8));
                  }

                  @Override
                  public void close() throws IOException {}
                });
      case IO_EXCEPTION:
        return (Projection) ApiFutures.immediateFailedFuture(new IOException());
      case FAIL_FUTURE:
        return (Projection)
            ApiFutures.immediateFailedFuture(new StorageException(404, "Not Found"));
      case TIMEOUT_FUTURE:
        return (Projection) neverCompleteFuture;
      case DEFAULT:
      default:
        assertThat(readProjectionConfig).isInstanceOf(ReadAsFutureByteString.class);
        return (Projection)
            ApiFutures.immediateFuture(
                new DisposableByteString() {
                  @Override
                  public ByteString byteString() {
                    return ByteString.copyFrom(
                        getSubString(range).getBytes(StandardCharsets.UTF_8));
                  }

                  @Override
                  public void close() throws IOException {}
                });
    }
  }

  private String getSubString(RangeSpec range) {
    return TEST_STRING.substring(
        Math.toIntExact(range.begin()),
        Math.toIntExact(range.begin() + range.maxLength().getAsLong()));
  }

  private String getPartialSubstring(RangeSpec range) {
    return TEST_STRING.substring(
        Math.toIntExact(range.begin()),
        Math.toIntExact(range.begin() + (range.maxLength().getAsLong()) / 2));
  }

  @Override
  public void close() throws IOException {}
}
