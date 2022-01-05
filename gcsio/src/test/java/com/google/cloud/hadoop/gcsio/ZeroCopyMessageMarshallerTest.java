/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.storage.v2.ReadObjectRequest;
import io.grpc.StatusRuntimeException;
import io.grpc.internal.ReadableBuffer;
import io.grpc.internal.ReadableBuffers;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ZeroCopyMessageMarshallerTest {
  private final ReadObjectRequest REQUEST =
      ReadObjectRequest.newBuilder().setBucket("b").setObject("o").build();

  private ZeroCopyMessageMarshaller<ReadObjectRequest> createMarshaller() {
    return new ZeroCopyMessageMarshaller<>(ReadObjectRequest.getDefaultInstance());
  }

  private byte[] dropLastOneByte(byte[] bytes) {
    return Arrays.copyOfRange(bytes, 0, bytes.length - 1);
  }

  private InputStream createInputStream(byte[] bytes, boolean isZeroCopyable) {
    ReadableBuffer buffer =
        isZeroCopyable ? ReadableBuffers.wrap(ByteBuffer.wrap(bytes)) : ReadableBuffers.wrap(bytes);
    return ReadableBuffers.openStream(buffer, true);
  }

  @Test
  public void testParseOnFastPath() throws IOException {
    InputStream stream = createInputStream(REQUEST.toByteArray(), true);
    ZeroCopyMessageMarshaller<ReadObjectRequest> marshaller = createMarshaller();
    ReadObjectRequest request = marshaller.parse(stream);
    assertThat(request).isEqualTo(REQUEST);
    InputStream stream2 = marshaller.popStream(request);
    assertThat(stream2).isNotNull();
    stream2.close();
    InputStream stream3 = marshaller.popStream(request);
    assertThat(stream3).isNull();
  }

  @Test
  public void testParseOnSlowPath() {
    InputStream stream = createInputStream(REQUEST.toByteArray(), false);
    ZeroCopyMessageMarshaller<ReadObjectRequest> marshaller = createMarshaller();
    ReadObjectRequest request = marshaller.parse(stream);
    assertThat(request).isEqualTo(REQUEST);
    InputStream stream2 = marshaller.popStream(request);
    assertThat(stream2).isNull();
  }

  @Test
  public void testParseBrokenMessageOnFastPath() {
    InputStream stream = createInputStream(dropLastOneByte(REQUEST.toByteArray()), true);
    ZeroCopyMessageMarshaller<ReadObjectRequest> marshaller = createMarshaller();
    assertThrows(StatusRuntimeException.class, () -> marshaller.parse(stream));
  }

  @Test
  public void testParseBrokenMessageOnSlowPath() {
    InputStream stream = createInputStream(dropLastOneByte(REQUEST.toByteArray()), false);
    ZeroCopyMessageMarshaller<ReadObjectRequest> marshaller = createMarshaller();
    assertThrows(StatusRuntimeException.class, () -> marshaller.parse(stream));
  }
}
