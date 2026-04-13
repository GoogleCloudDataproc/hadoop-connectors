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

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

public class FakeReadChannel implements ReadChannel {
  public static final int CHUNK_SIZE = 1024;

  public static enum REQUEST_TYPE {
    READ_CHUNK,
    READ_EXCEPTION,
    PARTIAL_READ,
    NEGATIVE_READ,
    ZERO_READ,
    MORE_THAN_CHANNEL_LENGTH,
    MORE_THAN_OBJECT_SIZE
  }

  private final List<REQUEST_TYPE> orderRequestsList;

  private final Iterator<REQUEST_TYPE> requestTypesIterator;
  private boolean open = true;
  private long position;
  private long limit = Long.MAX_VALUE;
  private ByteString content;

  private long currentPosition = 0;

  public FakeReadChannel(ByteString content) {
    this.content = content;
    this.orderRequestsList = null;
    this.requestTypesIterator = null;
  }

  public FakeReadChannel(ByteString content, List<REQUEST_TYPE> orderRequestsList) {
    this.content = content;
    this.orderRequestsList = orderRequestsList;
    this.requestTypesIterator = orderRequestsList.iterator();
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() {
    open = false;
  }

  @Override
  public void seek(long position) throws IOException {
    this.position = position;
    this.currentPosition = position;
  }

  public ReadChannel limit(long limit) {
    this.limit = limit;
    return this;
  }

  @Override
  public void setChunkSize(int i) {}

  @Override
  public RestorableState<ReadChannel> capture() {
    return null;
  }

  private int readInternal(ByteBuffer dst, long startIndex, long bytesToRead) {
    if (currentPosition >= limit) {
      return -1;
    }
    long readStart = startIndex;
    long readEnd =
        min(
            min(startIndex + bytesToRead, startIndex + dst.remaining()),
            min(content.size(), limit));
    ByteString messageData = content.substring(toIntExact(readStart), toIntExact(readEnd));
    for (Byte messageDatum : messageData) {
      dst.put(messageDatum);
    }
    return messageData.size();
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    REQUEST_TYPE requestType = REQUEST_TYPE.READ_CHUNK;
    if (requestTypesIterator != null && requestTypesIterator.hasNext()) {
      requestType = requestTypesIterator.next();
    }
    int bytesRead = 0;
    switch (requestType) {
      case ZERO_READ:
        bytesRead = 0;
        break;
      case NEGATIVE_READ:
        bytesRead = -1;
        break;
      case MORE_THAN_CHANNEL_LENGTH:
        bytesRead = toIntExact(limit + 1);
        break;
      case MORE_THAN_OBJECT_SIZE:
        bytesRead = content.size() + 1;
        break;
      case READ_EXCEPTION:
        throw new IOException("Exception occurred in read");
      case PARTIAL_READ:
        bytesRead = readInternal(dst, currentPosition, dst.remaining() / 2);
        currentPosition += bytesRead;
        throw new IOException("Partial Read Exception");
      default:
        bytesRead = readInternal(dst, currentPosition, CHUNK_SIZE);
        break;
    }

    if (bytesRead > 0) {
      currentPosition += bytesRead;
    }
    return bytesRead;
  }
}
