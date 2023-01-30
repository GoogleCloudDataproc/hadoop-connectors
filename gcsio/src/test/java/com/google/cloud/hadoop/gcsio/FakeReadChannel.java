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

import static java.lang.Math.toIntExact;

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;

public class FakeReadChannel implements ReadChannel {

  public static final int CHUNK_SIZE = 1024;
  private boolean open = true;

  private final boolean throwExceptionOnRead;

  private long position;
  private long limit = Long.MAX_VALUE;

  private boolean readStarted = false;

  private ByteString content;

  private long currentPosition = 0;

  public FakeReadChannel(ByteString content) {
    this.content = content;
    this.throwExceptionOnRead = false;
  }

  public FakeReadChannel(ByteString content, boolean throwExceptionOnRead) {
    this.content = content;
    this.throwExceptionOnRead = throwExceptionOnRead;
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

  @Override
  public int read(ByteBuffer dst) throws IOException {
    readStarted = true;
    if (throwExceptionOnRead) {
      throw new IOException("Intentionally triggered");
    }
    if (currentPosition == limit) {
      return -1;
    }
    long readStart = currentPosition;
    long readEnd =
        Math.min(
            Math.min(currentPosition + CHUNK_SIZE, currentPosition + dst.remaining()),
            Math.min(content.size(), limit));

    ByteString messageData = content.substring(toIntExact(readStart), toIntExact(readEnd));
    for (Byte messageDatum : messageData) {
      dst.put(messageDatum);
    }
    currentPosition += messageData.size();
    return messageData.size();
  }
}
