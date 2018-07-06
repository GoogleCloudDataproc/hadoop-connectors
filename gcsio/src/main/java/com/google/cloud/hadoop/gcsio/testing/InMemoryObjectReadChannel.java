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

package com.google.cloud.hadoop.gcsio.testing;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadChannel;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A SeekableByteChannel based on GoogleCloudStorageReadChannel that supports reading from in-memory
 * byte stream.
 */
public class InMemoryObjectReadChannel extends GoogleCloudStorageReadChannel {

  // All reads return data from this byte array. Set at construction time.
  private final byte[] channelContents;

  /** Creates a new instance of InMemoryObjectReadChannel. */
  public InMemoryObjectReadChannel(byte[] channelContents) throws IOException {
    this(channelContents, GoogleCloudStorageReadOptions.DEFAULT);
  }

  /**
   * Creates a new instance of InMemoryObjectReadChannel with {@code readOptions} plumbed into the
   * base class.
   */
  public InMemoryObjectReadChannel(
      byte[] channelContents, GoogleCloudStorageReadOptions readOptions) throws IOException {
    super(readOptions);
    this.channelContents = checkNotNull(channelContents, "channelContents could not be null");
    // fileEncoding and size should be initialized in constructor, the same as with super-class
    setFileEncoding(FileEncoding.OTHER);
    setSize(channelContents.length);
  }

  /** No-op, because file encoding and size are set in constructor. */
  @Override
  protected void initFileEncodingAndSize() {}

  /**
   * Opens the underlying byte array stream, sets its position to currentPosition and sets size to
   * size of the byte array.
   *
   * @param limit ignored.
   * @throws IOException on IO error
   */
  @Override
  protected InputStream openStream(long limit) throws IOException {
    InputStream inputStream = new ByteArrayInputStream(channelContents);
    inputStream.skip(currentPosition);
    return inputStream;
  }
}
