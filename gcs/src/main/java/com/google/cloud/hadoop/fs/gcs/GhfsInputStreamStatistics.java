/*
 * Copyright 2021 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

/**
 * Statistics updated by a {@link GoogleHadoopFSInputStreamBase} during its use. It also contains
 * getters for tests.
 */
interface GhfsInputStreamStatistics extends AutoCloseable, GhfsStatisticInterface {

  /**
   * Seek backwards, incrementing the seek and backward seek counters.
   *
   * @param negativeOffset how far was the seek? This is expected to be negative.
   */
  void seekBackwards(long negativeOffset);

  /**
   * Record a forward seek, adding a seek operation, a forward seek operation.
   *
   * @param skipped bytes moved forward in stream If the seek was implemented by a close + reopen,
   *     set this to zero.
   */
  void seekForwards(long skipped);

  /** An ignored stream read exception was received. */
  void readException();

  /**
   * Increment the bytes read counter by the number of bytes; no-op if the argument is negative.
   *
   * @param bytes number of bytes read
   */
  void bytesRead(long bytes);

  /**
   * A {@code read(byte[] buf, int off, int len)} operation has started.
   *
   * @param pos starting position of the read
   * @param len length of bytes to read
   */
  void readOperationStarted(long pos, long len);

  /**
   * A read operation has completed.
   *
   * @param requested number of requested bytes
   * @param actual the actual number of bytes
   */
  void readOperationCompleted(int requested, int actual);

  @Override
  void close();

  /** The bytes read in read() operations. */
  long getBytesRead();

  /**
   * The total number of bytes read, including all read and discarded when closing streams or
   * skipped during seek calls.
   */
  long getTotalBytesRead();

  /** The total number of bytes skipped during seek calls. */
  long getBytesSkippedOnSeek();

  /** The total number of bytes skipped during backward seek calls. */
  long getBytesBackwardsOnSeek();

  /** The total number of seek operations in an input stream */
  long getSeekOperations();

  /** The total number of exceptions raised during input stream reads. */
  long getReadExceptions();

  /** The total number of times the read() operation in an input stream has been called. */
  long getReadOperations();

  /** The total number of Incomplete read() operations */
  long getReadsIncomplete();

  /** The total number of times the input stream has been closed. */
  long getCloseOperations();

  /** The total number of executed seek operations which went forward in an input stream. */
  long getForwardSeekOperations();

  /** The total number of executed seek operations which went backward in an input stream. */
  long getBackwardSeekOperations();

  /**
   * Get the value of a counter.
   *
   * @param name counter name
   * @return the value or null if no matching counter was found.
   */
  Long lookupCounterValue(String name);

  /**
   * Get the value of a gauge.
   *
   * @param name gauge name
   * @return the value or null if no matching gauge was found.
   */
  Long lookupGaugeValue(String name);
}
