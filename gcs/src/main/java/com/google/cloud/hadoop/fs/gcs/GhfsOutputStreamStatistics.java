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

import java.io.Closeable;

/**
 * Statistics updated by a {@link GoogleHadoopOutputStream} during its use. It also contains getters
 * for tests.
 */
interface GhfsOutputStreamStatistics extends Closeable, GhfsStatisticInterface {
  /**
   * Record bytes written.
   *
   * @param count number of bytes
   */
  void writeBytes(long count);

  /**
   * Get the current count of bytes written.
   *
   * @return the counter value.
   */
  long getBytesWritten();

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

  /** An ignored stream write exception was received. */
  void writeException();

  /** The total number of stream write exception. */
  long getWriteExceptions();

  /** Syncable.hflush() has been invoked. */
  void hflushInvoked();

  /** Syncable.hsync() has been invoked. */
  void hsyncInvoked();
}
