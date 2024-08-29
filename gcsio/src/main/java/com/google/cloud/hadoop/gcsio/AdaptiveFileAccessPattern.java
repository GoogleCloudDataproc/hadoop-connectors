/*
 * Copyright 2024 Google LLC
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

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.common.flogger.GoogleLogger;
import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.ListIterator;

class AdaptiveFileAccessPattern implements Closeable {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private final StorageResourceId resourceId;
  private final GoogleCloudStorageReadOptions readOptions;
  private boolean isPatternOverriden = false;
  private boolean randomAccess;
  private long lastServedIndex = -1;
  // Keeps track of distance between consecutive requests
  private BoundedList<Long> consecutiveRequestsDistances;

  @Override
  public void close() throws IOException {
    if (consecutiveRequestsDistances != null) {
      consecutiveRequestsDistances = null;
    }
  }

  class BoundedList<E> extends LinkedList<E> {
    private int limit;

    public BoundedList(int limit) {
      this.limit = limit;
    }

    @Override
    public boolean add(E o) {
      super.add(o);
      while (size() > limit) {
        super.removeFirst();
      }
      return true;
    }
  }

  public AdaptiveFileAccessPattern(
      StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions) {
    this.resourceId = resourceId;
    this.readOptions = readOptions;
    this.randomAccess =
        readOptions.getFadvise() == Fadvise.AUTO_RANDOM
            || readOptions.getFadvise() == Fadvise.RANDOM;
    if (readOptions.getFadvise() == Fadvise.AUTO_RANDOM) {
      consecutiveRequestsDistances = new BoundedList<>(readOptions.getFadviseRequestTrackCount());
    }
  }

  public void updateLastServedIndex(long position) {
    this.lastServedIndex = position;
  }

  public boolean isRandomAccessPattern() {
    return randomAccess;
  }

  public void updateAccessPattern(long currentPosition) {
    if (isPatternOverriden) {
      logger.atFiner().log(
          "Will bypass computing access pattern as it's overriden for resource :%s", resourceId);
      return;
    }
    if (readOptions.getFadvise() == Fadvise.AUTO_RANDOM) {
      if (isSequentialAccessPattern(currentPosition)) {
        unsetRandomAccess();
      }
    } else if (readOptions.getFadvise() == Fadvise.AUTO) {
      if (isRandomAccessPattern(currentPosition)) {
        setRandomAccess();
      }
    }
  }

  /**
   * This provides a way to override the access pattern, once overridden it will not be recomputed
   * for adaptive fadvise types.
   *
   * @param pattern, true, to override with random access else false
   */
  public void overrideAccessPattern(boolean pattern) {
    this.isPatternOverriden = true;
    this.randomAccess = pattern;
    logger.atInfo().log(
        "Overriding the random access pattern to %s for fadvise:%s for resource: %s ",
        pattern, readOptions.getFadvise(), resourceId);
  }

  private boolean isSequentialAccessPattern(long currentPosition) {
    if (lastServedIndex != -1 && consecutiveRequestsDistances != null) {
      consecutiveRequestsDistances.add(currentPosition - lastServedIndex);
    }

    if (!shouldDetectSequentialAccess()) {
      return false;
    }

    if (consecutiveRequestsDistances.size() < readOptions.getFadviseRequestTrackCount()) {
      return false;
    }

    ListIterator<Long> iterator = consecutiveRequestsDistances.listIterator();
    while (iterator.hasNext()) {
      Long distance = iterator.next();
      if (distance < 0 || distance > readOptions.DEFAULT_INPLACE_SEEK_LIMIT) {
        return false;
      }
    }
    logger.atInfo().log(
        "Detected %d consecutive read request within distance threshold %d with fadvise: %s switching to sequential IO for '%s'",
        consecutiveRequestsDistances.size(),
        readOptions.getInplaceSeekLimit(),
        readOptions.getFadvise(),
        resourceId);
    return true;
  }

  private boolean isRandomAccessPattern(long currentPosition) {
    if (!shouldDetectRandomAccess()) {
      return false;
    }
    if (lastServedIndex == -1) {
      return false;
    }

    if (currentPosition < lastServedIndex) {
      logger.atFine().log(
          "Detected backward read from %s to %s position, switching to random IO for '%s'",
          lastServedIndex, currentPosition, resourceId);
      return true;
    }
    if (lastServedIndex >= 0
        && lastServedIndex + readOptions.getInplaceSeekLimit() < currentPosition) {
      logger.atFine().log(
          "Detected forward read from %s to %s position over %s threshold,"
              + " switching to random IO for '%s'",
          lastServedIndex, currentPosition, readOptions.getInplaceSeekLimit(), resourceId);
      return true;
    }
    return false;
  }

  private boolean shouldDetectSequentialAccess() {
    return randomAccess && readOptions.getFadvise() == Fadvise.AUTO_RANDOM;
  }

  private boolean shouldDetectRandomAccess() {
    return !randomAccess && readOptions.getFadvise() == Fadvise.AUTO;
  }

  private void setRandomAccess() {
    randomAccess = true;
  }

  private void unsetRandomAccess() {
    randomAccess = false;
  }
}
