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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AdaptiveFileAccessPatternTest {

  private static final String BUCKET_NAME = "bucket-name";
  private static final String OBJECT_NAME = "object-name";
  private static final StorageResourceId RESOURCE_ID =
      new StorageResourceId(BUCKET_NAME, OBJECT_NAME);

  @Test
  public void defaultAccessPatterns() {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.DEFAULT.toBuilder().setFadvise(Fadvise.SEQUENTIAL).build();

    AdaptiveFileAccessPattern fileAccessPattern =
        new AdaptiveFileAccessPattern(RESOURCE_ID, readOptions);

    assertThat(fileAccessPattern.isRandomAccessPattern()).isFalse();

    readOptions =
        GoogleCloudStorageReadOptions.DEFAULT.toBuilder().setFadvise(Fadvise.RANDOM).build();
    fileAccessPattern = new AdaptiveFileAccessPattern(RESOURCE_ID, readOptions);
    assertThat(fileAccessPattern.isRandomAccessPattern()).isTrue();

    readOptions =
        GoogleCloudStorageReadOptions.DEFAULT.toBuilder().setFadvise(Fadvise.AUTO).build();
    fileAccessPattern = new AdaptiveFileAccessPattern(RESOURCE_ID, readOptions);
    assertThat(fileAccessPattern.isRandomAccessPattern()).isFalse();

    readOptions =
        GoogleCloudStorageReadOptions.DEFAULT.toBuilder().setFadvise(Fadvise.AUTO_RANDOM).build();
    fileAccessPattern = new AdaptiveFileAccessPattern(RESOURCE_ID, readOptions);
    assertThat(fileAccessPattern.isRandomAccessPattern()).isTrue();
  }

  @Test
  public void testOverridenPattern() {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.DEFAULT.toBuilder().setFadvise(Fadvise.AUTO).build();
    long lastServedIndex = 10;
    long currentPosition = 0;
    // AUTO Adaptive access pattern type
    AdaptiveFileAccessPattern fileAccessPattern =
        new AdaptiveFileAccessPattern(RESOURCE_ID, readOptions);
    assertThat(fileAccessPattern.isRandomAccessPattern()).isFalse();
    // backward seek would result into adapting random pattern
    fileAccessPattern.updateLastServedIndex(lastServedIndex);
    fileAccessPattern.updateAccessPattern(currentPosition);
    assertThat(fileAccessPattern.isRandomAccessPattern()).isTrue();

    // overriding access pattern
    fileAccessPattern = new AdaptiveFileAccessPattern(RESOURCE_ID, readOptions);
    // override to use sequential pattern
    fileAccessPattern.overrideAccessPattern(false);
    // even with backward seek, pattern remains to be sequential
    fileAccessPattern.updateLastServedIndex(lastServedIndex);
    fileAccessPattern.updateAccessPattern(currentPosition);
    assertThat(fileAccessPattern.isRandomAccessPattern()).isFalse();

    // AUTO_RANDOM Adaptive access pattern type
    // just 2 request in sequential pattern will result in adaptation
    lastServedIndex = 10;
    currentPosition = 11;
    readOptions =
        GoogleCloudStorageReadOptions.DEFAULT
            .toBuilder()
            .setFadvise(Fadvise.AUTO_RANDOM)
            .setFadviseRequestTrackCount(1)
            .build();
    fileAccessPattern = new AdaptiveFileAccessPattern(RESOURCE_ID, readOptions);
    assertThat(fileAccessPattern.isRandomAccessPattern()).isTrue();
    // sequential read request will result in flipping to use sequential read pattern
    fileAccessPattern.updateLastServedIndex(lastServedIndex);
    fileAccessPattern.updateAccessPattern(currentPosition);
    assertThat(fileAccessPattern.isRandomAccessPattern()).isFalse();

    // overriding access pattern
    fileAccessPattern = new AdaptiveFileAccessPattern(RESOURCE_ID, readOptions);
    // override to use random pattern
    fileAccessPattern.overrideAccessPattern(true);
    assertThat(fileAccessPattern.isRandomAccessPattern()).isTrue();
    // even with sequential read request, patten remains to be random
    fileAccessPattern.updateLastServedIndex(lastServedIndex);
    fileAccessPattern.updateAccessPattern(currentPosition);
    assertThat(fileAccessPattern.isRandomAccessPattern()).isTrue();
  }

  @Test
  public void testAutoMode() {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.DEFAULT.toBuilder().setFadvise(Fadvise.AUTO).build();
    long lastServedIndex = 10;
    long currentPosition = 0;
    // AUTO Adaptive access pattern type
    AdaptiveFileAccessPattern fileAccessPattern =
        new AdaptiveFileAccessPattern(RESOURCE_ID, readOptions);
    assertThat(fileAccessPattern.isRandomAccessPattern()).isFalse();
    // backward seek would result into adapting random pattern
    fileAccessPattern.updateLastServedIndex(lastServedIndex);
    fileAccessPattern.updateAccessPattern(currentPosition);
    assertThat(fileAccessPattern.isRandomAccessPattern()).isTrue();
  }

  @Test
  public void testAutoRandomMode() {

    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.DEFAULT.toBuilder().setFadvise(Fadvise.AUTO_RANDOM).build();
    int readLength = 10;
    long lastServedIndex = 1;
    AdaptiveFileAccessPattern fileAccessPattern =
        new AdaptiveFileAccessPattern(RESOURCE_ID, readOptions);
    assertThat(fileAccessPattern.isRandomAccessPattern()).isTrue();
    fileAccessPattern.updateLastServedIndex(lastServedIndex);
    for (int i = 0; i < readOptions.getFadviseRequestTrackCount(); i++) {
      // sequential read
      long currentPosition = lastServedIndex + readOptions.getInplaceSeekLimit();
      lastServedIndex = currentPosition + readLength;
      fileAccessPattern.updateAccessPattern(currentPosition);
      if (i == readOptions.getFadviseRequestTrackCount() - 1) {
        assertThat(fileAccessPattern.isRandomAccessPattern()).isFalse();
      } else {
        assertThat(fileAccessPattern.isRandomAccessPattern()).isTrue();
      }
      fileAccessPattern.updateLastServedIndex(lastServedIndex);
    }
  }
}
