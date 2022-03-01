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

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.impl.StorageStatisticsFromIOStatistics;

/** Storage statistics for GCS, dynamically generated from the IOStatistics. */
public class GhfsStorageStatistics extends StorageStatisticsFromIOStatistics {

  public static final String NAME = "GhfsStorageStatistics";

  /** Create the Storage Statistics instance from the IOStatistics */
  public GhfsStorageStatistics(IOStatistics ioStatistics) {
    super(NAME, "Ghfs", ioStatistics);
  }
}
