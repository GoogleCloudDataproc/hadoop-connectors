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

import com.google.cloud.hadoop.util.RequesterPaysOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.time.Duration;
import java.util.function.Function;

@VisibleForTesting
public class RequesterPaysManager {
  private final RequesterPaysOptions requesterPaysOptions;
  private final Function<String, Boolean> shouldRequesterPay;
  private final LoadingCache<String, Boolean> autoBuckets =
      CacheBuilder.newBuilder()
          .expireAfterWrite(Duration.ofHours(1))
          .build(
              new CacheLoader<String, Boolean>() {
                @Override
                public Boolean load(String bucketName) {
                  return shouldRequesterPay.apply(bucketName);
                }
              });

  public RequesterPaysManager(
      RequesterPaysOptions requesterPaysOptions, Function<String, Boolean> shouldRequesterPay) {
    this.requesterPaysOptions = requesterPaysOptions;
    this.shouldRequesterPay = shouldRequesterPay;
  }

  public boolean requesterShouldPay(String bucketName) {
    if (bucketName == null) {
      return false;
    }
    switch (requesterPaysOptions.getMode()) {
      case ENABLED:
        return true;
      case CUSTOM:
        return requesterPaysOptions.getBuckets().contains(bucketName);
      case AUTO:
        return autoBuckets.getUnchecked(bucketName);
      default:
        return false;
    }
  }
}
