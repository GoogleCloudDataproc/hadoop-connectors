/*
 * Copyright 2014 Google Inc.
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.services.storage.Storage;
import com.google.auto.value.AutoValue;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.RedactedString;
import com.google.cloud.hadoop.util.RequesterPaysOptions;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.Map;
import javax.annotation.Nullable;

/** Configuration options for the GoogleCloudStorage class. */
@AutoValue
public abstract class GoogleCloudStorageOptions {
  public enum MetricsSink {
    NONE,
    CLOUD_MONITORING,
  }

  public static final GoogleCloudStorageOptions DEFAULT = builder().build();

  public static Builder builder() {
    return new AutoValue_GoogleCloudStorageOptions.Builder()
        .setAutoRepairImplicitDirectoriesEnabled(true)
        .setBatchThreads(15)
        .setCopyWithRewriteEnabled(true)
        .setDirectPathPreferred(true)
        .setGrpcEnabled(false)
        .setGrpcMessageTimeoutCheckInterval(Duration.ofSeconds(1))
        .setHttpRequestConnectTimeout(Duration.ofSeconds(5))
        .setHttpRequestHeaders(ImmutableMap.of())
        .setHttpRequestReadTimeout(Duration.ofSeconds(5))
        .setMaxHttpRequestRetries(10)
        .setMaxListItemsPerCall(5_000)
        .setMaxRequestsPerBatch(15)
        .setMaxRewriteChunkSize(512 * 1024 * 1024L)
        .setMaxWaitTimeForEmptyObjectCreation(Duration.ofSeconds(3))
        .setMetricsSink(MetricsSink.NONE)
        .setReadChannelOptions(GoogleCloudStorageReadOptions.DEFAULT)
        .setRequesterPaysOptions(RequesterPaysOptions.DEFAULT)
        .setStorageRootUrl(Storage.DEFAULT_ROOT_URL)
        .setStorageServicePath(Storage.DEFAULT_SERVICE_PATH)
        .setTraceLogEnabled(false)
        .setOperationTraceLogEnabled(false)
        .setTrafficDirectorEnabled(true)
        .setWriteChannelOptions(AsyncWriteChannelOptions.DEFAULT)
        .setHnBucketRenameEnabled(false)
        .setGrpcWriteEnabled(false)
        .setMoveOperationEnabled(true)
        .setStorageClientCachingEnabled(false)
        .setBidiEnabled(false)
        .setFinalizeBeforeClose(false)
        .setHnOptimizationEnabled(false);
  }

  public abstract Builder toBuilder();

  public abstract boolean isStorageClientCachingEnabled();

  public abstract boolean isGrpcEnabled();

  public abstract boolean isHnBucketRenameEnabled();

  public abstract boolean isTrafficDirectorEnabled();

  public abstract boolean isDirectPathPreferred();

  public abstract String getStorageRootUrl();

  public abstract String getStorageServicePath();

  public abstract boolean isGrpcWriteEnabled();

  @Nullable
  public abstract String getProjectId();

  @Nullable
  public abstract String getAppName();

  public abstract boolean isAutoRepairImplicitDirectoriesEnabled();

  public abstract Duration getMaxWaitTimeForEmptyObjectCreation();

  public abstract int getMaxListItemsPerCall();

  public abstract int getMaxRequestsPerBatch();

  public abstract int getBatchThreads();

  public abstract int getMaxHttpRequestRetries();

  public abstract Duration getHttpRequestConnectTimeout();

  public abstract Duration getHttpRequestReadTimeout();

  @Nullable
  public abstract String getProxyAddress();

  @Nullable
  public abstract RedactedString getProxyUsername();

  @Nullable
  public abstract RedactedString getProxyPassword();

  public abstract boolean isCopyWithRewriteEnabled();

  public abstract long getMaxRewriteChunkSize();

  public abstract GoogleCloudStorageReadOptions getReadChannelOptions();

  public abstract AsyncWriteChannelOptions getWriteChannelOptions();

  public abstract RequesterPaysOptions getRequesterPaysOptions();

  public abstract ImmutableMap<String, String> getHttpRequestHeaders();

  @Nullable
  public abstract String getEncryptionAlgorithm();

  @Nullable
  public abstract RedactedString getEncryptionKey();

  @Nullable
  public abstract RedactedString getEncryptionKeyHash();

  public abstract Duration getGrpcMessageTimeoutCheckInterval();

  public abstract MetricsSink getMetricsSink();

  public abstract boolean isTraceLogEnabled();

  public abstract boolean isOperationTraceLogEnabled();

  public abstract boolean isMoveOperationEnabled();

  public abstract boolean isBidiEnabled();

  public abstract boolean isFinalizeBeforeClose();

  public abstract boolean isHnOptimizationEnabled();

  public RetryHttpInitializerOptions toRetryHttpInitializerOptions() {
    return RetryHttpInitializerOptions.builder()
        .setDefaultUserAgent(getAppName())
        .setHttpHeaders(getHttpRequestHeaders())
        .setMaxRequestRetries(getMaxHttpRequestRetries())
        .setConnectTimeout(getHttpRequestConnectTimeout())
        .setReadTimeout(getHttpRequestReadTimeout())
        .build();
  }

  public void throwIfNotValid() {
    checkArgument(!isNullOrEmpty(getAppName()), "appName must not be null or empty");
  }

  /** Mutable builder for the {@link GoogleCloudStorageOptions} class. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setGrpcEnabled(boolean grpcEnabled);

    public abstract Builder setTrafficDirectorEnabled(boolean trafficDirectorEnabled);

    public abstract Builder setDirectPathPreferred(boolean directPathPreferred);

    public abstract Builder setStorageRootUrl(String rootUrl);

    public abstract Builder setStorageServicePath(String servicePath);

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setAppName(String appName);

    public abstract Builder setAutoRepairImplicitDirectoriesEnabled(boolean autoRepair);

    public abstract Builder setMaxWaitTimeForEmptyObjectCreation(Duration maxWaitTime);

    public abstract Builder setMaxListItemsPerCall(int maxListItemsPerCall);

    // According to https://developers.google.com/storage/docs/json_api/v1/how-tos/batch
    // there is a maximum of 1000 requests per batch.
    public abstract Builder setMaxRequestsPerBatch(int maxRequestsPerBatch);

    public abstract Builder setBatchThreads(int batchThreads);

    public abstract Builder setMaxHttpRequestRetries(int maxHttpRequestRetries);

    public abstract Builder setHttpRequestConnectTimeout(Duration httpRequestConnectTimeout);

    public abstract Builder setHttpRequestReadTimeout(Duration httpRequestReadTimeout);

    public abstract Builder setProxyAddress(String proxyAddress);

    public abstract Builder setProxyUsername(RedactedString proxyUsername);

    public abstract Builder setProxyPassword(RedactedString proxyPassword);

    public abstract Builder setCopyWithRewriteEnabled(boolean copyWithRewrite);

    public abstract Builder setMaxRewriteChunkSize(long bytes);

    public abstract Builder setReadChannelOptions(GoogleCloudStorageReadOptions readChannelOptions);

    public abstract Builder setWriteChannelOptions(AsyncWriteChannelOptions writeChannelOptions);

    public abstract Builder setRequesterPaysOptions(RequesterPaysOptions requesterPaysOptions);

    public abstract Builder setHttpRequestHeaders(Map<String, String> httpRequestHeaders);

    public abstract Builder setEncryptionAlgorithm(String encryptionAlgorithm);

    public abstract Builder setEncryptionKey(RedactedString encryptionKey);

    public abstract Builder setEncryptionKeyHash(RedactedString encryptionKeyHash);

    public abstract Builder setGrpcMessageTimeoutCheckInterval(
        Duration grpcMessageTimeoutInMillisCheckInterval);

    public abstract Builder setMetricsSink(MetricsSink metricsSink);

    public abstract Builder setTraceLogEnabled(Boolean enable);

    public abstract Builder setOperationTraceLogEnabled(Boolean enable);

    public abstract Builder setHnBucketRenameEnabled(boolean enabled);

    public abstract Builder setGrpcWriteEnabled(boolean grpcWriteEnabled);

    public abstract Builder setMoveOperationEnabled(boolean moveOperationEnabled);

    public abstract Builder setStorageClientCachingEnabled(boolean isCachingEnabled);

    public abstract Builder setHnOptimizationEnabled(boolean hnOptimizationEnabled);

    /** Sets the property to use the bidirectional Rapid Storage Api. */
    public abstract Builder setBidiEnabled(boolean bidiEnabled);

    public abstract Builder setFinalizeBeforeClose(boolean finalizeBeforeClose);

    abstract GoogleCloudStorageOptions autoBuild();

    public GoogleCloudStorageOptions build() {
      GoogleCloudStorageOptions instance = autoBuild();
      checkArgument(
          instance.getMaxRewriteChunkSize() <= 0
              || instance.getMaxRewriteChunkSize() % (1024 * 1024) == 0,
          "maxRewriteChunkSize must be an integral multiple of 1 MiB (1048576), but was: %s",
          instance.getMaxRewriteChunkSize());
      checkArgument(
          instance.getProxyAddress() != null
              || (instance.getProxyUsername() == null && instance.getProxyPassword() == null),
          "if proxyAddress is null then proxyUsername and proxyPassword should be null too");
      checkArgument(
          (instance.getProxyUsername() == null) == (instance.getProxyPassword() == null),
          "both proxyUsername and proxyPassword should be null or not null together");
      checkArgument(
          isAllEncryptionOptionsSetOrUnset(instance),
          "encryptionAlgorithm, encryptionKey and encryptionKeyHash should be null or not null"
              + " together");
      return instance;
    }

    private boolean isAllEncryptionOptionsSetOrUnset(GoogleCloudStorageOptions instance) {
      return (instance.getEncryptionAlgorithm() != null
              && instance.getEncryptionKey() != null
              && instance.getEncryptionKeyHash() != null)
          || (instance.getEncryptionAlgorithm() == null
              && instance.getEncryptionKey() == null
              && instance.getEncryptionKeyHash() == null);
    }
  }
}
