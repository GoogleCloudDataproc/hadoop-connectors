package com.google.cloud.hadoop.gcsio;

import com.google.auto.value.AutoValue;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Optional;

@AutoValue
public abstract class StorageClientProviderCacheKey {

  public static StorageClientProviderCacheKey.Builder builder() {
    return new AutoValue_StorageClientProviderCacheKey.Builder();
  }

  /** Whether tracing is requested. */
  public abstract boolean getIsTracingEnabled();

  public abstract Optional<ImmutableList<Object>> getCredentialsKey();

  /** Whether downscoped tokens are used for authenticating with the GCS backend. */
  public abstract boolean getIsDownScopingEnabled();

  public abstract Optional<ImmutableMap<String, String>> getHttpHeaders();

  public abstract Optional<AsyncWriteChannelOptions> getWriteChannelOptions();

  public abstract Optional<String> getProjectId();

  public abstract boolean getIsDirectPathPreferred();

  public abstract StorageClientProviderCacheKey.Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setIsTracingEnabled(boolean value);

    public abstract Builder setCredentialsKey(ImmutableList<Object> value);

    public abstract Builder setIsDownScopingEnabled(boolean value);

    public abstract Builder setHttpHeaders(ImmutableMap<String, String> value);

    public abstract Builder setWriteChannelOptions(AsyncWriteChannelOptions value);

    public abstract Builder setProjectId(String value);

    public abstract Builder setIsDirectPathPreferred(boolean value);

    public abstract StorageClientProviderCacheKey build();
  }
}
