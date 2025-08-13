package com.google.cloud.hadoop.gcsio;

import com.google.auto.value.AutoValue;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import javax.annotation.Nullable;

@AutoValue
public abstract class StorageClientProviderCacheKey {

  public static StorageClientProviderCacheKey.Builder builder() {
    return new AutoValue_StorageClientProviderCacheKey.Builder();
  }

  /** Whether tracing is requested. */
  public abstract boolean getIsTracingEnabled();

  @Nullable
  public abstract List<Object> getCredentialsKey();

  @Nullable
  public abstract ImmutableMap<String, String> getHttpHeaders();

  @Nullable
  public abstract AsyncWriteChannelOptions getWriteChannelOptions();

  @Nullable
  public abstract String getProjectId();

  public abstract boolean getIsDirectPathPreferred();

  public abstract StorageClientProviderCacheKey.Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setIsTracingEnabled(boolean value);

    public abstract Builder setCredentialsKey(List<Object> value);

    public abstract Builder setHttpHeaders(ImmutableMap<String, String> value);

    public abstract Builder setWriteChannelOptions(AsyncWriteChannelOptions value);

    public abstract Builder setProjectId(String value);

    public abstract Builder setIsDirectPathPreferred(boolean value);

    public abstract StorageClientProviderCacheKey build();
  }
}
