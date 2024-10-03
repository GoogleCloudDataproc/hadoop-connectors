package com.google.cloud.hadoop.gcsio;

import com.google.auth.Credentials;
import com.google.auto.value.AutoValue;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.common.collect.ImmutableMap;
import javax.annotation.Nullable;

@AutoValue
public abstract class StorageProviderCacheKey {

  public static StorageProviderCacheKey.Builder builder() {
    return new AutoValue_StorageProviderCacheKey.Builder();
  }

  /** Whether tracing is requested. */
  public abstract boolean getIsTracingEnabled();

  /** Credentials to use for the GCS client, otherwise null. */
  @Nullable
  public abstract Credentials getCredentials();

  /** Whether downscoped tokens are used for authenticating with the GCS backend. */
  public abstract boolean getIsDownScopingEnabled();

  @Nullable
  public abstract ImmutableMap<String, String> getHttpHeaders();

  @Nullable
  public abstract AsyncWriteChannelOptions getWriteChannelOptions();

  @Nullable
  public abstract String getProjectId();

  public abstract boolean getIsDirectPathPreferred();

  public abstract StorageProviderCacheKey.Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setIsTracingEnabled(boolean value);

    public abstract Builder setCredentials(Credentials value);

    public abstract Builder setIsDownScopingEnabled(boolean value);

    public abstract Builder setHttpHeaders(ImmutableMap<String, String> value);

    public abstract Builder setWriteChannelOptions(AsyncWriteChannelOptions value);

    public abstract Builder setProjectId(String value);

    public abstract Builder setIsDirectPathPreferred(boolean value);

    public abstract StorageProviderCacheKey build();
  }
}
