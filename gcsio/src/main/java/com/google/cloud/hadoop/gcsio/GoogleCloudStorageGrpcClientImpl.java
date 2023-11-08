package com.google.cloud.hadoop.gcsio;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.auth.Credentials;
import com.google.auto.value.AutoBuilder;
import com.google.cloud.NoCredentials;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.GcsClientStatisticInterface;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.grpc.ClientInterceptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Provides read/write access to Google Cloud Storage (GCS), using Java nio channel semantics. Uses
 * java-storage client to interact with GCS APIs via the gRPC transport.
 */
public class GoogleCloudStorageGrpcClientImpl extends GoogleCloudStorageClientImpl {

  GoogleCloudStorageGrpcClientImpl(
      GoogleCloudStorageOptions options,
      @Nullable Storage clientLibraryStorage,
      @Nullable Credentials credentials,
      @Nullable HttpTransport httpTransport,
      @Nullable HttpRequestInitializer httpRequestInitializer,
      @Nullable Function<List<AccessBoundary>, String> downscopedAccessTokenFn,
      @Nullable ExecutorService pCUExecutorService,
      @Nullable ImmutableList<ClientInterceptor> gRPCInterceptors,
      @Nullable GcsClientStatisticInterface gcsClientStatisticInterface)
      throws IOException {
    super(
        options,
        clientLibraryStorage,
        credentials,
        httpTransport,
        httpRequestInitializer,
        gRPCInterceptors,
        downscopedAccessTokenFn,
        pCUExecutorService,
        gcsClientStatisticInterface);
  }

  @Override
  Storage createStorage(
      Credentials credentials,
      GoogleCloudStorageOptions storageOptions,
      List<ClientInterceptor> interceptors,
      ExecutorService pCUExecutorService)
      throws IOException {
    return StorageOptions.grpc()
        .setAttemptDirectPath(storageOptions.isDirectPathPreferred())
        .setHeaderProvider(storageOptions::getHttpRequestHeaders)
        .setGrpcInterceptorProvider(
            () -> {
              List<ClientInterceptor> list = new ArrayList<>();
              if (interceptors != null && !interceptors.isEmpty()) {
                list.addAll(
                    interceptors.stream().filter(Objects::nonNull).collect(Collectors.toList()));
              }
              if (storageOptions.isTraceLogEnabled()) {
                list.add(new GoogleCloudStorageClientGrpcTracingInterceptor());
              }
              return ImmutableList.copyOf(list);
            })
        .setCredentials(credentials != null ? credentials : NoCredentials.getInstance())
        .setBlobWriteSessionConfig(
            getSessionConfig(storageOptions.getWriteChannelOptions(), pCUExecutorService))
        .build()
        .getService();
  }

  public static Builder builder() {
    return new AutoBuilder_GoogleCloudStorageGrpcClientImpl_Builder();
  }

  @AutoBuilder(ofClass = GoogleCloudStorageGrpcClientImpl.class)
  public abstract static class Builder {

    public abstract Builder setOptions(GoogleCloudStorageOptions options);

    public abstract Builder setHttpTransport(@Nullable HttpTransport httpTransport);

    public abstract Builder setCredentials(@Nullable Credentials credentials);

    @VisibleForTesting
    public abstract Builder setHttpRequestInitializer(
        @Nullable HttpRequestInitializer httpRequestInitializer);

    public abstract Builder setDownscopedAccessTokenFn(
        @Nullable Function<List<AccessBoundary>, String> downscopedAccessTokenFn);

    public abstract Builder setGRPCInterceptors(
        @Nullable ImmutableList<ClientInterceptor> gRPCInterceptors);

    public abstract Builder setGcsClientStatisticInterface(
        @Nullable GcsClientStatisticInterface gcsClientStatisticInterface);

    @VisibleForTesting
    public abstract Builder setClientLibraryStorage(@Nullable Storage clientLibraryStorage);

    @VisibleForTesting
    public abstract Builder setPCUExecutorService(@Nullable ExecutorService pCUExecutorService);

    public abstract GoogleCloudStorageGrpcClientImpl build() throws IOException;
  }
}
