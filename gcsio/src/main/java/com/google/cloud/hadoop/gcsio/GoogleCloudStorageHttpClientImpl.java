package com.google.cloud.hadoop.gcsio;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.auth.Credentials;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.GcsClientStatisticInterface;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import io.grpc.ClientInterceptor;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import javax.annotation.Nullable;
import jdk.jshell.spi.ExecutionControl.NotImplementedException;

/**
 * Provides read/write access to Google Cloud Storage (GCS), using Java nio channel semantics. Uses
 * java-storage client to interact with GCS APIs via the HTTP transport.
 */
// TODO : Implement GoogleCloudStorageHttpClientImpl.
public class GoogleCloudStorageHttpClientImpl extends GoogleCloudStorageClientImpl {
  GoogleCloudStorageHttpClientImpl(
      GoogleCloudStorageOptions options,
      @Nullable Storage clientLibraryStorage,
      @Nullable Credentials credentials,
      @Nullable HttpTransport httpTransport,
      @Nullable HttpRequestInitializer httpRequestInitializer,
      @Nullable ImmutableList<ClientInterceptor> gRPCInterceptors,
      @Nullable Function<List<AccessBoundary>, String> downscopedAccessTokenFn,
      @Nullable ExecutorService pCUExecutorService,
      @Nullable GcsClientStatisticInterface gcsClientStatisticInterface)
      throws IOException, NotImplementedException {
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
    throw new NotImplementedException("Storage Client via HTTP transport is not implemented. ");
  }

  @Override
  Storage createStorage(
      Credentials credentials,
      GoogleCloudStorageOptions storageOptions,
      List<ClientInterceptor> interceptors,
      ExecutorService pCUExecutorService)
      throws IOException {
    return null;
  }
}
