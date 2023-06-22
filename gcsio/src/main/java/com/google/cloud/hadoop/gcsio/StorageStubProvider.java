/*
 * Copyright 2022 Google LLC
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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auth.Credentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.storage.v2.StorageGrpc;
import com.google.storage.v2.StorageGrpc.StorageBlockingStub;
import com.google.storage.v2.StorageGrpc.StorageStub;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.alts.GoogleDefaultChannelCredentials;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import java.util.concurrent.ExecutorService;

/** Provides gRPC stubs for accessing the Storage gRPC API. */
class StorageStubProvider {
  static final Metadata.Key<String> GOOG_REQUEST_PARAMS =
      Metadata.Key.of("x-goog-request-params", Metadata.ASCII_STRING_MARSHALLER);

  private final GoogleCloudStorageOptions options;
  private final ExecutorService backgroundTasksThreadPool;
  private final GrpcDecorator grpcDecorator;
  private ManagedChannel channel;

  @VisibleForTesting
  GrpcDecorator getGrpcDecorator() {
    return grpcDecorator;
  }

  StorageStubProvider(
      GoogleCloudStorageOptions options,
      ExecutorService backgroundTasksThreadPool,
      GrpcDecorator grpcDecorator) {
    this.options = options;
    this.backgroundTasksThreadPool = backgroundTasksThreadPool;
    this.grpcDecorator = checkNotNull(grpcDecorator, "grpcDecorator cannot be null");
  }

  private ManagedChannel buildManagedChannel() {
    String target = options.getGrpcServerAddress();
    return grpcDecorator
        .createChannelBuilder(target)
        .enableRetry()
        .userAgent(options.getAppName())
        .build();
  }

  public final StorageBlockingStub newBlockingStub(String bucketName) {
    return newBlockingStubInternal()
        .withInterceptors(
            MetadataUtils.newAttachHeadersInterceptor(getRequestHeaderMetadata(bucketName)));
  }

  public final StorageStub newAsyncStub(String bucketName) {
    return newAsyncStubInternal()
        .withInterceptors(
            MetadataUtils.newAttachHeadersInterceptor(getRequestHeaderMetadata(bucketName)));
  }

  protected StorageBlockingStub newBlockingStubInternal() {
    StorageBlockingStub stub = StorageGrpc.newBlockingStub(getManagedChannel());
    return (StorageBlockingStub) grpcDecorator.applyCallOption(stub);
  }

  protected StorageStub newAsyncStubInternal() {
    StorageStub stub =
        StorageGrpc.newStub(getManagedChannel()).withExecutor(backgroundTasksThreadPool);
    return (StorageStub) grpcDecorator.applyCallOption(stub);
  }

  private static Metadata getRequestHeaderMetadata(String bucketName) {
    Metadata metadata = new Metadata();
    metadata.put(GOOG_REQUEST_PARAMS, String.format("bucket=projects/_/buckets/%s", bucketName));
    return metadata;
  }

  private synchronized ManagedChannel getManagedChannel() {
    if (channel == null) {
      channel = buildManagedChannel();
    }
    return channel;
  }

  public void shutdown() {
    if (channel != null) channel.shutdown();
  }

  interface GrpcDecorator {
    ManagedChannelBuilder<?> createChannelBuilder(String target);

    AbstractStub<?> applyCallOption(AbstractStub<?> stub);
  }

  static class CloudPathGrpcDecorator implements GrpcDecorator {
    private final Credentials credentials;

    CloudPathGrpcDecorator(Credentials credentials) {
      this.credentials = credentials;
    }

    public ManagedChannelBuilder<?> createChannelBuilder(String target) {
      return ManagedChannelBuilder.forTarget(target);
    }

    public AbstractStub<?> applyCallOption(AbstractStub<?> stub) {
      return stub.withCallCredentials(MoreCallCredentials.from(credentials));
    }
  }

  static class DirectPathGrpcDecorator implements GrpcDecorator {
    private static final ImmutableMap<String, Object> GRPC_SERVICE_CONFIG =
        ImmutableMap.of(
            "loadBalancingConfig",
            ImmutableList.of(
                ImmutableMap.of(
                    "grpclb",
                    ImmutableMap.of(
                        "childPolicy",
                        ImmutableList.of(ImmutableMap.of("round_robin", ImmutableMap.of()))))));

    private final Credentials credentials;

    DirectPathGrpcDecorator(Credentials credentials) {
      this.credentials = credentials;
    }

    public ManagedChannelBuilder<?> createChannelBuilder(String target) {
      GoogleDefaultChannelCredentials.Builder credentialsBuilder =
          GoogleDefaultChannelCredentials.newBuilder();
      if (credentials != null) {
        credentialsBuilder.callCredentials(MoreCallCredentials.from(credentials));
      }
      return Grpc.newChannelBuilder(target, credentialsBuilder.build())
          .defaultServiceConfig(GRPC_SERVICE_CONFIG);
    }

    public AbstractStub<?> applyCallOption(AbstractStub<?> stub) {
      return stub;
    }
  }

  static class TrafficDirectorGrpcDecorator implements GrpcDecorator {
    private final Credentials credentials;

    TrafficDirectorGrpcDecorator(Credentials credentials) {
      this.credentials = credentials;
    }

    public ManagedChannelBuilder<?> createChannelBuilder(String target) {
      ChannelCredentials credentialsBuilder =
          GoogleDefaultChannelCredentials.newBuilder()
              .callCredentials(MoreCallCredentials.from(credentials))
              .build();
      // TODO(veblush): Remove experimental suffix once this code is proven stable.
      return Grpc.newChannelBuilder("google-c2p-experimental:///" + target, credentialsBuilder);
    }

    public AbstractStub<?> applyCallOption(AbstractStub<?> stub) {
      return stub;
    }
  }

  public static StorageStubProvider newInstance(
      GoogleCloudStorageOptions options,
      ExecutorService backgroundTasksThreadPool,
      Credentials credentials) {
    return new StorageStubProvider(
        options, backgroundTasksThreadPool, getGrpcDecorator(options, credentials));
  }

  private static GrpcDecorator getGrpcDecorator(
      GoogleCloudStorageOptions options, Credentials credentials) {
    if (options.isTrafficDirectorEnabled()) {
      return new TrafficDirectorGrpcDecorator(credentials);
    } else if (options.isDirectPathPreferred()) {
      return new DirectPathGrpcDecorator(credentials);
    } else {
      return new CloudPathGrpcDecorator(credentials);
    }
  }
}
