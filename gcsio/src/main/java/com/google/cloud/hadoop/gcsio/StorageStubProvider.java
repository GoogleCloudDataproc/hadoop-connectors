package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.storage.v2.StorageGrpc;
import com.google.storage.v2.StorageGrpc.StorageBlockingStub;
import com.google.storage.v2.StorageGrpc.StorageStub;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.alts.GoogleDefaultChannelBuilder;
import io.grpc.alts.GoogleDefaultChannelCredentials;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.stub.AbstractStub;
import java.util.concurrent.ExecutorService;

/** Provides gRPC stubs for accessing the Storage gRPC API. */
class StorageStubProvider {

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

  public StorageBlockingStub newBlockingStub() {
    StorageBlockingStub stub = StorageGrpc.newBlockingStub(getManagedChannel());
    return (StorageBlockingStub) grpcDecorator.applyCallOption(stub);
  }

  public StorageStub newAsyncStub() {
    StorageStub stub =
        StorageGrpc.newStub(getManagedChannel()).withExecutor(backgroundTasksThreadPool);
    return (StorageStub) grpcDecorator.applyCallOption(stub);
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

    public ManagedChannelBuilder<?> createChannelBuilder(String target) {
      return GoogleDefaultChannelBuilder.forTarget(target)
          .defaultServiceConfig(GRPC_SERVICE_CONFIG);
    }

    public AbstractStub<?> applyCallOption(AbstractStub<?> stub) {
      return stub;
    }
  }

  static class TrafficDirectorGrpcDecorator implements GrpcDecorator {
    TrafficDirectorGrpcDecorator() {}

    public ManagedChannelBuilder<?> createChannelBuilder(String target) {
      return Grpc.newChannelBuilder(
          // TODO(veblush): Remove experimental suffix once this code is proven stable.
          "google-c2p-experimental:///" + target, GoogleDefaultChannelCredentials.create());
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
    if (credentials instanceof ComputeEngineCredentials) {
      if (options.isTrafficDirectorEnabled()) {
        return new TrafficDirectorGrpcDecorator();
      }
      if (options.isDirectPathPreferred()) {
        return new DirectPathGrpcDecorator();
      }
    }
    return new CloudPathGrpcDecorator(credentials);
  }
}
