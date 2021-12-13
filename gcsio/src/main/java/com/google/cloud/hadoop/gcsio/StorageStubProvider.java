package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.compute.ComputeCredential;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.cloud.hadoop.util.CredentialAdapter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.storage.v2.StorageGrpc;
import com.google.storage.v2.StorageGrpc.StorageBlockingStub;
import com.google.storage.v2.StorageGrpc.StorageStub;
import com.google.storage.v2.StorageProto;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.alts.GoogleDefaultChannelBuilder;
import io.grpc.alts.GoogleDefaultChannelCredentials;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.stub.AbstractStub;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/** Provides gRPC stubs for accessing the Storage gRPC API. */
class StorageStubProvider {

  // The maximum number of times to automatically retry gRPC requests.
  private static final double GRPC_MAX_RETRY_ATTEMPTS = 10;

  private static final ImmutableSet<Status.Code> STUB_BROKEN_ERROR_CODES =
      ImmutableSet.of(Status.Code.DEADLINE_EXCEEDED, Status.Code.UNAVAILABLE);

  private final GoogleCloudStorageOptions options;
  private final String userAgent;
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
    this.userAgent = options.getAppName();
    this.backgroundTasksThreadPool = backgroundTasksThreadPool;
    this.grpcDecorator = checkNotNull(grpcDecorator, "grpcDecorator cannot be null");
  }

  private ManagedChannel buildManagedChannel() {
    String target = options.getGrpcServerAddress();
    ManagedChannel channel =
        grpcDecorator.createChannelBuilder(target).enableRetry().userAgent(userAgent).build();
    return channel;
  }

  public static boolean isStubBroken(Status.Code statusCode) {
    return STUB_BROKEN_ERROR_CODES.contains(statusCode);
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
    private final GoogleCloudStorageReadOptions readOptions;

    DirectPathGrpcDecorator(GoogleCloudStorageReadOptions readOptions) {
      this.readOptions = readOptions;
    }

    public ManagedChannelBuilder<?> createChannelBuilder(String target) {
      return GoogleDefaultChannelBuilder.forTarget(target)
          .defaultServiceConfig(getGrpcServiceConfig());
    }

    public AbstractStub<?> applyCallOption(AbstractStub<?> stub) {
      return stub;
    }

    private Map<String, Object> getGrpcServiceConfig() {
      Map<String, Object> childLbStrategy = ImmutableMap.of("round_robin", ImmutableMap.of());
      Map<String, Object> childPolicy =
          ImmutableMap.of("childPolicy", ImmutableList.of(childLbStrategy));
      Map<String, Object> grpcLbPolicy = ImmutableMap.of("grpclb", childPolicy);
      return ImmutableMap.of("loadBalancingConfig", ImmutableList.of(grpcLbPolicy));
    }
  }

  static class TrafficDirectorGrpcDecorator implements GrpcDecorator {
    TrafficDirectorGrpcDecorator() {}

    public ManagedChannelBuilder<?> createChannelBuilder(String target) {
      return Grpc.newChannelBuilder(
          "google-c2p:///" + target, GoogleDefaultChannelCredentials.create());
    }

    public AbstractStub<?> applyCallOption(AbstractStub<?> stub) {
      return stub;
    }
  }

  public static StorageStubProvider newInstance(
      GoogleCloudStorageOptions options,
      ExecutorService backgroundTasksThreadPool,
      Credential credential) {
    boolean isDefaultServiceAccount =
        credential != null
            && java.util.Objects.equals(
                credential.getTokenServerEncodedUrl(), ComputeCredential.TOKEN_SERVER_ENCODED_URL);
    GrpcDecorator decorator;
    if (options.isTrafficDirectorEnabled() && isDefaultServiceAccount) {
      decorator = new TrafficDirectorGrpcDecorator();
    } else if (options.isDirectPathPreferred() && isDefaultServiceAccount) {
      decorator = new DirectPathGrpcDecorator(options.getReadChannelOptions());
    } else {
      decorator = new CloudPathGrpcDecorator(new CredentialAdapter(credential));
    }
    return new StorageStubProvider(options, backgroundTasksThreadPool, decorator);
  }

  public static StorageStubProvider newInstance(
      GoogleCloudStorageOptions options,
      ExecutorService backgroundTasksThreadPool,
      Credentials credentials) {
    boolean isDefaultServiceAccount = credentials instanceof ComputeEngineCredentials;
    GrpcDecorator decorator;
    if (options.isTrafficDirectorEnabled() && isDefaultServiceAccount) {
      decorator = new TrafficDirectorGrpcDecorator();
    } else if (options.isDirectPathPreferred() && isDefaultServiceAccount) {
      decorator = new DirectPathGrpcDecorator(options.getReadChannelOptions());
    } else {
      decorator = new CloudPathGrpcDecorator(credentials);
    }
    return new StorageStubProvider(options, backgroundTasksThreadPool, decorator);
  }
}
