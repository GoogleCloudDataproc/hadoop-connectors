package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.ClientProto;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.compute.ComputeCredential;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.cloud.hadoop.util.CredentialAdapter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.util.Durations;
import com.google.storage.v2.StorageGrpc;
import com.google.storage.v2.StorageGrpc.StorageBlockingStub;
import com.google.storage.v2.StorageGrpc.StorageStub;
import com.google.storage.v2.StorageProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.alts.GoogleDefaultChannelBuilder;
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

  // The GCS gRPC server address.
  private static final String DEFAULT_GCS_GRPC_SERVER_ADDRESS =
      StorageProto.getDescriptor()
          .findServiceByName("Storage")
          .getOptions()
          .getExtension(ClientProto.defaultHost);

  private final GoogleCloudStorageReadOptions readOptions;
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
    this.readOptions = options.getReadChannelOptions();
    this.userAgent = options.getAppName();
    this.backgroundTasksThreadPool = backgroundTasksThreadPool;
    this.grpcDecorator = checkNotNull(grpcDecorator, "grpcDecorator cannot be null");
  }

  private ManagedChannel buildManagedChannel() {
    String target =
        isNullOrEmpty(readOptions.getGrpcServerAddress())
            ? DEFAULT_GCS_GRPC_SERVER_ADDRESS
            : readOptions.getGrpcServerAddress();
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
    channel.shutdownNow();
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
      Map<String, Object> name = ImmutableMap.of("service", "google.storage.v1.Storage");

      Map<String, Object> retryPolicy =
          ImmutableMap.<String, Object>builder()
              .put("maxAttempts", GRPC_MAX_RETRY_ATTEMPTS)
              .put(
                  "initialBackoff",
                  Durations.toString(
                      Durations.fromMillis(readOptions.getBackoffInitialIntervalMillis())))
              .put(
                  "maxBackoff",
                  Durations.toString(
                      Durations.fromMillis(readOptions.getBackoffMaxIntervalMillis())))
              .put("backoffMultiplier", readOptions.getBackoffMultiplier())
              .put("retryableStatusCodes", ImmutableList.of("UNAVAILABLE", "RESOURCE_EXHAUSTED"))
              .build();

      Map<String, Object> methodConfig =
          ImmutableMap.of("name", ImmutableList.of(name), "retryPolicy", retryPolicy);

      Map<String, Object> childLbStrategy = ImmutableMap.of("round_robin", ImmutableMap.of());

      Map<String, Object> childPolicy =
          ImmutableMap.of("childPolicy", ImmutableList.of(childLbStrategy));

      Map<String, Object> grpcLbPolicy = ImmutableMap.of("grpclb", childPolicy);

      return ImmutableMap.of(
          "methodConfig",
          ImmutableList.of(methodConfig),
          "loadBalancingConfig",
          ImmutableList.of(grpcLbPolicy));
    }
  }

  public static StorageStubProvider newInstance(
      GoogleCloudStorageOptions options,
      ExecutorService backgroundTasksThreadPool,
      Credential credential) {
    boolean useDirectpath =
        options.isDirectPathPreffered()
            && credential != null
            && java.util.Objects.equals(
                credential.getTokenServerEncodedUrl(), ComputeCredential.TOKEN_SERVER_ENCODED_URL);
    return new StorageStubProvider(
        options,
        backgroundTasksThreadPool,
        useDirectpath
            ? new DirectPathGrpcDecorator(options.getReadChannelOptions())
            : new CloudPathGrpcDecorator(new CredentialAdapter(credential)));
  }

  public static StorageStubProvider newInstance(
      GoogleCloudStorageOptions options,
      ExecutorService backgroundTasksThreadPool,
      Credentials credentials) {
    boolean useDirectpath =
        options.isDirectPathPreffered() && credentials instanceof ComputeEngineCredentials;
    return new StorageStubProvider(
        options,
        backgroundTasksThreadPool,
        useDirectpath
            ? new DirectPathGrpcDecorator(options.getReadChannelOptions())
            : new CloudPathGrpcDecorator(credentials));
  }
}
