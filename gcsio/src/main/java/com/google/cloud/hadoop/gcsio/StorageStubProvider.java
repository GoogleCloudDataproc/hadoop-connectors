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
import com.google.google.storage.v1.StorageGrpc;
import com.google.google.storage.v1.StorageGrpc.StorageBlockingStub;
import com.google.google.storage.v1.StorageGrpc.StorageStub;
import com.google.google.storage.v1.StorageOuterClass;
import com.google.protobuf.util.Durations;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.alts.GoogleDefaultChannelBuilder;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.stub.AbstractStub;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/** Provides gRPC stubs for accessing the Storage gRPC API. */
class StorageStubProvider {

  // The maximum number of times to automatically retry gRPC requests.
  private static final double GRPC_MAX_RETRY_ATTEMPTS = 10;

  // The maximum number of media channels this provider will hand out. If more channels are
  // requested, the provider will reuse existing ones, favoring the one with the fewest ongoing
  // requests.
  private static final int MEDIA_CHANNEL_MAX_POOL_SIZE = 12;

  private static final ImmutableSet<Status.Code> STUB_BROKEN_ERROR_CODES =
      ImmutableSet.of(Status.Code.DEADLINE_EXCEEDED, Status.Code.UNAVAILABLE);

  // The GCS gRPC server address.
  private static final String DEFAULT_GCS_GRPC_SERVER_ADDRESS =
      StorageOuterClass.getDescriptor()
          .findServiceByName("Storage")
          .getOptions()
          .getExtension(ClientProto.defaultHost);

  private final GoogleCloudStorageReadOptions readOptions;
  private final String userAgent;
  private final ExecutorService backgroundTasksThreadPool;
  private final List<ChannelAndRequestCounter> mediaChannelPool;
  private final GrpcDecorator grpcDecorator;

  @VisibleForTesting
  GrpcDecorator getGrpcDecorator() {
    return grpcDecorator;
  }

  // An interceptor that can be added around a gRPC channel which keeps a count of the number
  // of requests that are active at any given moment.
  final class ActiveRequestCounter implements ClientInterceptor {

    // A count of the number of RPCs currently underway for one gRPC channel channel.
    private final AtomicInteger ongoingRequestCount;

    public ActiveRequestCounter() {
      ongoingRequestCount = new AtomicInteger(0);
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
      ClientCall<ReqT, RespT> newCall = channel.newCall(methodDescriptor, callOptions);
      AtomicBoolean countedCancel = new AtomicBoolean(false);

      // A streaming call might be terminated in one of several possible ways:
      // * The call completes normally -> onClose() will be invoked.
      // * The context is cancelled -> CancellationListener.cancelled() will be called.
      // * The call itself is cancelled (doesn't currently happen) -> ClientCall.cancel() called.
      //
      // It's possible more than one of these could happen, so we use countedCancel to make sure we
      // don't double count a decrement.
      Context.current()
          .addListener(
              context -> {
                if (countedCancel.compareAndSet(false, true)) {
                  ongoingRequestCount.decrementAndGet();
                }
              },
              backgroundTasksThreadPool);

      return new SimpleForwardingClientCall<ReqT, RespT>(newCall) {
        @Override
        public void cancel(@Nullable String message, @Nullable Throwable cause) {
          if (countedCancel.compareAndSet(false, true)) {
            ongoingRequestCount.decrementAndGet();
          }
          super.cancel(message, cause);
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          ongoingRequestCount.incrementAndGet();
          this.delegate()
              .start(
                  new SimpleForwardingClientCallListener<RespT>(responseListener) {
                    @Override
                    public void onClose(Status status, Metadata trailers) {
                      if (countedCancel.compareAndSet(false, true)) {
                        ongoingRequestCount.decrementAndGet();
                      }
                      super.onClose(status, trailers);
                    }
                  },
                  headers);
        }
      };
    }
  }

  class ChannelAndRequestCounter {
    private final ManagedChannel channel;
    private final ActiveRequestCounter counter;

    public ChannelAndRequestCounter(ManagedChannel channel, ActiveRequestCounter counter) {
      this.channel = channel;
      this.counter = counter;
    }

    public int activeRequests() {
      return counter.ongoingRequestCount.get();
    }
  }

  StorageStubProvider(
      GoogleCloudStorageOptions options,
      ExecutorService backgroundTasksThreadPool,
      GrpcDecorator grpcDecorator) {
    this.readOptions = options.getReadChannelOptions();
    this.userAgent = options.getAppName();
    this.backgroundTasksThreadPool = backgroundTasksThreadPool;
    this.mediaChannelPool = new ArrayList<>();
    this.grpcDecorator = checkNotNull(grpcDecorator, "grpcDecorator cannot be null");
  }

  private ChannelAndRequestCounter buildManagedChannel() {
    ActiveRequestCounter counter = new ActiveRequestCounter();
    String target =
        isNullOrEmpty(readOptions.getGrpcServerAddress())
            ? DEFAULT_GCS_GRPC_SERVER_ADDRESS
            : readOptions.getGrpcServerAddress();
    ManagedChannel channel = grpcDecorator.createChannelBuilder(target)
        .enableRetry()
        .intercept(counter)
        .userAgent(userAgent)
        .build();
    return new ChannelAndRequestCounter(channel, counter);
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
    if (mediaChannelPool.size() >= MEDIA_CHANNEL_MAX_POOL_SIZE) {
      return mediaChannelPool.stream()
          .min(Comparator.comparingInt(ChannelAndRequestCounter::activeRequests))
          .get()
          .channel;
    }

    ChannelAndRequestCounter channel = buildManagedChannel();
    mediaChannelPool.add(channel);
    return channel.channel;
  }

  public void shutdown() {
    mediaChannelPool.parallelStream().forEach(c -> c.channel.shutdownNow());
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
      return GoogleDefaultChannelBuilder.forTarget(target).defaultServiceConfig(getGrpcServiceConfig());
    }

    public AbstractStub<?> applyCallOption(AbstractStub<?> stub) {
      return stub;
    }

    private Map<String, Object> getGrpcServiceConfig() {
      Map<String, Object> name = ImmutableMap.of("service", "google.storage.v1.Storage");

      Map<String, Object> retryPolicy = ImmutableMap.<String, Object>builder()
          .put("maxAttempts", GRPC_MAX_RETRY_ATTEMPTS)
          .put("initialBackoff",
              Durations.toString(Durations.fromMillis(readOptions.getBackoffInitialIntervalMillis())))
          .put("maxBackoff", Durations.toString(Durations.fromMillis(readOptions.getBackoffMaxIntervalMillis())))
          .put("backoffMultiplier", readOptions.getBackoffMultiplier())
          .put("retryableStatusCodes", ImmutableList.of("UNAVAILABLE", "RESOURCE_EXHAUSTED")).build();

      Map<String, Object> methodConfig = ImmutableMap.of("name", ImmutableList.of(name), "retryPolicy", retryPolicy);

      // When channel pooling is enabled, force the pick_first grpclb strategy.
      // This is necessary to avoid the multiplicative effect of creating channel pool
      // with
      // `poolSize` number of `ManagedChannel`s, each with a `subSetting` number of
      // number of
      // subchannels.
      // See the service config proto definition for more details:
      // https://github.com/grpc/grpc-proto/blob/master/grpc/service_config/service_config.proto#L182
      Map<String, Object> pickFirstStrategy = ImmutableMap.of("pick_first", ImmutableMap.of());

      Map<String, Object> childPolicy = ImmutableMap.of("childPolicy", ImmutableList.of(pickFirstStrategy));

      Map<String, Object> grpcLbPolicy = ImmutableMap.of("grpclb", childPolicy);

      return ImmutableMap.of("methodConfig", ImmutableList.of(methodConfig), "loadBalancingConfig",
          ImmutableList.of(grpcLbPolicy));
    }
  }

  public static StorageStubProvider newInstance(GoogleCloudStorageOptions options, ExecutorService backgroundTasksThreadPool,
      Credential credential) {
    boolean useDirectpath = options.isDirectPathPreffered() && credential != null
        && java.util.Objects.equals(credential.getTokenServerEncodedUrl(), ComputeCredential.TOKEN_SERVER_ENCODED_URL);
    return new StorageStubProvider(options, backgroundTasksThreadPool,
        useDirectpath ? new DirectPathGrpcDecorator(options.getReadChannelOptions())
            : new CloudPathGrpcDecorator(new CredentialAdapter(credential)));
  }

  public static StorageStubProvider newInstance(GoogleCloudStorageOptions options, ExecutorService backgroundTasksThreadPool,
      Credentials credentials) {
    boolean useDirectpath = options.isDirectPathPreffered() && credentials instanceof ComputeEngineCredentials;
    return new StorageStubProvider(options, backgroundTasksThreadPool,
        useDirectpath ? new DirectPathGrpcDecorator(options.getReadChannelOptions())
            : new CloudPathGrpcDecorator(credentials));
  }
}
