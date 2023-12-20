package com.google.cloud.hadoop.gcsio.testing;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.GrpcStorageOptions;
import com.google.cloud.storage.StorageOptions;
import com.google.storage.v2.StorageGrpc;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public final class FakeServer implements AutoCloseable {

  private final Server server;
  private final GrpcStorageOptions grpcStorageOptions;

  FakeServer(Server server, GrpcStorageOptions grpcStorageOptions) {
    this.server = server;
    this.grpcStorageOptions = grpcStorageOptions;
  }

  public GrpcStorageOptions getGrpcStorageOptions() {
    return grpcStorageOptions;
  }

  @Override
  public void close() throws InterruptedException {
    server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
  }

  public static FakeServer of(StorageGrpc.StorageImplBase service) throws IOException {
    InetSocketAddress address = new InetSocketAddress("localhost", 0);
    Server server = NettyServerBuilder.forAddress(address).addService(service).build();
    server.start();
    String endpoint = String.format("%s:%d", address.getHostString(), server.getPort());
    GrpcStorageOptions grpcStorageOptions =
        StorageOptions.grpc()
            .setHost("http://" + endpoint)
            .setProjectId("test-proj")
            .setCredentials(NoCredentials.getInstance())
            .build();
    return new FakeServer(server, grpcStorageOptions);
  }
}