/*
 * Copyright 2021 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.cloud.hadoop.util.HttpTransportFactory;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import com.google.cloud.hadoop.util.testing.FakeCredentials;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests that require a particular configuration of GoogleCloudStorageImpl. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageImplCreateTest {

  @Test
  public void create_grpcAndVmComputeEngineCredentials_useDirectpath() throws IOException {
    GoogleCloudStorageImpl gcs =
        new GoogleCloudStorageImpl(
            GoogleCloudStorageOptions.builder().setAppName("app").setGrpcEnabled(true).build(),
            createStorage());
    assertThat(gcs.getStorageStubProvider().getGrpcDecorator())
        .isInstanceOf(StorageStubProvider.DirectPathGrpcDecorator.class);
  }

  @Test
  public void create_grpcAndDisableDirectPathAndVmComputeEngineCredentials_useCloudpath()
      throws IOException {
    GoogleCloudStorageImpl gcs =
        new GoogleCloudStorageImpl(
            GoogleCloudStorageOptions.builder()
                .setAppName("app")
                .setGrpcEnabled(true)
                .setDirectPathPreferred(false)
                .build(),
            createStorage());
    assertThat(gcs.getStorageStubProvider().getGrpcDecorator())
        .isInstanceOf(StorageStubProvider.CloudPathGrpcDecorator.class);
  }

  @Test
  public void create_grpcAndNonComputeEngineCredentials_useCloudpath() throws IOException {
    GoogleCloudStorageImpl gcs =
        new GoogleCloudStorageImpl(
            GoogleCloudStorageOptions.builder().setAppName("app").setGrpcEnabled(true).build(),
            createStorage(new FakeCredentials()));
    assertThat(gcs.getStorageStubProvider().getGrpcDecorator())
        .isInstanceOf(StorageStubProvider.CloudPathGrpcDecorator.class);
  }

  @Test
  public void create_grpcAndNullCredentials_useCloudpath() throws IOException {
    GoogleCloudStorageImpl gcs =
        new GoogleCloudStorageImpl(
            GoogleCloudStorageOptions.builder().setAppName("app").setGrpcEnabled(true).build(),
            createStorage(/* credentials= */ null));
    assertThat(gcs.getStorageStubProvider().getGrpcDecorator())
        .isInstanceOf(StorageStubProvider.CloudPathGrpcDecorator.class);
  }

  @Test
  public void create_grpcAndTrafficDirector_useTrafficDirector() throws IOException {
    GoogleCloudStorageImpl gcs =
        new GoogleCloudStorageImpl(
            GoogleCloudStorageOptions.builder()
                .setAppName("app")
                .setGrpcEnabled(true)
                .setTrafficDirectorEnabled(true)
                .build(),
            createStorage());
    assertThat(gcs.getStorageStubProvider().getGrpcDecorator())
        .isInstanceOf(StorageStubProvider.TrafficDirectorGrpcDecorator.class);
  }

  private static Storage createStorage() throws IOException {
    return createStorage(ComputeEngineCredentials.create());
  }

  private static Storage createStorage(Credentials credentials) throws IOException {
    return new Storage.Builder(
            HttpTransportFactory.createHttpTransport(),
            JacksonFactory.getDefaultInstance(),
            new RetryHttpInitializer(
                /* delegate= */ null,
                credentials,
                RetryHttpInitializerOptions.builder()
                    .setDefaultUserAgent("foo-user-agent")
                    .build()))
        .build();
  }
}
