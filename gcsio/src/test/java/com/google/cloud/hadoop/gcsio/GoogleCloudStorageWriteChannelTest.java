/*
 * Copyright 2019 Google LLC
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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.BUCKET_NAME;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.HTTP_TRANSPORT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.OBJECT_NAME;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.junit.Assert.assertThrows;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.storage.Storage;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import com.google.cloud.hadoop.util.testing.FakeCredentials;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GoogleCloudStorageWriteChannel} class. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageWriteChannelTest {

  @Test
  public void createRequest_shouldSetKmsKeyName() throws IOException {
    String kmsKeyName = "testKmsKey";

    GoogleCloudStorageWriteChannel writeChannel =
        new GoogleCloudStorageWriteChannel(
            new Storage(HTTP_TRANSPORT, GsonFactory.getDefaultInstance(), r -> {}),
            new ClientRequestHelper<>(),
            newDirectExecutorService(),
            AsyncWriteChannelOptions.DEFAULT,
            new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
            CreateObjectOptions.DEFAULT_NO_OVERWRITE.toBuilder().setKmsKeyName(kmsKeyName).build(),
            ObjectWriteConditions.NONE);

    Storage.Objects.Insert request =
        writeChannel.createRequest(
            new InputStreamContent("plain/text", new ByteArrayInputStream(new byte[0])));

    assertThat(request.getKmsKeyName()).isEqualTo(kmsKeyName);
  }

  @Test
  public void write_shouldSetTrailingChecksumOnFinalRequest() throws IOException {
    String bucketName = "test-bucket";
    String objectName = "test-object";
    byte[] testData = {0x01, 0x02, 0x03, 0x04, 0x05};
    String expectedCrc32c =
        BaseEncoding.base64()
            .encode(Ints.toByteArray(Hashing.crc32c().hashBytes(testData).asInt()));

    AsyncWriteChannelOptions options = AsyncWriteChannelOptions.builder().build();

    // Custom class to capture the method
    class CapturingHttpRequest extends MockLowLevelHttpRequest {
      private final String method;

      CapturingHttpRequest(String method) {
        this.method = method;
      }

      String getMethod() {
        return method;
      }
    }

    List<CapturingHttpRequest> requests = Collections.synchronizedList(new ArrayList<>());
    MockHttpTransport capturingTransport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            CapturingHttpRequest request =
                new CapturingHttpRequest(method) {
                  @Override
                  public LowLevelHttpResponse execute() throws IOException {
                    MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                    if (method.equals("POST")) {
                      response.setStatusCode(200);
                      response.addHeader("Location", "http://test-upload-url");
                      return response;
                    }
                    if (method.equals("PUT")) {
                      response.setStatusCode(200);
                      response.setContent(
                          "{\"bucket\": \""
                              + bucketName
                              + "\", \"name\": \""
                              + objectName
                              + "\", \"crc32c\": \""
                              + expectedCrc32c
                              + "\"}");
                      return response;
                    }
                    return response.setStatusCode(404);
                  }
                };
            requests.add(request);
            return request;
          }
        };
    HttpRequestInitializer initializer =
        new RetryHttpInitializer(
            new FakeCredentials(), RetryHttpInitializerOptions.builder().build());

    Storage storage =
        new Storage(capturingTransport, GsonFactory.getDefaultInstance(), initializer);
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(
            storage,
            new ClientRequestHelper<>(),
            Executors.newCachedThreadPool(),
            options,
            new StorageResourceId(bucketName, objectName),
            CreateObjectOptions.DEFAULT_OVERWRITE,
            ObjectWriteConditions.NONE);

    channel.initialize();
    channel.write(ByteBuffer.wrap(testData));
    channel.close();

    CapturingHttpRequest finalRequest =
        requests.stream()
            .filter(r -> "PUT".equals(r.getMethod()))
            .reduce((first, second) -> second)
            .orElseThrow(() -> new AssertionError("No PUT request sent"));

    List<String> hashHeaders = finalRequest.getHeaders().get("x-goog-hash");

    assertThat(hashHeaders).isNotNull();
    assertThat(hashHeaders).isNotEmpty();
    String hashHeader = hashHeaders.get(0);
    assertThat(hashHeader).contains("crc32c=" + expectedCrc32c);
  }

  @Test
  public void write_whenServerRejectsTrailingChecksum_shouldThrowException() throws IOException {
    AsyncWriteChannelOptions options = AsyncWriteChannelOptions.builder().build();

    MockHttpTransport mockGcsServer =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() throws IOException {
                if (method.equals("POST")) {
                  MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                  response.setStatusCode(200);
                  response.addHeader("Location", "http://test-upload-url");
                  return response;
                }
                // Handle Final Chunk Upload
                if (method.equals("PUT")) {
                  // Simulate error response for checksum validation failure
                  MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                  response.setStatusCode(400);
                  response.setContentType("application/json");
                  response.setContent(
                      "{"
                          + "  \"error\": {"
                          + "    \"code\": 400,"
                          + "    \"message\": \"Provided CRC32C \\\"AAAAAA==\\\" doesn't match calculated CRC32C \\\"Zn6Cvg==\\\".\","
                          + "    \"errors\": [{"
                          + "      \"domain\": \"global\","
                          + "      \"reason\": \"invalid\","
                          + "      \"message\": \"Provided CRC32C ... doesn't match ...\""
                          + "    }]"
                          + "  }"
                          + "}");
                  return response;
                }
                return new MockLowLevelHttpResponse().setStatusCode(404);
              }
            };
          }
        };

    HttpRequestInitializer initializer =
        new RetryHttpInitializer(
            new FakeCredentials(), RetryHttpInitializerOptions.builder().build());
    Storage storage = new Storage(mockGcsServer, GsonFactory.getDefaultInstance(), initializer);

    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(
            storage,
            new ClientRequestHelper<>(),
            java.util.concurrent.Executors.newCachedThreadPool(),
            options,
            new StorageResourceId("bucket", "object"),
            CreateObjectOptions.DEFAULT_OVERWRITE,
            ObjectWriteConditions.NONE);

    channel.initialize();
    channel.write(ByteBuffer.wrap(new byte[] {1, 2, 3}));

    // Verify Exception
    IOException e = assertThrows(IOException.class, () -> channel.close());
    assertThat(e).hasCauseThat().isInstanceOf(HttpResponseException.class);
    // Verify error message confirms it was a checksum issue
    assertThat(e).hasCauseThat().hasMessageThat().contains("Provided CRC32C");
  }
}
