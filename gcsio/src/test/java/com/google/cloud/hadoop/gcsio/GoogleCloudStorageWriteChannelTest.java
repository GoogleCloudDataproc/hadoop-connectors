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

import com.google.api.client.http.HttpMethods;
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
    byte[] testData = {0x01, 0x02, 0x03, 0x04, 0x05};
    String expectedCrc32c = calculateCrc32c(testData);

    List<CapturingHttpRequest> requests = Collections.synchronizedList(new ArrayList<>());
    MockHttpTransport transport =
        createCapturingTransport(
            requests, createPutSuccessResponse("test-bucket", "test-object", expectedCrc32c));

    GoogleCloudStorageWriteChannel channel =
        createWriteChannel(transport, 0, "test-bucket", "test-object");

    channel.initialize();
    channel.write(ByteBuffer.wrap(testData));
    channel.close();

    CapturingHttpRequest finalRequest = getLastPutRequest(requests);
    assertThat(finalRequest.getFirstHeaderValue("x-goog-hash"))
        .isEqualTo("crc32c=" + expectedCrc32c);
  }

  @Test
  public void write_retriesOnFinalChunk_succeedsWithChecksum() throws IOException {
    byte[] testData = {1, 2, 3};
    String expectedCrc32c = calculateCrc32c(testData);

    List<CapturingHttpRequest> requests = Collections.synchronizedList(new ArrayList<>());
    MockHttpTransport transport =
        createCapturingTransport(
            requests,
            new MockLowLevelHttpResponse().setStatusCode(503),
            createPutSuccessResponse("bucket", "object", expectedCrc32c));

    GoogleCloudStorageWriteChannel channel = createWriteChannel(transport, 3, "bucket", "object");

    channel.initialize();
    channel.write(ByteBuffer.wrap(testData));
    channel.close();

    // Verify the successful retry request still had the checksum header
    CapturingHttpRequest finalRequest = getLastPutRequest(requests);
    assertThat(finalRequest.getFirstHeaderValue("x-goog-hash"))
        .isEqualTo("crc32c=" + expectedCrc32c);
  }

  @Test
  public void write_whenServerRejectsTrailingChecksum_shouldThrowException() throws IOException {
    List<CapturingHttpRequest> requests = Collections.synchronizedList(new ArrayList<>());

    MockLowLevelHttpResponse errorResponse = new MockLowLevelHttpResponse();
    errorResponse.setStatusCode(400);
    errorResponse.setContentType("application/json");
    errorResponse.setContent(
        "{\"error\": {\"code\": 400, \"message\": \"Provided CRC32C \\\"AAAAAA==\\\" doesn't match...\"}}");

    MockHttpTransport transport = createCapturingTransport(requests, errorResponse);
    GoogleCloudStorageWriteChannel channel = createWriteChannel(transport, 0, "bucket", "object");

    channel.initialize();
    channel.write(ByteBuffer.wrap(new byte[] {1, 2, 3}));

    IOException e = assertThrows(IOException.class, () -> channel.close());
    assertThat(e).hasCauseThat().isInstanceOf(HttpResponseException.class);
    assertThat(e).hasCauseThat().hasMessageThat().contains("Provided CRC32C");
  }

  private String calculateCrc32c(byte[] data) {
    return BaseEncoding.base64().encode(Ints.toByteArray(Hashing.crc32c().hashBytes(data).asInt()));
  }

  /** Wrapper to expose the request method of the mocked HTTP request. */
  private static class CapturingHttpRequest extends MockLowLevelHttpRequest {
    private final String method;

    CapturingHttpRequest(String method) {
      this.method = method;
    }

    String getMethod() {
      return method;
    }
  }

  /**
   * Creates a MockHttpTransport that safely handles the initial POST request, returns a given
   * sequence of responses for PUT requests, and captures all requests.
   */
  private MockHttpTransport createCapturingTransport(
      List<CapturingHttpRequest> capturedRequests, MockLowLevelHttpResponse... putResponses) {

    return new MockHttpTransport() {
      int putAttempt = 0;

      @Override
      public LowLevelHttpRequest buildRequest(String method, String url) {
        CapturingHttpRequest request =
            new CapturingHttpRequest(method) {
              @Override
              public LowLevelHttpResponse execute() throws IOException {
                if (HttpMethods.POST.equals(method)) {
                  MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                  response.setStatusCode(200);
                  response.addHeader("Location", "http://test-upload-url");
                  return response;
                }
                if (HttpMethods.PUT.equals(method)) {
                  if (putAttempt < putResponses.length) {
                    return putResponses[putAttempt++];
                  }
                }
                return new MockLowLevelHttpResponse().setStatusCode(404);
              }
            };
        capturedRequests.add(request);
        return request;
      }
    };
  }

  private MockLowLevelHttpResponse createPutSuccessResponse(
      String bucketName, String objectName, String crc32c) {
    MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
    response.setStatusCode(200);
    response.setContent(
        String.format(
            "{\"bucket\": \"%s\", \"name\": \"%s\", \"crc32c\": \"%s\"}",
            bucketName, objectName, crc32c));
    return response;
  }

  private GoogleCloudStorageWriteChannel createWriteChannel(
      MockHttpTransport transport, int maxRetries, String bucketName, String objectName) {

    HttpRequestInitializer initializer =
        new RetryHttpInitializer(
            new FakeCredentials(),
            RetryHttpInitializerOptions.builder().setMaxRequestRetries(maxRetries).build());

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), initializer);

    return new GoogleCloudStorageWriteChannel(
        storage,
        new ClientRequestHelper<>(),
        Executors.newCachedThreadPool(),
        AsyncWriteChannelOptions.builder().build(),
        new StorageResourceId(bucketName, objectName),
        CreateObjectOptions.DEFAULT_OVERWRITE,
        ObjectWriteConditions.NONE);
  }

  private CapturingHttpRequest getLastPutRequest(List<CapturingHttpRequest> requests) {
    return requests.stream()
        .filter(r -> HttpMethods.PUT.equals(r.getMethod()))
        .reduce((first, second) -> second)
        .orElseThrow(() -> new AssertionError("No PUT request sent"));
  }
}
