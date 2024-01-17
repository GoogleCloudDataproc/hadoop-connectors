package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopFileSystem;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_CLIENT_RATE_LIMIT_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_CLIENT_SIDE_ERROR_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_REQUEST_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_SERVER_SIDE_ERROR_COUNT;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.emptyResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonError.ErrorInfo;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.Json;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.HttpTesting;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import org.apache.hadoop.fs.StorageStatistics;
import org.junit.Before;
import org.junit.Test;

public class GoogleCloudStorageStatisticsTest {

  private GoogleJsonResponseException accessDenied; // STATUS_CODE_FORBIDDEN
  private GoogleJsonResponseException statusOk; // STATUS_CODE_OK

  private GoogleHadoopFileSystem myGhfs;

  private final ApiErrorExtractor errorExtractor = ApiErrorExtractor.INSTANCE;

  @Before
  public void setUp() throws Exception {

    myGhfs = createInMemoryGoogleHadoopFileSystem();

    accessDenied =
        googleJsonResponseException(
            HttpStatusCodes.STATUS_CODE_FORBIDDEN, "Forbidden", "Forbidden");
    statusOk = googleJsonResponseException(HttpStatusCodes.STATUS_CODE_OK, "A reason", "ok");
  }

  @Test
  public void gcs_request_count_status_metrics() throws Exception {
    StorageStatistics stats = TestUtils.getStorageStatistics();
    mockStatusCode(429);
    TestUtils.verifyCounter((GhfsStorageStatistics) stats, GCS_REQUEST_COUNT, 1);
  }

  @Test
  public void gcs_client_429_status_metrics() throws Exception {
    StorageStatistics stats = TestUtils.getStorageStatistics();
    mockStatusCode(429);
    TestUtils.verifyCounterNotZero((GhfsStorageStatistics) stats, GCS_CLIENT_RATE_LIMIT_COUNT);
  }

  /** Validates accessDenied(). */
  @Test
  public void testAccessDenied() {
    StorageStatistics stats = TestUtils.getStorageStatistics();

    // Check success case.
    assertThat(errorExtractor.accessDenied(accessDenied)).isTrue();
    assertThat(errorExtractor.accessDenied(new IOException(accessDenied))).isTrue();
    assertThat(errorExtractor.accessDenied(new IOException(new IOException(accessDenied))))
        .isTrue();

    // Check failure case.
    assertThat(errorExtractor.accessDenied(statusOk)).isFalse();
    assertThat(errorExtractor.accessDenied(new IOException(statusOk))).isFalse();

    TestUtils.verifyCounterNotZero((GhfsStorageStatistics) stats, GCS_CLIENT_SIDE_ERROR_COUNT);
  }

  @Test
  public void isClientError_GoogleJsonErrorWithStatusBadGatewayReturnFalse() throws IOException {
    StorageStatistics stats = TestUtils.getStorageStatistics();

    IOException withJsonError =
        googleJsonResponseException(
            HttpStatusCodes.STATUS_CODE_BAD_GATEWAY, "Bad gateway", "Bad gateway", "Bad gateway");
    assertThat(errorExtractor.clientError(withJsonError)).isFalse();

    TestUtils.verifyCounterNotZero((GhfsStorageStatistics) stats, GCS_SERVER_SIDE_ERROR_COUNT);
  }

  /** Builds a fake GoogleJsonResponseException for testing API error handling. */
  private static GoogleJsonResponseException googleJsonResponseException(
      int httpStatus, String reason, String message) throws IOException {
    return googleJsonResponseException(httpStatus, reason, message, message);
  }

  /** Builds a fake GoogleJsonResponseException for testing API error handling. */
  private static GoogleJsonResponseException googleJsonResponseException(
      int httpStatus, String reason, String message, String httpStatusString) throws IOException {
    ErrorInfo errorInfo = new ErrorInfo();
    errorInfo.setReason(reason);
    errorInfo.setMessage(message);
    return googleJsonResponseException(httpStatus, errorInfo, httpStatusString);
  }

  private static GoogleJsonResponseException googleJsonResponseException(
      int status, ErrorInfo errorInfo, String httpStatusString) throws IOException {
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    HttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) throws IOException {
            errorInfo.setFactory(jsonFactory);
            GoogleJsonError jsonError = new GoogleJsonError();
            jsonError.setCode(status);
            jsonError.setErrors(Collections.singletonList(errorInfo));
            jsonError.setMessage(httpStatusString);
            jsonError.setFactory(jsonFactory);
            GenericJson errorResponse = new GenericJson();
            errorResponse.set("error", jsonError);
            errorResponse.setFactory(jsonFactory);
            return new MockLowLevelHttpRequest()
                .setResponse(
                    new MockLowLevelHttpResponse()
                        .setContent(errorResponse.toPrettyString())
                        .setContentType(Json.MEDIA_TYPE)
                        .setStatusCode(status));
          }
        };
    HttpRequest request =
        transport.createRequestFactory().buildGetRequest(HttpTesting.SIMPLE_GENERIC_URL);
    request.setThrowExceptionOnExecuteError(false);
    HttpResponse response = request.execute();
    return GoogleJsonResponseException.from(jsonFactory, response);
  }

  private void mockStatusCode(int statusCode) throws IOException {
    RetryHttpInitializer retryHttpInitializer =
        new RetryHttpInitializer(
            null,
            RetryHttpInitializerOptions.builder()
                .setDefaultUserAgent("foo-user-agent")
                .setHttpHeaders(ImmutableMap.of("header-key", "header-value"))
                .setMaxRequestRetries(5)
                .setConnectTimeout(Duration.ofSeconds(5))
                .setReadTimeout(Duration.ofSeconds(5))
                .build());

    HttpRequestFactory requestFactory =
        mockTransport(emptyResponse(statusCode), emptyResponse(statusCode), emptyResponse(200))
            .createRequestFactory(retryHttpInitializer);

    HttpRequest req = requestFactory.buildGetRequest(new GenericUrl("http://fake-url.com"));
    HttpResponse res = req.execute();
  }
}
