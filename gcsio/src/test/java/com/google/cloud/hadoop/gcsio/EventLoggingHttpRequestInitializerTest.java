package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class EventLoggingHttpRequestInitializerTest {
  private static final String requestUrl = "http://google.com";

  @Mock private LowLevelHttpRequest mockLowLevelRequest;
  @Mock private LowLevelHttpResponse mockLowLevelResponse;

  private HttpRequest httpRequest;
  private EventLoggingHttpRequestInitializer requestInitializer;
  private EventLoggingHttpRequestInitializer requestInitializerSpy;
  private ArgumentCaptor<HashMap<String, Object>> logDetailsCaptor;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
    when(mockLowLevelRequest.execute()).thenReturn(mockLowLevelResponse);
    when(mockLowLevelResponse.getStatusCode()).thenReturn(200);

    MockHttpTransport fakeTransport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return mockLowLevelRequest;
          }
        };

    HttpRequestFactory requestFactory = fakeTransport.createRequestFactory();
    this.httpRequest = requestFactory.buildGetRequest(new GenericUrl(requestUrl));
    this.requestInitializer = new EventLoggingHttpRequestInitializer();
    this.requestInitializerSpy = spy(requestInitializer);
    this.logDetailsCaptor = ArgumentCaptor.forClass(HashMap.class);
  }

  @Test
  public void testBasicOperation() throws IOException {
    requestInitializer.initialize(httpRequest);
    HttpResponse res = httpRequest.execute();

    assertThat(res).isNotNull();
    verify(mockLowLevelRequest).execute();
  }

  @Test
  public void testTracingDetailsSet() throws IOException {
    requestInitializerSpy.initialize(httpRequest);
    httpRequest.execute();

    verify(requestInitializerSpy).logDetails(logDetailsCaptor.capture());
    verify(requestInitializerSpy, times(1)).logDetails(any());
    Map<String, Object> captured = logDetailsCaptor.getValue();

    assertThat(captured.get("response_time")).isNotNull();
    assertThat(captured.get("request_url")).isEqualTo(requestUrl);
    assertThat(captured.get("response_status_code")).isEqualTo(200);
  }

  @Test
  public void testIOExceptionNoCallback() throws IOException {
    requestInitializerSpy.initialize(httpRequest);

    when(mockLowLevelRequest.execute()).thenThrow(new IOException("fake IOException"));
    when(mockLowLevelResponse.getStatusCode()).thenReturn(200);

    try {
      httpRequest.execute();
    } catch (IOException ioe) {
    }

    verify(requestInitializerSpy, times(0)).logDetails(any());
  }

  @Test
  public void testZombieRequestDoesNotCrash() throws IOException {
    HttpResponse httpResponse = httpRequest.execute();
    requestInitializerSpy.logAndRemoveRequestFromTracking(httpResponse);

    verify(requestInitializerSpy).logDetails(logDetailsCaptor.capture());
    verify(requestInitializerSpy, times(1)).logDetails(any());

    Map<String, Object> captured = logDetailsCaptor.getValue();

    assertThat(captured.get("response_time")).isEqualTo(Integer.MAX_VALUE);
    assertThat(captured.get("request_url")).isEqualTo(requestUrl);
    assertThat(captured.get("response_status_code")).isEqualTo(200);
    assertThat(captured.get("unexpected_error")).isEqualTo("Zombie request. This is unexpected.");
  }
}
