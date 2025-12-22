/*
 * Copyright 2013 Google Inc.
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

package com.google.cloud.hadoop.util;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpIOExceptionHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.util.ExponentialBackOff;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.hadoop.util.interceptors.InvocationIdInterceptor;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import com.google.common.flogger.LogContext;
import java.io.IOException;
import java.util.Set;

/** An implementation of {@link HttpRequestInitializer} with retries. */
public class RetryHttpInitializer implements HttpRequestInitializer {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final ExponentialBackOff.Builder BACKOFF_BUILDER =
      new ExponentialBackOff.Builder()
          // Set initial timeout to 1.25 seconds to have a 1-second minimum initial interval
          // after 0.2 randomization factor will be applied
          .setInitialIntervalMillis(1_250)
          .setMultiplier(1.6)
          .setRandomizationFactor(0.2)
          .setMaxIntervalMillis(20_000)
          // 30 minutes
          .setMaxElapsedTimeMillis(1_800_000);

  // To be used as a request interceptor for filling in the "Authorization" header field, as well
  // as a response handler for certain unsuccessful error codes wherein the Credentials must refresh
  // its token for a retry.
  private final HttpCredentialsAdapter credentials;

  private final RetryHttpInitializerOptions options;

  /**
   * @param credentials A credentials which will be used to initialize on HttpRequests and as the
   *     delegate for a {@link UnsuccessfulResponseHandler}.
   * @param options An options that configure {@link RetryHttpInitializer} instance behaviour.
   */
  public RetryHttpInitializer(Credentials credentials, RetryHttpInitializerOptions options) {
    this.credentials = credentials == null ? null : new HttpCredentialsAdapter(credentials);
    this.options = options;
  }

  @Override
  public void initialize(HttpRequest request) throws IOException {
    // Initialize request with credentials and let CredentialsOrBackoffResponseHandler
    // to refresh credentials later if necessary
    if (credentials != null) {
      credentials.initialize(request);
    }

    RequestTracker tracker = getRequestTracker(request);

    request
        // Request will be retried if server errors (5XX) or I/O errors are encountered.
        .setNumberOfRetries(options.getMaxRequestRetries())
        // Set the timeout configurations.
        .setConnectTimeout(toIntExact(options.getConnectTimeout().toMillis()))
        .setReadTimeout(toIntExact(options.getReadTimeout().toMillis()))
        .setUnsuccessfulResponseHandler(new UnsuccessfulResponseHandler(credentials, tracker))
        .setIOExceptionHandler(new IoExceptionHandler(tracker))
        .setResponseInterceptor(tracker::trackResponse);

    HttpHeaders headers = request.getHeaders();
    if (isNullOrEmpty(headers.getUserAgent()) && !isNullOrEmpty(options.getDefaultUserAgent())) {
      logger.atFiner().log(
          "Request is missing a user-agent header, adding default value of '%s'",
          options.getDefaultUserAgent());
      headers.setUserAgent(options.getDefaultUserAgent());
    }
    headers.putAll(options.getHttpHeaders());
    request.setInterceptor(
        new JsonIdempotencyTokenInterceptor(
            new InvocationIdInterceptor(request.getInterceptor(), tracker)));
  }

  protected RequestTracker getRequestTracker(HttpRequest request) {
    return RequestTracker.create(request);
  }

  public Credentials getCredentials() {
    return credentials == null ? null : credentials.getCredentials();
  }

  /**
   * Handles unsuccessful responses:
   *
   * <ul>
   *   <li>log responses based on the response code
   *   <li>401 Unauthorized responses are handled by the {@link HttpCredentialsAdapter}
   *   <li>5XX are handled by the a backoff handler.
   * </ul>
   */
  private static class UnsuccessfulResponseHandler implements HttpUnsuccessfulResponseHandler {

    private static final int HTTP_SC_GONE = 410;

    /** HTTP status code indicating too many requests in a given amount of time. */
    private static final int HTTP_SC_TOO_MANY_REQUESTS = 429;

    /**
     * HTTP status code indicating that the server has decided to close the connection rather than
     * continue waiting
     */
    private static final int HTTP_REQUEST_TIMEOUT = 408;

    private static final Set<Integer> RETRYABLE_CODES =
        ImmutableSet.of(HTTP_SC_TOO_MANY_REQUESTS, HTTP_REQUEST_TIMEOUT);

    // The set of response codes to log URLs for with a rate limit.
    private static final ImmutableSet<Integer> RESPONSE_CODES_TO_LOG_WITH_RATE_LIMIT =
        ImmutableSet.of(HTTP_SC_TOO_MANY_REQUESTS);

    // The set of response codes to log URLs for.
    private static final ImmutableSet<Integer> RESPONSE_CODES_TO_LOG =
        ImmutableSet.<Integer>builder()
            .addAll(RESPONSE_CODES_TO_LOG_WITH_RATE_LIMIT)
            .add(HTTP_SC_GONE, HttpStatusCodes.STATUS_CODE_SERVICE_UNAVAILABLE)
            .build();

    // Base implementation of BackOffRequired determining the default set of cases where we'll retry
    // on unsuccessful HTTP responses; we'll mix in additional retryable response cases on top
    // of the bases cases defined by this instance.
    private static final HttpBackOffUnsuccessfulResponseHandler.BackOffRequired BACK_OFF_REQUIRED =
        response ->
            RETRYABLE_CODES.contains(response.getStatusCode())
                || HttpBackOffUnsuccessfulResponseHandler.BackOffRequired.ON_SERVER_ERROR
                    .isRequired(response);

    private final HttpCredentialsAdapter credentials;
    private final HttpBackOffUnsuccessfulResponseHandler delegate;
    private final RequestTracker tracker;

    public UnsuccessfulResponseHandler(HttpCredentialsAdapter credentials, RequestTracker tracker) {
      this.credentials = credentials;
      this.delegate =
          new HttpBackOffUnsuccessfulResponseHandler(BACKOFF_BUILDER.build())
              .setBackOffRequired(BACK_OFF_REQUIRED);
      this.tracker = tracker;
    }

    @Override
    public boolean handleResponse(HttpRequest request, HttpResponse response, boolean supportsRetry)
        throws IOException {

      logResponseCode(request, response);
      tracker.trackUnsuccessfulResponseHandler(response);

      if (credentials != null && credentials.handleResponse(request, response, supportsRetry)) {
        // If credentials decides it can handle it, the return code or message indicated something
        // specific to authentication, and no backoff is desired.
        return true;
      }

      long backOffStartTime = System.currentTimeMillis();
      if (delegate.handleResponse(request, response, supportsRetry)) {
        tracker.trackBackOffCompleted(backOffStartTime);
        // Otherwise, we defer to the judgement of our internal backoff handler.
        tracker.trackRetryStarted();

        return true;
      }

      tracker.trackRetrySkipped(true);

      escapeRedirectPath(request, response);

      return false;
    }

    private void logResponseCode(HttpRequest request, HttpResponse response) {
      if (RESPONSE_CODES_TO_LOG.contains(response.getStatusCode())) {
        logger
            .atInfo()
            // Apply rate limit (atMostEvery) based on the response status code
            .with(LogContext.Key.LOG_SITE_GROUPING_KEY, response.getStatusCode())
            .atMostEvery(
                RESPONSE_CODES_TO_LOG_WITH_RATE_LIMIT.contains(response.getStatusCode()) ? 10 : 0,
                SECONDS)
            .log(
                "Encountered status code %d when sending %s request to URL '%s'."
                    + " Delegating to response handler for possible retry.",
                response.getStatusCode(), request.getRequestMethod(), request.getUrl());
      }
    }

    private void escapeRedirectPath(HttpRequest request, HttpResponse response) {
      if (HttpStatusCodes.isRedirect(response.getStatusCode())
          && request.getFollowRedirects()
          && response.getHeaders() != null
          && response.getHeaders().getLocation() != null) {
        // Hack: Reach in and fix any '+' in the URL but still report 'false'. The client library
        // incorrectly tries to decode '+' into ' ', even though the backend servers treat '+'
        // as a legitimate path character, and so do not encode it. This is safe to do regardless
        // whether the client library fixes the bug, since %2B will correctly be decoded as '+'
        // even after the fix.
        String redirectLocation = response.getHeaders().getLocation();
        if (redirectLocation.contains("+")) {
          String escapedLocation = redirectLocation.replace("+", "%2B");
          logger.atFine().log(
              "Redirect path '%s' contains unescaped '+', replacing with '%%2B': '%s'",
              redirectLocation, escapedLocation);
          response.getHeaders().setLocation(escapedLocation);
        }
      }
    }
  }

  private static class IoExceptionHandler implements HttpIOExceptionHandler {

    private final HttpIOExceptionHandler delegate;
    private final RequestTracker tracker;

    public IoExceptionHandler(RequestTracker tracker) {
      // Retry IOExceptions such as "socket timed out" of "insufficient bytes written" with backoff.
      this.delegate = new HttpBackOffIOExceptionHandler(BACKOFF_BUILDER.build());
      this.tracker = tracker;
    }

    @Override
    public boolean handleIOException(HttpRequest httpRequest, boolean supportsRetry)
        throws IOException {
      // We sadly don't get anything helpful to see if this is something we want to log.
      // As a result we'll turn down the logging level to debug.
      logger.atFine().log("Encountered an IOException when accessing URL %s", httpRequest.getUrl());
      tracker.trackIOException();

      long backoffStartTime = System.currentTimeMillis();
      boolean result = delegate.handleIOException(httpRequest, supportsRetry);

      tracker.trackBackOffCompleted(backoffStartTime);

      if (result) {
        tracker.trackRetryStarted();
      } else {
        tracker.trackRetrySkipped(false);
      }

      return result;
    }
  }
}
