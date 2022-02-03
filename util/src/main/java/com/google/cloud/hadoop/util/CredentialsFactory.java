/*
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.util;

import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.client.http.HttpTransport;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.flogger.GoogleLogger;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.function.Supplier;

/** Miscellaneous helper methods for getting a {@code Credentials} from various sources. */
public class CredentialsFactory {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";

  static final String CREDENTIALS_ENV_VAR = "GOOGLE_APPLICATION_CREDENTIALS";

  private final CredentialsOptions options;

  private final java.util.function.Supplier<HttpTransport> transport;

  public CredentialsFactory(CredentialsOptions options) {
    this(
        options,
        Suppliers.memoize(
            () -> {
              try {
                return HttpTransportFactory.createHttpTransport(
                    options.getProxyAddress(),
                    options.getProxyUsername(),
                    options.getProxyPassword());
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            }));
  }

  @VisibleForTesting
  CredentialsFactory(CredentialsOptions options, Supplier<HttpTransport> transport) {
    this.options = options;
    this.transport = transport;
  }

  /**
   * Initializes OAuth2 credentials using preconfigured ServiceAccount settings on the local GCE VM.
   * See: <a href="https://developers.google.com/compute/docs/authentication">Authenticating from
   * Google Compute Engine</a>.
   */
  public GoogleCredentials getCredentialsFromMetadataServiceAccount() {
    logger.atFine().log("Getting service account credentials from metadata service.");
    return ComputeEngineCredentials.newBuilder().setHttpTransportFactory(transport::get).build();
  }

  /** Get credentials listed in a JSON file. */
  private GoogleCredentials getCredentialsFromJsonKeyFile() throws IOException {
    logger.atFine().log(
        "getCredentialsFromJsonKeyFile() from '%s'", options.getServiceAccountJsonKeyFile());
    try (FileInputStream fis = new FileInputStream(options.getServiceAccountJsonKeyFile())) {
      return ServiceAccountCredentials.fromStream(fis, transport::get)
          .createScoped(CLOUD_PLATFORM_SCOPE);
    }
  }

  /**
   * Determines whether Application Default Credentials have been configured as an environment
   * variable.
   *
   * <p>In this class for testability.
   */
  private static boolean isApplicationDefaultCredentialsConfigured() {
    return System.getenv(CREDENTIALS_ENV_VAR) != null;
  }

  /**
   * Get Google Application Default Credentials as described in <a
   * href="https://developers.google.com/identity/protocols/application-default-credentials#callingjava"
   * >Google Application Default Credentials</a>
   */
  private GoogleCredentials getApplicationDefaultCredentials() throws IOException {
    logger.atFine().log("getApplicationDefaultCredentials()");
    return GoogleCredentials.getApplicationDefault(transport::get);
  }

  /**
   * Get the credentials as configured.
   *
   * <p>The following is the order in which properties are applied to create the Credentials:
   *
   * <ol>
   *   <li>If service accounts are enabled and no service account key file or service account
   *       parameters are set, use the metadata service.
   *   <li>If service accounts are enabled and a service-account json keyfile is provided, use
   *       service account authentication.
   *   <li>If service accounts are disabled and client id, client secret and OAuth credentials file
   *       is provided, use the Installed App authentication flow.
   *   <li>If service accounts are disabled and null credentials are enabled for unit testing,
   *       return null
   * </ol>
   *
   * @throws IllegalStateException if none of the above conditions are met and a Credentials cannot
   *     be created
   */
  public GoogleCredentials getCredentials() throws IOException, GeneralSecurityException {
    GoogleCredentials credentials = getCredentialsInternal();
    return credentials == null ? null : configureCredentials(credentials);
  }

  private GoogleCredentials getCredentialsInternal() throws IOException {
    if (options.isServiceAccountEnabled()) {
      logger.atFine().log("Using service account credentials");

      // By default, we want to use service accounts with the meta-data service
      // (assuming we're running in GCE).
      if (useMetadataService()) {
        return getCredentialsFromMetadataServiceAccount();
      }

      if (!isNullOrEmpty(options.getServiceAccountJsonKeyFile())) {
        return getCredentialsFromJsonKeyFile();
      }

      if (isApplicationDefaultCredentialsConfigured()) {
        return getApplicationDefaultCredentials();
      }
    } else if (options.isNullCredentialsEnabled()) {
      return null;
    }

    throw new IllegalStateException("No valid credentials configuration discovered: " + this);
  }

  private GoogleCredentials configureCredentials(GoogleCredentials credentials) {
    if (options.getTokenServerUrl() == null) {
      return credentials;
    }
    if (credentials instanceof ServiceAccountCredentials) {
      return ((ServiceAccountCredentials) credentials)
          .toBuilder().setTokenServerUri(URI.create(options.getTokenServerUrl())).build();
    }
    if (credentials instanceof UserCredentials) {
      return ((UserCredentials) credentials)
          .toBuilder().setTokenServerUri(URI.create(options.getTokenServerUrl())).build();
    }
    return credentials;
  }

  private boolean useMetadataService() {
    return isNullOrEmpty(options.getServiceAccountJsonKeyFile())
        && !isApplicationDefaultCredentialsConfigured()
        && !options.isNullCredentialsEnabled();
  }
}
