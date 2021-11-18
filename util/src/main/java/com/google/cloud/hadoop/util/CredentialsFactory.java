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

/** Miscellaneous helper methods for getting a {@code Credential} from various sources. */
public class CredentialsFactory {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  static final String CREDENTIAL_ENV_VAR = "GOOGLE_APPLICATION_CREDENTIALS";

  private final CredentialOptions options;

  private final java.util.function.Supplier<HttpTransport> transport;

  public CredentialsFactory(CredentialOptions options) {
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
  CredentialsFactory(CredentialOptions options, Supplier<HttpTransport> transport) {
    this.options = options;
    this.transport = transport;
  }

  /**
   * Initializes OAuth2 credential using preconfigured ServiceAccount settings on the local GCE VM.
   * See: <a href="https://developers.google.com/compute/docs/authentication">Authenticating from
   * Google Compute Engine</a>.
   */
  public GoogleCredentials getCredentialFromMetadataServiceAccount() {
    logger.atFine().log("Getting service account credentials from metadata service.");
    return ComputeEngineCredentials.newBuilder().setHttpTransportFactory(transport::get).build();
  }

  /** Get credentials listed in a JSON file. */
  private GoogleCredentials getCredentialFromJsonKeyFile() throws IOException {
    logger.atFine().log(
        "getCredentialFromJsonKeyFile() from '%s'", options.getServiceAccountJsonKeyFile());
    try (FileInputStream fis = new FileInputStream(options.getServiceAccountJsonKeyFile())) {
      return ServiceAccountCredentials.fromStream(fis, transport::get)
          .createScoped("https://www.googleapis.com/auth/cloud-platform");
    }
  }

  /**
   * Determines whether Application Default Credentials have been configured as an environment
   * variable.
   *
   * <p>In this class for testability.
   */
  private static boolean isApplicationDefaultCredentialsConfigured() {
    return System.getenv(CREDENTIAL_ENV_VAR) != null;
  }

  /**
   * Get Google Application Default Credentials as described in <a
   * href="https://developers.google.com/identity/protocols/application-default-credentials#callingjava"
   * >Google Application Default Credentials</a>
   */
  private GoogleCredentials getApplicationDefaultCredentials() throws IOException {
    logger.atFine().log("getApplicationDefaultCredential()");
    return GoogleCredentials.getApplicationDefault(transport::get);
  }

  /**
   * Get the credential as configured.
   *
   * <p>The following is the order in which properties are applied to create the Credential:
   *
   * <ol>
   *   <li>If service accounts are not disabled and no service account key file or service account
   *       parameters are set, use the metadata service.
   *   <li>If service accounts are not disabled and a service-account email and keyfile, or service
   *       account parameters are provided, use service account authentication with the given
   *       parameters.
   *   <li>If service accounts are disabled and client id, client secret and OAuth credential file
   *       is provided, use the Installed App authentication flow.
   *   <li>If service accounts are disabled and null credentials are enabled for unit testing,
   *       return null
   * </ol>
   *
   * @throws IllegalStateException if none of the above conditions are met and a Credential cannot
   *     be created
   */
  public GoogleCredentials getCredentials() throws IOException, GeneralSecurityException {
    GoogleCredentials credentials = getCredentialsInternal();
    return credentials == null ? null : configureCredentials(credentials);
  }

  private GoogleCredentials getCredentialsInternal() throws IOException, GeneralSecurityException {
    if (options.isServiceAccountEnabled()) {
      logger.atFine().log("Using service account credentials");

      // By default, we want to use service accounts with the meta-data service
      // (assuming we're running in GCE).
      if (useMetadataService()) {
        return getCredentialFromMetadataServiceAccount();
      }

      if (!isNullOrEmpty(options.getServiceAccountJsonKeyFile())) {
        return getCredentialFromJsonKeyFile();
      }

      if (isApplicationDefaultCredentialsConfigured()) {
        return getApplicationDefaultCredentials();
      }
    } else if (options.isNullCredentialEnabled()) {
      return null;
    }

    throw new IllegalStateException("No valid credential configuration discovered: " + this);
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
        && !options.isNullCredentialEnabled();
  }
}
