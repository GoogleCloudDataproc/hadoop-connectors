/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/** Configuration for how components should obtain Credentials. */
@AutoValue
public abstract class CredentialsOptions {

  public enum AuthenticationType {
    ACCESS_TOKEN_PROVIDER,
    APPLICATION_DEFAULT,
    GCE_METADATA_SERVICE,
    SERVICE_ACCOUNT_JSON_KEYFILE,
    UNAUTHENTICATED,
  }

  public static Builder builder() {
    return new AutoValue_CredentialsOptions.Builder()
        .setAuthenticationType(AuthenticationType.GCE_METADATA_SERVICE);
  }

  public abstract Builder toBuilder();

  public abstract AuthenticationType getAuthenticationType();

  @Nullable
  public abstract String getServiceAccountJsonKeyFile();

  @Nullable
  public abstract Class<? extends AccessTokenProvider> getAccessTokenProviderClass();

  @Nullable
  public abstract String getTokenServerUrl();

  @Nullable
  public abstract String getProxyAddress();

  @Nullable
  public abstract RedactedString getProxyUsername();

  @Nullable
  public abstract RedactedString getProxyPassword();

  /** Builder for {@link CredentialsOptions} */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setAuthenticationType(AuthenticationType authenticationType);

    public abstract Builder setServiceAccountJsonKeyFile(String serviceAccountJsonKeyFile);

    public abstract Builder setAccessTokenProviderClass(
        Class<? extends AccessTokenProvider> accessTokenProviderClass);

    public abstract Builder setTokenServerUrl(String tokenServerUrl);

    public abstract Builder setProxyAddress(String proxyAddress);

    public abstract Builder setProxyUsername(RedactedString proxyUsername);

    public abstract Builder setProxyPassword(RedactedString proxyPassword);

    abstract CredentialsOptions autoBuild();

    public CredentialsOptions build() {
      CredentialsOptions options = autoBuild();

      switch (options.getAuthenticationType()) {
        case ACCESS_TOKEN_PROVIDER:
          checkArgument(
              options.getAccessTokenProviderClass() != null,
              "Access token provider class should not be specified for %s authentication",
              AuthenticationType.ACCESS_TOKEN_PROVIDER);
          checkArgument(
              isNullOrEmpty(options.getServiceAccountJsonKeyFile()),
              "Service account JSON keyfile should not be specified for %s authentication",
              AuthenticationType.ACCESS_TOKEN_PROVIDER);
          break;
        case APPLICATION_DEFAULT:
          checkArgument(
              options.getAccessTokenProviderClass() == null,
              "Access token provider class should not be specified for %s authentication",
              AuthenticationType.ACCESS_TOKEN_PROVIDER);
          checkArgument(
              isNullOrEmpty(options.getServiceAccountJsonKeyFile()),
              "Service account JSON keyfile should not be specified for %s authentication",
              AuthenticationType.ACCESS_TOKEN_PROVIDER);
          break;
        case GCE_METADATA_SERVICE:
          checkArgument(
              options.getAccessTokenProviderClass() == null,
              "Access token provider class should not be specified for %s authentication",
              AuthenticationType.GCE_METADATA_SERVICE);
          checkArgument(
              isNullOrEmpty(options.getServiceAccountJsonKeyFile()),
              "Service account JSON keyfile should not be specified for %s authentication",
              AuthenticationType.GCE_METADATA_SERVICE);
          break;
        case SERVICE_ACCOUNT_JSON_KEYFILE:
          checkArgument(
              options.getAccessTokenProviderClass() == null,
              "Access token provider class should not be specified for %s authentication",
              AuthenticationType.SERVICE_ACCOUNT_JSON_KEYFILE);
          checkArgument(
              !isNullOrEmpty(options.getServiceAccountJsonKeyFile()),
              "Service account JSON keyfile should be specified for %s authentication",
              AuthenticationType.SERVICE_ACCOUNT_JSON_KEYFILE);
          break;
        case UNAUTHENTICATED:
          checkArgument(
              options.getAccessTokenProviderClass() == null,
              "Access token provider class should not be specified for %s authentication",
              AuthenticationType.UNAUTHENTICATED);
          checkArgument(
              isNullOrEmpty(options.getServiceAccountJsonKeyFile()),
              "Service account JSON keyfile should not be specified for %s authentication",
              AuthenticationType.UNAUTHENTICATED);
          break;
        default:
          throw new IllegalArgumentException("Unknown authentication ");
      }

      return options;
    }
  }
}
