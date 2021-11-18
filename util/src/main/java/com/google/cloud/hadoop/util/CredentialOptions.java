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

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/** Configuration for how components should obtain Credentials. */
@AutoValue
public abstract class CredentialOptions {

  static final boolean SERVICE_ACCOUNT_ENABLED_DEFAULT = true;

  static final boolean NULL_CREDENTIALS_ENABLED_DEFAULT = false;
  ;

  public static Builder builder() {
    return new AutoValue_CredentialOptions.Builder()
        .setServiceAccountEnabled(SERVICE_ACCOUNT_ENABLED_DEFAULT)
        .setNullCredentialEnabled(NULL_CREDENTIALS_ENABLED_DEFAULT);
  }

  public abstract Builder toBuilder();

  public abstract boolean isServiceAccountEnabled();

  @Nullable
  public abstract String getServiceAccountJsonKeyFile();

  public abstract boolean isNullCredentialEnabled();

  @Nullable
  public abstract String getTokenServerUrl();

  @Nullable
  public abstract String getProxyAddress();

  @Nullable
  public abstract RedactedString getProxyUsername();

  @Nullable
  public abstract RedactedString getProxyPassword();

  /** Builder for {@link CredentialOptions} */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setServiceAccountEnabled(boolean value);

    public abstract Builder setServiceAccountJsonKeyFile(String serviceAccountJsonKeyFile);

    public abstract Builder setNullCredentialEnabled(boolean nullCredentialEnabled);

    public abstract Builder setTokenServerUrl(String tokenServerUrl);

    public abstract Builder setProxyAddress(String proxyAddress);

    public abstract Builder setProxyUsername(RedactedString proxyUsername);

    public abstract Builder setProxyPassword(RedactedString proxyPassword);

    abstract CredentialOptions autoBuild();

    public CredentialOptions build() {
      CredentialOptions options = autoBuild();

      if (!options.isServiceAccountEnabled()) {
        checkArgument(
            options.isNullCredentialEnabled(),
            "No valid credential configuration discovered: ",
            options);
      }

      return options;
    }
  }
}
