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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;

/**
 * CredentialConfiguration based on configuration objects that implement our Entries interface.
 *
 * When reading configuration this class makes use of a list of key prefixes that are each applied
 * to key suffixes to create a complete configuration key. There is a base prefix of 'google.cloud.'
 * that is included by the builder for each EntriesCredentialConfiguration created. When
 * constructing, other prefixes can be specified. Prefixes specified later can be used to override
 * the values of previously set values. In this way a set of global credentials can be specified
 * for most connectors with an override specified for any connectors that need different
 * credentials.
 */
public class EntriesCredentialConfiguration extends CredentialConfiguration {

  /** The interface to interact with the configuration object. */
  public interface Entries {

    /** Returns the value of an entry. */
    String get(String key);

    /** Sets the value of an entry. */
    void set(String key, String value);

    /** Sets the value of an entry to a boolean value. */
    void setBoolean(String key, boolean value);

    /** Return the vale of an entry */
    String getPassword(String key);
  }

  /**
   * All instances constructed using the builder will use "google.cloud" as the first
   * prefix checked. Other prefixes can be added and will override values in the google.cloud
   * prefix.
   */
  public static final String BASE_KEY_PREFIX = "google.cloud";
  /**
   * Key suffix used to disable service accounts. A value of 'false' will disable the use of
   * service accounts. The default is to use a service account.
   */
  public static final String ENABLE_SERVICE_ACCOUNTS_SUFFIX = ".auth.service.account.enable";
  /** Key suffix used to control which email address is associated with the service account. */
  public static final String SERVICE_ACCOUNT_EMAIL_SUFFIX = ".auth.service.account.email";
  /** Key suffix used to specify private key id for the service account. */
  public static final String SERVICE_ACCOUNT_PRIVATE_KEY_ID_SUFFIX =
      ".auth.service.account.private.key.id";
  /** Key suffix used to specify private key for the service account. */
  public static final String SERVICE_ACCOUNT_PRIVATE_KEY_SUFFIX =
      ".auth.service.account.private.key";
  /**
   * Key suffix used to indicate the path to the service account p12 keyfile. If provided, triggers
   * private keyfile service account authentication. The file will be required to be present on all
   * nodes and at the same location on all nodes.
   */
  public static final String SERVICE_ACCOUNT_KEYFILE_SUFFIX = ".auth.service.account.keyfile";
  /**
   * Key suffix used to indicate the path to a JSON file containing a Service Account key and
   * identifier (email). Technically, this could be a JSON containing a non-service account user,
   * but this setting is only used in the service account flow and is namespaced as such.
   */
  public static final String JSON_KEYFILE_SUFFIX = ".auth.service.account.json.keyfile";
  /**
   * For OAuth-based Installed App authentication, the key suffix specifying the client ID for
   * the credentials.
   */
  public static final String CLIENT_ID_SUFFIX = ".auth.client.id";
  /**
   * For OAuth-based Installed App authentication, the key suffix specifying the client secret
   * for the credentials.
   */
  public static final String CLIENT_SECRET_SUFFIX = ".auth.client.secret";
  /**
   * For OAuth-based Installed App authentication, the key suffix specifying the file containing
   * credentials (JWT).
   */
  public static final String OAUTH_CLIENT_FILE_SUFFIX = ".auth.client.file";
  /**
   * For unit-testing, the key suffix allowing null to be returned from credential creation
   * instead of logging an error and aborting.
   */
  public static final String ENABLE_NULL_CREDENTIAL_SUFFIX = ".auth.null.enable";

  /**
   * Configuration key for setting a proxy for the connector to use to connect to GCS. The proxy
   * must be an HTTP proxy of the form "host:port".
   */
  public static final String PROXY_ADDRESS_KEY = "fs.gs.proxy.address";

  /** Default to no proxy. */
  public static final String PROXY_ADDRESS_DEFAULT = null;

  /**
   * Configuration key for setting a proxy username for the connector to use to authenticate with
   * proxy used to connect to GCS.
   */
  public static final String PROXY_USERNAME_KEY = "fs.gs.proxy.username";

  /** Default to no username. */
  public static final String PROXY_USERNAME_DEFAULT = null;

  /**
   * Configuration key for setting a proxy password for the connector to use to authenticate with
   * proxy used to connect to GCS.
   */
  public static final String PROXY_PASSWORD_KEY = "fs.gs.proxy.password";

  /** Default to no password. */
  public static final String PROXY_PASSWORD_DEFAULT = null;

  /**
   * Configuration key for the name of HttpTransport class to use for connecting to GCS. Must be the
   * name of an HttpTransportFactory.HttpTransportType (APACHE or JAVA_NET).
   */
  public static final String HTTP_TRANSPORT_KEY = "fs.gs.http.transport.type";

  /** Default to the default specified in HttpTransportFactory. */
  public static final HttpTransportFactory.HttpTransportType HTTP_TRANSPORT_DEFAULT =
      HttpTransportFactory.DEFAULT_TRANSPORT_TYPE;

  /** Builder for constructing CredentialConfiguration instances. */
  public abstract static class Builder<
      B extends Builder<B, T>, T extends EntriesCredentialConfiguration> {
    protected List<String> prefixes = new ArrayList<>();
    private Entries configuration;
    private CredentialFactory credentialFactory = new CredentialFactory();

    public Builder () {
      prefixes.add(BASE_KEY_PREFIX);
    }

    /**
     * Return "this" of the appropriate Builder type.
     * Builder subclass must override to return "this".
     */
    protected abstract B self();

    /**
     * Return an instance of the concrete type specified as the second template argument of this
     * builder, optionally performing any type-specific initialization before this builder
     * finishes off the rest of the build() method using methods defined within
     * the EntriesCredentialConfiguration base class.
     * Subclasses must override to return a constructed instance of the concrete class being built.
     */
    protected abstract T beginBuild();

    /** Return the fully-assembled concrete object for which this is a builder. */
    public T build() {
      T concreteCredentialConfiguration = beginBuild();
      if (configuration != null) {
        concreteCredentialConfiguration.setConfiguration(configuration);
      }
      if (credentialFactory != null) {
        concreteCredentialConfiguration.setCredentialFactory(credentialFactory);
      }
      return concreteCredentialConfiguration;
    }

    @VisibleForTesting
    B withCredentialFactory(CredentialFactory credentialFactory) {
      this.credentialFactory = credentialFactory;
      return self();
    }

    public B withConfiguration(Entries configuration) {
      this.configuration = configuration;
      return self();
    }

    public B withOverridePrefix(String prefix) {
      this.prefixes.add(prefix);
      return self();
    }
  }

  /**
   * A builder for use without a subclasses of EntriesCredentialConfiguration.
   */
  public static class EntriesBuilder
      extends Builder<EntriesBuilder, EntriesCredentialConfiguration> {
    @Override
    public EntriesBuilder self() {
      return this;
    }

    @Override
    protected EntriesCredentialConfiguration beginBuild() {
      return new EntriesCredentialConfiguration(prefixes);
    }
  }

  /**
   * Create a builder for this class.
   */
  public static EntriesBuilder newEntriesBuilder() {
    return new EntriesBuilder();
  }

  private static Optional<Boolean> maybeGetBoolean(Entries config, String key) {
    String value = config.get(key);
    if (value == null) {
      return Optional.absent();
    }
    return Optional.of(Boolean.valueOf(value));
  }

  private final List<String> prefixes;
  public EntriesCredentialConfiguration(List<String> prefixes) {
    this.prefixes = ImmutableList.copyOf(prefixes);
  }

  /** Gets our parameters and fills it into the specified configuration.
   * Typically the passed-in configuration is an empty
   * instance of a configuration object that extends Entries.
   */
  public void getConfigurationInto(Entries configuration) {
    for (String prefix : prefixes) {
      configuration.setBoolean(prefix + ENABLE_SERVICE_ACCOUNTS_SUFFIX, isServiceAccountEnabled());
      if (getServiceAccountEmail() != null) {
        configuration.set(prefix + SERVICE_ACCOUNT_EMAIL_SUFFIX, getServiceAccountEmail());
      }
      if (getServiceAccountPrivateKeyId() != null) {
        configuration.set(
            prefix + SERVICE_ACCOUNT_PRIVATE_KEY_ID_SUFFIX, getServiceAccountPrivateKeyId());
      }
      if (getServiceAccountPrivateKey() != null) {
        configuration.set(
            prefix + SERVICE_ACCOUNT_PRIVATE_KEY_SUFFIX, getServiceAccountPrivateKey());
      }
      if (getServiceAccountKeyFile() != null) {
        configuration.set(prefix + SERVICE_ACCOUNT_KEYFILE_SUFFIX, getServiceAccountKeyFile());
      }
      if (getServiceAccountJsonKeyFile() != null) {
        configuration.set(prefix + JSON_KEYFILE_SUFFIX, getServiceAccountJsonKeyFile());
      }
      if (getClientId() != null) {
        configuration.set(prefix + CLIENT_ID_SUFFIX, getClientId());
      }
      if (getClientSecret() != null) {
        configuration.set(prefix + CLIENT_SECRET_SUFFIX, getClientSecret());
      }
      if (getOAuthCredentialFile() != null) {
        configuration.set(prefix + OAUTH_CLIENT_FILE_SUFFIX, getOAuthCredentialFile());
      }
      configuration.setBoolean(prefix + ENABLE_NULL_CREDENTIAL_SUFFIX, isNullCredentialEnabled());
    }

    // Transport configuration does not use prefixes
    if (getProxyAddress() != null) {
      configuration.set(PROXY_ADDRESS_KEY, getProxyAddress());
    }
    if (getProxyUsername() != null) {
      configuration.set(PROXY_USERNAME_KEY, getProxyUsername());
    }
    if (getProxyPassword() != null) {
      configuration.set(PROXY_PASSWORD_KEY, getProxyPassword());
    }
    configuration.set(HTTP_TRANSPORT_KEY, getTransportType().name());
  }

  /**
   * Load configuration values from the provided configuration source. For any key that does not
   * have a corresponding value in the configuration, no changes will be made to the state of this
   * object.
   */
  public void setConfiguration(Entries entries) {
    for (String prefix : prefixes) {
      Optional<Boolean> enableServiceAccounts =
          maybeGetBoolean(entries, prefix + ENABLE_SERVICE_ACCOUNTS_SUFFIX);

      if (enableServiceAccounts.isPresent()) {
        setEnableServiceAccounts(enableServiceAccounts.get());
      }

      String serviceEmailAccount = entries.getPassword(prefix + SERVICE_ACCOUNT_EMAIL_SUFFIX);
      if (serviceEmailAccount != null) {
        setServiceAccountEmail(serviceEmailAccount);
      }

      // Parameters for ServiceAccount directly in Configuration
      String serviceAccountPrivateKeyId =
          entries.getPassword(prefix + SERVICE_ACCOUNT_PRIVATE_KEY_ID_SUFFIX);
      if (serviceAccountPrivateKeyId != null) {
        setServiceAccountPrivateKeyId(serviceAccountPrivateKeyId);
      }
      String serviceAccountPrivateKey =
          entries.getPassword(prefix + SERVICE_ACCOUNT_PRIVATE_KEY_SUFFIX);
      if (serviceAccountPrivateKey != null) {
        setServiceAccountPrivateKey(serviceAccountPrivateKey);
      }

      // Parameters for file based credentials
      String serviceAccountKeyFile = entries.get(prefix + SERVICE_ACCOUNT_KEYFILE_SUFFIX);
      if (serviceAccountKeyFile != null) {
        setServiceAccountKeyFile(serviceAccountKeyFile);
      }

      String serviceAccountJsonKeyFile = entries.get(prefix + JSON_KEYFILE_SUFFIX);
      if (serviceAccountJsonKeyFile != null) {
        setServiceAccountJsonKeyFile(serviceAccountJsonKeyFile);
      }

      String clientId = entries.get(prefix + CLIENT_ID_SUFFIX);
      if (clientId != null) {
        setClientId(clientId);
      }

      String clientSecret = entries.get(prefix + CLIENT_SECRET_SUFFIX);
      if (clientSecret != null) {
        setClientSecret(clientSecret);
      }

      String oAuthCredentialPath = entries.get(prefix + OAUTH_CLIENT_FILE_SUFFIX);
      if (oAuthCredentialPath != null) {
        setOAuthCredentialFile(oAuthCredentialPath);
      }

      Optional<Boolean> enableNullCredential = maybeGetBoolean(entries,
          prefix + ENABLE_NULL_CREDENTIAL_SUFFIX);
      if (enableNullCredential.isPresent()) {
        setNullCredentialEnabled(enableNullCredential.get());
      }
    }

    // Transport configuration does not use prefixes
    String proxyAddress = entries.get(PROXY_ADDRESS_KEY);
    if (proxyAddress != null) {
      setProxyAddress(proxyAddress);
    }
    String proxyUsername = entries.get(PROXY_USERNAME_KEY);
    if (proxyUsername != null) {
      setProxyUsername(proxyUsername);
    }
    String proxyPassword = entries.get(PROXY_PASSWORD_KEY);
    if (proxyPassword != null) {
      setProxyPassword(proxyPassword);
    }

    String transportType = entries.get(HTTP_TRANSPORT_KEY);
    if (transportType != null) {
      setTransportType(HttpTransportFactory.getTransportTypeOf(transportType));
    }
  }
}
