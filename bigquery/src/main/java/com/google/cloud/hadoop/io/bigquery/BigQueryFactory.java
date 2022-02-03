/*
 * Copyright 2017 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.io.bigquery;

import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.BIGQUERY_CONFIG_PREFIX;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.BQ_ROOT_URL;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.auth.Credentials;
import com.google.cloud.hadoop.util.AccessTokenProviderCredentialsFactory;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import com.google.cloud.hadoop.util.PropertyUtil;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.apache.hadoop.conf.Configuration;

/** Helper class to get BigQuery from environment credentials. */
public class BigQueryFactory {

  // Environment variable name for variable specifying path of SA JSON key file for BigQuery
  // authentication.
  public static final String BIGQUERY_SERVICE_ACCOUNT_JSON_KEYFILE =
      "BIGQUERY_SERVICE_ACCOUNT_JSON_KEYFILE";

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // A resource file containing bigquery related build properties.
  public static final String PROPERTIES_FILE = "bigquery.properties";

  // The key in the PROPERTIES_FILE that contains the version built.
  public static final String VERSION_PROPERTY = "bigquery.connector.version";

  // The version returned when one cannot be found in properties.
  public static final String UNKNOWN_VERSION = "0.0.0";

  // Current version.
  public static final String VERSION;

  // Identifies this version of the Hadoop BigQuery Connector library.
  public static final String BQC_ID;

  /** Static instance of the BigQueryFactory. */
  public static final BigQueryFactory INSTANCE = new BigQueryFactory();

  static {
    VERSION =
        PropertyUtil.getPropertyOrDefault(
            BigQueryFactory.class, PROPERTIES_FILE, VERSION_PROPERTY, UNKNOWN_VERSION);
    logger.atInfo().log("BigQuery connector version %s", VERSION);
    BQC_ID = String.format("Hadoop BigQuery Connector/%s", VERSION);
  }

  // Objects for handling HTTP transport and JSON formatting of API calls
  private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  /**
   * Construct credentials from the passed Configuration.
   *
   * @throws IOException on IO Error.
   * @throws GeneralSecurityException on General Security Error.
   */
  public Credentials createBigQueryCredential(Configuration config)
      throws GeneralSecurityException, IOException {
    Credentials credentials =
        AccessTokenProviderCredentialsFactory.credentials(
            config, ImmutableList.of(BIGQUERY_CONFIG_PREFIX));
    return credentials == null
        ? HadoopCredentialConfiguration.getCredentialsFactory(config, BIGQUERY_CONFIG_PREFIX)
            .getCredentials()
        : credentials;
  }

  /** Constructs a BigQueryHelper from a raw Bigquery constructed with {@link #getBigQuery}. */
  public BigQueryHelper getBigQueryHelper(Configuration config)
      throws GeneralSecurityException, IOException {
    return new BigQueryHelper(getBigQuery(config));
  }

  /**
   * Constructs a BigQuery from the credential constructed from the environment.
   *
   * @throws IOException on IO Error.
   * @throws GeneralSecurityException on General Security Error.
   */
  public Bigquery getBigQuery(Configuration config) throws GeneralSecurityException, IOException {
    logger.atInfo().log("Creating BigQuery from default credential.");
    Credentials credentials = createBigQueryCredential(config);
    // Use the credential to create an authorized BigQuery client
    return getBigQueryFromCredential(config, credentials, BQC_ID);
  }

  /** Constructs a BigQuery from a given Credential. */
  public Bigquery getBigQueryFromCredential(
      Configuration config, Credentials credentials, String appName) {
    logger.atInfo().log("Creating BigQuery from given credential.");
    // Use the credential to create an authorized BigQuery client
    if (credentials != null) {
      return new Bigquery.Builder(
              HTTP_TRANSPORT,
              JSON_FACTORY,
              new RetryHttpInitializer(
                  /* delegate= */ null,
                  credentials,
                  RetryHttpInitializerOptions.builder().setDefaultUserAgent(appName).build()))
          .setApplicationName(appName)
          .build();
    }
    return new Bigquery.Builder(HTTP_TRANSPORT, JSON_FACTORY, /* httpRequestInitializer= */ null)
        .setRootUrl(BQ_ROOT_URL.get(config, config::get))
        .setApplicationName(appName)
        .build();
  }
}
