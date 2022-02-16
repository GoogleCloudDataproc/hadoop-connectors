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
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.auth.Credentials;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration;
import com.google.cloud.hadoop.util.PropertyUtil;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.apache.hadoop.conf.Configuration;

/** Helper class to get BigQuery from environment credentials. */
public class BigQueryFactory {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Environment variable name for variable specifying path of SA JSON keyfile for BigQuery
  // authentication.
  public static final String BIGQUERY_SERVICE_ACCOUNT_JSON_KEYFILE =
      "BIGQUERY_SERVICE_ACCOUNT_JSON_KEYFILE";

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

  /**
   * Construct credentials from the passed Configuration.
   *
   * @throws IOException on IO Error.
   */
  public Credentials createBigQueryCredentials(Configuration config) throws IOException {
    return HadoopCredentialsConfiguration.getCredentials(config, BIGQUERY_CONFIG_PREFIX);
  }

  /** Constructs a BigQueryHelper from a raw Bigquery constructed with {@link #getBigQuery}. */
  public BigQueryHelper getBigQueryHelper(Configuration config)
      throws GeneralSecurityException, IOException {
    return new BigQueryHelper(getBigQuery(config));
  }

  /**
   * Constructs a BigQuery from the credentials constructed from the environment.
   *
   * @throws IOException on IO Error.
   * @throws GeneralSecurityException on General Security Error.
   */
  public Bigquery getBigQuery(Configuration config) throws GeneralSecurityException, IOException {
    logger.atInfo().log("Creating BigQuery from default credentials.");
    Credentials credentials = createBigQueryCredentials(config);
    // Use the credentials to create an authorized BigQuery client
    return getBigQueryFromCredentials(config, credentials, BQC_ID);
  }

  /** Constructs a BigQuery from a given Credentials. */
  public Bigquery getBigQueryFromCredentials(
      Configuration config, Credentials credentials, String appName) {
    logger.atFine().log("Creating BigQuery from given credentials.");
    return new Bigquery.Builder(
            HTTP_TRANSPORT,
            GsonFactory.getDefaultInstance(),
            new RetryHttpInitializer(
                credentials,
                RetryHttpInitializerOptions.builder().setDefaultUserAgent(appName).build()))
        .setApplicationName(appName)
        .setRootUrl(BQ_ROOT_URL.get(config, config::get))
        .build();
  }
}
