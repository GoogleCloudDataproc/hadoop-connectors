/*
 * Copyright 2019 Google LLC
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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.gcsio.testing.TestConfiguration.GCS_TEST_PROJECT_ID;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.ClientType;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AuthenticationType;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;

public final class GoogleHadoopFileSystemIntegrationHelper {

  private static final String ENV_VAR_MSG_FMT = "Environment variable %s should be set";

  public static GoogleHadoopFileSystem createGhfs(URI path, Configuration config) throws Exception {
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(path, config);
    return ghfs;
  }

  /**
   * Helper to load all the GHFS-specific config values from environment variables, such as those
   * needed for setting up the credentials of a real GoogleCloudStorage.
   */
  public static Configuration getTestConfig() {
    Configuration config = new Configuration();
    config.setBoolean("fs.gs.implicit.dir.repair.enable", true);
    // Allow buckets to be deleted in test cleanup:
    config.setBoolean("fs.gs.bucket.delete.enable", true);

    // Configure test authentication
    TestConfiguration testConf = TestConfiguration.getInstance();
    String projectId = checkNotNull(testConf.getProjectId(), ENV_VAR_MSG_FMT, GCS_TEST_PROJECT_ID);
    config.set("fs.gs.project.id", projectId);

    Boolean isApplicationDefaultModeEnabled =
        TestConfiguration.getInstance().isApplicationDefaultModeEnabled();

    if (testConf.getServiceAccountJsonKeyFile() != null) {
      config.setEnum("fs.gs.auth.type", AuthenticationType.SERVICE_ACCOUNT_JSON_KEYFILE);
      config.set(
          "fs.gs.auth.service.account.json.keyfile", testConf.getServiceAccountJsonKeyFile());

    } else if (isApplicationDefaultModeEnabled) {
      config.setEnum("fs.gs.auth.type", AuthenticationType.APPLICATION_DEFAULT);
    }

    config.setBoolean("fs.gs.grpc.directpath.enable", testConf.isDirectPathPreferred());

    return config;
  }

  public static Configuration getBidiTestConfiguration() {
    Configuration config = getTestConfig();
    config.setBoolean("fs.gs.bidi.enable", true);
    config.setEnum("fs.gs.client.type", ClientType.STORAGE_CLIENT);
    config.setBoolean("fs.gs.grpc.enable", true);
    return config;
  }

  private GoogleHadoopFileSystemIntegrationHelper() {}
}
