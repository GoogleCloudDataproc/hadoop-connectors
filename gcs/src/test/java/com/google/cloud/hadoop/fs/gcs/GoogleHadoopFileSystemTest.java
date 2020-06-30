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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_LAZY_INITIALIZATION_ENABLE;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.cloud.hadoop.gcsio.MethodOutcome;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.testing.TestingAccessTokenProvider;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unittests for {@link GoogleHadoopFileSystem} class. */
@RunWith(JUnit4.class)
public class GoogleHadoopFileSystemTest extends GoogleHadoopFileSystemIntegrationTest {

  @ClassRule
  public static NotInheritableExternalResource storageResource =
      new NotInheritableExternalResource(GoogleHadoopFileSystemTest.class) {
        @Override
        public void before() throws Throwable {
          // Disable logging.
          // Normally you would need to keep a strong reference to any logger used for
          // configuration, but the "root" logger is always present.
          Logger.getLogger("").setLevel(Level.OFF);

          ghfs = GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopFileSystem();
          ghfsFileSystemDescriptor = (FileSystemDescriptor) ghfs;

          GoogleHadoopFileSystemIntegrationTest.postCreateInit();
        }

        @Override
        public void after() {
          GoogleHadoopFileSystemIntegrationTest.storageResource.after();
        }
      };

  @Test
  public void testVersionString() {
    assertThat(GoogleHadoopFileSystemBase.VERSION).isNotNull();
    assertThat(
            GoogleHadoopFileSystemBase.UNKNOWN_VERSION.equals(GoogleHadoopFileSystemBase.VERSION))
        .isFalse();
  }

  @Test
  public void lazyInitialization_succeeds_withInvalidCredentialsConfiguration() throws Exception {
    new GoogleHadoopFileSystem();
    Configuration lazyConf = new Configuration();
    lazyConf.setBoolean(GCS_LAZY_INITIALIZATION_ENABLE.getKey(), true);
    lazyConf.set(
        GCS_CONFIG_PREFIX + SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.getKey(), "non-existent.json");
    GoogleHadoopFileSystem lazyFs = new GoogleHadoopFileSystem();

    lazyFs.initialize(new URI("gs://test-non-existent/"), lazyConf);
    lazyFs.close();
  }

  @Test
  public void lazyInitialization_deleteCall_fails_withInvalidCredentialsConfiguration()
      throws Exception {
    Configuration lazyConf = new Configuration();
    lazyConf.setBoolean(GCS_LAZY_INITIALIZATION_ENABLE.getKey(), true);
    lazyConf.set(
        GCS_CONFIG_PREFIX + SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.getKey(), "non-existent.json");
    GoogleHadoopFileSystem lazyFs = new GoogleHadoopFileSystem();

    lazyFs.initialize(new URI("gs://test-non-existent"), lazyConf);

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> lazyFs.delete(new Path("gs://test-non-existent/dir"), false));

    assertThat(exception).hasMessageThat().isEqualTo("Failed to create GCS FS");
    assertThat(exception).hasCauseThat().isInstanceOf(FileNotFoundException.class);
    assertThat(exception)
        .hasCauseThat()
        .hasMessageThat()
        .isAnyOf(
            "non-existent.json (No such file or directory)",
            "non-existent.json (The system cannot find the file specified)");

    lazyFs.close();
  }

  @Test
  public void eagerInitialization_fails_withInvalidCredentialsConfiguration() {
    new GoogleHadoopFileSystem();
    Configuration eagerConf = new Configuration();
    eagerConf.setBoolean(GCS_LAZY_INITIALIZATION_ENABLE.getKey(), false);
    eagerConf.set(
        GCS_CONFIG_PREFIX + SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.getKey(), "non-existent.json");
    FileSystem eagerFs = new GoogleHadoopFileSystem();

    FileNotFoundException exception =
        assertThrows(
            FileNotFoundException.class,
            () -> eagerFs.initialize(new URI("gs://test-non-existent"), eagerConf));

    assertThat(exception)
        .hasMessageThat()
        .isAnyOf(
            "non-existent.json (No such file or directory)",
            "non-existent.json (The system cannot find the file specified)");
  }

  // -----------------------------------------------------------------
  // Tests that exercise behavior defined in HdfsBehavior.
  // -----------------------------------------------------------------

  /** Validates {@link GoogleHadoopFileSystem#rename(Path, Path)}. */
  @Test
  @Override
  public void testRename() throws Exception {
    renameHelper(
        new HdfsBehavior() {
          /**
           * Returns the {@link MethodOutcome} of trying to rename an existing file into the root
           * directory.
           */
          @Override
          public MethodOutcome renameFileIntoRootOutcome() {
            return new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE);
          }
        });
  }

  @Test
  public void testCouldUseFlatGlob() throws IOException, URISyntaxException {
    Configuration lazyConf = new Configuration();
    lazyConf.setBoolean(GCS_LAZY_INITIALIZATION_ENABLE.getKey(), true);
    try (GoogleHadoopFileSystem lazyFs = new GoogleHadoopFileSystem()) {
      assertThat(lazyFs.couldUseFlatGlob(new Path(new URI("gs://**/test/")))).isFalse();
    }
  }

  @Test
  public void testTrimToPrefixWithoutGlob() {
    GoogleHadoopFileSystem lazyFs = new GoogleHadoopFileSystem();
    lazyFs.trimToPrefixWithoutGlob("gs://**/test");
    assertThat(lazyFs.trimToPrefixWithoutGlob("gs://**/test")).isEqualTo("gs://");
  }

  @Override
  @Test
  public void testGetGcsPath() throws URISyntaxException {
    GoogleHadoopFileSystem myghfs = (GoogleHadoopFileSystem) ghfs;
    URI gcsPath = new URI("gs://" + myghfs.getUri().getAuthority() + "/dir/obj");
    assertThat(myghfs.getGcsPath(new Path(gcsPath))).isEqualTo(gcsPath);
  }

  @Test
  public void testGetDefaultPortIndicatesPortsAreNotUsed() throws Exception {
    Configuration config = new Configuration();
    config.set("fs.gs.auth.access.token.provider.impl", TestingAccessTokenProvider.class.getName());
    URI gsUri = new URI("gs://foobar/");

    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gsUri, config);

    assertThat(ghfs.getDefaultPort()).isEqualTo(-1);
  }

  @Test
  public void testImpsersonationServiceAccountUsed() throws Exception {
    Configuration config = new Configuration();
    config.setClass(
        "fs.gs.auth.access.token.provider.impl",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        GCS_CONFIG_PREFIX + IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey(), "test-service-account");

    URI gsUri = new URI("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();

    Exception exception =
        assertThrows(
            GoogleJsonResponseException.class,
            () -> {
              ghfs.initialize(gsUri, config);
            });
    assertThat(exception).hasMessageThat().startsWith("401 Unauthorized");
  }

  @Test
  public void testImpsersonationUserNameIdentifierUsed() throws Exception {
    Configuration config = new Configuration();
    config.setClass(
        "fs.gs.auth.access.token.provider.impl",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        GCS_CONFIG_PREFIX
            + USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey()
            + UserGroupInformation.getCurrentUser().getShortUserName(),
        "test-service-account");

    URI gsUri = new URI("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();

    Exception exception =
        assertThrows(
            GoogleJsonResponseException.class,
            () -> {
              ghfs.initialize(gsUri, config);
            });
    assertThat(exception).hasMessageThat().startsWith("401 Unauthorized");
  }

  @Test
  public void testImpsersonationGroupNameIdentifierUsed() throws Exception {
    Configuration config = new Configuration();
    config.setClass(
        "fs.gs.auth.access.token.provider.impl",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        GCS_CONFIG_PREFIX
            + GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey()
            + UserGroupInformation.getCurrentUser().getGroupNames()[0],
        "test-service-account");

    URI gsUri = new URI("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();

    Exception exception =
        assertThrows(
            GoogleJsonResponseException.class,
            () -> {
              ghfs.initialize(gsUri, config);
            });
    assertThat(exception).hasMessageThat().startsWith("401 Unauthorized");
  }

  @Test
  public void testImpsersonationUserAndGroupNameIdentifiersUsed() throws Exception {
    Configuration config = new Configuration();
    config.setClass(
        "fs.gs.auth.access.token.provider.impl",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        GCS_CONFIG_PREFIX
            + USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey()
            + UserGroupInformation.getCurrentUser().getShortUserName(),
        "test-service-account1");
    config.set(
        GCS_CONFIG_PREFIX
            + GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey()
            + UserGroupInformation.getCurrentUser().getGroupNames()[0],
        "test-service-account2");

    URI gsUri = new URI("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();

    Exception exception =
        assertThrows(
            GoogleJsonResponseException.class,
            () -> {
              ghfs.initialize(gsUri, config);
            });
    assertThat(exception).hasMessageThat().startsWith("401 Unauthorized");
  }

  @Test
  public void testImpsersonationServiceAccountAndUserAndGroupNameIdentifierUsed() throws Exception {
    Configuration config = new Configuration();
    config.setClass(
        "fs.gs.auth.access.token.provider.impl",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        GCS_CONFIG_PREFIX + IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey(), "test-service-account1");
    config.set(
        GCS_CONFIG_PREFIX
            + USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey()
            + UserGroupInformation.getCurrentUser().getShortUserName(),
        "test-service-account2");
    config.set(
        GCS_CONFIG_PREFIX
            + GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey()
            + UserGroupInformation.getCurrentUser().getGroupNames()[0],
        "test-service-account3");

    URI gsUri = new URI("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();

    Exception exception =
        assertThrows(
            GoogleJsonResponseException.class,
            () -> {
              ghfs.initialize(gsUri, config);
            });
    assertThat(exception).hasMessageThat().startsWith("401 Unauthorized");
  }

  @Test
  public void testImpsersonationInvalidUserNameIdentifierUsed() throws Exception {
    Configuration config = new Configuration();
    config.setClass(
        "fs.gs.auth.access.token.provider.impl",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        GCS_CONFIG_PREFIX + USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey() + "invalid-user",
        "test-service-account");

    URI gsUri = new URI("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gsUri, config);
  }

  // -----------------------------------------------------------------
  // Inherited tests that we suppress because their behavior differs
  // from the base class.
  // TODO(user): Add tests for subtleties of how global-rooted
  // initialization differs from bucket-rooted initialization.
  // -----------------------------------------------------------------
  @Override
  public void testInitializeSuccess() {}

  @Override
  public void testInitializeSucceedsWhenNoProjectIdConfigured() {}

  @Override
  public void testInitializeWithWorkingDirectory() {}

  @Override
  public void testIOExceptionIsThrowAfterClose() {}

  @Override
  public void testFileSystemIsRemovedFromCacheOnClose() {}

  @Override
  public void testConfigurablePermissions() {}

  @Override
  public void testFileStatusUser() {}

  @Override
  public void testCrc32cFileChecksum() {}

  @Override
  public void testMd5FileChecksum() {}
}
