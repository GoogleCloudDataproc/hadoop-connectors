package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.ClientType;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleHadoopFileSystemJavaStorageClientIntegrationTest
    extends GoogleHadoopFileSystemIntegrationTest {

  @Before
  public void before() throws Exception {
    storageClientType = ClientType.STORAGE_CLIENT;
    super.before();
  }

  @Ignore
  @Test
  public void testImpersonationGroupNameIdentifierUsed() {}

  @Ignore
  @Test
  public void testImpersonationServiceAccountAndUserAndGroupNameIdentifierUsed() {}

  @Ignore
  @Test
  public void testImpersonationServiceAccountUsed() {}

  @Ignore
  @Test
  public void testImpersonationUserAndGroupNameIdentifiersUsed() {}

  @Ignore
  @Test
  public void testImpersonationUserNameIdentifierUsed() {}

  @Ignore
  @Test
  public void unauthenticatedAccessToPublicBuckets_fsGsProperties() {}

  @Ignore
  @Test
  public void unauthenticatedAccessToPublicBuckets_googleCloudProperties() {}
}
