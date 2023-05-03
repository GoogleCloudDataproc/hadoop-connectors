package com.google.cloud.hadoop.gcsio;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.ClientType;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleCloudStorageFileSystemHTTPClientTest
    extends GoogleCloudStorageFileSystemTestBase {

  @Before
  public void before() throws Exception {
    storageClientType = ClientType.HTTP_API_CLIENT;
    super.before();
  }
}
