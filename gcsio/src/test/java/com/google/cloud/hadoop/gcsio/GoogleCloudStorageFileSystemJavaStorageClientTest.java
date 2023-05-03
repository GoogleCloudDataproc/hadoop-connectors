package com.google.cloud.hadoop.gcsio;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.ClientType;
import java.io.IOException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleCloudStorageFileSystemJavaStorageClientTest
    extends GoogleCloudStorageFileSystemTestBase {

  @Before
  public void before() throws Exception {
    storageClientType = ClientType.STORAGE_CLIENT;
    super.before();
  }

  @Override
  @Ignore("DirectPath is not supported with null credentials")
  @Test
  public void testConstructor() throws IOException {}

  @Override
  @Ignore("DirectPath is not supported with null credentials")
  @Test
  public void testClientType() {}
}
