/*
 * Copyright 2025 Google LLC
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

package com.google.cloud.hadoop.gcsio;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.ClientType;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class GoogleCloudStorageFileSystemBidiTest extends GoogleCloudStorageFileSystemTestBase {

  @Before
  @Override
  public void before() throws Exception {
    storageClientType = ClientType.STORAGE_CLIENT;
    super.before();
  }

  @Parameters(name = "bidiEnabled=true")
  public static Collection<Object[]> getParameters() {
    return Collections.singletonList(new Object[] {ClientType.STORAGE_CLIENT, true});
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
