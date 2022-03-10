/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio;

import static org.junit.Assert.assertThrows;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.storage.Storage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests storage requests creation and runtime behavior. */
@RunWith(JUnit4.class)
public class StorageRequestFactoryTest {

  public static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
  public static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

  private Storage storage;

  @Before
  public void setUp() {
    storage = new Storage(HTTP_TRANSPORT, JSON_FACTORY, /* httpRequestInitializer= */ null);
  }

  @Test
  public void throwExceptionWhenCallExecute() {
    String BUCKET = "TEST_BUCKET";
    String OBJECT = "TEST_OBJECT";
    assertThrows(
        StorageRequestFactory.WrongRequestTypeException.class,
        new StorageRequestFactory(storage).objectsGetData(BUCKET, OBJECT)::execute);
  }

  @Test
  public void throwExceptionWhenCallExecuteMedia() {
    String BUCKET = "TEST_BUCKET";
    String OBJECT = "TEST_OBJECT";
    assertThrows(
        StorageRequestFactory.WrongRequestTypeException.class,
        new StorageRequestFactory(storage).objectsGetMetadata(BUCKET, OBJECT)::executeMedia);
  }
}
