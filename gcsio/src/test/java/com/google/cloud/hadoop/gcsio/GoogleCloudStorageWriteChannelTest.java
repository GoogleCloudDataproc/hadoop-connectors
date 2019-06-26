/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.*;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GoogleCloudStorageWriteChannel} class. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageWriteChannelTest {

  @Test
  public void testKmsKeyName_shouldBeSetInRequest() throws IOException {
    int footeSize = 2;
    String kmsKeyName = "test";
    byte[] testData = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09};
    int footerStart = testData.length - footeSize;
    byte[] footer = Arrays.copyOfRange(testData, footerStart, testData.length);

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            // Footer prefetch response
            dataRangeResponse(footer, footerStart, testData.length),
            // Footer read miss request response
            dataResponse(new byte[] {testData[footerStart - 1]}));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, JSON_FACTORY, requests::add);

    ClientRequestHelper<StorageObject> requestHelper = new ClientRequestHelper<StorageObject>();
    GoogleCloudStorageWriteChannel writeChannel =
        new GoogleCloudStorageWriteChannel(
            MoreExecutors.newDirectExecutorService(),
            storage,
            requestHelper,
            BUCKET_NAME,
            OBJECT_NAME,
            "content-type",
            kmsKeyName,
            AsyncWriteChannelOptions.builder().build(),
            new ObjectWriteConditions(),
            null);
    InputStreamContent inputStreamContent =
        new InputStreamContent(
            "",
            new InputStream() {
              @Override
              public int read() throws IOException {
                return 0;
              }
            });

    Storage.Objects.Insert insert = writeChannel.createRequest(inputStreamContent);
    assertThat(insert.getKmsKeyName()).isEqualTo(kmsKeyName);
  }
}
