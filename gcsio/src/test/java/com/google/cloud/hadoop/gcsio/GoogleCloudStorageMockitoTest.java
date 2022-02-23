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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTest.newStorageObject;
import static com.google.cloud.hadoop.gcsio.MockGoogleCloudStorageImplFactory.mockedGcs;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for {@link GoogleCloudStorage} class that use Mockito. The underlying Storage API
 * object is mocked, in order to test behavior in response to various types of unexpected
 * exceptions/errors.
 */
@RunWith(JUnit4.class)
public class GoogleCloudStorageMockitoTest {

  private static final String PROJECT_ID = "google.com:foo-project";
  private static final String BUCKET_NAME = "foo-bucket";
  private static final String OBJECT_NAME = "bar-object";

  private GoogleCloudStorageImpl gcs;

  @Mock private ApiErrorExtractor mockErrorExtractor;
  @Mock private BatchHelper.Factory mockBatchFactory;
  @Mock private BatchHelper mockBatchHelper;

  /**
   * Sets up new mocks and create new instance of GoogleCloudStorage configured to only interact
   * with the mocks, run before every test case.
   */
  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    MockHttpTransport transport = mockTransport();
    gcs = mockedGcs(transport);
    gcs.setBatchFactory(mockBatchFactory);
    gcs.setErrorExtractor(mockErrorExtractor);

    when(mockBatchFactory.newBatchHelper(any(), any(Storage.class), anyLong(), anyLong(), anyInt()))
        .thenReturn(mockBatchHelper);
  }

  /**
   * Ensure that each test case precisely captures all interactions with the mocks, since all the
   * mocks represent external dependencies.
   */
  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockErrorExtractor);
    verifyNoMoreInteractions(mockBatchFactory);
    verifyNoMoreInteractions(mockBatchHelper);
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.delete(2).
   */
  @Test
  public void testDeleteObjectApiException() throws IOException {
    // Make the errorExtractor claim that our fake notFoundException.
    GoogleJsonError notFoundError = new GoogleJsonError();
    notFoundError.setMessage("Fake not-found exception");
    GoogleJsonError unexpectedError = new GoogleJsonError();
    unexpectedError.setMessage("Other API exception");

    doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              JsonBatchCallback<StorageObject> getCallback =
                  (JsonBatchCallback<StorageObject>) invocation.getArguments()[1];
              getCallback.onSuccess(
                  newStorageObject(BUCKET_NAME, OBJECT_NAME).setGeneration(1L), new HttpHeaders());
              return null;
            })
        .doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              @SuppressWarnings("unchecked")
              JsonBatchCallback<Void> callback = (JsonBatchCallback<Void>) args[1];
              try {
                callback.onFailure(notFoundError, new HttpHeaders());
              } catch (IOException ioe) {
                fail(ioe.toString());
              }
              return null;
            })
        .doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              JsonBatchCallback<StorageObject> getCallback =
                  (JsonBatchCallback<StorageObject>) invocation.getArguments()[1];
              getCallback.onSuccess(
                  newStorageObject(BUCKET_NAME, OBJECT_NAME).setGeneration(1L), new HttpHeaders());
              return null;
            })
        .doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              @SuppressWarnings("unchecked")
              JsonBatchCallback<Void> callback = (JsonBatchCallback<Void>) args[1];
              try {
                callback.onFailure(unexpectedError, new HttpHeaders());
              } catch (IOException ioe) {
                fail(ioe.toString());
              }
              return null;
            })
        .when(mockBatchHelper)
        .queue(any(), any());
    doReturn(true).doReturn(false).when(mockErrorExtractor).itemNotFound(any(IOException.class));
    when(mockErrorExtractor.preconditionNotMet(any(IOException.class))).thenReturn(false);

    // First time is the notFoundException; expect the impl to ignore it completely.
    try {
      gcs.deleteObjects(Lists.newArrayList(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));
    } catch (Exception e) {
      // Make the test output a little more friendly by specifying why an error may have leaked
      // through.
      fail("Expected no exception when mocking itemNotFound error from API call, got " + e);
    }

    // Second time is the unexpectedException.
    assertThrows(
        IOException.class,
        () ->
            gcs.deleteObjects(Lists.newArrayList(new StorageResourceId(BUCKET_NAME, OBJECT_NAME))));

    verify(mockBatchFactory, times(2))
        .newBatchHelper(any(), any(Storage.class), anyLong(), anyLong(), anyInt());
    verify(mockBatchHelper, times(4)).queue(any(), any());
    verify(mockErrorExtractor, times(2)).itemNotFound(any(IOException.class));
    verify(mockErrorExtractor).preconditionNotMet(any(IOException.class));
    verify(mockBatchHelper, times(2)).flush();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.copy(4) where srcBucketName == dstBucketName.
   */
  @Test
  public void testCopyObjectsApiExceptionSameBucket() throws IOException {
    GoogleJsonError notFoundError = new GoogleJsonError();
    notFoundError.setMessage("Fake not-found exception");
    GoogleJsonError unexpectedError = new GoogleJsonError();
    unexpectedError.setMessage("Other API exception");
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              @SuppressWarnings("unchecked")
              JsonBatchCallback<StorageObject> callback =
                  (JsonBatchCallback<StorageObject>) args[1];
              try {
                callback.onFailure(notFoundError, new HttpHeaders());
              } catch (IOException ioe) {
                fail(ioe.toString());
              }
              return null;
            })
        .doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              @SuppressWarnings("unchecked")
              JsonBatchCallback<StorageObject> callback =
                  (JsonBatchCallback<StorageObject>) args[1];
              try {
                callback.onFailure(unexpectedError, new HttpHeaders());
              } catch (IOException ioe) {
                fail(ioe.toString());
              }
              return null;
            })
        .when(mockBatchHelper)
        .queue(any(Storage.Objects.Copy.class), any());
    doReturn(true).doReturn(false).when(mockErrorExtractor).itemNotFound(any(IOException.class));

    // Make the test output a little more friendly in case the exception class differs.
    String dstObjectName = OBJECT_NAME + "-copy";
    assertThrows(
        FileNotFoundException.class,
        () ->
            gcs.copy(
                BUCKET_NAME, ImmutableList.of(OBJECT_NAME),
                BUCKET_NAME, ImmutableList.of(dstObjectName)));

    assertThrows(
        IOException.class,
        () ->
            gcs.copy(
                BUCKET_NAME, ImmutableList.of(OBJECT_NAME),
                BUCKET_NAME, ImmutableList.of(dstObjectName)));

    verify(mockBatchFactory, times(2))
        .newBatchHelper(any(), any(Storage.class), anyLong(), anyLong(), anyInt());
    verify(mockBatchHelper, times(2)).queue(any(Storage.Objects.Copy.class), any());
    verify(mockErrorExtractor, times(2)).itemNotFound(any(IOException.class));
    verify(mockBatchHelper, times(2)).flush();
  }

  /** Test {@link BatchHelper} error handling. */
  @Test
  public void testGetItemInfosApiException() throws IOException {
    // Set up the return for the Bucket fetch.
    GoogleJsonError unexpectedError = new GoogleJsonError();
    unexpectedError.setMessage("Unexpected API exception ");
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              @SuppressWarnings("unchecked")
              JsonBatchCallback<Bucket> callback = (JsonBatchCallback<Bucket>) args[1];
              try {
                callback.onFailure(unexpectedError, new HttpHeaders());
              } catch (IOException ioe) {
                fail(ioe.toString());
              }
              return null;
            })
        .when(mockBatchHelper)
        .queue(any(Storage.Buckets.Get.class), any());

    // Set up the return for the StorageObject fetch.
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              @SuppressWarnings("unchecked")
              JsonBatchCallback<StorageObject> callback =
                  (JsonBatchCallback<StorageObject>) args[1];
              try {
                callback.onFailure(unexpectedError, new HttpHeaders());
              } catch (IOException ioe) {
                fail(ioe.toString());
              }
              return null;
            })
        .when(mockBatchHelper)
        .queue(any(Storage.Objects.Get.class), any());

    // We will claim both GoogleJsonErrors are unexpected errors.
    when(mockErrorExtractor.itemNotFound(any(IOException.class))).thenReturn(false);

    // Call in order of StorageObject, ROOT, Bucket.
    IOException ioe =
        assertThrows(
            IOException.class,
            () ->
                gcs.getItemInfos(
                    ImmutableList.of(
                        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                        StorageResourceId.ROOT,
                        new StorageResourceId(BUCKET_NAME))));
    assertThat(ioe.getSuppressed()).isNotNull();
    assertThat(ioe.getSuppressed()).hasLength(2);
    // All invocations still should have been attempted; the exception should have been thrown
    // at the very end.
    verify(mockBatchFactory)
        .newBatchHelper(any(), any(Storage.class), anyLong(), anyLong(), anyInt());
    verify(mockBatchHelper).queue(any(Storage.Buckets.Get.class), any());
    verify(mockBatchHelper).queue(any(Storage.Objects.Get.class), any());
    verify(mockErrorExtractor, times(2)).itemNotFound(any(IOException.class));
    verify(mockBatchHelper).flush();
  }
}
