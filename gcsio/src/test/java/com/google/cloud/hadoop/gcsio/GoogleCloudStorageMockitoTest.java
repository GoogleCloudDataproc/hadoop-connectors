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

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTest.newStorageObject;
import static com.google.cloud.hadoop.gcsio.MockGoogleCloudStorageImplFactory.mockedGcsImpl;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.Timestamp;
import com.google.storage.control.v2.CreateFolderRequest;
import com.google.storage.control.v2.Folder;
import com.google.storage.control.v2.FolderName;
import com.google.storage.control.v2.GetFolderRequest;
import com.google.storage.control.v2.StorageControlClient;
import com.google.storage.control.v2.stub.StorageControlStub;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
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
  @Mock private StorageControlStub mockStorageControlStub;
  @Mock private UnaryCallable<CreateFolderRequest, Folder> mockCreateFolderCallable;
  @Mock private UnaryCallable<GetFolderRequest, Folder> mockGetFolderCallable;
  @Mock private AlreadyExistsException mockAlreadyExistsException;
  @Mock PermissionDeniedException mockPermissionDeniedException;
  @Mock private Storage.Buckets mockBuckets;
  @Mock private Storage.Buckets.Get mockGetBucket;

  /**
   * Sets up new mocks and create new instance of GoogleCloudStorage configured to only interact
   * with the mocks, run before every test case.
   */
  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);

    MockHttpTransport transport = mockTransport();
    GoogleCloudStorageOptions gcsOptions =
        GoogleCloudStorageOptions.builder()
            .setAppName("gcsio-unit-test")
            .setProjectId(PROJECT_ID)
            .setCopyWithRewriteEnabled(false)
            .build();
    gcs = mockedGcsImpl(gcsOptions, transport);
    gcs.setBatchFactory(mockBatchFactory);
    gcs.setErrorExtractor(mockErrorExtractor);

    StorageControlClient testClient = StorageControlClient.create(mockStorageControlStub);
    gcs.setStorageControlClient(testClient);

    when(mockBatchFactory.newBatchHelper(
            any(), any(Storage.class), anyInt(), anyInt(), anyInt(), any()))
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
        .newBatchHelper(any(), any(Storage.class), anyInt(), anyInt(), anyInt(), any());
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
        .newBatchHelper(any(), any(Storage.class), anyInt(), anyInt(), anyInt(), any());
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
        .newBatchHelper(any(), any(Storage.class), anyInt(), anyInt(), anyInt(), any());
    verify(mockBatchHelper).queue(any(Storage.Buckets.Get.class), any());
    verify(mockBatchHelper).queue(any(Storage.Objects.Get.class), any());
    verify(mockErrorExtractor, times(2)).itemNotFound(any(IOException.class));
    verify(mockBatchHelper).flush();
  }

  @Test
  public void testCreateFolder_nonRecursive_Success() throws IOException {
    StorageResourceId folderId = new StorageResourceId(BUCKET_NAME, "new-folder/");
    Folder apiResponse = Folder.newBuilder().setName("folders/new-folder/").build();

    // mock STUB to return the mock CALLABLE.
    when(mockStorageControlStub.createFolderCallable()).thenReturn(mockCreateFolderCallable);

    when(mockCreateFolderCallable.call(any(CreateFolderRequest.class))).thenReturn(apiResponse);

    gcs.createFolder(folderId, /* recursive= */ false);

    ArgumentCaptor<CreateFolderRequest> requestCaptor =
        ArgumentCaptor.forClass(CreateFolderRequest.class);

    // Verify the stub and callable were invoked correctly and capture the argument.
    verify(mockStorageControlStub, times(1)).createFolderCallable();
    verify(mockCreateFolderCallable, times(1)).call(requestCaptor.capture());

    CreateFolderRequest capturedRequest = requestCaptor.getValue();
    assertThat(capturedRequest.getParent()).isEqualTo("projects/_/buckets/" + BUCKET_NAME);
    assertThat(capturedRequest.getFolderId()).isEqualTo("new-folder/");
    assertThat(capturedRequest.getRecursive()).isFalse();
  }

  @Test
  public void testCreateFolder_nonRecursive_alreadyExists_isRethrownAsFileAlreadyExists() {
    StorageResourceId folderId = new StorageResourceId(BUCKET_NAME, "existing-folder/");

    // Stub the stub to return the mock callable.
    when(mockStorageControlStub.createFolderCallable()).thenReturn(mockCreateFolderCallable);

    // Stub the callable to throw the MOCKED exception.
    when(mockCreateFolderCallable.call(any(CreateFolderRequest.class)))
        .thenThrow(mockAlreadyExistsException);

    FileAlreadyExistsException exception =
        assertThrows(FileAlreadyExistsException.class, () -> gcs.createFolder(folderId, false));

    assertThat(exception.getCause()).isEqualTo(mockAlreadyExistsException);
    assertThat(exception.getMessage()).contains("already exists");

    verify(mockStorageControlStub).createFolderCallable();
    verify(mockCreateFolderCallable).call(any(CreateFolderRequest.class));
  }

  @Test
  public void testCreateFolder_nonRecursive_permissionDenied_isRethrown() {
    StorageResourceId folderId = new StorageResourceId(BUCKET_NAME, "protected-folder/");
    when(mockStorageControlStub.createFolderCallable()).thenReturn(mockCreateFolderCallable);
    when(mockCreateFolderCallable.call(any(CreateFolderRequest.class)))
        .thenThrow(mockPermissionDeniedException);

    PermissionDeniedException exception =
        assertThrows(PermissionDeniedException.class, () -> gcs.createFolder(folderId, false));

    assertThat(exception).isEqualTo(mockPermissionDeniedException);
    verify(mockStorageControlStub).createFolderCallable();
  }

  @Test
  public void testCreateFolder_recursive_Success() throws IOException {
    StorageResourceId folderId = new StorageResourceId(BUCKET_NAME, "a/b/c/");
    Folder apiResponse = Folder.newBuilder().setName("folders/a/b/c/").build();

    when(mockStorageControlStub.createFolderCallable()).thenReturn(mockCreateFolderCallable);
    when(mockCreateFolderCallable.call(any(CreateFolderRequest.class))).thenReturn(apiResponse);

    gcs.createFolder(folderId, /* recursive= */ true);

    ArgumentCaptor<CreateFolderRequest> requestCaptor =
        ArgumentCaptor.forClass(CreateFolderRequest.class);

    verify(mockStorageControlStub).createFolderCallable();
    verify(mockCreateFolderCallable).call(requestCaptor.capture());

    CreateFolderRequest capturedRequest = requestCaptor.getValue();
    assertThat(capturedRequest.getParent()).isEqualTo("projects/_/buckets/" + BUCKET_NAME);
    assertThat(capturedRequest.getFolderId()).isEqualTo("a/b/c/");
    assertThat(capturedRequest.getRecursive()).isTrue();
  }

  @Test
  public void testCreateFolder_recursive_alreadyExists_isRethrownAsFileAlreadyExists()
      throws IOException {
    StorageResourceId folderId = new StorageResourceId(BUCKET_NAME, "existing-folder/");

    when(mockStorageControlStub.createFolderCallable()).thenReturn(mockCreateFolderCallable);
    when(mockCreateFolderCallable.call(any(CreateFolderRequest.class)))
        .thenThrow(mockAlreadyExistsException);

    assertThrows(
        FileAlreadyExistsException.class, () -> gcs.createFolder(folderId, /* recursive= */ true));

    verify(mockStorageControlStub).createFolderCallable();
    verify(mockCreateFolderCallable).call(any(CreateFolderRequest.class));
  }

  @Test
  public void testCreateFolder_recursive_permissionDenied_isRethrown() {
    StorageResourceId folderId = new StorageResourceId(BUCKET_NAME, "a/b/protected/");

    when(mockStorageControlStub.createFolderCallable()).thenReturn(mockCreateFolderCallable);
    when(mockCreateFolderCallable.call(any(CreateFolderRequest.class)))
        .thenThrow(mockPermissionDeniedException);

    PermissionDeniedException exception =
        assertThrows(PermissionDeniedException.class, () -> gcs.createFolder(folderId, true));

    assertThat(exception).isEqualTo(mockPermissionDeniedException);

    verify(mockStorageControlStub).createFolderCallable();
    verify(mockCreateFolderCallable).call(any(CreateFolderRequest.class));
  }

  @Test
  public void testGetFolderInfo_Success() throws IOException {
    StorageResourceId folderId = new StorageResourceId(BUCKET_NAME, "existing-folder/");
    String folderName = FolderName.of("_", BUCKET_NAME, "existing-folder/").toString();

    Folder apiResponse =
        Folder.newBuilder()
            .setName(folderName)
            .setMetageneration(1L)
            .setCreateTime(
                Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build())
            .build();

    // Stub the stub to return the mock callable.
    when(mockStorageControlStub.getFolderCallable()).thenReturn(mockGetFolderCallable);

    when(mockGetFolderCallable.call(any(GetFolderRequest.class))).thenReturn(apiResponse);

    GoogleCloudStorageItemInfo itemInfo = gcs.getFolderInfo(folderId);

    assertThat(itemInfo).isNotNull();
    assertThat(itemInfo.exists()).isTrue();
    assertThat(itemInfo.getResourceId()).isEqualTo(folderId);
    assertThat(itemInfo.isNativeHNSFolder()).isTrue();

    verify(mockStorageControlStub).getFolderCallable();
    verify(mockGetFolderCallable).call(any(GetFolderRequest.class));
  }

  @Test
  public void testGetFolderInfo_whenApiThrowsPermissionDenied_swallowsErrorAndReturnsNotFound()
      throws IOException {
    StorageResourceId folderId = new StorageResourceId(BUCKET_NAME, "permission-denied-folder/");
    when(mockStorageControlStub.getFolderCallable()).thenReturn(mockGetFolderCallable);
    when(mockGetFolderCallable.call(any(GetFolderRequest.class)))
        .thenThrow(mockPermissionDeniedException);

    GoogleCloudStorageItemInfo itemInfo = gcs.getFolderInfo(folderId);

    assertThat(itemInfo).isNotNull();
    assertThat(itemInfo.exists()).isFalse();
    verify(mockStorageControlStub).getFolderCallable();
  }

  /** Verifies that for bucket resources, valid bucket resource is returned */
  @Test
  public void testGetFolderInfo_whenResourceIdIsBucket_returnsNotFound() throws IOException {
    StorageResourceId bucketResourceId = new StorageResourceId(BUCKET_NAME);

    Bucket fakeBucket =
        new Bucket()
            .setName(BUCKET_NAME)
            .setTimeCreated(new com.google.api.client.util.DateTime(System.currentTimeMillis()))
            .setUpdated(new com.google.api.client.util.DateTime(System.currentTimeMillis()));

    Storage spiedStorage = spy(gcs.storage);
    doReturn(mockBuckets).when(spiedStorage).buckets();
    doReturn(mockGetBucket).when(mockBuckets).get(BUCKET_NAME);
    doReturn(fakeBucket).when(mockGetBucket).execute();
    gcs.storage = spiedStorage;

    GoogleCloudStorageItemInfo itemInfo = gcs.getFolderInfo(bucketResourceId);

    assertThat(itemInfo).isNotNull();
    assertThat(itemInfo.exists()).isTrue();
    assertThat(itemInfo.isBucket()).isTrue();
    assertThat(itemInfo.getResourceId()).isEqualTo(bucketResourceId);

    // Verify that the folder-specific API was not called
    verify(mockStorageControlStub, never()).getFolderCallable();
  }
}
