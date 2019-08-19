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

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.PATH_DELIMITER;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.createItemInfoForBucket;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.createItemInfoForStorageObject;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo.createInferredDirectory;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.HTTP_TRANSPORT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.JSON_FACTORY;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.dataResponse;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.emptyResponse;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.fakeResponse;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.jsonDataResponse;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.resumableUploadResponse;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.copyRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.deleteBucketRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.deleteRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getBucketRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listBucketsRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.resumableUploadChunkRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.resumableUploadRequestString;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.DateTime;
import com.google.api.client.util.NanoClock;
import com.google.api.client.util.Sleeper;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Buckets;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.BackOffFactory;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;
import javax.net.ssl.SSLException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

/**
 * UnitTests for GoogleCloudStorage class. The underlying Storage API object is mocked, in order
 * to test behavior in response to various types of unexpected exceptions/errors.
 */
@RunWith(JUnit4.class)
public class GoogleCloudStorageTest {
  private static final String APP_NAME = "GCS-Unit-Test";
  private static final String PROJECT_ID = "google.com:foo-project";
  private static final String BUCKET_NAME = "foo-bucket";
  private static final String OBJECT_NAME = "bar-object";
  private static final String[][] ILLEGAL_OBJECTS = {
      {null, "bar-object"}, {"foo-bucket", null}, {"", "bar-object"}, {"foo-bucket", ""}
  };

  private static final GoogleCloudStorageOptions GCS_OPTIONS =
      GoogleCloudStorageOptions.builder()
          .setProjectId(PROJECT_ID)
          .setAppName("gcs-impl-test")
          .build();

  private static final ImmutableMap<String, byte[]> EMPTY_METADATA = ImmutableMap.of();

  private ExecutorService executorService;

  @Mock private Storage mockStorage;
  @Mock private Storage.Objects mockStorageObjects;
  @Mock private Storage.Objects.Insert mockStorageObjectsInsert;
  @Mock private Storage.Objects.Delete mockStorageObjectsDelete;
  @Mock private Storage.Objects.Get mockStorageObjectsGet;
  @Mock private Storage.Objects.Copy mockStorageObjectsCopy;
  @Mock private Storage.Objects.Compose mockStorageObjectsCompose;
  @Mock private Storage.Objects.List mockStorageObjectsList;
  @Mock private Storage.Buckets mockStorageBuckets;
  @Mock private Storage.Buckets.Insert mockStorageBucketsInsert;
  @Mock private Storage.Buckets.Delete mockStorageBucketsDelete;
  @Mock private Storage.Buckets.Get mockStorageBucketsGet;
  @Mock private Storage.Buckets.Get mockStorageBucketsGet2;
  @Mock private Storage.Buckets.List mockStorageBucketsList;
  @Mock private ApiErrorExtractor mockErrorExtractor;
  @Mock private BatchHelper.Factory mockBatchFactory;
  @Mock private BatchHelper mockBatchHelper;
  @Mock private HttpHeaders mockHeaders;
  @Mock private ClientRequestHelper<StorageObject> mockClientRequestHelper;
  @Mock private Sleeper mockSleeper;
  @Mock private NanoClock mockClock;
  @Mock private BackOff mockBackOff;
  @Mock private BackOff mockReadBackOff;
  @Mock private BackOffFactory mockBackOffFactory;

  private GoogleCloudStorage gcs;
  private TrackingHttpRequestInitializer trackingHttpRequestInitializer;

  /**
   * Sets up new mocks and create new instance of GoogleCloudStorage configured to only interact
   * with the mocks, run before every test case.
   */
  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    executorService = Executors.newCachedThreadPool();
    gcs = createTestInstance();
    trackingHttpRequestInitializer =
        new TrackingHttpRequestInitializer(/* replaceRequestParams= */ false);
  }

  /**
   * Creates an instance of GoogleCloudStorage using GoogleCloudStorageImpl as the concrete type and
   * setting up the proper mocks.
   */
  protected GoogleCloudStorageOptions.Builder createDefaultCloudStorageOptionsBuilder() {
    return GoogleCloudStorageOptions.builder().setAppName(APP_NAME).setProjectId(PROJECT_ID);
  }

  /**
   * Creates an instance of GoogleCloudStorage using GoogleCloudStorageImpl
   * as the concrete type and setting up the proper mocks.
   */
  protected GoogleCloudStorage createTestInstance() {
    return createTestInstance(createDefaultCloudStorageOptionsBuilder().build());
  }

  /**
   * Creates an instance of GoogleCloudStorage with the specified options, using
   * GoogleCloudStorageImpl as the concrete type, and setting up the proper mocks.
   */
  protected GoogleCloudStorage createTestInstance(GoogleCloudStorageOptions options) {
    GoogleCloudStorageImpl gcsTestInstance = new GoogleCloudStorageImpl(options, mockStorage);
    gcsTestInstance.setThreadPool(executorService);
    gcsTestInstance.setErrorExtractor(mockErrorExtractor);
    gcsTestInstance.setClientRequestHelper(mockClientRequestHelper);
    gcsTestInstance.setBatchFactory(mockBatchFactory);
    gcsTestInstance.setSleeper(mockSleeper);
    gcsTestInstance.setBackOffFactory(mockBackOffFactory);
    return gcsTestInstance;
  }

  /**
   * Creates an instance of GoogleCloudStorage using GoogleCloudStorageImpl as the concrete type and
   * setting up the proper mocks, and the specified value for inferImplicitDirectories.
   */
  private GoogleCloudStorage createTestInstanceWithInferImplicit(boolean inferImplicitDirectories) {
    return createTestInstance(
        createDefaultCloudStorageOptionsBuilder()
            .setInferImplicitDirectoriesEnabled(inferImplicitDirectories)
            .build());
  }

  protected void setupNonConflictedWrite(Throwable t) throws IOException {
    setupNonConflictedWrite(
        invocation -> {
          throw t;
        });
  }

  protected void setupNonConflictedWrite(Answer<StorageObject> answer)
      throws IOException {
    when(mockStorageObjects.get(BUCKET_NAME, OBJECT_NAME)).thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute()).thenThrow(new IOException("NotFound"));
    when(mockErrorExtractor.itemNotFound(any(IOException.class))).thenReturn(true);
    when(mockStorageObjectsInsert.execute()).thenAnswer(answer);
  }

  protected StorageObject getStorageObjectForEmptyObjectWithMetadata(
      Map<String, byte[]> metadata) {
    return newStorageObject(BUCKET_NAME, OBJECT_NAME)
        .setSize(BigInteger.ZERO)
        .setMetadata(metadata == null ? null : GoogleCloudStorageImpl.encodeMetadata(metadata));
  }

  protected GoogleCloudStorageItemInfo getItemInfoForEmptyObjectWithMetadata(
      Map<String, byte[]> metadata) {
    return createItemInfoForStorageObject(
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
        getStorageObjectForEmptyObjectWithMetadata(metadata));
  }

  /**
   * Ensure that each test case precisely captures all interactions with the mocks, since all
   * the mocks represent external dependencies.
   */
  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockStorage);
    verifyNoMoreInteractions(mockStorageObjects);
    verifyNoMoreInteractions(mockStorageObjectsInsert);
    verifyNoMoreInteractions(mockStorageObjectsDelete);
    verifyNoMoreInteractions(mockStorageObjectsGet);
    verifyNoMoreInteractions(mockStorageObjectsCopy);
    verifyNoMoreInteractions(mockStorageObjectsCompose);
    verifyNoMoreInteractions(mockStorageObjectsList);
    verifyNoMoreInteractions(mockStorageBuckets);
    verifyNoMoreInteractions(mockStorageBucketsInsert);
    verifyNoMoreInteractions(mockStorageBucketsDelete);
    verifyNoMoreInteractions(mockStorageBucketsGet);
    verifyNoMoreInteractions(mockStorageBucketsGet2);
    verifyNoMoreInteractions(mockStorageBucketsList);
    verifyNoMoreInteractions(mockErrorExtractor);
    verifyNoMoreInteractions(mockBatchFactory);
    verifyNoMoreInteractions(mockBatchHelper);
    verifyNoMoreInteractions(mockHeaders);
    verifyNoMoreInteractions(mockClientRequestHelper);
    verifyNoMoreInteractions(mockSleeper);
    verifyNoMoreInteractions(mockClock);
    verifyNoMoreInteractions(mockBackOff);
    verifyNoMoreInteractions(mockReadBackOff);
    verifyNoMoreInteractions(mockBackOffFactory);
  }

  /** Test argument sanitization for GoogleCloudStorage.create(2). */
  @Test
  public void testCreateObjectIllegalArguments() {
    for (String[] objectPair : ILLEGAL_OBJECTS) {
      assertThrows(
          IllegalArgumentException.class,
          () -> gcs.create(new StorageResourceId(objectPair[0], objectPair[1])));
    }
  }

  /** Test successful operation of GoogleCloudStorage.create(2). */
  @Test
  public void testCreateObjectNormalOperation() throws Exception {
    byte[] testData = {0x01, 0x02, 0x03, 0x05, 0x08, 0x09};

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            emptyResponse(HttpStatusCodes.STATUS_CODE_NOT_FOUND),
            resumableUploadResponse(BUCKET_NAME, OBJECT_NAME),
            jsonDataResponse(
                newStorageObject(BUCKET_NAME, OBJECT_NAME)
                    .setSize(BigInteger.valueOf(testData.length))));

    Storage storage = new Storage(transport, JSON_FACTORY, trackingHttpRequestInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(GCS_OPTIONS, storage);

    try (WritableByteChannel writeChannel =
        gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME))) {
      assertThat(writeChannel.isOpen()).isTrue();
      writeChannel.write(ByteBuffer.wrap(testData));
    }

    ImmutableList<HttpRequest> requests = trackingHttpRequestInitializer.getAllRequests();
    assertThat(requests).hasSize(3);

    HttpRequest writeRequest = requests.get(2);
    assertThat(writeRequest.getContent().getLength()).isEqualTo(testData.length);
    try (ByteArrayOutputStream writtenData = new ByteArrayOutputStream(testData.length)) {
      writeRequest.getContent().writeTo(writtenData);
      assertThat(writtenData.toByteArray()).isEqualTo(testData);
    }
  }

  /** Test successful operation of GoogleCloudStorage.create(2) with generationId. */
  @Test
  public void testCreateObjectWithGenerationId() throws Exception {
    int generationId = 134;
    byte[] testData = {0x01, 0x02, 0x03, 0x05, 0x08, 0x09};

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            resumableUploadResponse(BUCKET_NAME, OBJECT_NAME),
            jsonDataResponse(
                newStorageObject(BUCKET_NAME, OBJECT_NAME)
                    .setSize(BigInteger.valueOf(testData.length))));

    Storage storage = new Storage(transport, JSON_FACTORY, trackingHttpRequestInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(GCS_OPTIONS, storage);

    try (WritableByteChannel writeChannel =
        gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME, generationId))) {
      assertThat(writeChannel.isOpen()).isTrue();
      writeChannel.write(ByteBuffer.wrap(testData));
    }

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            resumableUploadRequestString(
                BUCKET_NAME, OBJECT_NAME, generationId, /* replaceGenerationId= */ false),
            resumableUploadChunkRequestString(BUCKET_NAME, OBJECT_NAME, /* uploadId= */ 1))
        .inOrder();

    HttpRequest writeRequest = trackingHttpRequestInitializer.getAllRequests().get(1);
    assertThat(writeRequest.getContent().getLength()).isEqualTo(testData.length);
    try (ByteArrayOutputStream writtenData = new ByteArrayOutputStream(testData.length)) {
      writeRequest.getContent().writeTo(writtenData);
      assertThat(writtenData.toByteArray()).isEqualTo(testData);
    }
  }

  /**
   * Test handling when the parent thread waiting for the write to finish via the close call is
   * interrupted, that the actual write is cancelled and interrupted as well.
   */
  @Test
  public void testCreateObjectApiInterruptedException() throws Exception {
    // Prepare the mock return values before invoking the method being tested.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);

    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);

    // Set up the mock Insert to wait forever.
    CountDownLatch waitForEverLatch = new CountDownLatch(1);
    CountDownLatch writeStartedLatch = new CountDownLatch(2);
    CountDownLatch threadsDoneLatch = new CountDownLatch(2);
    setupNonConflictedWrite(
        unused -> {
          try {
            writeStartedLatch.countDown();
            waitForEverLatch.await();
            fail("Unexpected to get here.");
            return null;
          } finally {
            threadsDoneLatch.countDown();
          }
        });

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    Future<?> write =
        executorService.submit(
            () -> {
              writeStartedLatch.countDown();
              try {
                IOException ioe = assertThrows(IOException.class, writeChannel::close);
                assertThat(ioe).isInstanceOf(ClosedByInterruptException.class);
              } finally {
                threadsDoneLatch.countDown();
              }
            });
    // Wait for the insert object to be executed, then cancel the writing thread, and finally wait
    // for the two threads to finish.
    assertWithMessage("Neither thread started.")
        .that(writeStartedLatch.await(5000, TimeUnit.MILLISECONDS))
        .isTrue();
    write.cancel(/* interrupt= */ true);
    assertWithMessage("Failed to wait for tasks to get interrupted.")
        .that(threadsDoneLatch.await(5000, TimeUnit.MILLISECONDS))
        .isTrue();

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects)
        .insert(eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert).setName(eq(OBJECT_NAME));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper).setChunkSize(any(Storage.Objects.Insert.class), anyInt());
    verify(mockStorageObjectsInsert).setIfGenerationMatch(eq(0L));
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet).execute();
    verify(mockErrorExtractor).itemNotFound(any(IOException.class));
    verify(mockStorageObjectsInsert).execute();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.create(2).
   */
  @Test
  public void testCreateObjectApiIOException() throws IOException {
    // Prepare the mock return values before invoking the method being tested.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);

    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);

    // Set up the mock Insert to throw an exception when execute() is called.
    IOException fakeException = new IOException("Fake IOException");
    setupNonConflictedWrite(fakeException);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    IOException thrown = assertThrows(IOException.class, writeChannel::close);
    assertThat(thrown).hasCauseThat().isEqualTo(fakeException);

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects)
        .insert(eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert).setName(eq(OBJECT_NAME));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper).setChunkSize(any(Storage.Objects.Insert.class), anyInt());
    verify(mockStorageObjectsInsert).setIfGenerationMatch(eq(0L));
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet).execute();
    verify(mockErrorExtractor).itemNotFound(any(IOException.class));
    verify(mockStorageObjectsInsert).execute();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.create(2).
   */
  @Test
  public void testCreateObjectApiRuntimeException() throws IOException {
    // Prepare the mock return values before invoking the method being tested.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);

    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);

    // Set up the mock Insert to throw an exception when execute() is called.
    RuntimeException fakeException = new RuntimeException("Fake exception");
    setupNonConflictedWrite(fakeException);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    IOException thrown = assertThrows(IOException.class, writeChannel::close);
    assertThat(thrown).hasCauseThat().isEqualTo(fakeException);

    verify(mockStorageObjectsInsert).execute();
    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects)
        .insert(eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockErrorExtractor, atLeastOnce()).itemNotFound(any(IOException.class));
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjectsInsert).setName(eq(OBJECT_NAME));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockStorageObjects).get(anyString(), anyString());
    verify(mockClientRequestHelper).setChunkSize(any(Storage.Objects.Insert.class), anyInt());
    verify(mockStorageObjectsInsert).setIfGenerationMatch(anyLong());
  }

  /**
   * Test handling of various types of Errors thrown during JSON API call for
   * GoogleCloudStorage.create(2).
   */
  @Test
  public void testCreateObjectApiError() throws IOException {
    // Prepare the mock return values before invoking the method being tested.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);

    // Set up the mock Insert to throw an exception when execute() is called.
    Error fakeError = new Error("Fake error");
    setupNonConflictedWrite(fakeError);

    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    Error thrown = assertThrows(Error.class, writeChannel::close);
    assertThat(thrown).isEqualTo(fakeError);

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects)
        .insert(eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjects).get(BUCKET_NAME, OBJECT_NAME);
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjectsInsert).setName(eq(OBJECT_NAME));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockStorageObjectsInsert).setIfGenerationMatch(eq(0L));
    verify(mockErrorExtractor).itemNotFound(any(IOException.class));
    verify(mockClientRequestHelper).setChunkSize(any(Storage.Objects.Insert.class), anyInt());
    verify(mockStorageObjectsInsert).execute();
  }

  /**
   * Test successful operation of GoogleCloudStorage.createEmptyObject(1).
   */
  @Test
  public void testCreateEmptyObject()
      throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);

    gcs.createEmptyObject(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    verify(mockStorage).objects();
    ArgumentCaptor<StorageObject> storageObjectCaptor =
        ArgumentCaptor.forClass(StorageObject.class);
    ArgumentCaptor<AbstractInputStreamContent> inputStreamCaptor =
        ArgumentCaptor.forClass(AbstractInputStreamContent.class);
    verify(mockStorageObjects).insert(
        eq(BUCKET_NAME), storageObjectCaptor.capture(), inputStreamCaptor.capture());
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert).execute();

    assertThat(storageObjectCaptor.getValue().getName()).isEqualTo(OBJECT_NAME);
    assertThat(inputStreamCaptor.getValue().getLength()).isEqualTo(0);
  }

  /**
   * Helper for the shared boilerplate of setting up the low-level "API objects" like
   * mockStorage.objects(), etc., that is common between test cases targeting {@code
   * GoogleCloudStorage.open(StorageResourceId)}.
   */
  private void setUpBasicMockBehaviorForOpeningReadChannel() throws IOException {
    setUpBasicMockBehaviorForOpeningReadChannel(111);
  }

  /**
   * Helper for the shared boilerplate of setting up the low-level "API objects" like
   * mockStorage.objects(), etc., that is common between test cases targeting {@code
   * GoogleCloudStorage.open(StorageResourceId)}.
   *
   * @param size {@link StorageObject} size
   */
  private void setUpBasicMockBehaviorForOpeningReadChannel(long size) throws IOException {
    setUpBasicMockBehaviorForOpeningReadChannel(size, null);
  }

  /**
   * Helper for the shared boilerplate of setting up the low-level "API objects" like
   * mockStorage.objects(), etc., that is common between test cases targeting {@code
   * GoogleCloudStorage.open(StorageResourceId)}.
   *
   * @param size {@link StorageObject} size
   * @param encoding {@link StorageObject} encoding
   */
  private void setUpBasicMockBehaviorForOpeningReadChannel(long size, String encoding)
      throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsGet)))
        .thenReturn(mockHeaders);
    when(mockStorageObjectsGet.execute())
        .thenReturn(
            new StorageObject()
                .setBucket(BUCKET_NAME)
                .setName(OBJECT_NAME)
                .setUpdated(new DateTime(11L))
                .setSize(BigInteger.valueOf(size))
                .setContentEncoding(encoding)
                .setGeneration(1L)
                .setMetageneration(1L));
  }

  /**
   * Helper for test cases involving {@code GoogleCloudStorage.open(StorageResourceId)} to set up
   * the shared sleeper/clock/backoff mocks and set {@code maxRetries}. Also checks basic invariants
   * of a fresh readChannel, such as its position() and isOpen().
   */
  private void setUpAndValidateReadChannelMocksAndSetMaxRetries(
      GoogleCloudStorageReadChannel readChannel, int maxRetries) throws IOException {
    readChannel.setSleeper(mockSleeper);
    readChannel.setNanoClock(mockClock);
    readChannel.setReadBackOff(mockReadBackOff);
    readChannel.setMaxRetries(maxRetries);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);
  }

  /** Test argument sanitization for GoogleCloudStorage.open(2). */
  @Test
  public void testOpenObjectIllegalArguments() {
    for (String[] objectPair : ILLEGAL_OBJECTS) {
      assertThrows(
          IllegalArgumentException.class,
          () -> gcs.open(new StorageResourceId(objectPair[0], objectPair[1])));
    }
  }

  @Test
  public void testGcsReadChannelCloseIdempotent() throws IOException {
    setUpBasicMockBehaviorForOpeningReadChannel();

    GoogleCloudStorageReadChannel channel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));

    channel.close();
    channel.close();

    verify(mockStorage).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet).execute();
  }

  @Test
  public void testOpenWithSomeExceptionsDuringRead() throws Exception {
    setUpBasicMockBehaviorForOpeningReadChannel();

    // First returned timeout stream will timout; we'll expect a re-opening where we'll return the
    // real input stream.
    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(fakeResponse("Content-Length", testData.length, mockExceptionStream))
        .thenReturn(fakeResponse("Content-Length", testData.length, mockExceptionStream))
        .thenReturn(fakeResponse("Content-Length", testData.length, mockExceptionStream))
        .thenReturn(
            fakeResponse("Content-Length", testData.length, new ByteArrayInputStream(testData)));

    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenThrow(new SocketTimeoutException("fake timeout"))
        .thenThrow(new SSLException("fake SSLException"))
        .thenThrow(new IOException("fake generic IOException"));

    when(mockReadBackOff.nextBackOffMillis()).thenReturn(111L).thenReturn(222L).thenReturn(333L);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 3);

    byte[] actualData = new byte[testData.length];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(4)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(4)).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders, times(4)).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjectsGet, times(4)).executeMedia();
    verify(mockStorageObjectsGet, times(4)).setGeneration(eq(null));
    verify(mockReadBackOff).reset();
    verify(mockReadBackOff, times(3)).nextBackOffMillis();
    verify(mockSleeper).sleep(eq(111L));
    verify(mockSleeper).sleep(eq(222L));
    verify(mockSleeper).sleep(eq(333L));

    assertThat(bytesRead).isEqualTo(testData.length);
    assertThat(actualData).isEqualTo(testData);
  }

  @Test
  public void testOpenWithExceptionDuringReadAndCloseForRetry() throws Exception {
    setUpBasicMockBehaviorForOpeningReadChannel();

    // First returned timeout stream will timout; we'll expect a re-opening where we'll return the
    // real input stream.
    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(fakeResponse("Content-Length", testData.length, mockExceptionStream))
        .thenReturn(
            fakeResponse("Content-Length", testData.length, new ByteArrayInputStream(testData)));

    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenThrow(new SSLException("fake SSLException"));
    doThrow(new SSLException("fake SSLException on close()"))
        .doThrow(new SSLException("second fake SSLException on close()"))
        .when(mockExceptionStream).close();

    when(mockReadBackOff.nextBackOffMillis()).thenReturn(111L);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 3);

    byte[] actualData = new byte[testData.length];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(2)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(2)).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders, times(2)).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjectsGet, times(2)).executeMedia();
    verify(mockStorageObjectsGet, times(2)).setGeneration(eq(null));
    verify(mockReadBackOff).reset();
    verify(mockReadBackOff).nextBackOffMillis();
    verify(mockSleeper).sleep(eq(111L));

    assertThat(bytesRead).isEqualTo(testData.length);
    assertThat(actualData).isEqualTo(testData);
  }

  @Test
  public void testClosesWithRuntimeExceptionDuringReadAndClose() throws IOException {
    setUpBasicMockBehaviorForOpeningReadChannel();

    // First returned timeout stream will timeout;
    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(fakeResponse("Content-Length", testData.length, mockExceptionStream));

    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenThrow(new RuntimeException("fake RuntimeException"));
    doThrow(new SSLException("fake SSLException on close()"))
        .when(mockExceptionStream).close();


    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 3);

    byte[] actualData = new byte[testData.length];

    assertThrows(RuntimeException.class, () -> readChannel.read(ByteBuffer.wrap(actualData)));

    assertThat(readChannel.contentChannel).isNull();
    assertThat(readChannel.contentChannelPosition).isEqualTo(-1);

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjectsGet).executeMedia();
    verify(mockStorageObjectsGet).setGeneration(eq(null));
  }

  @Test
  public void testCloseWithExceptionDuringClose() throws IOException {
    setUpBasicMockBehaviorForOpeningReadChannel();

    // First returned timeout stream will timout;
    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };

    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(fakeResponse("Content-Length", testData.length, mockExceptionStream));

    doThrow(new SSLException("fake SSLException on close()"))
        .when(mockExceptionStream).close();

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 0);
    readChannel.performLazySeek(/* bytesToRead= */ 1);
    assertThat(readChannel.contentChannel).isNotNull();

    // Should not throw exception. If it does, it will be caught by the test harness.
    readChannel.close();

    assertThat(readChannel.contentChannel).isNull();
    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjectsGet).executeMedia();
    verify(mockStorageObjectsGet).setGeneration(eq(null));
  }

  @Test
  public void testOpenAndReadWithPrematureEndOfStreamRetriesFail() throws Exception {
    setUpBasicMockBehaviorForOpeningReadChannel();

    // We'll claim a Content-Length of testData.length, but then only return a stream containing
    // truncatedData. The channel should throw an exception upon detecting this premature
    // end-of-stream.
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    byte[] truncatedData = { 0x01, 0x02, 0x03 };
    byte[] truncatedRetryData = { 0x05 };
    InputStream content = new ByteArrayInputStream(new byte[0]);
    when(mockStorageObjectsGet.executeMedia())
        // First time: Claim  we'll provide 5 bytes, but only give 3.
        .thenReturn(
            fakeResponse(
                "Content-Length", testData.length, new ByteArrayInputStream(truncatedData)))
        // Second time: Claim we'll provide the 2 remaining bytes, but only give one byte.
        // This retry counts toward the maxRetries of the "first" attempt, but the nonzero bytes
        // returned resets the counter; when this ends prematurely we'll expect yet another "retry"
        // even though we'll set maxRetries == 1.
        .thenReturn(fakeResponse("Content-Length", 2, new ByteArrayInputStream(truncatedRetryData)))
        // Third time, we claim we'll deliver the one remaining byte, but give none. Since no
        // progress is made, the retry counter does not get reset and we've exhausted all retries.
        .thenReturn(fakeResponse("Content-Length", 1, content));

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    // Only allow one retry for this test.
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 1);

    byte[] actualData = new byte[testData.length];
    assertThrows(IOException.class, () -> readChannel.read(ByteBuffer.wrap(actualData)));

    // Both "retries" reset the mockBackOff, since they are "independent" retries with progress
    // in-between. One initial retry for getMetadata.
    verify(mockReadBackOff, times(2)).reset();
    verify(mockReadBackOff, times(2)).nextBackOffMillis();
    verify(mockSleeper, times(2)).sleep(anyLong());
    verify(mockStorage, times(4)).objects();
    verify(mockStorageObjects, times(4)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(3)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(3)).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockHeaders).setRange(eq("bytes=3-"));
    verify(mockHeaders).setRange(eq("bytes=4-"));
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjectsGet, times(3)).executeMedia();
    verify(mockStorageObjectsGet, times(3)).setGeneration(eq(null));
  }

  @Test
  public void testOpenAndReadWithPrematureEndOfStreamRetriesSucceed() throws Exception {
    setUpBasicMockBehaviorForOpeningReadChannel();

    // We'll claim a Content-Length of testData.length, but then only return a stream containing
    // truncatedData. The channel should throw an exception upon detecting this premature
    // end-of-stream.
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    byte[] truncatedData = { 0x01, 0x02, 0x03 };
    byte[] truncatedRetryData = { 0x05 };
    byte[] finalRetryData = { 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        // First time: Claim  we'll provide 5 bytes, but only give 3.
        .thenReturn(
            fakeResponse(
                "Content-Length", testData.length, new ByteArrayInputStream(truncatedData)))
        // Second time: Claim we'll provide the 2 remaining bytes, but only give one byte.
        // This retry counts toward the maxRetries of the "first" attempt, but the nonzero bytes
        // returned resets the counter; when this ends prematurely we'll expect yet another "retry"
        // even though we'll set maxRetries == 1.
        .thenReturn(fakeResponse("Content-Length", 2, new ByteArrayInputStream(truncatedRetryData)))
        // Third time, we claim we'll deliver the one remaining byte, but give none. Since no
        // progress is made, the retry counter does not get reset and we've exhausted all retries.
        .thenReturn(fakeResponse("Content-Length", 1, new ByteArrayInputStream(finalRetryData)));

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    // Only allow one retry for this test.
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 1);

    byte[] actualData = new byte[testData.length];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    // Both "retries" reset the mockReadBackOff, since they are "independent"
    // retries with progress in-between. One initial retry for getMetadata.
    verify(mockReadBackOff, times(2)).reset();
    verify(mockReadBackOff, times(2)).nextBackOffMillis();
    verify(mockSleeper, times(2)).sleep(anyLong());
    verify(mockStorage, times(4)).objects();
    verify(mockStorageObjects, times(4)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(3)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(3)).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockHeaders).setRange(eq("bytes=3-"));
    verify(mockHeaders).setRange(eq("bytes=4-"));
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjectsGet, times(3)).executeMedia();
    verify(mockStorageObjectsGet, times(3)).setGeneration(eq(null));

    assertThat(bytesRead).isEqualTo(testData.length);
    assertThat(actualData).isEqualTo(testData);
  }

  @Test
  public void testOpenExceptionsDuringReadTotalElapsedTimeTooGreat() throws Exception {
    InputStream mockExceptionStream = mock(InputStream.class);
    int dataLength = 1;

    setUpBasicMockBehaviorForOpeningReadChannel(dataLength);
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(fakeResponse("Content-Length", dataLength, mockExceptionStream));
    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(dataLength)))
        .thenThrow(new IOException("fake generic IOException"));

    when(mockClock.nanoTime())
        .thenReturn(1000000L)
        .thenReturn(1000001L)
        .thenReturn(1000002L)
        .thenReturn(
            (GoogleCloudStorageReadOptions.DEFAULT_BACKOFF_MAX_ELAPSED_TIME_MILLIS + 3) * 1000000L);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 3);
    readChannel.setReadBackOff(readChannel.createBackOff());

    IOException thrown =
        assertThrows(IOException.class, () -> readChannel.read(ByteBuffer.allocate(dataLength)));
    assertThat(thrown).hasMessageThat().isEqualTo("fake generic IOException");

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet).execute();
    verify(mockClock, times(4)).nanoTime();
    verify(mockSleeper).sleep(anyLong());
    verify(mockClientRequestHelper, times(2)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(2)).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders, times(2)).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet, times(2)).setGeneration(eq(null));
    verify(mockStorageObjectsGet, times(2)).executeMedia();
  }

  @Test
  public void testOpenExceptionsDuringReadInterruptedDuringSleep() throws Exception {
    setUpBasicMockBehaviorForOpeningReadChannel();

    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(fakeResponse("Content-Length", testData.length, mockExceptionStream));
    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenThrow(new IOException("fake generic IOException"));

    when(mockReadBackOff.nextBackOffMillis()).thenReturn(111L);
    InterruptedException interrupt = new InterruptedException("fake interrupt");
    doThrow(interrupt)
        .when(mockSleeper).sleep(eq(111L));

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 3);

    byte[] actualData = new byte[testData.length];
    IOException thrown =
        assertThrows(IOException.class, () -> readChannel.read(ByteBuffer.wrap(actualData)));
    assertThat(thrown).hasMessageThat().isEqualTo("fake generic IOException");
    assertThat(thrown.getSuppressed()).hasLength(1);
    assertThat(thrown.getSuppressed()[0]).isEqualTo(interrupt);
    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet).setGeneration(eq(null));
    verify(mockStorageObjectsGet).executeMedia();
    verify(mockStorageObjectsGet).execute();
    verify(mockReadBackOff).reset();
    verify(mockReadBackOff).nextBackOffMillis();
    verify(mockSleeper).sleep(eq(111L));
  }

  @Test
  public void testOpenTooManyExceptionsDuringRead() throws Exception {
    setUpBasicMockBehaviorForOpeningReadChannel();

    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(fakeResponse("Content-Length", testData.length, mockExceptionStream))
        .thenReturn(fakeResponse("Content-Length", testData.length, mockExceptionStream))
        .thenReturn(fakeResponse("Content-Length", testData.length, mockExceptionStream));

    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenThrow(new SocketTimeoutException("fake timeout"))
        .thenThrow(new SSLException("fake SSLException"))
        .thenThrow(new IOException("fake generic IOException"));

    when(mockReadBackOff.nextBackOffMillis()).thenReturn(111L).thenReturn(222L);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 2);

    byte[] actualData = new byte[testData.length];
    IOException thrown =
        assertThrows(IOException.class, () -> readChannel.read(ByteBuffer.wrap(actualData)));
    assertThat(thrown).hasMessageThat().isEqualTo("fake generic IOException");

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(3)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(3)).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders, times(3)).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet, times(3)).executeMedia();
    verify(mockStorageObjectsGet, times(3)).setGeneration(eq(null));
    verify(mockStorageObjectsGet).execute();
    verify(mockReadBackOff).reset();
    verify(mockReadBackOff, times(2)).nextBackOffMillis();
    verify(mockSleeper).sleep(eq(111L));
    verify(mockSleeper).sleep(eq(222L));
  }

  @Test
  public void testOpenTwoTimeoutsWithIntermittentProgress() throws Exception {
    setUpBasicMockBehaviorForOpeningReadChannel();

    // This stream will immediately timeout.
    InputStream mockTimeoutStream = mock(InputStream.class);

    // This stream will first read 2 bytes and then timeout.
    InputStream mockFlakyStream = mock(InputStream.class);

    final byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    final byte[] testData2 = { 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(fakeResponse("Content-Length", testData.length, mockTimeoutStream))
        .thenReturn(fakeResponse("Content-Length", testData.length, mockFlakyStream))
        .thenReturn(
            fakeResponse("Content-Length", testData2.length, new ByteArrayInputStream(testData2)));

    when(mockTimeoutStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenThrow(new SocketTimeoutException("fake timeout 1"));
    when(mockFlakyStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              byte[] inputBuf = (byte[]) args[0];
              // Mimic successfully reading two bytes.
              System.arraycopy(testData, 0, inputBuf, 0, 2);
              return 2;
            });
    // After reading two bytes, there will be 3 remaining slots in the input buffer.
    when(mockFlakyStream.read(any(byte[].class), eq(0), eq(3)))
        .thenAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              byte[] inputBuf = (byte[]) args[0];
              // Mimic successfully reading one byte from position 3 of testData.
              System.arraycopy(testData, 2, inputBuf, 0, 1);
              return 1;
            });
    when(mockFlakyStream.read(any(byte[].class), eq(0), eq(2)))
        .thenThrow(new SocketTimeoutException("fake timeout 2"));
    when(mockFlakyStream.available())
        .thenReturn(0)  // After first two bytes, claim none are available.
        // Will check after reading 3rd byte, claim 1 more byte is available, but actually throw
        // exception when trying to retrieve the 4th byte.
        .thenReturn(1);

    when(mockReadBackOff.nextBackOffMillis()).thenReturn(111L);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 1);

    // Should succeed even though, in total, there were more retries than maxRetries, since we
    // made progress between errors.
    byte[] actualData = new byte[testData.length];
    assertThat(readChannel.read(ByteBuffer.wrap(actualData))).isEqualTo(testData.length);
    assertThat(readChannel.position()).isEqualTo(5);

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(3)).executeMedia();
    verify(mockStorageObjectsGet, times(3)).setGeneration(eq(null));
    verify(mockStorageObjectsGet).execute();
    verify(mockClientRequestHelper, times(3)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(3)).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders, times(2)).setRange(eq("bytes=0-"));
    verify(mockHeaders).setRange(eq("bytes=3-"));
    verify(mockReadBackOff, times(2)).reset();
    verify(mockReadBackOff, times(2)).nextBackOffMillis();
    verify(mockSleeper, times(2)).sleep(eq(111L));
  }

  /**
   * Generate a random byte array from a fixed size.
   */
  private static byte[] generateTestData(int size) {
    Random random = new Random(0);
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }

  /**
   * Test successful operation of GoogleCloudStorage.open(2) with Content-Encoding: gzip files.
   */
  @Test
  public void testOpenGzippedObjectNormalOperation() throws IOException {
    // Generate 1k test data to compress.
    byte[] testData = generateTestData(1000);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
    gzipOutputStream.write(testData);
    byte[] compressedData = outputStream.toByteArray();

    setUpBasicMockBehaviorForOpeningReadChannel(compressedData.length, "gzip");
    // content. Mock this by providing a stream that simply provides the uncompressed content.
    Map<String, Object> responseHeaders =
        ImmutableMap.of("Content-Length", compressedData.length, "Content-Encoding", "gzip");
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(fakeResponse(responseHeaders, new ByteArrayInputStream(testData)))
        .thenReturn(fakeResponse(responseHeaders, new ByteArrayInputStream(testData)))
        .thenReturn(fakeResponse(responseHeaders, new ByteArrayInputStream(testData)));

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);
    assertThat(readChannel.size()).isEqualTo(Long.MAX_VALUE);
    byte[] actualData = new byte[testData.length];
    assertThat(readChannel.read(ByteBuffer.wrap(actualData))).isEqualTo(testData.length);
    assertThat(readChannel.size()).isEqualTo(Long.MAX_VALUE);
    assertThat(readChannel.contentChannel != null).isTrue();
    assertThat(actualData).isEqualTo(testData);
    assertThat(readChannel.position()).isEqualTo(testData.length);

    // Repositioning to an invalid position fails.
    assertThrows(EOFException.class, () -> readChannel.position(-1));

    // Repositioning to a position both before and after size() succeeds.
    readChannel.position(2);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(2);
    byte[] partialData = Arrays.copyOfRange(testData, 2, testData.length);
    actualData = new byte[partialData.length];
    assertThat(readChannel.read(ByteBuffer.wrap(actualData))).isEqualTo(partialData.length);
    assertThat(actualData).isEqualTo(partialData);
    assertThat(readChannel.position()).isEqualTo(testData.length);

    int compressedLength = compressedData.length;
    readChannel.position(compressedLength);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(compressedLength);
    partialData = Arrays.copyOfRange(testData, compressedLength, testData.length);
    actualData = new byte[partialData.length];
    assertThat(readChannel.read(ByteBuffer.wrap(actualData))).isEqualTo(partialData.length);
    assertThat(actualData).isEqualTo(partialData);
    assertThat(readChannel.position()).isEqualTo(testData.length);

    verify(mockStorage, times(4)).objects();
    verify(mockStorageObjects, times(4)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(3)).executeMedia();
    verify(mockStorageObjectsGet, times(3)).setGeneration(eq(null));
    verify(mockStorageObjectsGet).execute();
    verify(mockClientRequestHelper, times(3)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(3)).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders, times(3)).setRange(eq(null));
  }

  /**
   * If we disable the supportGzipEncoding option when opening a channel, and disable failing fast
   * on nonexistent objects we should expect no extraneous metadata-GET calls at all.
   */
  @Test
  public void testOpenNoSupportGzipEncodingAndNoFailFastOnNotFound() throws Exception {
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };

    setUpBasicMockBehaviorForOpeningReadChannel(testData.length);
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(
            fakeResponse("Content-Length", testData.length, new ByteArrayInputStream(testData)));

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel)
            gcs.open(
                new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                GoogleCloudStorageReadOptions.builder().setFastFailOnNotFound(false).build());

    byte[] actualData = new byte[testData.length];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));
    verify(mockStorage).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet).setGeneration(eq(null));
    verify(mockStorageObjectsGet).executeMedia();

    assertThat(bytesRead).isEqualTo(testData.length);
    assertThat(actualData).isEqualTo(testData);
  }

  /** Test in-place forward seeks smaller than seek buffer, smaller than limit. */
  @Test
  public void testInplaceSeekSmallerThanSeekLimit() throws Exception {
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };

    setUpBasicMockBehaviorForOpeningReadChannel(testData.length);
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(
            fakeResponse("Content-Length", testData.length, new ByteArrayInputStream(testData)));
    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel)
            gcs.open(
                new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                GoogleCloudStorageReadOptions.builder()
                    .setFastFailOnNotFound(false)
                    .setInplaceSeekLimit(2)
                    .build());

    byte[] actualData1 = new byte[1];
    int bytesRead1 = readChannel.read(ByteBuffer.wrap(actualData1));

    // Jump 2 bytes forwards; this should be done in-place without any new executeMedia() call.
    readChannel.position(3);
    assertThat(readChannel.position()).isEqualTo(3);

    byte[] actualData2 = new byte[2];
    int bytesRead2 = readChannel.read(ByteBuffer.wrap(actualData2));

    assertThat(bytesRead1).isEqualTo(1);
    assertThat(actualData1).isEqualTo(new byte[] {0x01});

    assertThat(bytesRead2).isEqualTo(2);
    assertThat(actualData2).isEqualTo(new byte[] {0x05, 0x08});

    verify(mockStorage).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockStorageObjectsGet).setGeneration(eq(null));
    verify(mockHeaders).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet).executeMedia();
  }

  /**
   * Test in-place forward seeks larger than seek buffer but smaller than limit.
   */
  @Test
  public void testInplaceSeekLargerThanSeekBuffer() throws Exception {
    byte[] testData = new byte[GoogleCloudStorageReadChannel.SKIP_BUFFER_SIZE + 5];
    testData[0] = 0x01;
    testData[testData.length - 4] = 0x02;
    testData[testData.length - 3] = 0x03;
    testData[testData.length - 2] = 0x05;
    testData[testData.length - 1] = 0x08;

    setUpBasicMockBehaviorForOpeningReadChannel(testData.length);
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(
            fakeResponse("Content-Length", testData.length, new ByteArrayInputStream(testData)));

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel)
            gcs.open(
                new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                GoogleCloudStorageReadOptions.builder()
                    .setFastFailOnNotFound(false)
                    .setInplaceSeekLimit(2 * GoogleCloudStorageReadChannel.SKIP_BUFFER_SIZE)
                    .build());

    byte[] actualData1 = new byte[1];
    int bytesRead1 = readChannel.read(ByteBuffer.wrap(actualData1));

    // Jump 2 bytes + SKIP_BUFFER_SIZE forwards; this should be done in-place without any
    // new executeMedia() call.
    readChannel.position(GoogleCloudStorageReadChannel.SKIP_BUFFER_SIZE + 3);
    assertThat(readChannel.position())
        .isEqualTo(GoogleCloudStorageReadChannel.SKIP_BUFFER_SIZE + 3);

    byte[] actualData2 = new byte[2];
    int bytesRead2 = readChannel.read(ByteBuffer.wrap(actualData2));

    verify(mockStorage).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet).setGeneration(eq(null));
    verify(mockStorageObjectsGet).executeMedia();

    assertThat(bytesRead1).isEqualTo(1);
    assertThat(bytesRead2).isEqualTo(2);
    assertThat(actualData1).isEqualTo(new byte[] {0x01});
    assertThat(actualData2).isEqualTo(new byte[] {0x05, 0x08});
  }

  /** Test in-place backward seek does not trigger a re-read if it isn't used */
  @Test
  public void testUnusedBackwardSeekIgnored() throws Exception {
    setUpBasicMockBehaviorForOpeningReadChannel();

    byte[] testData = {0x01, 0x02, 0x03, 0x05, 0x08};
    AtomicInteger executeMediaCount = new AtomicInteger();
    when(mockStorageObjectsGet.executeMedia())
        .thenAnswer(
            invocation -> {
              // Answer to re-create the underlying InputStream and track invocation count.
              executeMediaCount.incrementAndGet();
              return fakeResponse(
                  "Content-Length", testData.length, new ByteArrayInputStream(testData));
            });
    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel)
            gcs.open(
                new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                GoogleCloudStorageReadOptions.builder().setInplaceSeekLimit(2).build());

    byte[] actualData1 = new byte[1];
    int bytesRead1 = readChannel.read(ByteBuffer.wrap(actualData1));

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects, times(2)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjectsGet).executeMedia();
    verify(mockStorageObjectsGet).setGeneration(eq(null));

    assertThat(readChannel.position()).isEqualTo(1);

    // Position back to 0, but don't read anything. Verify position returned to caller.
    readChannel.position(0);
    assertThat(readChannel.position()).isEqualTo(0);
    assertThat(executeMediaCount.get()).isEqualTo(1);

    // Re-position forward before the next read. No new executeMedia calls.
    readChannel.position(3);
    assertThat(readChannel.position()).isEqualTo(3);
    assertThat(executeMediaCount.get()).isEqualTo(1);

    byte[] actualData2 = new byte[2];
    int bytesRead2 = readChannel.read(ByteBuffer.wrap(actualData2));
    verify(mockStorageObjectsGet).executeMedia();
    verify(mockStorageObjectsGet).setGeneration(eq(null));
    assertThat(executeMediaCount.get()).isEqualTo(1);

    // Position back to 0, and read. Should lead to another executeMedia call.
    readChannel.position(0);
    byte[] actualData3 = new byte[2];
    int bytesRead3 = readChannel.read(ByteBuffer.wrap(actualData3));
    assertThat(readChannel.position()).isEqualTo(2);
    assertThat(executeMediaCount.get()).isEqualTo(2);
    verify(mockStorage, times(3)).objects();
    verify(mockStorageObjects, times(3)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(2)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(2)).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders, times(2)).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet, times(2)).executeMedia();
    verify(mockStorageObjectsGet, times(2)).setGeneration(eq(null));

    assertThat(bytesRead1).isEqualTo(1);
    assertThat(bytesRead2).isEqualTo(2);
    assertThat(bytesRead3).isEqualTo(2);
    assertThat(actualData1).isEqualTo(new byte[] {0x01});
    assertThat(actualData2).isEqualTo(new byte[] {0x05, 0x08});
    assertThat(actualData3).isEqualTo(new byte[] {0x01, 0x02});
  }

  /**
   * Test operation of GoogleCloudStorage.open(2) with Content-Encoding: gzip files when exceptions
   * occur during reading.
   */
  @Test
  public void testOpenGzippedObjectExceptionsDuringRead() throws Exception {
    final byte[] testData = generateTestData(1000);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    GZIPOutputStream gzipper = new GZIPOutputStream(os);
    gzipper.write(testData);
    byte[] compressedData = os.toByteArray();
    final int compressedLength = compressedData.length;

    setUpBasicMockBehaviorForOpeningReadChannel(compressedData.length, "gzip");
    // Content-Length will be the size of the compressed data. During the first read attempt,
    // we will read < size bytes before throwing an exception. During the second read
    // attempt, we will throw an exception after > size bytes have been read. Finally, we
    // will finish reading.
    InputStream firstTimeoutStream = mock(InputStream.class);
    InputStream secondTimeoutStream = mock(InputStream.class);
    when(firstTimeoutStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              byte[] inputBuf = (byte[]) args[0];
              // Read < size bytes
              System.arraycopy(testData, 0, inputBuf, 0, compressedLength / 2);
              return compressedLength / 2;
            });
    when(firstTimeoutStream.read(
            any(byte[].class), eq(0), eq(testData.length - compressedLength / 2)))
        .thenThrow(new SocketTimeoutException("fake timeout 1"));

    when(secondTimeoutStream.read(
            any(byte[].class), eq(0), eq(testData.length - compressedLength / 2)))
        .thenAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              byte[] inputBuf = (byte[]) args[0];
              // Read > size bytes
              System.arraycopy(testData, compressedLength / 2, inputBuf, 0, compressedLength / 2);
              return compressedLength / 2;
            });
    when(secondTimeoutStream.read(any(byte[].class), eq(0), eq(testData.length - compressedLength)))
        .thenThrow(new SocketTimeoutException("fake timeout 2"));

    when(firstTimeoutStream.available())
        .thenReturn(testData.length)
        .thenReturn(testData.length - compressedLength / 2);
    when(firstTimeoutStream.skip(anyLong())).thenReturn((long) compressedLength / 2);

    when(secondTimeoutStream.available())
        .thenReturn(testData.length - compressedLength / 2)
        .thenReturn(testData.length - compressedLength);
    when(secondTimeoutStream.skip(anyLong())).thenReturn((long) compressedLength / 2);

    Map<String, Object> responseHeaders =
        ImmutableMap.of("Content-Length", testData.length, "Content-Encoding", "gzip");
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(fakeResponse(responseHeaders, firstTimeoutStream))
        .thenReturn(fakeResponse(responseHeaders, secondTimeoutStream))
        .thenReturn(fakeResponse(responseHeaders, new ByteArrayInputStream(testData)));

    when(mockReadBackOff.nextBackOffMillis()).thenReturn(1L);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 3);

    byte[] actualData = new byte[testData.length];
    assertThat(readChannel.read(ByteBuffer.wrap(actualData))).isEqualTo(testData.length);
    assertThat(readChannel.position()).isEqualTo(testData.length);

    verify(mockStorage, times(4)).objects();
    verify(mockStorageObjects, times(4)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(3)).executeMedia();
    verify(mockStorageObjectsGet, times(3)).setGeneration(eq(null));
    verify(mockStorageObjectsGet).execute();
    verify(mockClientRequestHelper, times(3)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(3)).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders, times(3)).setRange(eq(null));
    verify(mockReadBackOff, times(2)).reset();
    verify(mockReadBackOff, times(2)).nextBackOffMillis();
    verify(mockSleeper, times(2)).sleep(eq(1L));
  }

  /**
   * Test successful operation of GoogleCloudStorage.open(2).
   */
  @Test
  public void testOpenObjectNormalOperation()
      throws IOException {
    // Make the response return some fake data, prepare for a second API call when we call
    // 'position' on the returned channel.
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    byte[] testData2 = { 0x03, 0x05, 0x08 };
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsGet)))
        .thenReturn(mockHeaders);
    when(mockBackOffFactory.newBackOff()).thenReturn(mockBackOff);
    when(mockStorageObjectsGet.execute())
        .thenReturn(
            newStorageObject(BUCKET_NAME, OBJECT_NAME)
                .setSize(BigInteger.valueOf(testData.length)));
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(
            fakeResponse("Content-Length", testData.length, new ByteArrayInputStream(testData)))
        .thenReturn(
            fakeResponse(
                "Content-Range", "bytes=0-123/" + testData2.length,
                new ByteArrayInputStream(testData2)));

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);
    byte[] actualData = new byte[testData.length];
    assertThat(readChannel.read(ByteBuffer.wrap(actualData))).isEqualTo(testData.length);
    assertThat(readChannel.contentChannel != null).isTrue();
    assertThat(actualData).isEqualTo(testData);
    assertThat(readChannel.position()).isEqualTo(testData.length);

    readChannel.position(2);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(2);

    // Repositioning to invalid position fails.
    assertThrows(EOFException.class, () -> readChannel.position(-1));
    assertThrows(EOFException.class, () -> readChannel.position(testData.length));

    // Repositioning to current position should result in no API calls.
    readChannel.position(2);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(2);

    // Reading into a buffer with no room should have no effect.
    assertThat(readChannel.read(ByteBuffer.wrap(new byte[0]))).isEqualTo(0);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(2);

    byte[] actualData2 = new byte[testData2.length];
    assertThat(readChannel.read(ByteBuffer.wrap(actualData2))).isEqualTo(testData2.length);
    assertThat(actualData2).isEqualTo(testData2);

    // Note that position will be testData.length, *not* testData2.length (5, not 3).
    assertThat(readChannel.position()).isEqualTo(testData.length);

    // Verify the request being made.
    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(2)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(2)).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockHeaders).setRange(eq("bytes=2-"));
    verify(mockStorageObjectsGet, times(2)).executeMedia();
    verify(mockStorageObjectsGet, times(2)).setGeneration(eq(null));
    verify(mockStorageObjectsGet).execute();

    readChannel.close();
    assertThat(readChannel.isOpen()).isFalse();

    // After closing the channel, future reads should throw a ClosedChannelException.
    assertThrows(ClosedChannelException.class, () -> readChannel.read(ByteBuffer.wrap(actualData)));
    assertThrows(ClosedChannelException.class, () -> readChannel.position(0));
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.open(2).
   */
  @Test
  public void testOpenObjectApiException() throws IOException {
    IOException notFoundException = new IOException("Fake not-found exception");
    IOException rangeNotSatisfiableException =
        new IOException("Fake range-not-satisfiable exception");
    IOException unexpectedException = new IOException("Other API exception");

    setUpBasicMockBehaviorForOpeningReadChannel();
    reset(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute())
        .thenThrow(notFoundException)
        .thenReturn(newStorageObject(BUCKET_NAME, OBJECT_NAME))
        .thenReturn(newStorageObject(BUCKET_NAME, OBJECT_NAME));

    when(mockStorageObjectsGet.executeMedia())
        .thenThrow(rangeNotSatisfiableException)
        .thenThrow(unexpectedException);
    when(mockErrorExtractor.itemNotFound(eq(notFoundException)))
        .thenReturn(true);
    when(mockErrorExtractor.itemNotFound(eq(rangeNotSatisfiableException)))
        .thenReturn(false);
    when(mockErrorExtractor.itemNotFound(eq(unexpectedException)))
        .thenReturn(false);
    when(mockErrorExtractor.rangeNotSatisfiable(eq(rangeNotSatisfiableException)))
        .thenReturn(true);
    when(mockErrorExtractor.rangeNotSatisfiable(eq(unexpectedException)))
        .thenReturn(false);

    StorageResourceId objectId = new StorageResourceId(BUCKET_NAME, OBJECT_NAME);

    // First time is the notFoundException.
    assertThrows(FileNotFoundException.class, () -> gcs.open(objectId));

    // Second time is the rangeNotSatisfiableException.
    SeekableByteChannel readChannel2 = gcs.open(objectId);
    EOFException thrown2 =
        assertThrows(EOFException.class, () -> readChannel2.read(ByteBuffer.allocate(1)));
    assertThat(thrown2).hasCauseThat().isEqualTo(rangeNotSatisfiableException);

    // Third time is the unexpectedException.
    SeekableByteChannel readChannel3 = gcs.open(objectId);
    IOException thrown3 =
        assertThrows(IOException.class, () -> readChannel3.read(ByteBuffer.allocate(1)));
    assertThat(thrown3).hasCauseThat().isEqualTo(unexpectedException);

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(2)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(2)).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders, times(2)).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet, times(3)).execute();
    verify(mockStorageObjectsGet, times(2)).executeMedia();
    verify(mockStorageObjectsGet, times(2)).setGeneration(eq(null));
    verify(mockErrorExtractor, times(3)).itemNotFound(any(IOException.class));
    verify(mockErrorExtractor, atLeastOnce()).rangeNotSatisfiable(any(IOException.class));
  }

  /** Test argument sanitization for GoogleCloudStorage.create(String). */
  @Test
  public void testCreateBucketIllegalArguments() {
    assertThrows(IllegalArgumentException.class, () -> gcs.create((String) null));
    assertThrows(IllegalArgumentException.class, () -> gcs.create(""));
  }

  /**
   * Test successful operation of GoogleCloudStorage.create(String).
   */
  @Test
  public void testCreateBucketNormalOperation()
      throws IOException {
    when(mockBackOffFactory.newBackOff()).thenReturn(mockBackOff);
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.insert(eq(PROJECT_ID), any(Bucket.class)))
        .thenReturn(mockStorageBucketsInsert);
    gcs.create(BUCKET_NAME);
    verify(mockStorage).buckets();
    verify(mockBackOffFactory).newBackOff();
    verify(mockStorageBuckets).insert(eq(PROJECT_ID), any(Bucket.class));
    verify(mockStorageBucketsInsert).execute();
  }

  /**
   * Test successful operation of GoogleCloudStorage.create(String, CreateBucketOptions).
   */
  @Test
  public void testCreateBucketWithOptionsNormalOperation()
      throws IOException {
    final Bucket[] bucketWithOptions = new Bucket[1];
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockBackOffFactory.newBackOff()).thenReturn(mockBackOff);

    final Storage.Buckets.Insert finalMockInsert = mockStorageBucketsInsert;
    when(mockStorageBuckets.insert(eq(PROJECT_ID), any(Bucket.class)))
        .thenAnswer(
            invocation -> {
              bucketWithOptions[0] = (Bucket) invocation.getArguments()[1];
              return finalMockInsert;
            });
    gcs.create(BUCKET_NAME, new CreateBucketOptions("some-location", "storage-class"));

    assertThat(bucketWithOptions[0].getName()).isEqualTo(BUCKET_NAME);
    assertThat(bucketWithOptions[0].getLocation()).isEqualTo("some-location");
    assertThat(bucketWithOptions[0].getStorageClass()).isEqualTo("storage-class");

    verify(mockStorage).buckets();
    verify(mockBackOffFactory).newBackOff();
    verify(mockStorageBuckets).insert(eq(PROJECT_ID), any(Bucket.class));
    verify(mockStorageBucketsInsert).execute();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.create(String).
   */
  @Test
  public void testCreateBucketApiException()
      throws IOException {
    when(mockBackOffFactory.newBackOff()).thenReturn(mockBackOff);
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.insert(eq(PROJECT_ID), any(Bucket.class)))
        .thenReturn(mockStorageBucketsInsert);
    when(mockStorageBucketsInsert.execute())
        .thenThrow(new IOException("Fake exception"));
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(false);

    // TODO(user): Switch to testing for FileExistsException once implemented.
    assertThrows(IOException.class, () -> gcs.create(BUCKET_NAME));

    verify(mockStorage).buckets();
    verify(mockBackOffFactory).newBackOff();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockStorageBuckets).insert(eq(PROJECT_ID), any(Bucket.class));
    verify(mockStorageBucketsInsert).execute();
  }

  /** Test handling of rate-limiting and back-off in GoogleCloudStorage.create(String). */
  @Test
  public void testCreateBucketRateLimited() throws Exception {
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.insert(eq(PROJECT_ID), any(Bucket.class)))
        .thenReturn(mockStorageBucketsInsert);
    when(mockBackOffFactory.newBackOff()).thenReturn(mockBackOff);
    when(mockStorageBucketsInsert.execute())
        .thenThrow(new IOException("Fake exception")) // Will be interpreted as rate-limited
        .thenReturn(new Bucket());
    when(mockBackOff.nextBackOffMillis()).thenReturn(100L);
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(true);

    gcs.create(BUCKET_NAME);

    verify(mockStorage).buckets();
    verify(mockBackOffFactory).newBackOff();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockBackOff).nextBackOffMillis();
    verify(mockSleeper).sleep(100L);
    verify(mockStorageBuckets).insert(eq(PROJECT_ID), any(Bucket.class));
    verify(mockStorageBucketsInsert, times(2)).execute();
  }

  /** Test argument sanitization for GoogleCloudStorage.delete(1). */
  @Test
  public void testDeleteBucketIllegalArguments() {
    assertThrows(
        IllegalArgumentException.class, () -> gcs.deleteBuckets(Lists.newArrayList((String) null)));
    assertThrows(IllegalArgumentException.class, () -> gcs.deleteBuckets(Lists.newArrayList("")));
  }

  /** Test successful operation of GoogleCloudStorage.delete(1). */
  @Test
  public void testDeleteBucketNormalOperation() throws IOException {
    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            emptyResponse(HttpStatusCodes.STATUS_CODE_NO_CONTENT));

    Storage storage = new Storage(transport, JSON_FACTORY, trackingHttpRequestInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(GCS_OPTIONS, storage);

    gcs.deleteBuckets(ImmutableList.of(BUCKET_NAME));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(deleteBucketRequestString(BUCKET_NAME));
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.delete(1).
   */
  @Test
  public void testDeleteBucketApiException() {
    String bucket1 = BUCKET_NAME + 1;
    String bucket2 = BUCKET_NAME + 2;

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            emptyResponse(HttpStatusCodes.STATUS_CODE_SERVER_ERROR),
            emptyResponse(HttpStatusCodes.STATUS_CODE_NOT_FOUND));

    Storage storage = new Storage(transport, JSON_FACTORY, trackingHttpRequestInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(GCS_OPTIONS, storage);

    assertThrows(IOException.class, () -> gcs.deleteBuckets(ImmutableList.of(bucket1)));
    assertThrows(FileNotFoundException.class, () -> gcs.deleteBuckets(ImmutableList.of(bucket2)));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(deleteBucketRequestString(bucket1), deleteBucketRequestString(bucket2))
        .inOrder();
  }

  /** Test handling of rate-limiting and back-off in GoogleCloudStorage.delete(1). */
  @Test
  public void testDeleteBucketRateLimited() throws Exception {
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.delete(eq(BUCKET_NAME)))
        .thenReturn(mockStorageBucketsDelete);
    when(mockBackOffFactory.newBackOff()).thenReturn(mockBackOff);
    doThrow(new IOException("Fake Exception"))
        .doNothing()
        .when(mockStorageBucketsDelete)
        .execute();
    when(mockBackOff.nextBackOffMillis()).thenReturn(100L);
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(true);

    gcs.deleteBuckets(ImmutableList.of(BUCKET_NAME));

    verify(mockStorage).buckets();
    verify(mockBackOffFactory).newBackOff();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockBackOff).nextBackOffMillis();
    verify(mockSleeper).sleep(100L);
    verify(mockStorageBuckets).delete(eq(BUCKET_NAME));
    verify(mockStorageBucketsDelete, times(2)).execute();
  }

  /** Test argument sanitization for GoogleCloudStorage.delete(2). */
  @Test
  public void testDeleteObjectIllegalArguments() {
    for (String[] objectPair : ILLEGAL_OBJECTS) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              gcs.deleteObjects(
                  Lists.newArrayList(new StorageResourceId(objectPair[0], objectPair[1]))));
    }
  }

  /** Test successful operation of GoogleCloudStorage.delete(2). */
  @Test
  public void testDeleteObjectNormalOperation() throws IOException {
    when(mockBatchFactory.newBatchHelper(any(), any(Storage.class), anyLong(), anyLong(), anyInt()))
        .thenReturn(mockBatchHelper);
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjects.delete(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsDelete);

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
              @SuppressWarnings("unchecked")
              JsonBatchCallback<Void> callback =
                  (JsonBatchCallback<Void>) invocation.getArguments()[1];
              callback.onSuccess(null, new HttpHeaders());
              return null;
            })
        .when(mockBatchHelper)
        .queue(any(), any());

    gcs.deleteObjects(Lists.newArrayList(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));

    verify(mockBatchFactory).newBatchHelper(any(), eq(mockStorage), anyLong(), anyLong(), anyInt());
    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects).delete(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsDelete).setIfGenerationMatch(eq(1L));
    verify(mockBatchHelper).flush();
    verify(mockBatchHelper, times(2)).queue(any(), any());
  }

  /** Test successful operation of GoogleCloudStorage.delete(2) with generationId. */
  @Test
  public void testDeleteObjectWithGenerationId() throws IOException {
    int generationId = 222;

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            emptyResponse(HttpStatusCodes.STATUS_CODE_NO_CONTENT));

    Storage storage = new Storage(transport, JSON_FACTORY, trackingHttpRequestInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(GCS_OPTIONS, storage);

    gcs.deleteObjects(
        ImmutableList.of(new StorageResourceId(BUCKET_NAME, OBJECT_NAME, generationId)));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            deleteRequestString(
                BUCKET_NAME, OBJECT_NAME, generationId, /* replaceGenerationId= */ false));
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.delete(2).
   */
  @Test
  public void testDeleteObjectApiException() throws IOException {
    when(mockBatchFactory.newBatchHelper(any(), any(Storage.class), anyLong(), anyLong(), anyInt()))
        .thenReturn(mockBatchHelper);
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.delete(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsDelete);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);

    // Make the errorExtractor claim that our fake notFoundException.
    final GoogleJsonError notFoundError = new GoogleJsonError();
    notFoundError.setMessage("Fake not-found exception");
    final GoogleJsonError unexpectedError = new GoogleJsonError();
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

    when(mockErrorExtractor.itemNotFound(eq(notFoundError)))
        .thenReturn(true);
    when(mockErrorExtractor.itemNotFound(eq(unexpectedError)))
        .thenReturn(false);
    when(mockErrorExtractor.preconditionNotMet(any(GoogleJsonError.class)))
        .thenReturn(false);

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
        .newBatchHelper(any(), eq(mockStorage), anyLong(), anyLong(), anyInt());
    verify(mockStorage, times(4)).objects();
    verify(mockStorageObjects, times(2)).delete(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjects, times(2)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsDelete, times(2)).setIfGenerationMatch(eq(1L));
    verify(mockBatchHelper, times(4)).queue(any(), any());
    verify(mockErrorExtractor, times(2)).itemNotFound(any(GoogleJsonError.class));
    verify(mockErrorExtractor).preconditionNotMet(any(GoogleJsonError.class));
    verify(mockBatchHelper, times(2)).flush();
  }

  /** Test argument sanitization for GoogleCloudStorage.copy(4). */
  @Test
  public void testCopyObjectsIllegalArguments() {
    String b = BUCKET_NAME;
    List<String> o = ImmutableList.of(OBJECT_NAME);

    Storage storage = new Storage(HTTP_TRANSPORT, JSON_FACTORY, r -> {});
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(GCS_OPTIONS, storage);

    for (String[] objectPair : ILLEGAL_OBJECTS) {
      String badBucket = objectPair[0];
      List<String> badObject = Collections.singletonList(objectPair[1]);
      // Src is bad.
      assertThrows(IllegalArgumentException.class, () -> gcs.copy(badBucket, badObject, b, o));
      // Dst is bad.
      assertThrows(IllegalArgumentException.class, () -> gcs.copy(b, o, badBucket, badObject));
    }

    // Failure if src == dst.
    assertThrows(IllegalArgumentException.class, () -> gcs.copy(b, o, b, o));

    // Null lists.
    assertThrows(IllegalArgumentException.class, () -> gcs.copy(b, null, b, o));
    assertThrows(IllegalArgumentException.class, () -> gcs.copy(b, o, b, null));

    // Mismatched number of objects.
    List<String> objects = ImmutableList.of(OBJECT_NAME + "1", OBJECT_NAME + "2");
    assertThrows(IllegalArgumentException.class, () -> gcs.copy(b, o, b, objects));
  }

  /**
   * Test successful operation of GoogleCloudStorage.copy(4) where srcBucketName == dstBucketName.
   */
  @Test
  public void testCopyObjectsNormalOperationSameBucket() throws IOException {
    String dstObject = OBJECT_NAME + "-copy";
    StorageObject object = newStorageObject(BUCKET_NAME, dstObject);
    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            jsonDataResponse(new Objects().setItems(ImmutableList.of(object))));

    Storage storage = new Storage(transport, JSON_FACTORY, trackingHttpRequestInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(GCS_OPTIONS, storage);

    gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME), BUCKET_NAME, ImmutableList.of(dstObject));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            copyRequestString(BUCKET_NAME, OBJECT_NAME, BUCKET_NAME, dstObject, "copyTo"));
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.copy(4) where srcBucketName == dstBucketName.
   */
  @Test
  public void testCopyObjectsApiExceptionSameBucket()
      throws IOException {
    String dstObjectName = OBJECT_NAME + "-copy";
    when(mockBatchFactory.newBatchHelper(any(), any(Storage.class), anyLong(), anyLong(), anyInt()))
        .thenReturn(mockBatchHelper);
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.copy(
            eq(BUCKET_NAME), eq(OBJECT_NAME), eq(BUCKET_NAME), eq(dstObjectName), isNull()))
        .thenReturn(mockStorageObjectsCopy);
    final GoogleJsonError notFoundError = new GoogleJsonError();
    notFoundError.setMessage("Fake not-found exception");
    final GoogleJsonError unexpectedError = new GoogleJsonError();
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
        .queue(eq(mockStorageObjectsCopy), any());
    when(mockErrorExtractor.itemNotFound(eq(notFoundError)))
        .thenReturn(true);
    when(mockErrorExtractor.itemNotFound(eq(unexpectedError)))
        .thenReturn(false);

    // Make the test output a little more friendly in case the exception class differs.
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
        .newBatchHelper(any(), eq(mockStorage), anyLong(), anyLong(), anyInt());
    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects, times(2))
        .copy(eq(BUCKET_NAME), eq(OBJECT_NAME), eq(BUCKET_NAME), eq(dstObjectName), isNull());
    verify(mockBatchHelper, times(2)).queue(eq(mockStorageObjectsCopy), any());
    verify(mockErrorExtractor, times(2)).itemNotFound(any(GoogleJsonError.class));
    verify(mockBatchHelper, times(2)).flush();
  }

  /**
   * Test successful operation of GoogleCloudStorage.copy(4) where srcBucketName != dstBucketName.
   */
  @Test
  public void testCopyObjectsNormalOperationDifferentBucket() throws IOException {
    String dstBucket = BUCKET_NAME + "-copy";
    String dstObject = OBJECT_NAME + "-copy";

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            jsonDataResponse(newBucket(BUCKET_NAME)),
            jsonDataResponse(newBucket(dstBucket)),
            dataResponse("{\"done\": true}".getBytes(StandardCharsets.UTF_8)));

    Storage storage = new Storage(transport, JSON_FACTORY, trackingHttpRequestInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(GCS_OPTIONS, storage);

    gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME), dstBucket, ImmutableList.of(dstObject));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getBucketRequestString(BUCKET_NAME),
            getBucketRequestString(dstBucket),
            copyRequestString(BUCKET_NAME, OBJECT_NAME, dstBucket, dstObject, "copyTo"))
        .inOrder();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.copy(4) where srcBucketName != dstBucketName.
   */
  @Test
  public void testCopyObjectsApiExceptionDifferentBucket()
      throws IOException {
    String dstObjectName = OBJECT_NAME + "-copy";
    String dstBucketName = BUCKET_NAME + "-copy";
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);

    // mockStorageBucketsGet corresponds to fetches of the srcBucket, and mockStorageBucketsGet2
    // corresponds to fetches of the dstBucket.
    when(mockStorageBuckets.get(eq(BUCKET_NAME))).thenReturn(mockStorageBucketsGet);
    when(mockStorageBuckets.get(eq(dstBucketName))).thenReturn(mockStorageBucketsGet2);
    IOException notFoundException = new IOException("Fake not-found exception");
    IOException unexpectedException = new IOException("Other API exception");
    IOException wrappedUnexpectedException1 =
        new IOException("Error accessing Bucket " + BUCKET_NAME, unexpectedException);
    IOException wrappedUnexpectedException2 =
        new IOException("Error accessing Bucket " + dstBucketName, unexpectedException);
    Bucket returnedBucket = new Bucket()
        .setName(BUCKET_NAME)
        .setTimeCreated(new DateTime(1111L))
        .setLocation("us-west-123")
        .setStorageClass("class-af4");
    when(mockStorageBucketsGet.execute())
        .thenThrow(notFoundException)
        .thenThrow(unexpectedException)
        .thenReturn(returnedBucket)
        .thenReturn(returnedBucket);
    when(mockStorageBucketsGet2.execute())
        .thenThrow(notFoundException)
        .thenThrow(unexpectedException);
    when(mockErrorExtractor.itemNotFound(eq(notFoundException)))
        .thenReturn(true);
    when(mockErrorExtractor.itemNotFound(eq(unexpectedException)))
        .thenReturn(false);

    // Order of exceptions:
    // 1. Src 404
    FileNotFoundException srcFileNotFoundException =
        assertThrows(
            FileNotFoundException.class,
            () ->
                gcs.copy(
                    BUCKET_NAME,
                    ImmutableList.of(OBJECT_NAME),
                    dstBucketName,
                    ImmutableList.of(dstObjectName)));
    assertThat(srcFileNotFoundException).hasMessageThat().contains(BUCKET_NAME);

    // 2. Src unexpected error
    IOException srcIOException =
        assertThrows(
            IOException.class,
            () ->
                gcs.copy(
                    BUCKET_NAME,
                    ImmutableList.of(OBJECT_NAME),
                    dstBucketName,
                    ImmutableList.of(dstObjectName)));
    assertThat(srcIOException).hasMessageThat().isEqualTo(wrappedUnexpectedException1.getMessage());
    assertThat(srcIOException).hasCauseThat().isEqualTo(unexpectedException);

    // 3. Dst 404
    FileNotFoundException dstFileNotFoundException =
        assertThrows(
            FileNotFoundException.class,
            () ->
                gcs.copy(
                    BUCKET_NAME,
                    ImmutableList.of(OBJECT_NAME),
                    dstBucketName,
                    ImmutableList.of(dstObjectName)));
    assertThat(dstFileNotFoundException).hasMessageThat().contains(dstBucketName);

    // 4. Dst unexpected error
    IOException dstIOException =
        assertThrows(
            IOException.class,
            () ->
                gcs.copy(
                    BUCKET_NAME,
                    ImmutableList.of(OBJECT_NAME),
                    dstBucketName,
                    ImmutableList.of(dstObjectName)));
    assertThat(dstIOException).hasMessageThat().isEqualTo(wrappedUnexpectedException2.getMessage());
    assertThat(dstIOException).hasCauseThat().isEqualTo(unexpectedException);

    verify(mockStorage, times(6)).buckets();
    verify(mockStorageBuckets, times(6)).get(any(String.class));
    verify(mockStorageBucketsGet, times(4)).execute();
    verify(mockStorageBucketsGet2, times(2)).execute();
    verify(mockErrorExtractor, times(4)).itemNotFound(any(IOException.class));
  }

  /**
   * Test behavior of GoogleCloudStorage.copy(4) where srcBucketName != dstBucketName and the
   * retrieved bucket metadata indicates they are not compatible for copying.
   */
  @Test
  public void testCopyObjectsIncompatibleBuckets() throws IOException {
    List<String> srcObject = ImmutableList.of(OBJECT_NAME);
    String dstBucket = BUCKET_NAME + "-copy";
    List<String> dstObject = ImmutableList.of(OBJECT_NAME + "-copy");

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            jsonDataResponse(newBucket(BUCKET_NAME).setLocation("us-east-incomp")),
            jsonDataResponse(newBucket(dstBucket)),
            jsonDataResponse(newBucket(BUCKET_NAME).setStorageClass("class-be2-incomp")),
            jsonDataResponse(newBucket(dstBucket)));

    Storage storage = new Storage(transport, JSON_FACTORY, trackingHttpRequestInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(GCS_OPTIONS, storage);

    UnsupportedOperationException locationException =
        assertThrows(
            UnsupportedOperationException.class,
            () -> gcs.copy(BUCKET_NAME, srcObject, dstBucket, dstObject));
    assertThat(locationException).hasMessageThat().contains("not supported");
    assertThat(locationException).hasMessageThat().contains("storage location");

    UnsupportedOperationException storageClassException =
        assertThrows(
            UnsupportedOperationException.class,
            () -> gcs.copy(BUCKET_NAME, srcObject, dstBucket, dstObject));
    assertThat(storageClassException).hasMessageThat().contains("not supported");
    assertThat(storageClassException).hasMessageThat().contains("storage class");

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getBucketRequestString(BUCKET_NAME),
            getBucketRequestString(dstBucket),
            getBucketRequestString(BUCKET_NAME),
            getBucketRequestString(dstBucket))
        .inOrder();
  }

  /** Test for GoogleCloudStorage.listBucketNames(0). */
  @Test
  public void testListBucketNames() throws IOException {
    List<Bucket> buckets =
        ImmutableList.of(newBucket("bucket0"), newBucket("bucket1"), newBucket("bucket2"));

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            jsonDataResponse(new Buckets().setItems(buckets)),
            jsonDataResponse(new StorageObject().setBucket(BUCKET_NAME)));

    Storage storage = new Storage(transport, JSON_FACTORY, trackingHttpRequestInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(GCS_OPTIONS, storage);

    List<String> bucketNames = gcs.listBucketNames();

    assertThat(bucketNames).containsExactly("bucket0", "bucket1", "bucket2");
    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(listBucketsRequestString(PROJECT_ID));
  }

  /** Test for GoogleCloudStorage.listBucketInfo(0). */
  @Test
  public void testListBucketInfo() throws IOException {
    List<Bucket> buckets =
        ImmutableList.of(newBucket("bucket0"), newBucket("bucket1"), newBucket("bucket2"));

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            jsonDataResponse(new Buckets().setItems(buckets)));

    Storage storage = new Storage(transport, JSON_FACTORY, trackingHttpRequestInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(GCS_OPTIONS, storage);

    List<GoogleCloudStorageItemInfo> bucketInfos = gcs.listBucketInfo();

    assertThat(bucketInfos)
        .containsExactlyElementsIn(
            buckets.stream()
                .map(b -> createItemInfoForBucket(new StorageResourceId(b.getName()), b))
                .toArray());
    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(listBucketsRequestString(PROJECT_ID));
  }

  /** Test successful operation of GoogleCloudStorage.listObjectNames(3). */
  @Test
  public void testListObjectNamesPrefix() throws IOException {
    String prefix = "foo/bar/baz/";
    String pageToken = "pageToken0";

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            jsonDataResponse(
                new Objects()
                    .setPrefixes(ImmutableList.of("foo/bar/baz/dir0/", "foo/bar/baz/dir1/"))
                    .setNextPageToken(pageToken)),
            jsonDataResponse(
                new Objects()
                    .setItems(
                        ImmutableList.of(
                            newStorageObject(BUCKET_NAME, "foo/bar/baz/"),
                            newStorageObject(BUCKET_NAME, "foo/bar/baz/obj0"),
                            newStorageObject(BUCKET_NAME, "foo/bar/baz/obj1")))
                    .setNextPageToken(null)));

    Storage storage = new Storage(transport, JSON_FACTORY, trackingHttpRequestInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(GCS_OPTIONS, storage);

    List<String> objectNames = gcs.listObjectNames(BUCKET_NAME, prefix, PATH_DELIMITER);

    assertThat(objectNames)
        .containsExactly(
            "foo/bar/baz/dir0/", "foo/bar/baz/dir1/", "foo/bar/baz/obj0", "foo/bar/baz/obj1")
        .inOrder();
    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            listRequestString(BUCKET_NAME, prefix, /* maxResults= */ 1024, pageToken),
            listRequestString(BUCKET_NAME, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  /** Test GoogleCloudStorage.listObjectNames(3) with maxResults set. */
  @Test
  public void testListObjectNamesPrefixLimited() throws IOException {
    String prefix = "foo/bar/baz/";
    int maxResults = 3;

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            jsonDataResponse(
                new Objects()
                    .setPrefixes(ImmutableList.of("foo/bar/baz/dir0/", "foo/bar/baz/dir1/"))
                    .setItems(
                        ImmutableList.of(
                            newStorageObject(BUCKET_NAME, "foo/bar/baz/"),
                            newStorageObject(BUCKET_NAME, "foo/bar/baz/obj0")))
                    .setNextPageToken("pageToken0")));

    Storage storage = new Storage(transport, JSON_FACTORY, trackingHttpRequestInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(GCS_OPTIONS, storage);

    List<String> objectNames = gcs.listObjectNames(BUCKET_NAME, prefix, PATH_DELIMITER, maxResults);

    assertThat(objectNames)
        .containsExactly("foo/bar/baz/dir0/", "foo/bar/baz/dir1/", "foo/bar/baz/obj0")
        .inOrder();
    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            listRequestString(BUCKET_NAME, prefix, maxResults + 1, /* pageToken= */ null));
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.listObjectNames(3).
   */
  @Test
  public void testListObjectNamesPrefixApiException() throws IOException {
    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.list(eq(BUCKET_NAME)))
        .thenReturn(mockStorageObjectsList);

    IOException notFoundException = new IOException("Fake not-found exception");
    IOException unexpectedException = new IOException("Other API exception");
    when(mockStorageObjectsList.getPrefix()).thenReturn(objectPrefix);
    when(mockStorageObjectsList.getBucket()).thenReturn(BUCKET_NAME);
    when(mockStorageObjectsList.execute())
        .thenThrow(notFoundException)
        .thenThrow(unexpectedException);
    when(mockErrorExtractor.itemNotFound(eq(notFoundException)))
        .thenReturn(true);
    when(mockErrorExtractor.itemNotFound(eq(unexpectedException)))
        .thenReturn(false);

    // First time should just return empty list.
    List<String> objectNames = gcs.listObjectNames(BUCKET_NAME, objectPrefix, delimiter);
    assertThat(objectNames).isEmpty();

    // Second time throws.
    assertThrows(
        IOException.class, () -> gcs.listObjectNames(BUCKET_NAME, objectPrefix, delimiter));

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects, times(2)).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList, times(2)).setMaxResults(
        eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList, times(2)).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList, times(2)).setIncludeTrailingDelimiter(eq(Boolean.FALSE));
    verify(mockStorageObjectsList, times(2)).setPrefix(eq(objectPrefix));
    verify(mockStorageObjectsList, times(2)).getBucket();
    verify(mockStorageObjectsList, times(2)).getPrefix();
    verify(mockStorageObjectsList, times(2)).execute();
    verify(mockErrorExtractor, times(2)).itemNotFound(any(IOException.class));
  }

  @Test
  public void testListObjectInfoBasic() throws IOException {
    String prefix = "foo/bar/baz/";

    StorageObject object1 = newStorageObject(BUCKET_NAME, "foo/bar/baz/obj0");
    StorageObject object2 = newStorageObject(BUCKET_NAME, "foo/bar/baz/obj1");

    List<StorageObject> objects =
        ImmutableList.of(newStorageObject(BUCKET_NAME, "foo/bar/baz/"), object1, object2);

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            jsonDataResponse(new Objects().setItems(objects).setNextPageToken(null)));

    Storage storage = new Storage(transport, JSON_FACTORY, trackingHttpRequestInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(GCS_OPTIONS, storage);

    List<GoogleCloudStorageItemInfo> objectInfos =
        gcs.listObjectInfo(BUCKET_NAME, prefix, PATH_DELIMITER);

    // The item exactly matching the input prefix will be discarded.
    assertThat(objectInfos)
        .containsExactly(
            createItemInfoForStorageObject(
                new StorageResourceId(BUCKET_NAME, object1.getName()), object1),
            createItemInfoForStorageObject(
                new StorageResourceId(BUCKET_NAME, object2.getName()), object2));
    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            listRequestString(
                BUCKET_NAME,
                /* includeTrailingDelimiter= */ true,
                prefix,
                /* maxResults= */ 1024,
                /* pageToken= */ null));
  }

  @Test
  public void testListObjectInfoReturnPrefixes()
      throws IOException {
    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";
    String dir0Name = "foo/bar/baz/dir0/";
    String dir1Name = "foo/bar/baz/dir1/";
    StorageObject dir0 =
        new StorageObject()
            .setName(dir0Name)
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(11L))
            .setSize(BigInteger.valueOf(111L))
            .setGeneration(1L)
            .setMetageneration(1L);
    StorageObject dir1 =
        new StorageObject()
            .setName(dir1Name)
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(22L))
            .setSize(BigInteger.valueOf(222L))
            .setGeneration(2L)
            .setMetageneration(2L);

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.list(eq(BUCKET_NAME))).thenReturn(mockStorageObjectsList);
    when(mockStorageObjectsList.getPrefix()).thenReturn(objectPrefix);
    when(mockStorageObjectsList.execute())
        .thenReturn(
            new Objects()
                .setPrefixes(ImmutableList.of(dir0Name, dir1Name))
                .setItems(ImmutableList.of(dir0, dir1))
                .setNextPageToken(null));

    List<GoogleCloudStorageItemInfo> objectInfos =
        gcs.listObjectInfo(BUCKET_NAME, objectPrefix, delimiter);

    assertThat(objectInfos)
        .containsExactly(
            createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir0Name), dir0),
            createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir1Name), dir1));

    verify(mockStorage).objects();
    verify(mockStorageObjects).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList)
        .setMaxResults(eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList).setIncludeTrailingDelimiter(eq(Boolean.TRUE));
    verify(mockStorageObjectsList).setPrefix(eq(objectPrefix));
    verify(mockStorageObjectsList).getPrefix();
    verify(mockStorageObjectsList).execute();
  }

  @Test
  public void testListObjectInfoReturnPrefixesNotFound()
      throws IOException {
    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";
    String dir0Name = "foo/bar/baz/dir0/";
    String dir1Name = "foo/bar/baz/dir1/";
    String dir2Name = "foo/bar/baz/dir2/";
    StorageObject dir0 =
        new StorageObject()
            .setName(dir0Name)
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(11L))
            .setSize(BigInteger.valueOf(111L))
            .setGeneration(1L)
            .setMetageneration(1L);
    StorageObject dir1 =
        new StorageObject()
            .setName(dir1Name)
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(22L))
            .setSize(BigInteger.valueOf(222L))
            .setGeneration(2L)
            .setMetageneration(2L);
    StorageObject dir2 =
        new StorageObject()
            .setName(dir2Name)
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(33L))
            .setSize(BigInteger.valueOf(333L))
            .setGeneration(3L)
            .setMetageneration(3L);

    // Set up the initial list to return three prefixes, two of which don't exist.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.list(any(String.class))).thenReturn(mockStorageObjectsList);
    when(mockStorageObjectsList.getPrefix()).thenReturn(objectPrefix);
    when(mockStorageObjectsList.execute())
        .thenReturn(
            new Objects()
                .setPrefixes(ImmutableList.of(dir0Name, dir1Name, dir2Name))
                .setItems(ImmutableList.of(dir1))
                .setNextPageToken(null));

    // Set up the follow-up getItemInfos to just return a batch with "not found".
    when(mockBatchFactory.newBatchHelper(any(), any(Storage.class), anyLong(), anyLong(), anyInt()))
        .thenReturn(mockBatchHelper);
    when(mockStorageObjects.get(any(String.class), any(String.class)))
        .thenReturn(mockStorageObjectsGet);
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              @SuppressWarnings("unchecked")
              JsonBatchCallback<StorageObject> callback =
                  (JsonBatchCallback<StorageObject>) args[1];
              try {
                callback.onSuccess(dir0, new HttpHeaders());
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
                callback.onSuccess(dir2, new HttpHeaders());
              } catch (IOException ioe) {
                fail(ioe.toString());
              }
              return null;
            })
        .when(mockBatchHelper)
        .queue(eq(mockStorageObjectsGet), any());

    List<GoogleCloudStorageItemInfo> objectInfos =
        gcs.listObjectInfo(BUCKET_NAME, objectPrefix, delimiter);

    // objects().list
    verify(mockStorage).objects();
    verify(mockStorageObjects).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList)
        .setMaxResults(eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList).setIncludeTrailingDelimiter(eq(Boolean.TRUE));
    verify(mockStorageObjectsList).setPrefix(eq(objectPrefix));
    verify(mockStorageObjectsList).getPrefix();
    verify(mockStorageObjectsList).execute();

    // Check logical contents after all the "verify" calls, otherwise the mock verifications won't
    // be executed and we'll have misleading "NoInteractionsWanted" errors.
    assertThat(objectInfos)
        .containsExactly(
            createInferredDirectory(new StorageResourceId(BUCKET_NAME, dir0Name)),
            createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir1Name), dir1),
            createInferredDirectory(new StorageResourceId(BUCKET_NAME, dir2Name)));
  }

  @Test
  public void testListObjectInfoWithoutInferImplicit() throws IOException {
    runTestListObjectInfo(false);
  }

  private void runTestListObjectInfo(boolean inferImplicit) throws IOException {
    GoogleCloudStorage gcs = createTestInstanceWithInferImplicit(inferImplicit);

    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";
    String dir0Name = "foo/bar/baz/dir0/";
    String dir1Name = "foo/bar/baz/dir1/";
    String dir2Name = "foo/bar/baz/dir2/";
    StorageObject dir1 =
        new StorageObject()
            .setName(dir1Name)
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(22L))
            .setSize(BigInteger.valueOf(222L))
            .setGeneration(2L)
            .setMetageneration(2L);

    // Set up the initial list to return three prefixes and one item
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.list(any(String.class))).thenReturn(mockStorageObjectsList);
    when(mockStorageObjectsList.getPrefix()).thenReturn(objectPrefix);
    when(mockStorageObjectsList.execute())
        .thenReturn(
            new Objects()
                .setPrefixes(ImmutableList.of(dir0Name, dir1Name, dir2Name))
                .setItems(ImmutableList.of(dir1))
                .setNextPageToken(null));

    // List the objects
    List<GoogleCloudStorageItemInfo> objectInfos =
        gcs.listObjectInfo(BUCKET_NAME, objectPrefix, delimiter);

    // objects().list
    verify(mockStorage).objects();
    verify(mockStorageObjects).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList).setMaxResults(
        eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList).setIncludeTrailingDelimiter(eq(Boolean.TRUE));
    verify(mockStorageObjectsList).setPrefix(eq(objectPrefix));
    verify(mockStorageObjectsList).getPrefix();
    verify(mockStorageObjectsList).execute();

    // Check logical contents after all the "verify" calls, otherwise the
    // mock verifications won't be executed and we'll have misleading
    // "NoInteractionsWanted" errors.

    if (gcs.getOptions().isInferImplicitDirectoriesEnabled()) {
      assertThat(objectInfos)
          .containsExactly(
              createInferredDirectory(new StorageResourceId(BUCKET_NAME, dir0Name)),
              createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir1Name), dir1),
              createInferredDirectory(new StorageResourceId(BUCKET_NAME, dir2Name)));
    } else {
      assertThat(objectInfos)
          .containsExactly(
              createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir1Name), dir1));
    }
  }

  @Test
  public void testListObjectInfoInferImplicit()
      throws IOException {
    GoogleCloudStorage gcsInferImplicit =
        createTestInstanceWithInferImplicit(true);

    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";
    String dir0Name = "foo/bar/baz/dir0/";
    String dir1Name = "foo/bar/baz/dir1/";
    String dir2Name = "foo/bar/baz/dir2/";
    StorageObject dir1 =
        new StorageObject()
            .setName(dir1Name)
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(22L))
            .setSize(BigInteger.valueOf(222L))
            .setGeneration(2L)
            .setMetageneration(2L);

    // Set up the initial list to return three prefixes,
    // two of which don't exist.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.list(any(String.class))).thenReturn(mockStorageObjectsList);
    when(mockStorageObjectsList.getPrefix()).thenReturn(objectPrefix);
    when(mockStorageObjectsList.execute())
        .thenReturn(
            new Objects()
                .setPrefixes(ImmutableList.of(dir0Name, dir1Name, dir2Name))
                .setItems(ImmutableList.of(dir1))
                .setNextPageToken(null));

    // List the objects
    List<GoogleCloudStorageItemInfo> objectInfos =
        gcsInferImplicit.listObjectInfo(BUCKET_NAME, objectPrefix, delimiter);

    // objects().list
    verify(mockStorage).objects();
    verify(mockStorageObjects).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList).setMaxResults(
        eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList).setIncludeTrailingDelimiter(eq(Boolean.TRUE));
    verify(mockStorageObjectsList).setPrefix(eq(objectPrefix));
    verify(mockStorageObjectsList).getPrefix();
    verify(mockStorageObjectsList).execute();

    // Check logical contents after all the "verify" calls, otherwise the
    // mock verifications won't be executed and we'll have misleading
    // "NoInteractionsWanted" errors.

    // Only one of our three directory objects existed.
    assertThat(objectInfos)
        .containsExactly(
            createInferredDirectory(new StorageResourceId(BUCKET_NAME, dir0Name)),
            createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir1Name), dir1),
            createInferredDirectory(new StorageResourceId(BUCKET_NAME, dir2Name)));
  }

  /**
   * Test GoogleCloudStorage.getItemInfo(StorageResourceId) when arguments represent ROOT.
   */
  @Test
  public void testGetItemInfoRoot()
      throws IOException {
    GoogleCloudStorageItemInfo info = gcs.getItemInfo(StorageResourceId.ROOT);
    assertThat(info).isEqualTo(GoogleCloudStorageItemInfo.ROOT_INFO);
  }

  /**
   * Test GoogleCloudStorage.getItemInfo(StorageResourceId) when arguments represent only a bucket.
   */
  @Test
  public void testGetItemInfoBucket() throws IOException {
    Bucket bucket = newBucket(BUCKET_NAME);
    StorageResourceId bucketId = new StorageResourceId(bucket.getName());

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(jsonDataResponse(bucket));

    Storage storage = new Storage(transport, JSON_FACTORY, trackingHttpRequestInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(GCS_OPTIONS, storage);

    GoogleCloudStorageItemInfo info = gcs.getItemInfo(bucketId);

    assertThat(info).isEqualTo(createItemInfoForBucket(bucketId, bucket));
    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(getBucketRequestString(BUCKET_NAME));
  }

  /** Test handling of mismatch in Bucket.getName() vs StorageResourceId.getBucketName(). */
  @Test
  public void testGetItemInfoBucketReturnMismatchedName() throws IOException {
    Bucket bucket = newBucket("wrong-bucket-name");
    StorageResourceId bucketId = new StorageResourceId(BUCKET_NAME);

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(jsonDataResponse(bucket));

    Storage storage = new Storage(transport, JSON_FACTORY, trackingHttpRequestInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(GCS_OPTIONS, storage);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> gcs.getItemInfo(bucketId));

    String expectedMsg =
        String.format(
            "resourceId.getBucketName() must equal bucket.getName(): '%s' vs '%s'",
            BUCKET_NAME, bucket.getName());
    assertThat(e).hasMessageThat().isEqualTo(expectedMsg);
    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(getBucketRequestString(BUCKET_NAME));
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.getItemInfo(2) when arguments represent only a bucket.
   */
  @Test
  public void testGetItemInfoBucketApiException()
      throws IOException {
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.get(eq(BUCKET_NAME))).thenReturn(mockStorageBucketsGet);
    IOException notFoundException = new IOException("Fake not-found exception");
    IOException unexpectedException = new IOException("Other API exception");
    when(mockStorageBucketsGet.execute())
        .thenThrow(notFoundException)
        .thenThrow(unexpectedException);
    when(mockErrorExtractor.itemNotFound(eq(notFoundException)))
        .thenReturn(true);
    when(mockErrorExtractor.itemNotFound(eq(unexpectedException)))
        .thenReturn(false);

    // Not found.
    GoogleCloudStorageItemInfo info = gcs.getItemInfo(new StorageResourceId(BUCKET_NAME));
    GoogleCloudStorageItemInfo expected = new GoogleCloudStorageItemInfo(
        new StorageResourceId(BUCKET_NAME), 0L, -1L, null, null);
    assertThat(info).isEqualTo(expected);

    // Throw.
    assertThrows(IOException.class, () -> gcs.getItemInfo(new StorageResourceId(BUCKET_NAME)));

    verify(mockStorage, times(2)).buckets();
    verify(mockStorageBuckets, times(2)).get(eq(BUCKET_NAME));
    verify(mockStorageBucketsGet, times(2)).execute();
    verify(mockErrorExtractor, times(2)).itemNotFound(any(IOException.class));
  }

  /**
   * Test GoogleCloudStorage.getItemInfo(StorageResourceId) when arguments represent an object in
   * a bucket.
   */
  @Test
  public void testGetItemInfoObject()
      throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute())
        .thenReturn(new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(OBJECT_NAME)
            .setUpdated(new DateTime(1234L))
            .setSize(BigInteger.valueOf(42L))
            .setContentType("text/plain")
            .setGeneration(1L)
            .setMetageneration(1L));
    GoogleCloudStorageItemInfo info =
        gcs.getItemInfo(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    GoogleCloudStorageItemInfo expected =
        new GoogleCloudStorageItemInfo(
            new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
            1234L,
            42L,
            null,
            null,
            "text/plain",
            null,
            EMPTY_METADATA,
            1L,
            1L);
    assertThat(info).isEqualTo(expected);

    verify(mockStorage).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet).execute();
  }

  /**
   * Test handling of mismatch in StorageObject.getBucket() and StorageObject.getName() vs
   * respective values in the queried StorageResourceId.
   */
  @Test
  public void testGetItemInfoObjectReturnMismatchedName()
      throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute())
        .thenReturn(newStorageObject("wrong-bucket-name", OBJECT_NAME))
        .thenReturn(newStorageObject(BUCKET_NAME, "wrong-object-name"));

    assertThrows(
        IllegalArgumentException.class,
        () -> gcs.getItemInfo(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));
    assertThrows(
        IllegalArgumentException.class,
        () -> gcs.getItemInfo(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects, times(2)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(2)).execute();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.getItemInfo(StorageResourceId) when arguments represent an object in a
   * bucket.
   */
  @Test
  public void testGetItemInfoObjectApiException()
      throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    IOException notFoundException = new IOException("Fake not-found exception");
    IOException unexpectedException = new IOException("Other API exception");
    when(mockStorageObjectsGet.execute())
        .thenThrow(notFoundException)
        .thenThrow(unexpectedException);
    when(mockErrorExtractor.itemNotFound(eq(notFoundException)))
        .thenReturn(true);
    when(mockErrorExtractor.itemNotFound(eq(unexpectedException)))
        .thenReturn(false);

    // Not found.
    GoogleCloudStorageItemInfo info =
        gcs.getItemInfo(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    GoogleCloudStorageItemInfo expected =
        new GoogleCloudStorageItemInfo(
            new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
            0L,
            -1L,
            null,
            null,
            null,
            null,
            EMPTY_METADATA,
            /* contentGeneration= */ 0,
            /* metaGeneration= */ 0);
    assertThat(info).isEqualTo(expected);

    // Throw.
    assertThrows(
        IOException.class, () -> gcs.getItemInfo(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects, times(2)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(2)).execute();
    verify(mockErrorExtractor, times(2)).itemNotFound(any(IOException.class));
  }

  @Test
  public void testGetItemInfos()
      throws IOException {
    when(mockBatchFactory.newBatchHelper(any(), any(Storage.class), anyLong(), anyLong(), anyInt()))
        .thenReturn(mockBatchHelper);

    // Set up the return for the Bucket fetch.
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.get(eq(BUCKET_NAME))).thenReturn(mockStorageBucketsGet);
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              @SuppressWarnings("unchecked")
              JsonBatchCallback<Bucket> callback = (JsonBatchCallback<Bucket>) args[1];
              try {
                callback.onSuccess(
                    new Bucket()
                        .setName(BUCKET_NAME)
                        .setTimeCreated(new DateTime(1234L))
                        .setLocation("us-west-123")
                        .setStorageClass("class-af4"),
                    new HttpHeaders());
              } catch (IOException ioe) {
                fail(ioe.toString());
              }
              return null;
            })
        .when(mockBatchHelper)
        .queue(eq(mockStorageBucketsGet), any());

    // Set up the return for the StorageObject fetch.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              @SuppressWarnings("unchecked")
              JsonBatchCallback<StorageObject> callback =
                  (JsonBatchCallback<StorageObject>) args[1];
              try {
                callback.onSuccess(
                    new StorageObject()
                        .setBucket(BUCKET_NAME)
                        .setName(OBJECT_NAME)
                        .setUpdated(new DateTime(1234L))
                        .setSize(BigInteger.valueOf(42L))
                        .setContentType("image/png")
                        .setGeneration(1L)
                        .setMetageneration(1L),
                    new HttpHeaders());
              } catch (IOException ioe) {
                fail(ioe.toString());
              }
              return null;
            })
        .when(mockBatchHelper)
        .queue(eq(mockStorageObjectsGet), any());

    // Call in order of StorageObject, ROOT, Bucket.
    List<GoogleCloudStorageItemInfo> itemInfos = gcs.getItemInfos(ImmutableList.of(
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
        StorageResourceId.ROOT,
        new StorageResourceId(BUCKET_NAME)));

    GoogleCloudStorageItemInfo expectedObject =
        new GoogleCloudStorageItemInfo(
            new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
            1234L,
            42L,
            null,
            null,
            "image/png",
            null,
            EMPTY_METADATA,
            /* contentGeneration= */ 1,
            /* metaGeneration= */ 1);
    GoogleCloudStorageItemInfo expectedRoot = GoogleCloudStorageItemInfo.ROOT_INFO;
    GoogleCloudStorageItemInfo expectedBucket = new GoogleCloudStorageItemInfo(
        new StorageResourceId(BUCKET_NAME), 1234L, 0L, "us-west-123", "class-af4");

    assertThat(itemInfos.get(0)).isEqualTo(expectedObject);
    assertThat(itemInfos.get(1)).isEqualTo(expectedRoot);
    assertThat(itemInfos.get(2)).isEqualTo(expectedBucket);

    verify(mockBatchFactory).newBatchHelper(any(), eq(mockStorage), anyLong(), anyLong(), anyInt());
    verify(mockStorage).buckets();
    verify(mockStorageBuckets).get(eq(BUCKET_NAME));
    verify(mockBatchHelper).queue(eq(mockStorageBucketsGet), any());
    verify(mockStorage).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockBatchHelper).queue(eq(mockStorageObjectsGet), any());
    verify(mockBatchHelper).flush();
  }

  @Test
  public void testGetItemInfosNotFound()
      throws IOException {
    when(mockBatchFactory.newBatchHelper(any(), any(Storage.class), anyLong(), anyLong(), anyInt()))
        .thenReturn(mockBatchHelper);

    // Set up the return for the Bucket fetch.
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.get(eq(BUCKET_NAME))).thenReturn(mockStorageBucketsGet);
    final GoogleJsonError notFoundError = new GoogleJsonError();
    notFoundError.setMessage("Fake not-found error");
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              @SuppressWarnings("unchecked")
              JsonBatchCallback<Bucket> callback = (JsonBatchCallback<Bucket>) args[1];
              try {
                callback.onFailure(notFoundError, new HttpHeaders());
              } catch (IOException ioe) {
                fail(ioe.toString());
              }
              return null;
            })
        .when(mockBatchHelper)
        .queue(eq(mockStorageBucketsGet), any());

    // Set up the return for the StorageObject fetch.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
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
        .when(mockBatchHelper)
        .queue(eq(mockStorageObjectsGet), any());

    // We will claim both GoogleJsonErrors are "not found" errors.
    when(mockErrorExtractor.itemNotFound(eq(notFoundError)))
        .thenReturn(true);

    // Call in order of StorageObject, ROOT, Bucket.
    List<GoogleCloudStorageItemInfo> itemInfos = gcs.getItemInfos(ImmutableList.of(
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
        StorageResourceId.ROOT,
        new StorageResourceId(BUCKET_NAME)));

    GoogleCloudStorageItemInfo expectedObject =
        GoogleCloudStorageItemInfo.createNotFound(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    GoogleCloudStorageItemInfo expectedRoot = GoogleCloudStorageItemInfo.ROOT_INFO;
    GoogleCloudStorageItemInfo expectedBucket =
        GoogleCloudStorageItemInfo.createNotFound(new StorageResourceId(BUCKET_NAME));

    assertThat(itemInfos.get(0)).isEqualTo(expectedObject);
    assertThat(itemInfos.get(1)).isEqualTo(expectedRoot);
    assertThat(itemInfos.get(2)).isEqualTo(expectedBucket);

    verify(mockBatchFactory).newBatchHelper(any(), eq(mockStorage), anyLong(), anyLong(), anyInt());
    verify(mockStorage).buckets();
    verify(mockStorageBuckets).get(eq(BUCKET_NAME));
    verify(mockBatchHelper).queue(eq(mockStorageBucketsGet), any());
    verify(mockStorage).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockBatchHelper).queue(eq(mockStorageObjectsGet), any());
    verify(mockErrorExtractor, times(2)).itemNotFound(any(GoogleJsonError.class));
    verify(mockBatchHelper).flush();
  }

  @Test
  public void testGetItemInfosApiException()
      throws IOException {
    when(mockBatchFactory.newBatchHelper(any(), any(Storage.class), anyLong(), anyLong(), anyInt()))
        .thenReturn(mockBatchHelper);

    // Set up the return for the Bucket fetch.
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.get(eq(BUCKET_NAME))).thenReturn(mockStorageBucketsGet);
    final GoogleJsonError unexpectedError = new GoogleJsonError();
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
        .queue(eq(mockStorageBucketsGet), any());

    // Set up the return for the StorageObject fetch.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
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
        .queue(eq(mockStorageObjectsGet), any());

    // We will claim both GoogleJsonErrors are unexpected errors.
    when(mockErrorExtractor.itemNotFound(eq(unexpectedError)))
        .thenReturn(false);

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
    verify(mockBatchFactory).newBatchHelper(any(), eq(mockStorage), anyLong(), anyLong(), anyInt());
    verify(mockStorage).buckets();
    verify(mockStorageBuckets).get(eq(BUCKET_NAME));
    verify(mockBatchHelper).queue(eq(mockStorageBucketsGet), any());
    verify(mockStorage).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockBatchHelper).queue(eq(mockStorageObjectsGet), any());
    verify(mockErrorExtractor, times(2)).itemNotFound(any(GoogleJsonError.class));
    verify(mockBatchHelper).flush();
  }

  /**
   * Test for GoogleCloudStorage.close(0).
   */
  @Test
  public void testClose() {
    gcs.close();
    assertThat(executorService.isShutdown()).isTrue();
  }

  @Test
  public void testComposeSuccess() throws Exception {
    String destination = "composedObject";
    List<String> sources = ImmutableList.of("object1", "object2");
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(BUCKET_NAME, destination)).thenReturn(mockStorageObjectsGet);
    when(mockStorageObjects.compose(eq(BUCKET_NAME), eq(destination), any(ComposeRequest.class)))
        .thenReturn(mockStorageObjectsCompose);
    when(mockStorageObjectsCompose.execute())
        .thenReturn(newStorageObject(BUCKET_NAME, destination));

    gcs.compose(BUCKET_NAME, sources, destination, CreateFileOptions.DEFAULT_CONTENT_TYPE);

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(destination));
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjects).compose(eq(BUCKET_NAME), eq(destination), any(ComposeRequest.class));
    verify(mockStorageObjectsCompose).setIfGenerationMatch(0L);
    verify(mockStorageObjectsCompose).execute();
  }

  @Test
  public void testComposeObjectsWithGenerationId() throws Exception {
    String destination = "composedObject";
    StorageResourceId destinationId = new StorageResourceId(BUCKET_NAME, destination, 222L);
    List<StorageResourceId> sources = ImmutableList.of(
        new StorageResourceId(BUCKET_NAME, "object1"),
        new StorageResourceId(BUCKET_NAME, "object2"));
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.compose(eq(BUCKET_NAME), eq(destination), any(ComposeRequest.class)))
        .thenReturn(mockStorageObjectsCompose);
    when(mockStorageObjectsCompose.execute())
        .thenReturn(newStorageObject(BUCKET_NAME, destination));

    Map<String, byte[]> rawMetadata =
        ImmutableMap.of("foo", new byte[] { 0x01 }, "bar", new byte[] { 0x02 });
    gcs.composeObjects(
        sources, destinationId, new CreateObjectOptions(false, "text-content", rawMetadata));

    ArgumentCaptor<ComposeRequest> composeCaptor = ArgumentCaptor.forClass(ComposeRequest.class);
    verify(mockStorage).objects();
    verify(mockStorageObjects).compose(eq(BUCKET_NAME), eq(destination), composeCaptor.capture());
    verify(mockStorageObjectsCompose).setIfGenerationMatch(222L);
    verify(mockStorageObjectsCompose).execute();

    assertThat(composeCaptor.getValue().getSourceObjects()).hasSize(2);
    assertThat(composeCaptor.getValue().getSourceObjects().get(0).getName())
        .isEqualTo(sources.get(0).getObjectName());
    assertThat(composeCaptor.getValue().getSourceObjects().get(1).getName())
        .isEqualTo(sources.get(1).getObjectName());
    assertThat(composeCaptor.getValue().getDestination().getContentType())
        .isEqualTo("text-content");
    assertThat(composeCaptor.getValue().getDestination().getMetadata())
        .isEqualTo(GoogleCloudStorageImpl.encodeMetadata(rawMetadata));
  }

  /**
   * Test validation of Storage passing constructor.
   */
  @Test
  public void testStoragePassedConstructor() {
    GoogleCloudStorageOptions.Builder optionsBuilder =
        GoogleCloudStorageOptions.builder().setAppName("appName").setProjectId("projectId");

    // Verify that fake projectId/appName and mock storage does not throw.
    new GoogleCloudStorageImpl(optionsBuilder.build(), mockStorage);

    // Verify that projectId == null or empty does not throw.
    optionsBuilder.setProjectId(null);
    new GoogleCloudStorageImpl(optionsBuilder.build(), mockStorage);

    optionsBuilder.setProjectId("");
    new GoogleCloudStorageImpl(optionsBuilder.build(), mockStorage);

    optionsBuilder.setProjectId("projectId");

    // Verify that appName == null or empty throws IllegalArgumentException.

    optionsBuilder.setAppName(null);
    assertThrows(
        IllegalArgumentException.class,
        () -> new GoogleCloudStorageImpl(optionsBuilder.build(), mockStorage));

    optionsBuilder.setAppName("");
    assertThrows(
        IllegalArgumentException.class,
        () -> new GoogleCloudStorageImpl(optionsBuilder.build(), mockStorage));

    optionsBuilder.setAppName("appName");

    // Verify that gcs == null throws NullPointerException.
    assertThrows(
        NullPointerException.class,
        () -> new GoogleCloudStorageImpl(optionsBuilder.build(), (Storage) null));
  }

  /**
   * Provides coverage for default constructor. No real validation is performed.
   */
  @Test
  public void testCoverDefaultConstructor() {
    new GoogleCloudStorageImpl();
  }

  /**
   * Coverage for GoogleCloudStorageItemInfo.metadataEquals.
   */
  @Test
  public void testItemInfoMetadataEquals() {
    assertThat(getItemInfoForEmptyObjectWithMetadata(EMPTY_METADATA).metadataEquals(EMPTY_METADATA))
        .isTrue();

    // The factory method changes 'null' to the empty map, but that doesn't mean an empty
    // metadata setting equals 'null' as the parameter passed to metadataEquals.
    assertThat(getItemInfoForEmptyObjectWithMetadata(null).metadataEquals(EMPTY_METADATA)).isTrue();
    assertThat(getItemInfoForEmptyObjectWithMetadata(null).metadataEquals(null)).isFalse();

    //  Basic equality case.
    assertThat(
            getItemInfoForEmptyObjectWithMetadata(
                    ImmutableMap.of("foo", new byte[] {0x01}, "bar", new byte[] {0x02}))
                .metadataEquals(
                    ImmutableMap.of("foo", new byte[] {0x01}, "bar", new byte[] {0x02})))
        .isTrue();

    // Equality across different map implementations.
    assertThat(
            getItemInfoForEmptyObjectWithMetadata(
                    new HashMap<>(
                        ImmutableMap.of("foo", new byte[] {0x01}, "bar", new byte[] {0x02})))
                .metadataEquals(
                    new TreeMap<>(
                        ImmutableMap.of("foo", new byte[] {0x01}, "bar", new byte[] {0x02}))))
        .isTrue();

    // Even though the keySet() is equal for the two and the set of values() is equal for the two,
    // since we inverted which key points to which value, they should not be deemed equal.
    assertThat(
            getItemInfoForEmptyObjectWithMetadata(
                    ImmutableMap.of("foo", new byte[] {0x01}, "bar", new byte[] {0x02}))
                .metadataEquals(
                    ImmutableMap.of("foo", new byte[] {0x02}, "bar", new byte[] {0x01})))
        .isFalse();

    // Only a subset is equal.
    assertThat(
            getItemInfoForEmptyObjectWithMetadata(
                    ImmutableMap.of("foo", new byte[] {0x01}, "bar", new byte[] {0x02}))
                .metadataEquals(ImmutableMap.of("foo", new byte[] {0x01})))
        .isFalse();
  }

  @Test
  public void testItemInfoEqualityIncludesMetadata() {
    assertThat(
            getItemInfoForEmptyObjectWithMetadata(
                    ImmutableMap.of("foo", new byte[] {0x01}, "bar", new byte[] {0x02}))
                .equals(getItemInfoForEmptyObjectWithMetadata(null)))
        .isFalse();
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObject() throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockStorageObjectsInsert.execute())
        .thenThrow(new IOException("rateLimitExceeded"));
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(true);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute())
        .thenReturn(
            getStorageObjectForEmptyObjectWithMetadata(ImmutableMap.of("foo", new byte[0])));

    gcs.createEmptyObject(
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
        new CreateObjectOptions(true, ImmutableMap.of("foo", new byte[0])));

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects).insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert).execute();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet).execute();
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjectMismatchMetadata() throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockStorageObjectsInsert.execute())
        .thenThrow(new IOException("rateLimitExceeded"));
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(true);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute())
        .thenReturn(getStorageObjectForEmptyObjectWithMetadata(EMPTY_METADATA));

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                gcs.createEmptyObject(
                    new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                    new CreateObjectOptions(true, ImmutableMap.of("foo", new byte[0]))));
    assertThat(thrown).hasMessageThat().contains("rateLimitExceeded");

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects)
        .insert(eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert).execute();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet).execute();
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjectMismatchMetadataButOptionsHasNoMetadata()
      throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockStorageObjectsInsert.execute())
        .thenThrow(new IOException("rateLimitExceeded"));
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(true);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute())
        .thenReturn(
            getStorageObjectForEmptyObjectWithMetadata(ImmutableMap.of("foo", new byte[0])));

    // The fetch will "mismatch" with more metadata than our default EMPTY_METADATA used in the
    // default CreateObjectOptions, but we won't care because the metadata-check requirement
    // will be false, so the call will complete successfully.
    gcs.createEmptyObject(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects).insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert).execute();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet).execute();
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjects() throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockStorageObjectsInsert.execute())
        .thenThrow(new IOException("rateLimitExceeded"));
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(true);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute())
        .thenReturn(getStorageObjectForEmptyObjectWithMetadata(EMPTY_METADATA));

    gcs.createEmptyObjects(ImmutableList.of(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects).insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert).execute();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet).execute();
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjectsNonIgnorableException() throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockStorageObjectsInsert.execute())
        .thenThrow(new IOException("forbidden"));
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(false);
    when(mockErrorExtractor.isInternalServerError(any(IOException.class))).thenReturn(false);

    assertThrows(
        IOException.class,
        () ->
            gcs.createEmptyObjects(
                ImmutableList.of(new StorageResourceId(BUCKET_NAME, OBJECT_NAME))));

    verify(mockStorage).objects();
    verify(mockStorageObjects)
        .insert(eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert).execute();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockErrorExtractor).isInternalServerError(any(IOException.class));
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjectsErrorOnRefetch() throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(
            eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert)
        .thenReturn(mockStorageObjectsInsert);
    when(mockStorageObjectsInsert.execute())
        .thenThrow(new IOException("rateLimitExceeded"));
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(true);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet)
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute())
        .thenThrow(new RuntimeException("error while fetching"));

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                gcs.createEmptyObjects(
                    ImmutableList.of(
                        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                        new StorageResourceId(BUCKET_NAME, OBJECT_NAME))));
    assertThat(thrown).hasMessageThat().contains("Multiple IOExceptions");

    verify(mockStorage, times(4)).objects();
    verify(mockStorageObjects, times(2))
        .insert(eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert, times(2)).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper, times(2))
        .setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert, times(2)).execute();
    verify(mockErrorExtractor, times(2)).rateLimited(any(IOException.class));
    verify(mockStorageObjects, times(2)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(2)).execute();
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjectsWithMultipleRetries() throws Exception {
    IOException notFoundException = new IOException("NotFound");
    IOException rateLimitException = new IOException("RateLimited");
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockStorageObjectsInsert.execute())
        .thenThrow(rateLimitException);
    when(mockErrorExtractor.rateLimited(eq(rateLimitException))).thenReturn(true);
    when(mockErrorExtractor.itemNotFound(eq(notFoundException))).thenReturn(true);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute())
        .thenThrow(notFoundException)
        .thenThrow(notFoundException)
        .thenReturn(getStorageObjectForEmptyObjectWithMetadata(EMPTY_METADATA));

    gcs.createEmptyObjects(ImmutableList.of(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));

    verify(mockStorage, times(4)).objects(); // 1 insert, 3 gets
    verify(mockStorageObjects).insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert).execute();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockErrorExtractor, times(2)).itemNotFound(eq(notFoundException));
    verify(mockStorageObjects, times(3)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(3)).execute();
    verify(mockSleeper, times(2)).sleep(anyLong());
  }

  @Test
  public void testReadWithFailedInplaceSeekSucceeds() throws IOException {
    byte[] testData = {0x01, 0x02, 0x03, 0x05, 0x08};
    byte[] testData2 = Arrays.copyOfRange(testData, 3, testData.length);

    setUpBasicMockBehaviorForOpeningReadChannel(testData.length);

    InputStream mockExceptionStream = mock(InputStream.class);
    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(1))).thenReturn(1);
    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(2)))
        .thenThrow(new IOException("In-place seek IOException"));

    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(fakeResponse("Content-Length", testData.length, mockExceptionStream))
        .thenReturn(
            fakeResponse("Content-Length", testData2.length, new ByteArrayInputStream(testData2)));

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel)
            gcs.open(
                new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                GoogleCloudStorageReadOptions.builder().setInplaceSeekLimit(3).build());

    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 3);

    assertThat(readChannel.read(ByteBuffer.wrap(new byte[1]))).isEqualTo(1);

    readChannel.position(3);

    byte[] byte3 = new byte[1];
    assertThat(readChannel.read(ByteBuffer.wrap(byte3))).isEqualTo(1);

    assertThat(byte3).isEqualTo(new byte[] {testData[3]});

    verify(mockStorage, times(3)).objects();
    verify(mockStorageObjects, times(3)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(2)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(2)).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockHeaders).setRange(eq("bytes=3-"));
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjectsGet, times(2)).executeMedia();
    verify(mockStorageObjectsGet, times(2)).setGeneration(eq(null));
    verify(mockExceptionStream, times(2)).read(any(byte[].class), eq(0), anyInt());
  }

  private static Bucket newBucket(String name) {
    return new Bucket()
        .setName(name)
        .setLocation("us-central1-a")
        .setStorageClass("class-af4")
        .setTimeCreated(new DateTime(1111L));
  }

  private static StorageObject newStorageObject(String bucketName, String objectName) {
    Random r = new Random();
    return new StorageObject()
        .setBucket(bucketName)
        .setName(objectName)
        .setSize(BigInteger.valueOf(r.nextInt(Integer.MAX_VALUE)))
        .setGeneration((long) r.nextInt(Integer.MAX_VALUE))
        .setMetageneration((long) r.nextInt(Integer.MAX_VALUE))
        .setUpdated(new DateTime(new Date()));
  }
}
