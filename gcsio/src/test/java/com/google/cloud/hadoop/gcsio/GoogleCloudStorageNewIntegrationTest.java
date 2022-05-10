/*
 * Copyright 2019 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.decodeMetadata;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.batchRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.composeRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.copyRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.deleteRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getBucketRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getMediaRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestWithTrailingDelimiter;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.postRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.resumableUploadChunkRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.resumableUploadRequestString;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getStandardOptionBuilder;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertThrows;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for GoogleCloudStorageFileSystem class. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageNewIntegrationTest {

  private static final CreateObjectOptions GZIP_CREATE_OPTIONS =
      CreateObjectOptions.DEFAULT_NO_OVERWRITE.toBuilder().setContentEncoding("gzip").build();

  private static final ListObjectOptions INCLUDE_PREFIX_LIST_OPTIONS =
      ListObjectOptions.DEFAULT.toBuilder().setIncludePrefix(true).build();

  private static GoogleCloudStorageOptions gcsOptions;
  private static RetryHttpInitializer httpRequestsInitializer;
  private static GoogleCloudStorageFileSystemIntegrationHelper gcsfsIHelper;

  @Rule public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Throwable {
    Credential credential =
        checkNotNull(GoogleCloudStorageTestHelper.getCredential(), "credential must not be null");

    gcsOptions = getStandardOptionBuilder().build();
    httpRequestsInitializer =
        new RetryHttpInitializer(credential, gcsOptions.toRetryHttpInitializerOptions());

    GoogleCloudStorageFileSystem gcsfs =
        new GoogleCloudStorageFileSystemImpl(
            credential,
            GoogleCloudStorageFileSystemOptions.builder()
                .setBucketDeleteEnabled(true)
                .setCloudStorageOptions(gcsOptions)
                .build());
    gcsfsIHelper = new GoogleCloudStorageFileSystemIntegrationHelper(gcsfs);
    gcsfsIHelper.beforeAllTests();
  }

  @AfterClass
  public static void afterClass() throws Throwable {
    gcsfsIHelper.afterAllTests();
  }

  @Test
  public void listObjectInfo_nonExistentBucket_nullPrefix() throws Exception {
    list_nonExistentBucket_nullPrefix(
        (gcs, bucket, prefix) -> getObjectNames(gcs.listObjectInfo(bucket, prefix)),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_nonExistentBucket_nullPrefix() throws Exception {
    list_nonExistentBucket_nullPrefix(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  private void list_nonExistentBucket_nullPrefix(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = "list-neb-np_" + UUID.randomUUID();

    List<String> listResult = listFn.apply(gcs, testBucket, null);

    assertThat(listResult).isEmpty();
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(requestFn.apply(testBucket, /* prefix= */ null));
  }

  @Test
  public void listObjectInfo_emptyBucket_nullPrefix() throws Exception {
    list_emptyBucket_nullPrefix(
        (gcs, bucket, prefix) -> getObjectNames(gcs.listObjectInfo(bucket, prefix)),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_emptyBucket_nullPrefix() throws Exception {
    list_emptyBucket_nullPrefix(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  private void list_emptyBucket_nullPrefix(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = createUniqueBucket("list-eb-np");

    List<String> listResult = listFn.apply(gcs, testBucket, null);

    assertThat(listResult).isEmpty();
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(requestFn.apply(testBucket, /* prefix= */ null));
  }

  @Test
  public void listObjectInfo_nonEmptyBucket_nullPrefix_object() throws Exception {
    list_nonEmptyBucket_nullPrefix_object(
        (gcs, bucket, prefix) -> getObjectNames(gcs.listObjectInfo(bucket, prefix)),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_nonEmptyBucket_nullPrefix_object() throws Exception {
    list_nonEmptyBucket_nullPrefix_object(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  private void list_nonEmptyBucket_nullPrefix_object(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = createUniqueBucket("list-neb-np-o");
    gcsfsIHelper.createObjects(testBucket, "obj");

    List<String> listResult = listFn.apply(gcs, testBucket, null);

    assertThat(listResult).containsExactly("obj");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(requestFn.apply(testBucket, /* prefix= */ null));
  }

  @Test
  public void listObjectInfo_nonEmptyBucket_nullPrefix_emptyDir() throws Exception {
    list_nonEmptyBucket_nullPrefix_emptyDir(
        (gcs, bucket, prefix) -> getObjectNames(gcs.listObjectInfo(bucket, prefix)),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_nonEmptyBucket_nullPrefix_emptyDir() throws Exception {
    list_nonEmptyBucket_nullPrefix_emptyDir(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  private void list_nonEmptyBucket_nullPrefix_emptyDir(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = createUniqueBucket("list-neb-np-ed_");
    gcsfsIHelper.createObjects(testBucket, "dir/");

    List<String> listResult = listFn.apply(gcs, testBucket, null);

    assertThat(listResult).containsExactly("dir/");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(requestFn.apply(testBucket, /* prefix= */ null));
  }

  @Test
  public void listObjectInfo_nonEmptyBucket_nullPrefix_subDir() throws Exception {
    list_nonEmptyBucket_nullPrefix_subDir(
        (gcs, bucket, prefix) -> getObjectNames(gcs.listObjectInfo(bucket, prefix)),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_nonEmptyBucket_nullPrefix_subDir() throws Exception {
    list_nonEmptyBucket_nullPrefix_subDir(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  private void list_nonEmptyBucket_nullPrefix_subDir(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = createUniqueBucket("list-neb-np-sd");
    gcsfsIHelper.createObjects(testBucket, "subdir/", "subdir/obj");

    List<String> listResult = listFn.apply(gcs, testBucket, null);

    assertThat(listResult).containsExactly("subdir/");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(requestFn.apply(testBucket, /* prefix= */ null));
  }

  @Test
  public void listObjectInfo_nonEmptyBucket_nullPrefix_implicitSubDir() throws Exception {
    list_nonEmptyBucket_nullPrefix_implicitSubDir(
        (gcs, bucket, prefix) -> getObjectNames(gcs.listObjectInfo(bucket, prefix)),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_nonEmptyBucket_nullPrefix_implicitSubDir() throws Exception {
    list_nonEmptyBucket_nullPrefix_implicitSubDir(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  private void list_nonEmptyBucket_nullPrefix_implicitSubDir(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = createUniqueBucket("list-neb-np-isd");
    gcsfsIHelper.createObjects(testBucket, "subdir/obj");

    List<String> listResult = listFn.apply(gcs, testBucket, null);

    assertThat(listResult).containsExactly("subdir/");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(requestFn.apply(testBucket, /* prefix= */ null));
  }

  @Test
  public void listObjectInfo_prefix_doesNotExist() throws Exception {
    list_prefix_doesNotExist(
        (gcs, bucket, prefix) -> getObjectNames(gcs.listObjectInfo(bucket, prefix)),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_prefix_doesNotExist() throws Exception {
    list_prefix_doesNotExist(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  private void list_prefix_doesNotExist(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = String.format("list_prefix_doesNotExist_%s/", UUID.randomUUID());

    List<String> listResult = listFn.apply(gcs, testBucket, testDir);

    assertThat(listResult).isEmpty();
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(requestFn.apply(testBucket, testDir));
  }

  @Test
  public void listObjectInfo_prefixObject_empty() throws Exception {
    list_prefixObject_empty(
        (gcs, bucket, prefix) -> getObjectNames(gcs.listObjectInfo(bucket, prefix)),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_prefixObject_empty() throws Exception {
    list_prefixObject_empty(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  private void list_prefixObject_empty(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = getTestResource() + "/";
    gcsfsIHelper.createObjects(testBucket, testDir);

    List<String> listResult = listFn.apply(gcs, testBucket, testDir);

    assertThat(listResult).isEmpty();
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(requestFn.apply(testBucket, testDir));
  }

  @Test
  public void listObjectInfo_prefixObject_withObject() throws Exception {
    list_prefixObject_withObject(
        (gcs, bucket, prefix) -> getObjectNames(gcs.listObjectInfo(bucket, prefix)),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_prefixObject_withObject() throws Exception {
    list_prefixObject_withObject(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  private void list_prefixObject_withObject(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "obj");

    List<String> listResult = listFn.apply(gcs, testBucket, testDir);

    assertThat(listResult).containsExactly(testDir + "obj");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(requestFn.apply(testBucket, testDir));
  }

  @Test
  public void listObjectInfo_prefixObject_withSubdir() throws Exception {
    list_prefixObject_withSubdir(
        (gcs, bucket, prefix) -> getObjectNames(gcs.listObjectInfo(bucket, prefix)),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_prefixObject_withSubdir() throws Exception {
    list_prefixObject_withSubdir(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  private void list_prefixObject_withSubdir(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "sub/");

    List<String> listResult = listFn.apply(gcs, testBucket, testDir);

    assertThat(listResult).containsExactly(testDir + "sub/");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(requestFn.apply(testBucket, testDir));
  }

  @Test
  public void listObjectInfo_prefixObject_withImplicitSubdir() throws Exception {
    list_prefixObject_withImplicitSubdir(
        (gcs, bucket, prefix) -> getObjectNames(gcs.listObjectInfo(bucket, prefix)),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_prefixObject_withImplicitSubdir() throws Exception {
    list_prefixObject_withImplicitSubdir(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  private void list_prefixObject_withImplicitSubdir(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDirWithoutSubdirs(testBucket, "sub/obj");
    gcsfsIHelper.createObjects(testBucket, testDir);

    List<String> listResult = listFn.apply(gcs, testBucket, testDir);

    assertThat(listResult).containsExactly(testDir + "sub/");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(requestFn.apply(testBucket, testDir));
  }

  @Test
  public void listObjectInfo_implicitPrefix_withObject() throws Exception {
    list_implicitPrefix_withObject(
        (gcs, bucket, prefix) -> getObjectNames(gcs.listObjectInfo(bucket, prefix)),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_implicitPrefix_withObject() throws Exception {
    list_implicitPrefix_withObject(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  private void list_implicitPrefix_withObject(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDirWithoutSubdirs(testBucket, "obj");

    List<String> listResult = listFn.apply(gcs, testBucket, testDir);

    assertThat(listResult).containsExactly(testDir + "obj");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(requestFn.apply(testBucket, testDir));
  }

  @Test
  public void listObjectInfo_implicitPrefix_withSubdir() throws Exception {
    list_implicitPrefix_withSubdir(
        (gcs, bucket, prefix) -> getObjectNames(gcs.listObjectInfo(bucket, prefix)),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_implicitPrefix_withSubdir() throws Exception {
    list_implicitPrefix_withSubdir(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  private void list_implicitPrefix_withSubdir(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDirWithoutSubdirs(testBucket, "sub/");

    List<String> listResult = listFn.apply(gcs, testBucket, testDir);

    assertThat(listResult).containsExactly(testDir + "sub/");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(requestFn.apply(testBucket, testDir));
  }

  @Test
  public void listObjectInfo_implicitPrefix_withImplicitSubdir() throws Exception {
    list_implicitPrefix_withImplicitSubdir(
        (gcs, bucket, prefix) -> getObjectNames(gcs.listObjectInfo(bucket, prefix)),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_implicitPrefix_withImplicitSubdir() throws Exception {
    list_implicitPrefix_withImplicitSubdir(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(
                bucket, prefix, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  private void list_implicitPrefix_withImplicitSubdir(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDirWithoutSubdirs(testBucket, "sub/obj");

    List<String> listResult = listFn.apply(gcs, testBucket, testDir);

    assertThat(listResult).containsExactly(testDir + "sub/");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(requestFn.apply(testBucket, testDir));
  }

  @Test
  public void listObjectInfo_withLimit_oneGcsRequest() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(
            testBucket, testDir, ListObjectOptions.DEFAULT.toBuilder().setMaxResults(1).build());

    assertThat(getObjectNames(listedObjects)).containsExactly(testDir + "f1");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestString(
                testBucket, true, testDir, /* maxResults= */ 2, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_withLimit_multipleGcsRequests() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    int maxResultsPerRequest = 1;
    GoogleCloudStorageOptions options =
        gcsOptions.toBuilder().setMaxListItemsPerCall(maxResultsPerRequest).build();
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(options, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3", "f4");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(
            testBucket, testDir, ListObjectOptions.DEFAULT.toBuilder().setMaxResults(2).build());

    assertThat(getObjectNames(listedObjects)).containsExactly(testDir + "f1", testDir + "f2");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestString(
                testBucket, true, testDir, maxResultsPerRequest, /* pageToken= */ null),
            listRequestString(testBucket, true, testDir, maxResultsPerRequest, "token_1"),
            listRequestString(testBucket, true, testDir, maxResultsPerRequest, "token_2"));
  }

  @Test
  public void listObjectInfo_withoutLimits() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    int maxResultsPerRequest = 1;
    GoogleCloudStorageOptions options =
        gcsOptions.toBuilder().setMaxListItemsPerCall(maxResultsPerRequest).build();
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(options, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    List<GoogleCloudStorageItemInfo> listedObjects = gcs.listObjectInfo(testBucket, testDir);

    assertThat(getObjectNames(listedObjects))
        .containsExactly(testDir + "f1", testDir + "f2", testDir + "f3");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestString(
                testBucket, true, testDir, maxResultsPerRequest, /* pageToken= */ null),
            listRequestString(testBucket, true, testDir, maxResultsPerRequest, "token_1"),
            listRequestString(testBucket, true, testDir, maxResultsPerRequest, "token_2"),
            listRequestString(testBucket, true, testDir, maxResultsPerRequest, "token_3"));
  }

  @Test
  public void listObjectInfo_includePrefix_emptyBucket() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.createUniqueBucket("lst-objs_incl-pfx_empty-bckt");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, /* objectNamePrefix= */ null, INCLUDE_PREFIX_LIST_OPTIONS);

    assertThat(getObjectNames(listedObjects)).isEmpty();
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                testBucket, /* prefix= */ null, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_includePrefix_objectInBucket() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.createUniqueBucket("lst-objs_incl-pfx_obj-in-bckt");
    gcsfsIHelper.createObjects(testBucket, "obj");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, /* objectNamePrefix= */ null, INCLUDE_PREFIX_LIST_OPTIONS);

    assertThat(getObjectNames(listedObjects)).containsExactly("obj");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                testBucket, /* prefix= */ null, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_includePrefix_implicitDirInBucket() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.createUniqueBucket("lst-obj_inc-pfx_imp-dir-in-bkt");
    gcsfsIHelper.createObjects(testBucket, "implDir/obj");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, /* objectNamePrefix= */ null, INCLUDE_PREFIX_LIST_OPTIONS);

    assertThat(getObjectNames(listedObjects)).containsExactly("implDir/");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                testBucket, /* prefix= */ null, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_includePrefix_onlyPrefixObject() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "dir/") + "dir/";

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, testDir, INCLUDE_PREFIX_LIST_OPTIONS);

    assertThat(getObjectNames(listedObjects)).containsExactly(testDir);
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                testBucket, testDir, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_includePrefix_object() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "obj");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, testDir, INCLUDE_PREFIX_LIST_OPTIONS);

    assertThat(getObjectNames(listedObjects)).containsExactly(testDir, testDir + "obj");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                testBucket, testDir, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_includePrefix_subdir() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "dir/");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, testDir, INCLUDE_PREFIX_LIST_OPTIONS);

    assertThat(getObjectNames(listedObjects)).containsExactly(testDir, testDir + "dir/");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                testBucket, testDir, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_includePrefix_implicitPrefixObject() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDirWithoutSubdirs(testBucket, "obj");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, testDir, INCLUDE_PREFIX_LIST_OPTIONS);

    assertThat(getObjectNames(listedObjects)).containsExactly(testDir, testDir + "obj");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                testBucket, testDir, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_includePrefix_implicitSubdir() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDirWithoutSubdirs(testBucket, "dir/obj");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, testDir, INCLUDE_PREFIX_LIST_OPTIONS);

    assertThat(getObjectNames(listedObjects)).containsExactly(testDir, testDir + "dir/");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                testBucket, testDir, /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_allMetadataFieldsCorrect() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testDirName = name.getMethodName() + "/";
    StorageResourceId objectId =
        new StorageResourceId(gcsfsIHelper.sharedBucketName1, testDirName + "object");

    // Create gzipped file so Content-Encoding will be not null
    CreateObjectOptions createOptions =
        GZIP_CREATE_OPTIONS
            .toBuilder()
            .setMetadata(ImmutableMap.of("test-key", "val".getBytes(UTF_8)))
            .build();

    try (OutputStream os =
        new GZIPOutputStream(
            Channels.newOutputStream(gcsfsIHelper.gcs.create(objectId, createOptions)))) {
      os.write((objectId + "-content").getBytes(UTF_8));
    }

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(objectId.getBucketName(), testDirName);

    assertThat(getObjectNames(listedObjects)).containsExactly(objectId.getObjectName());
    assertObjectFields(objectId, listedObjects.get(0));
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                objectId.getBucketName(),
                testDirName,
                /* maxResults= */ 1024,
                /* pageToken= */ null));
  }

  @Test
  public void getItemInfo_allMetadataFieldsCorrect() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    StorageResourceId objectId =
        new StorageResourceId(gcsfsIHelper.sharedBucketName1, name.getMethodName());

    // Create gzipped file so Content-Encoding will be not null
    CreateObjectOptions createOptions =
        GZIP_CREATE_OPTIONS
            .toBuilder()
            .setMetadata(ImmutableMap.of("test-key", "val".getBytes(UTF_8)))
            .build();

    try (OutputStream os =
        new GZIPOutputStream(
            Channels.newOutputStream(gcsfsIHelper.gcs.create(objectId, createOptions)))) {
      os.write((objectId + "-content").getBytes(UTF_8));
    }

    GoogleCloudStorageItemInfo object = gcs.getItemInfo(objectId);

    assertObjectFields(objectId, object);
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(getRequestString(objectId.getBucketName(), objectId.getObjectName()));
  }

  @Test
  public void getItemInfo_oneGcsRequest() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1");

    GoogleCloudStorageItemInfo object =
        gcs.getItemInfo(new StorageResourceId(testBucket, testDir + "f1"));

    assertThat(object.getObjectName()).isEqualTo(testDir + "f1");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(getRequestString(testBucket, testDir + "f1"));
  }

  @Test
  public void getItemInfos_withoutLimits() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    List<StorageResourceId> resourceIdsList =
        ImmutableList.of(
            new StorageResourceId(testBucket, testDir + "f1"),
            new StorageResourceId(testBucket, testDir + "f2"),
            new StorageResourceId(testBucket, testDir + "f3"));

    List<GoogleCloudStorageItemInfo> objects = gcs.getItemInfos(resourceIdsList);

    assertThat(getObjectNames(objects))
        .containsExactly(testDir + "f1", testDir + "f2", testDir + "f3");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            batchRequestString(),
            getRequestString(testBucket, testDir + "f1"),
            getRequestString(testBucket, testDir + "f2"),
            getRequestString(testBucket, testDir + "f3"));
  }

  @Test
  public void getItemInfos_withLimit_zeroBatchGcsRequest() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs =
        new GoogleCloudStorageImpl(
            gcsOptions.toBuilder().setMaxRequestsPerBatch(1).build(), gcsRequestsTracker);
    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    List<StorageResourceId> resourceIdsList =
        ImmutableList.of(
            new StorageResourceId(testBucket, testDir + "f1"),
            new StorageResourceId(testBucket, testDir + "f2"),
            new StorageResourceId(testBucket, testDir + "f3"));

    List<GoogleCloudStorageItemInfo> objects = gcs.getItemInfos(resourceIdsList);

    assertThat(getObjectNames(objects))
        .containsExactly(testDir + "f1", testDir + "f2", testDir + "f3");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(testBucket, testDir + "f1"),
            getRequestString(testBucket, testDir + "f2"),
            getRequestString(testBucket, testDir + "f3"));
  }

  @Test
  public void getItemInfos_withLimit_multipleBatchGcsRequest() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs =
        new GoogleCloudStorageImpl(
            gcsOptions.toBuilder().setMaxRequestsPerBatch(2).build(), gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    List<StorageResourceId> resourceIdsList =
        ImmutableList.of(
            new StorageResourceId(testBucket, testDir + "f1"),
            new StorageResourceId(testBucket, testDir + "f2"),
            new StorageResourceId(testBucket, testDir + "f3"));

    List<GoogleCloudStorageItemInfo> objects = gcs.getItemInfos(resourceIdsList);

    assertThat(getObjectNames(objects))
        .containsExactly(testDir + "f1", testDir + "f2", testDir + "f3");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            batchRequestString(),
            getRequestString(testBucket, testDir + "f1"),
            getRequestString(testBucket, testDir + "f2"),
            batchRequestString(),
            getRequestString(testBucket, testDir + "f3"));
  }

  @Test
  public void updateItems_withoutLimits() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1");

    StorageResourceId resourceId = new StorageResourceId(testBucket, testDir + "f1");
    Map<String, byte[]> updatedMetadata =
        ImmutableMap.of("test-metadata", "test-value".getBytes(UTF_8));

    List<GoogleCloudStorageItemInfo> updatedObjects =
        gcs.updateItems(ImmutableList.of(new UpdatableItemInfo(resourceId, updatedMetadata)));

    assertThat(getObjectNames(updatedObjects)).containsExactly(testDir + "f1");
    assertThat(updatedObjects.get(0).getMetadata().keySet()).isEqualTo(updatedMetadata.keySet());

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(postRequestString(testBucket, testDir + "f1"));
  }

  @Test
  public void updateItems_withLimits_MultipleBatchGcsRequests() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs =
        new GoogleCloudStorageImpl(
            gcsOptions.toBuilder().setMaxRequestsPerBatch(2).build(), gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    Map<String, byte[]> updatedMetadata =
        ImmutableMap.of("test-metadata", "test-value".getBytes(UTF_8));

    List<GoogleCloudStorageItemInfo> updatedObjects =
        gcs.updateItems(
            ImmutableList.of(
                new UpdatableItemInfo(
                    new StorageResourceId(testBucket, testDir + "f1"), updatedMetadata),
                new UpdatableItemInfo(
                    new StorageResourceId(testBucket, testDir + "f2"), updatedMetadata),
                new UpdatableItemInfo(
                    new StorageResourceId(testBucket, testDir + "f3"), updatedMetadata)));

    assertThat(getObjectNames(updatedObjects))
        .containsExactly(testDir + "f1", testDir + "f2", testDir + "f3");
    assertThat(updatedObjects.get(0).getMetadata().keySet()).isEqualTo(updatedMetadata.keySet());

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            batchRequestString(),
            postRequestString(testBucket, testDir + "f1"),
            postRequestString(testBucket, testDir + "f2"),
            batchRequestString(),
            postRequestString(testBucket, testDir + "f3"));
  }

  @Test
  public void copy_withoutLimits_withDisabledCopyWithRewrites() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket1 = gcsfsIHelper.sharedBucketName1;
    String testBucket2 = gcsfsIHelper.sharedBucketName2;
    String testDir = createObjectsInTestDir(testBucket1, "f1", "f2", "f3");

    gcs.copy(
        testBucket1,
        ImmutableList.of(testDir + "f1", testDir + "f2"),
        testBucket2,
        ImmutableList.of(testDir + "f4", testDir + "f5"));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getBucketRequestString(testBucket1),
            getBucketRequestString(testBucket2),
            batchRequestString(),
            copyRequestString(testBucket1, testDir + "f1", testBucket2, testDir + "f4", "copyTo"),
            copyRequestString(testBucket1, testDir + "f2", testBucket2, testDir + "f5", "copyTo"));

    List<String> listedObjects = getObjectNames(gcs.listObjectInfo(testBucket2, testDir));
    assertThat(listedObjects).containsExactly(testDir + "f4", testDir + "f5");
  }

  @Test
  public void copy_withoutLimits_withEnabledCopyWithRewrites() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs =
        new GoogleCloudStorageImpl(
            gcsOptions.toBuilder().setCopyWithRewriteEnabled(true).build(), gcsRequestsTracker);

    String testBucket1 = gcsfsIHelper.sharedBucketName1;
    String testBucket2 = gcsfsIHelper.sharedBucketName2;
    String testDir = createObjectsInTestDir(testBucket1, "f1", "f2", "f3");

    gcs.copy(
        testBucket1,
        ImmutableList.of(testDir + "f1", testDir + "f2"),
        testBucket2,
        ImmutableList.of(testDir + "f4", testDir + "f5"));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getBucketRequestString(testBucket1),
            getBucketRequestString(testBucket2),
            batchRequestString(),
            copyRequestString(
                testBucket1, testDir + "f1", testBucket2, testDir + "f4", "rewriteTo"),
            copyRequestString(
                testBucket1, testDir + "f2", testBucket2, testDir + "f5", "rewriteTo"));

    List<String> listedObjects = getObjectNames(gcs.listObjectInfo(testBucket2, testDir));
    assertThat(listedObjects).containsExactly(testDir + "f4", testDir + "f5");
  }

  @Test
  public void deleteObjects_withoutLimit() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    gcs.deleteObjects(
        ImmutableList.of(
            new StorageResourceId(testBucket, testDir + "f1"),
            new StorageResourceId(testBucket, testDir + "f2")));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            batchRequestString(),
            getRequestString(testBucket, testDir + "f1", /* fields= */ "generation"),
            getRequestString(testBucket, testDir + "f2", /* fields= */ "generation"),
            batchRequestString(),
            deleteRequestString(testBucket, testDir + "f1", /* generationId= */ 1),
            deleteRequestString(testBucket, testDir + "f2", /* generationId= */ 2));

    List<String> listedObjects = getObjectNames(gcs.listObjectInfo(testBucket, testDir));
    assertThat(listedObjects).containsExactly(testDir + "f3");
  }

  @Test
  public void deleteObjects_withLimit_zeroBatchGcsRequest() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs =
        new GoogleCloudStorageImpl(
            gcsOptions.toBuilder().setMaxRequestsPerBatch(1).build(), gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    gcs.deleteObjects(
        ImmutableList.of(
            new StorageResourceId(testBucket, testDir + "f1"),
            new StorageResourceId(testBucket, testDir + "f2")));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(testBucket, testDir + "f1", /* fields= */ "generation"),
            deleteRequestString(testBucket, testDir + "f1", /* generationId= */ 1),
            getRequestString(testBucket, testDir + "f2", /* fields= */ "generation"),
            deleteRequestString(testBucket, testDir + "f2", /* generationId= */ 2));

    List<String> listedObjects = getObjectNames(gcs.listObjectInfo(testBucket, testDir));
    assertThat(listedObjects).containsExactly(testDir + "f3");
  }

  @Test
  public void composeObject_withoutLimit() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2");

    gcs.compose(testBucket, ImmutableList.of(testDir + "f1", testDir + "f2"), testDir + "f3", null);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(testBucket, testDir + "f3"),
            composeRequestString(testBucket, testDir + "f3", /* generationId= */ 1));

    List<String> listedObjects = getObjectNames(gcs.listObjectInfo(testBucket, testDir));
    assertThat(listedObjects).containsExactly(testDir + "f1", testDir + "f2", testDir + "f3");
  }

  @Test
  public void create_gzipEncodedFile() throws Exception {
    String testBucket = gcsfsIHelper.sharedBucketName1;
    StorageResourceId testFile = new StorageResourceId(testBucket, getTestResource());

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    try (OutputStream os =
        new GZIPOutputStream(Channels.newOutputStream(gcs.create(testFile, GZIP_CREATE_OPTIONS)))) {
      os.write("content".getBytes(UTF_8));
    }

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(testBucket, testFile.getObjectName()),
            resumableUploadRequestString(
                testBucket,
                testFile.getObjectName(),
                /* generationId= */ 1,
                /* replaceGenerationId= */ true),
            resumableUploadChunkRequestString(
                testBucket, testFile.getObjectName(), /* generationId= */ 2, /* uploadId= */ 1));

    assertThat(gcs.getItemInfo(testFile).getContentEncoding()).isEqualTo("gzip");
  }

  @Test
  public void open_gzipEncoded_succeeds_ifContentEncodingSupportEnabled() throws Exception {
    String testBucket = gcsfsIHelper.sharedBucketName1;
    StorageResourceId testFile = new StorageResourceId(testBucket, getTestResource());

    try (OutputStream os =
        new GZIPOutputStream(
            Channels.newOutputStream(gcsfsIHelper.gcs.create(testFile, GZIP_CREATE_OPTIONS)))) {
      os.write("content".getBytes(UTF_8));
    }

    long generationId = gcsfsIHelper.gcs.getItemInfo(testFile).getContentGeneration();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    byte[] readContent = new byte[7];
    final GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setSupportGzipEncoding(true).build();
    try (SeekableByteChannel channel = gcs.open(testFile, readOptions)) {
      channel.read(ByteBuffer.wrap(readContent));
    }

    assertThat(new String(readContent, UTF_8)).isEqualTo("content");

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(testBucket, testFile.getObjectName()),
            getMediaRequestString(testBucket, testFile.getObjectName(), generationId));
  }

  @Test
  public void open_gzipEncoded_succeeds_ifContentEncodingSupportDisabled() throws Exception {
    String testBucket = gcsfsIHelper.sharedBucketName1;
    StorageResourceId testFile = new StorageResourceId(testBucket, getTestResource());

    try (OutputStream os =
        new GZIPOutputStream(
            Channels.newOutputStream(gcsfsIHelper.gcs.create(testFile, GZIP_CREATE_OPTIONS)))) {
      os.write("content".getBytes(UTF_8));
    }

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setSupportGzipEncoding(false).build();
    IOException e = assertThrows(IOException.class, () -> gcs.open(testFile, readOptions));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Cannot read GZIP encoded files - content encoding support is disabled.");

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(getRequestString(testBucket, testFile.getObjectName()));
  }

  private static List<String> getObjectNames(List<GoogleCloudStorageItemInfo> listedObjects) {
    return listedObjects.stream().map(GoogleCloudStorageItemInfo::getObjectName).collect(toList());
  }

  private static String createUniqueBucket(String suffix) throws IOException {
    return gcsfsIHelper.createUniqueBucket(suffix + UUID.randomUUID().toString().substring(0, 8));
  }

  private static void assertObjectFields(
      StorageResourceId expectedObjectId, GoogleCloudStorageItemInfo object) throws IOException {
    Storage storage = ((GoogleCloudStorageImpl) gcsfsIHelper.gcs).storage;
    StorageObject expectedObject =
        storage
            .objects()
            .get(expectedObjectId.getBucketName(), expectedObjectId.getObjectName())
            .execute();

    // Use checkNotNull to make sure that all fields have valid values
    assertThat(object)
        .isEqualTo(
            GoogleCloudStorageItemInfo.createObject(
                checkNotNull(expectedObjectId),
                checkNotNull(expectedObject.getTimeCreated()).getValue(),
                checkNotNull(expectedObject.getUpdated()).getValue(),
                checkNotNull(expectedObject.getSize()).longValue(),
                checkNotNull(expectedObject.getContentType()),
                checkNotNull(expectedObject.getContentEncoding()),
                decodeMetadata(checkNotNull(expectedObject.getMetadata())),
                checkNotNull(expectedObject.getGeneration()),
                checkNotNull(expectedObject.getMetageneration()),
                new VerificationAttributes(
                    BaseEncoding.base64().decode(checkNotNull(expectedObject.getMd5Hash())),
                    BaseEncoding.base64().decode(checkNotNull(expectedObject.getCrc32c())))));

    // Verify that bucket-only fields are not set
    assertThat(object.getLocation()).isNull();
    assertThat(object.getStorageClass()).isNull();
  }

  private String createObjectsInTestDir(String bucketName, String... objects) throws Exception {
    String testDir = getTestResource() + "/";
    String[] objectPaths = Arrays.stream(objects).map(o -> testDir + o).toArray(String[]::new);
    gcsfsIHelper.createObjectsWithSubdirs(bucketName, objectPaths);
    return testDir;
  }

  private String createObjectsInTestDirWithoutSubdirs(String bucketName, String... objects)
      throws Exception {
    String testDir = getTestResource() + "/";
    String[] objectPaths = Arrays.stream(objects).map(o -> testDir + o).toArray(String[]::new);
    gcsfsIHelper.createObjects(bucketName, objectPaths);
    return testDir;
  }

  private String getTestResource() {
    return name.getMethodName() + "_" + UUID.randomUUID();
  }

  @FunctionalInterface
  private interface TriFunction<A, B, C, R> {
    R apply(A a, B b, C c) throws IOException;

    default <V> TriFunction<A, B, C, V> andThen(Function<? super R, ? extends V> after) {
      return (A a, B b, C c) -> checkNotNull(after).apply(apply(a, b, c));
    }
  }
}
