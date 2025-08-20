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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.LIST_MAX_RESULTS;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.batchRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.composeRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.copyRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.deleteRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getBucketRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getMediaRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestWithStartOffset;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestWithTrailingDelimiter;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.postRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.resumableUploadChunkRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.resumableUploadRequestString;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.GZIPOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Abstract base class for GoogleCloudStorageFileSystem Integration Test. TestSuite extending these
 * tests should initialize GoogleCloudStorageFileSystemIntegrationHelper as per its configuration.
 */
public abstract class GoogleCloudStorageNewIntegrationTestBase {

  protected static final CreateObjectOptions GZIP_CREATE_OPTIONS =
      CreateObjectOptions.DEFAULT_NO_OVERWRITE.toBuilder().setContentEncoding("gzip").build();

  private static final ListObjectOptions INCLUDE_PREFIX_LIST_OPTIONS =
      ListObjectOptions.DEFAULT.toBuilder().setIncludePrefix(true).build();

  protected static GoogleCloudStorageOptions gcsOptions;
  protected static RetryHttpInitializer httpRequestsInitializer;
  protected static GoogleCloudStorageFileSystemIntegrationHelper gcsfsIHelper;

  @Rule public TestName name = new TestName();
  protected TrackingHttpRequestInitializer gcsRequestsTracker;

  protected boolean isTracingSupported = true;

  private GoogleCloudStorage gcs;

  @Before
  public void before() {
    gcsRequestsTracker = new TrackingHttpRequestInitializer(httpRequestsInitializer);
  }

  @After
  public void after() {
    if (gcs != null) {
      gcs.close();
      gcs = null;
    }
  }

  @Test
  public void listObjectInfo_nonExistentBucket_nullPrefix() throws Exception {
    list_nonExistentBucket_nullPrefix(
        (gcs, bucket, prefix) -> getObjectNames(gcs.listObjectInfo(bucket, prefix)),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_nonExistentBucket_nullPrefix() throws Exception {
    list_nonExistentBucket_nullPrefix(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  private void list_nonExistentBucket_nullPrefix(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

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
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_emptyBucket_nullPrefix() throws Exception {
    list_emptyBucket_nullPrefix(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  private void list_emptyBucket_nullPrefix(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

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
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_nonEmptyBucket_nullPrefix_object() throws Exception {
    list_nonEmptyBucket_nullPrefix_object(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  private void list_nonEmptyBucket_nullPrefix_object(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

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
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_nonEmptyBucket_nullPrefix_emptyDir() throws Exception {
    list_nonEmptyBucket_nullPrefix_emptyDir(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  private void list_nonEmptyBucket_nullPrefix_emptyDir(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

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
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_nonEmptyBucket_nullPrefix_subDir() throws Exception {
    list_nonEmptyBucket_nullPrefix_subDir(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  private void list_nonEmptyBucket_nullPrefix_subDir(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

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
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_nonEmptyBucket_nullPrefix_implicitSubDir() throws Exception {
    list_nonEmptyBucket_nullPrefix_implicitSubDir(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  private void list_nonEmptyBucket_nullPrefix_implicitSubDir(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

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
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_prefix_doesNotExist() throws Exception {
    list_prefix_doesNotExist(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  private void list_prefix_doesNotExist(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

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
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_prefixObject_empty() throws Exception {
    list_prefixObject_empty(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  private void list_prefixObject_empty(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

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
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_prefixObject_withObject() throws Exception {
    list_prefixObject_withObject(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  private void list_prefixObject_withObject(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

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
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_prefixObject_withSubdir() throws Exception {
    list_prefixObject_withSubdir(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  private void list_prefixObject_withSubdir(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

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
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_prefixObject_withImplicitSubdir() throws Exception {
    list_prefixObject_withImplicitSubdir(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  private void list_prefixObject_withImplicitSubdir(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

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
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_implicitPrefix_withObject() throws Exception {
    list_implicitPrefix_withObject(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  private void list_implicitPrefix_withObject(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

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
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_implicitPrefix_withSubdir() throws Exception {
    list_implicitPrefix_withSubdir(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  private void list_implicitPrefix_withSubdir(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

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
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoPage_implicitPrefix_withImplicitSubdir() throws Exception {
    list_implicitPrefix_withImplicitSubdir(
        (gcs, bucket, prefix) ->
            getObjectNames(
                gcs.listObjectInfoPage(bucket, prefix, /* pageToken= */ null).getItems()),
        (bucket, prefix) ->
            listRequestWithTrailingDelimiter(bucket, prefix, /* pageToken= */ null));
  }

  private void list_implicitPrefix_withImplicitSubdir(
      TriFunction<GoogleCloudStorage, String, String, List<String>> listFn,
      BiFunction<String, String, String> requestFn)
      throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDirWithoutSubdirs(testBucket, "sub/obj");

    List<String> listResult = listFn.apply(gcs, testBucket, testDir);

    assertThat(listResult).containsExactly(testDir + "sub/");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(requestFn.apply(testBucket, testDir));
  }

  @Test
  public void listObjectInfo_withLimit_oneGcsRequest() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

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
    int maxResultsPerRequest = 1;
    gcs =
        createGoogleCloudStorage(
            gcsOptions.toBuilder().setMaxListItemsPerCall(maxResultsPerRequest).build());

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
    int maxResultsPerRequest = 1;
    GoogleCloudStorageOptions options =
        gcsOptions.toBuilder().setMaxListItemsPerCall(maxResultsPerRequest).build();

    gcs = createGoogleCloudStorage(options);

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
    gcs = createGoogleCloudStorage(gcsOptions);

    String testBucket = gcsfsIHelper.createUniqueBucket("lst-objs_incl-pfx_empty-bckt");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, /* objectNamePrefix= */ null, INCLUDE_PREFIX_LIST_OPTIONS);

    assertThat(getObjectNames(listedObjects)).isEmpty();
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                testBucket, /* prefix= */ null, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_includePrefix_objectInBucket() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

    String testBucket = gcsfsIHelper.createUniqueBucket("lst-objs_incl-pfx_obj-in-bckt");
    gcsfsIHelper.createObjects(testBucket, "obj");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, /* objectNamePrefix= */ null, INCLUDE_PREFIX_LIST_OPTIONS);

    assertThat(getObjectNames(listedObjects)).containsExactly("obj");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                testBucket, /* prefix= */ null, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_includePrefix_implicitDirInBucket() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

    String testBucket =
        gcsfsIHelper.createUniqueBucket(
            UUID.randomUUID().toString().replaceAll("-", "").substring(0, 10));
    gcsfsIHelper.createObjects(testBucket, "implDir/obj");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, /* objectNamePrefix= */ null, INCLUDE_PREFIX_LIST_OPTIONS);

    assertThat(getObjectNames(listedObjects)).containsExactly("implDir/");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                testBucket, /* prefix= */ null, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_includePrefix_onlyPrefixObject() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "dir/") + "dir/";

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, testDir, INCLUDE_PREFIX_LIST_OPTIONS);

    assertThat(getObjectNames(listedObjects)).containsExactly(testDir);
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(testBucket, testDir, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_includePrefix_object() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "obj");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, testDir, INCLUDE_PREFIX_LIST_OPTIONS);

    assertThat(getObjectNames(listedObjects)).containsExactly(testDir, testDir + "obj");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(testBucket, testDir, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_includePrefix_subdir() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "dir/");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, testDir, INCLUDE_PREFIX_LIST_OPTIONS);

    assertThat(getObjectNames(listedObjects)).containsExactly(testDir, testDir + "dir/");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(testBucket, testDir, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_includePrefix_implicitPrefixObject() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDirWithoutSubdirs(testBucket, "obj");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, testDir, INCLUDE_PREFIX_LIST_OPTIONS);

    assertThat(getObjectNames(listedObjects)).containsExactly(testDir, testDir + "obj");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(testBucket, testDir, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_includePrefix_implicitSubdir() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDirWithoutSubdirs(testBucket, "dir/obj");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, testDir, INCLUDE_PREFIX_LIST_OPTIONS);

    assertThat(getObjectNames(listedObjects)).containsExactly(testDir, testDir + "dir/");
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(testBucket, testDir, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_allMetadataFieldsCorrect() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

    String testDirName = name.getMethodName() + "/";
    StorageResourceId objectId =
        new StorageResourceId(gcsfsIHelper.sharedBucketName1, testDirName + "object");

    // Create gzipped file so Content-Encoding will be not null
    CreateObjectOptions createOptions =
        GZIP_CREATE_OPTIONS.toBuilder()
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
                objectId.getBucketName(), testDirName, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoStartingFrom_negativeMaxResult() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);
    String bucketName = gcsfsIHelper.sharedBucketName1;

    String testDirName = name.getMethodName() + "/";

    String fileName = testDirName + "I_am_file";
    StorageResourceId fileResource = new StorageResourceId(bucketName, fileName);

    gcsfsIHelper.gcs.createEmptyObject(fileResource);

    int maxResults = -1;

    ListObjectOptions listOptionsLimitResults =
        ListObjectOptions.DEFAULT_USING_START_OFFSET.toBuilder().setMaxResults(maxResults).build();
    gcs.listObjectInfoStartingFrom(bucketName, testDirName, listOptionsLimitResults);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithStartOffset(
                // maxResults is override with default value
                bucketName,
                testDirName,
                /* pageToken= */ null,
                /* maxResults= */ (int) LIST_MAX_RESULTS));
  }

  @Test
  public void listObjectInfoStartingFrom_startOffsetNotObject() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);
    String bucketName = gcsfsIHelper.sharedBucketName1;

    String testDirName = name.getMethodName() + "/";

    String fileName = testDirName + "I_am_file";
    StorageResourceId fileResource = new StorageResourceId(bucketName, fileName);

    gcsfsIHelper.gcs.createEmptyObject(fileResource);
    String startOffset = testDirName.substring(5, testDirName.length() - 1);

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfoStartingFrom(bucketName, startOffset);

    verifyListedFilesOrder(listedObjects, startOffset);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithStartOffset(bucketName, startOffset, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoStartingFrom_sortedListFiles() throws Exception {

    gcs = createGoogleCloudStorage(gcsOptions);
    String bucketName = gcsfsIHelper.sharedBucketName1;

    String testDirName = name.getMethodName() + "/";
    List<StorageResourceId> resources = new ArrayList<>();
    int filesCount = 50;
    for (int i = 0; i < filesCount; i++) {
      String uniqueFilename = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 12);
      StorageResourceId file = new StorageResourceId(bucketName, testDirName + uniqueFilename);
      gcsfsIHelper.gcs.createEmptyObject(file);
      resources.add(file);
    }
    List<StorageResourceId> sortedResources =
        resources.stream()
            .sorted(Comparator.comparing(StorageResourceId::getObjectName))
            .collect(Collectors.toList());
    assertTrue(sortedResources.size() == resources.size());

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfoStartingFrom(bucketName, testDirName);
    verifyListedFilesOrder(listedObjects, testDirName);
    // Can't asset that this is the only object we get in response, other object lexicographically
    // higher would also come in response.
    // Only thing we can assert strongly is, list would start with the files created in this
    // directory.
    for (int i = 0; i < filesCount; i++) {
      assertThat(getObjectNames(listedObjects).get(i))
          .isEqualTo(sortedResources.get(i).getObjectName());
    }

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithStartOffset(bucketName, testDirName, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoStartingFrom_invalidListOptions() throws Exception {
    String bucketName = gcsfsIHelper.sharedBucketName1;
    gcs = createGoogleCloudStorage(gcsOptions);

    String dir = name.getMethodName() + "2/";

    String fileName = dir + "I_am_file";
    StorageResourceId fileResource = new StorageResourceId(bucketName, fileName);

    gcsfsIHelper.gcs.createEmptyObject(fileResource);

    assertThrows(
        IOException.class,
        () ->
            gcs.listObjectInfoStartingFrom(
                gcsfsIHelper.sharedBucketName1, dir, ListObjectOptions.DEFAULT));
  }

  @Test
  public void listObjectInfoStartingFrom_multipleListCalls() throws Exception {

    String bucketName = gcsfsIHelper.sharedBucketName1;
    gcs = createGoogleCloudStorage(gcsOptions);

    String dir1 = name.getMethodName() + "1/";
    StorageResourceId dirResource1 = new StorageResourceId(bucketName, dir1);

    String dir2 = name.getMethodName() + "2/";
    StorageResourceId dirResource2 = new StorageResourceId(bucketName, dir2);

    String fileName = dir2 + "I_am_file";
    StorageResourceId fileResource = new StorageResourceId(bucketName, fileName);

    gcsfsIHelper.gcs.createEmptyObject(dirResource1);
    gcsfsIHelper.gcs.createEmptyObject(dirResource2);
    gcsfsIHelper.gcs.createEmptyObject(fileResource);
    int maxResults = 2;

    ListObjectOptions listOptionsLimitResults =
        ListObjectOptions.DEFAULT_USING_START_OFFSET.toBuilder().setMaxResults(maxResults).build();

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfoStartingFrom(
            gcsfsIHelper.sharedBucketName1, dir1, listOptionsLimitResults);
    assertThat(getObjectNames(listedObjects)).doesNotContain(dirResource1);
    assertThat(getObjectNames(listedObjects)).doesNotContain(dirResource2);

    // Can't asset that this is the only object we get in response, other object lexicographically
    // higher would also come in response.
    // Only thing we can assert strongly is, list would start with the files created in this
    // directory.
    assertThat(getObjectNames(listedObjects).get(0)).isEqualTo(fileResource.getObjectName());

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithStartOffset(bucketName, dir1, /* pageToken= */ null, maxResults),
            listRequestWithStartOffset(bucketName, dir1, /* pageToken= */ "token_1", maxResults));
  }

  @Test
  public void listObjectInfoStartingFrom_filterDirObjects() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

    String testDirName = name.getMethodName() + "/";
    StorageResourceId objectId1 =
        new StorageResourceId(gcsfsIHelper.sharedBucketName1, testDirName + "object1");
    String subDirName = testDirName + "subDir/";
    StorageResourceId directoryResource =
        new StorageResourceId(gcsfsIHelper.sharedBucketName1, subDirName);
    StorageResourceId objectId2 =
        new StorageResourceId(gcsfsIHelper.sharedBucketName1, subDirName + "subObject1");

    gcsfsIHelper.gcs.createEmptyObject(objectId1);
    gcsfsIHelper.gcs.createEmptyObject(directoryResource);
    gcsfsIHelper.gcs.createEmptyObject(objectId2);

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfoStartingFrom(gcsfsIHelper.sharedBucketName1, testDirName);
    verifyListedFilesOrder(listedObjects, testDirName);

    assertThat(getObjectNames(listedObjects)).doesNotContain(directoryResource);

    // Can't asset that this is the only object we get in response, other object lexicographically
    // higher would also come in response.
    // Only thing we can assert strongly is, list would start with the files created in this
    // directory.
    assertThat(getObjectNames(listedObjects).get(0)).isEqualTo(objectId1.getObjectName());
    assertThat(getObjectNames(listedObjects).get(1)).isEqualTo(objectId2.getObjectName());

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithStartOffset(
                objectId1.getBucketName(), testDirName, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoStartingFrom_listSubDirFiles() throws Exception {

    gcs = createGoogleCloudStorage(gcsOptions);

    String testDirName = name.getMethodName() + "/";
    StorageResourceId objectId1 =
        new StorageResourceId(gcsfsIHelper.sharedBucketName1, testDirName + "object1");
    String subDirName = testDirName + "subDir/";
    StorageResourceId objectId2 =
        new StorageResourceId(gcsfsIHelper.sharedBucketName1, subDirName + "subObject1");

    gcsfsIHelper.gcs.createEmptyObject(objectId2);
    gcsfsIHelper.gcs.createEmptyObject(objectId1);

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfoStartingFrom(gcsfsIHelper.sharedBucketName1, testDirName);
    verifyListedFilesOrder(listedObjects, testDirName);
    // Can't asset that this is the only object we get in response, other object lexicographically
    // higher would also come in response.
    // Only thing we can assert strongly is, list would start with the files created in this
    // directory.
    assertThat(getObjectNames(listedObjects).get(0)).isEqualTo(objectId1.getObjectName());
    assertThat(getObjectNames(listedObjects).get(1)).isEqualTo(objectId2.getObjectName());

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithStartOffset(
                objectId1.getBucketName(), testDirName, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfoStartingFrom_allMetadataFieldsCorrect() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

    String testDirName = name.getMethodName() + "/";
    StorageResourceId objectId2 =
        new StorageResourceId(gcsfsIHelper.sharedBucketName1, testDirName + "object2");
    StorageResourceId objectId1 =
        new StorageResourceId(gcsfsIHelper.sharedBucketName1, testDirName + "object1");

    // Create gzipped file so Content-Encoding will be not null
    CreateObjectOptions createOptions =
        GZIP_CREATE_OPTIONS.toBuilder()
            .setMetadata(ImmutableMap.of("test-key", "val".getBytes(UTF_8)))
            .build();

    gcsfsIHelper.gcs.createEmptyObject(objectId2, createOptions);
    gcsfsIHelper.gcs.createEmptyObject(objectId1, createOptions);

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfoStartingFrom(gcsfsIHelper.sharedBucketName1, testDirName);

    verifyListedFilesOrder(listedObjects, testDirName);
    // Can't asset that this is the only object we get in response, other object lexicographically
    // higher would also come in response.
    // Only thing we can assert strongly is, list would start with the files created in this
    // directory.
    assertThat(getObjectNames(listedObjects).get(0)).isEqualTo(objectId1.getObjectName());
    assertThat(getObjectNames(listedObjects).get(1)).isEqualTo(objectId2.getObjectName());

    assertObjectFields(objectId1, listedObjects.get(0));
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithStartOffset(
                objectId1.getBucketName(), testDirName, /* pageToken= */ null));
  }

  @Test
  public void getItemInfo_allMetadataFieldsCorrect() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

    StorageResourceId objectId =
        new StorageResourceId(gcsfsIHelper.sharedBucketName1, name.getMethodName());

    // Create gzipped file so Content-Encoding will be not null
    CreateObjectOptions createOptions =
        GZIP_CREATE_OPTIONS.toBuilder()
            .setMetadata(ImmutableMap.of("test-key", "val".getBytes(UTF_8)))
            .build();

    try (OutputStream os =
        new GZIPOutputStream(
            Channels.newOutputStream(gcsfsIHelper.gcs.create(objectId, createOptions)))) {
      os.write((objectId + "-content").getBytes(UTF_8));
    }

    GoogleCloudStorageItemInfo object = gcs.getItemInfo(objectId);

    assertObjectFields(objectId, object);
    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(getRequestString(objectId.getBucketName(), objectId.getObjectName()));
    }
  }

  @Test
  public void getItemInfo_oneGcsRequest() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1");

    GoogleCloudStorageItemInfo object =
        gcs.getItemInfo(new StorageResourceId(testBucket, testDir + "f1"));

    assertThat(object.getObjectName()).isEqualTo(testDir + "f1");
    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(getRequestString(testBucket, testDir + "f1"));
    }
  }

  @Test
  public void getItemInfos_withoutLimits() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

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
    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              batchRequestString(),
              getRequestString(testBucket, testDir + "f1"),
              getRequestString(testBucket, testDir + "f2"),
              getRequestString(testBucket, testDir + "f3"));
    }
  }

  @Test
  public void getItemInfos_withLimit_zeroBatchGcsRequest() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions.toBuilder().setMaxRequestsPerBatch(1).build());
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
    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(testBucket, testDir + "f1"),
              getRequestString(testBucket, testDir + "f2"),
              getRequestString(testBucket, testDir + "f3"));
    }
  }

  @Test
  public void getItemInfos_withLimit_multipleBatchGcsRequest() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions.toBuilder().setMaxRequestsPerBatch(2).build());

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
    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              batchRequestString(),
              getRequestString(testBucket, testDir + "f1"),
              getRequestString(testBucket, testDir + "f2"),
              batchRequestString(),
              getRequestString(testBucket, testDir + "f3"));
    }
  }

  @Test
  public void updateItems_withoutLimits() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1");

    StorageResourceId resourceId = new StorageResourceId(testBucket, testDir + "f1");
    Map<String, byte[]> updatedMetadata =
        ImmutableMap.of("test-metadata", "test-value".getBytes(UTF_8));

    List<GoogleCloudStorageItemInfo> updatedObjects =
        gcs.updateItems(ImmutableList.of(new UpdatableItemInfo(resourceId, updatedMetadata)));

    assertThat(getObjectNames(updatedObjects)).containsExactly(testDir + "f1");
    assertThat(updatedObjects.get(0).getMetadata().keySet()).isEqualTo(updatedMetadata.keySet());

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(postRequestString(testBucket, testDir + "f1"));
    }
  }

  @Test
  public void updateItems_withLimits_MultipleBatchGcsRequests() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions.toBuilder().setMaxRequestsPerBatch(2).build());

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

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              batchRequestString(),
              postRequestString(testBucket, testDir + "f1"),
              postRequestString(testBucket, testDir + "f2"),
              batchRequestString(),
              postRequestString(testBucket, testDir + "f3"));
    }
  }

  @Test
  public void copy_withoutLimits_withDisabledCopyWithRewrites() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

    String testBucket1 = gcsfsIHelper.sharedBucketName1;
    String testBucket2 = gcsfsIHelper.sharedBucketName2;
    String testDir = createObjectsInTestDir(testBucket1, "f1", "f2", "f3");

    gcs.copy(
        testBucket1,
        ImmutableList.of(testDir + "f1", testDir + "f2"),
        testBucket2,
        ImmutableList.of(testDir + "f4", testDir + "f5"));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getBucketRequestString(testBucket1),
              getBucketRequestString(testBucket2),
              batchRequestString(),
              copyRequestString(testBucket1, testDir + "f1", testBucket2, testDir + "f4", "copyTo"),
              copyRequestString(
                  testBucket1, testDir + "f2", testBucket2, testDir + "f5", "copyTo"));
    }

    List<String> listedObjects = getObjectNames(gcs.listObjectInfo(testBucket2, testDir));
    assertThat(listedObjects).containsExactly(testDir + "f4", testDir + "f5");
  }

  @Test
  public void copy_withoutLimits_withEnabledCopyWithRewrites() throws Exception {
    gcs =
        createGoogleCloudStorage(
            gcsOptions.toBuilder()
                .setBatchThreads(0)
                .setCopyWithRewriteEnabled(true)
                .setMaxRewriteChunkSize(0)
                .build());

    String testBucket1 = gcsfsIHelper.sharedBucketName1;
    String testBucket2 = gcsfsIHelper.sharedBucketName2;
    String testDir = createObjectsInTestDir(testBucket1, "f1", "f2", "f3");

    gcs.copy(
        testBucket1,
        ImmutableList.of(testDir + "f1", testDir + "f2"),
        testBucket2,
        ImmutableList.of(testDir + "f4", testDir + "f5"));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getBucketRequestString(testBucket1),
              getBucketRequestString(testBucket2),
              batchRequestString(),
              copyRequestString(
                  testBucket1, testDir + "f1", testBucket2, testDir + "f4", "rewriteTo"),
              copyRequestString(
                  testBucket1, testDir + "f2", testBucket2, testDir + "f5", "rewriteTo"));
    }

    List<String> listedObjects = getObjectNames(gcs.listObjectInfo(testBucket2, testDir));
    assertThat(listedObjects).containsExactly(testDir + "f4", testDir + "f5");
  }

  @Test
  public void deleteObjects_withoutLimit() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    gcs.deleteObjects(
        ImmutableList.of(
            new StorageResourceId(testBucket, testDir + "f1"),
            new StorageResourceId(testBucket, testDir + "f2")));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              batchRequestString(),
              getRequestString(testBucket, testDir + "f1", /* fields= */ "generation"),
              getRequestString(testBucket, testDir + "f2", /* fields= */ "generation"),
              batchRequestString(),
              deleteRequestString(testBucket, testDir + "f1", /* generationId= */ 1),
              deleteRequestString(testBucket, testDir + "f2", /* generationId= */ 2));
    }

    List<String> listedObjects = getObjectNames(gcs.listObjectInfo(testBucket, testDir));
    assertThat(listedObjects).containsExactly(testDir + "f3");
  }

  @Test
  public void deleteObjects_withLimit_zeroBatchGcsRequest() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions.toBuilder().setMaxRequestsPerBatch(1).build());

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    gcs.deleteObjects(
        ImmutableList.of(
            new StorageResourceId(testBucket, testDir + "f1"),
            new StorageResourceId(testBucket, testDir + "f2")));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(testBucket, testDir + "f1", /* fields= */ "generation"),
              deleteRequestString(testBucket, testDir + "f1", /* generationId= */ 1),
              getRequestString(testBucket, testDir + "f2", /* fields= */ "generation"),
              deleteRequestString(testBucket, testDir + "f2", /* generationId= */ 2));
    }

    List<String> listedObjects = getObjectNames(gcs.listObjectInfo(testBucket, testDir));
    assertThat(listedObjects).containsExactly(testDir + "f3");
  }

  @Test
  public void composeObject_withoutLimit() throws Exception {
    gcs = createGoogleCloudStorage(gcsOptions);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2");

    gcs.compose(testBucket, ImmutableList.of(testDir + "f1", testDir + "f2"), testDir + "f3", null);

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(testBucket, testDir + "f3"),
              composeRequestString(testBucket, testDir + "f3", /* generationId= */ 1));
    }

    List<String> listedObjects = getObjectNames(gcs.listObjectInfo(testBucket, testDir));
    assertThat(listedObjects).containsExactly(testDir + "f1", testDir + "f2", testDir + "f3");
  }

  @Test
  public void create_gzipEncodedFile() throws Exception {
    String testBucket = gcsfsIHelper.sharedBucketName1;
    StorageResourceId testFile = new StorageResourceId(testBucket, getTestResource());

    gcs = createGoogleCloudStorage(gcsOptions);

    try (OutputStream os =
        new GZIPOutputStream(Channels.newOutputStream(gcs.create(testFile, GZIP_CREATE_OPTIONS)))) {
      os.write("content".getBytes(UTF_8));
    }

    if (isTracingSupported) {
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
    }

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

    gcs = createGoogleCloudStorage(gcsOptions);

    byte[] readContent = new byte[7];
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setGzipEncodingSupportEnabled(true).build();
    try (SeekableByteChannel channel = gcs.open(testFile, readOptions)) {
      channel.read(ByteBuffer.wrap(readContent));
    }

    assertThat(new String(readContent, UTF_8)).isEqualTo("content");
    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(
                  testBucket,
                  testFile.getObjectName(),
                  /* fields= */ "contentEncoding,generation,size"),
              getMediaRequestString(testBucket, testFile.getObjectName(), generationId))
          .inOrder();
    }
  }

  @Test
  public void open_gzipEncoded_rangeRead_succeed() throws Exception {
    String testBucket = gcsfsIHelper.sharedBucketName1;
    StorageResourceId testFile = new StorageResourceId(testBucket, getTestResource());
    String data = "content";

    try (OutputStream os =
        new GZIPOutputStream(
            Channels.newOutputStream(gcsfsIHelper.gcs.create(testFile, GZIP_CREATE_OPTIONS)))) {
      os.write(data.getBytes(UTF_8));
    }

    long generationId = gcsfsIHelper.gcs.getItemInfo(testFile).getContentGeneration();

    gcs = createGoogleCloudStorage(gcsOptions);
    int startIndex = 2; // inclusive
    int endIndex = 4; // exclusive
    byte[] readContent = new byte[endIndex - startIndex];
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setGzipEncodingSupportEnabled(true).build();
    try (SeekableByteChannel channel = gcs.open(testFile, readOptions)) {
      //
      channel.position(startIndex);
      channel.read(ByteBuffer.wrap(readContent));
    }

    assertThat(new String(readContent, UTF_8)).isEqualTo(data.substring(startIndex, endIndex));
    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(
                  testBucket,
                  testFile.getObjectName(),
                  /* fields= */ "contentEncoding,generation,size"),
              getMediaRequestString(testBucket, testFile.getObjectName(), generationId))
          .inOrder();
    }
  }

  @Test
  public void open_itemInfo_gzipEncoded_succeeds_ifContentEncodingSupportEnabled()
      throws Exception {
    String testBucket = gcsfsIHelper.sharedBucketName1;
    StorageResourceId testFile = new StorageResourceId(testBucket, getTestResource());
    try (OutputStream os =
        new GZIPOutputStream(
            Channels.newOutputStream(gcsfsIHelper.gcs.create(testFile, GZIP_CREATE_OPTIONS)))) {
      os.write("content".getBytes(UTF_8));
    }
    GoogleCloudStorageItemInfo itemInfo = gcsfsIHelper.gcs.getItemInfo(testFile);

    gcs = createGoogleCloudStorage(gcsOptions);

    byte[] readContent = new byte[7];
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setGzipEncodingSupportEnabled(true).build();
    try (SeekableByteChannel channel = gcs.open(itemInfo, readOptions)) {
      channel.read(ByteBuffer.wrap(readContent));
    }

    assertThat(new String(readContent, UTF_8)).isEqualTo("content");
    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getMediaRequestString(
                  testBucket, testFile.getObjectName(), itemInfo.getContentGeneration()));
    }
  }

  @Test
  public void open_gzipEncoded_fails_ifContentEncodingSupportDisabled() throws Exception {
    String testBucket = gcsfsIHelper.sharedBucketName1;
    StorageResourceId testFile = new StorageResourceId(testBucket, getTestResource());

    try (OutputStream os =
        new GZIPOutputStream(
            Channels.newOutputStream(gcsfsIHelper.gcs.create(testFile, GZIP_CREATE_OPTIONS)))) {
      os.write("content".getBytes(UTF_8));
    }

    gcs = createGoogleCloudStorage(gcsOptions);

    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setGzipEncodingSupportEnabled(false).build();
    IOException e = assertThrows(IOException.class, () -> gcs.open(testFile, readOptions));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Cannot read GZIP encoded files - content encoding support is disabled.");
    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(
                  testBucket,
                  testFile.getObjectName(),
                  /* fields= */ "contentEncoding,generation,size"));
    }
  }

  @Test
  public void open_itemInfo_gzipEncoded_fails_ifContentEncodingSupportDisabled() throws Exception {
    String testBucket = gcsfsIHelper.sharedBucketName1;
    StorageResourceId testFile = new StorageResourceId(testBucket, getTestResource());

    try (OutputStream os =
        new GZIPOutputStream(
            Channels.newOutputStream(gcsfsIHelper.gcs.create(testFile, GZIP_CREATE_OPTIONS)))) {
      os.write("content".getBytes(UTF_8));
    }

    gcs = createGoogleCloudStorage(gcsOptions);
    GoogleCloudStorageItemInfo itemInfo = gcsfsIHelper.gcs.getItemInfo(testFile);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setGzipEncodingSupportEnabled(false).build();

    IOException e = assertThrows(IOException.class, () -> gcs.open(itemInfo, readOptions));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Cannot read GZIP encoded files - content encoding support is disabled.");

    assertThat(gcsRequestsTracker.getAllRequestStrings()).isEmpty();
  }

  @Test
  public void open_itemInfo_fails_ifInvalidItemInfo() throws Exception {
    String testBucket = gcsfsIHelper.sharedBucketName1;
    StorageResourceId testFile = new StorageResourceId(testBucket, getTestResource());

    gcs = createGoogleCloudStorage(gcsOptions);

    GoogleCloudStorageItemInfo itemInfo = GoogleCloudStorageItemInfo.createNotFound(testFile);
    GoogleCloudStorageReadOptions readOptions = GoogleCloudStorageReadOptions.builder().build();
    FileNotFoundException e =
        assertThrows(FileNotFoundException.class, () -> gcs.open(itemInfo, readOptions));

    assertThat(e).hasMessageThat().startsWith("Item not found");

    assertThat(gcsRequestsTracker.getAllRequestStrings()).isEmpty();
  }

  protected static List<String> getObjectNames(List<GoogleCloudStorageItemInfo> listedObjects) {
    return listedObjects.stream().map(GoogleCloudStorageItemInfo::getObjectName).collect(toList());
  }

  private static String createUniqueBucket(String suffix) throws IOException {
    return gcsfsIHelper.createUniqueBucket(suffix + UUID.randomUUID().toString().substring(0, 8));
  }

  private static void assertObjectFields(
      StorageResourceId expectedObjectId, GoogleCloudStorageItemInfo object) throws IOException {
    /*    Storage storage = ((GoogleCloudStorageImpl) gcsfsIHelper.gcs).storage;

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
                    BaseEncoding.base64().decode(checkNotNull(expectedObject.getCrc32c())))));*/

    GoogleCloudStorageItemInfo expectedObjectItemInfo =
        gcsfsIHelper.gcs.getItemInfo(expectedObjectId);
    assertThat(object).isEqualTo(expectedObjectItemInfo);

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

  private void verifyListedFilesOrder(
      List<GoogleCloudStorageItemInfo> listedObjects, String startOffset) {
    // provided item list is sorted
    if (listedObjects.size() > 1) {
      assertTrue(
          IntStream.range(0, listedObjects.size() - 1)
              .allMatch(
                  i ->
                      listedObjects
                              .get(i)
                              .getObjectName()
                              .compareTo(listedObjects.get(i + 1).getObjectName())
                          <= 0));
    }

    assertTrue(
        IntStream.range(0, listedObjects.size() - 1)
            .allMatch(i -> listedObjects.get(i).getObjectName().compareTo(startOffset) >= 0));
  }

  protected abstract GoogleCloudStorage createGoogleCloudStorage(GoogleCloudStorageOptions options)
      throws IOException;

  protected String getTestResource() {
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
