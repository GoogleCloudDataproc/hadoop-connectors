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

import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.batchRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.copyRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.deleteRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestWithStartOffset;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestWithTrailingDelimiter;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.moveRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.uploadRequestString;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getStandardOptionBuilder;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemImpl.PathTypeHint;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/** Abstract base class of integration tests for {@link GoogleCloudStorageFileSystemImpl} class. */
public abstract class GoogleCloudStorageFileSystemNewIntegrationTestBase {

  protected static GoogleCloudStorageOptions gcsOptions;
  protected static RetryHttpInitializer httpRequestsInitializer;
  protected static GoogleCloudStorageFileSystemIntegrationHelper gcsfsIHelper;

  @Rule public TestName name = new TestName();

  protected TrackingHttpRequestInitializer gcsRequestsTracker;

  protected GoogleCloudStorageFileSystem gcsFs;

  // The tests in this file explicitly check the request strings. The requests strings may vary
  // depending on the gcs client used. This flag can be set to false for disabling these request
  // checks.
  protected static boolean isTracingSupported = true;

  @Before
  public void before() {
    gcsRequestsTracker = new TrackingHttpRequestInitializer(httpRequestsInitializer);
  }

  @After
  public void after() {
    if (gcsFs != null) {
      gcsFs.close();
      gcsFs = null;
    }
  }

  @Test
  public void mkdir_shouldCreateNewDirectory() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve(dirObject);

    gcsFs.mkdir(dirObjectUri);

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRawRequestStrings())
          .containsExactly(
              uploadRequestString(
                  bucketName,
                  dirObject + "/",
                  /* generationId= */ 0,
                  /* replaceGenerationId= */ false));
    }

    assertThat(gcsFs.exists(dirObjectUri)).isTrue();
    assertThat(gcsFs.getFileInfo(dirObjectUri).isDirectory()).isTrue();
  }

  @Test
  public void mkdir_shouldFailSilentlyIfDirectoryExists() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve(dirObject);

    // create directory beforehand without tracking requests
    gcsfsIHelper.mkdir(bucketName, dirObject);
    gcsFs.mkdir(dirObjectUri);

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRawRequestStrings())
          .containsExactly(
              uploadRequestString(
                  bucketName,
                  dirObject + "/",
                  /* generationId= */ 0,
                  /* replaceGenerationId= */ false),
              // verifies directory exists
              getRequestString(bucketName, dirObject + "/"));
    }

    assertThat(gcsFs.exists(dirObjectUri)).isTrue();
    assertThat(gcsFs.getFileInfo(dirObjectUri).isDirectory()).isTrue();
  }

  @Test
  public void mkdirs_shouldCreateNewDirectory() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve(dirObject + "/d1/");

    gcsFs.mkdirs(dirObjectUri);

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRawRequestStrings())
          .containsExactly(
              batchRequestString(),
              getRequestString(bucketName, dirObject),
              getRequestString(bucketName, dirObject + "/d1"),
              uploadRequestString(
                  bucketName,
                  dirObject + "/d1/",
                  /* generationId= */ 0,
                  /* replaceGenerationId= */ false));
    }

    assertThat(gcsFs.exists(dirObjectUri)).isTrue();
    assertThat(gcsFs.getFileInfo(dirObjectUri).isDirectory()).isTrue();
  }

  @Test
  public void mkdirs_shouldFailSilentlyIfDirectoryExists() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve(dirObject + "/d1/");

    // create directory beforehand without tracking requests
    gcsfsIHelper.mkdirs(dirObjectUri);
    gcsFs.mkdirs(dirObjectUri);

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRawRequestStrings())
          .containsExactly(
              batchRequestString(),
              getRequestString(bucketName, dirObject),
              getRequestString(bucketName, dirObject + "/d1"),
              uploadRequestString(
                  bucketName,
                  dirObject + "/d1/",
                  /* generationId= */ 0,
                  /* replaceGenerationId= */ false),
              // verifies directory exists
              getRequestString(bucketName, dirObject + "/d1/"));
    }

    assertThat(gcsFs.exists(dirObjectUri)).isTrue();
    assertThat(gcsFs.getFileInfo(dirObjectUri).isDirectory()).isTrue();
  }

  @Test
  public void getFileInfo_sequential() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(false).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve("/" + dirObject);

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/file1", dirObject + "/file2", dirObject + "/file3");

    FileInfo dirInfo = gcsFs.getFileInfo(dirObjectUri);

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null))
          .inOrder();
    }

    assertThat(dirInfo.exists()).isTrue();
    assertThat(dirInfo.getPath().toString()).isEqualTo(dirObjectUri + "/");
  }

  @Test
  public void getFileInfo_parallel() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(true).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve("/" + dirObject);

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/file1", dirObject + "/file2", dirObject + "/file3");

    FileInfo dirInfo = gcsFs.getFileInfo(dirObjectUri);

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null));
    }

    assertThat(dirInfo.exists()).isTrue();
    assertThat(dirInfo.getPath().toString()).isEqualTo(dirObjectUri + "/");
  }

  @Test
  public void getFileInfo_single_file_sequential() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(false).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String fileObject = getTestResource() + "/f1";
    URI fileObjectUri = new URI("gs://" + bucketName).resolve("/" + fileObject);

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, fileObject);

    FileInfo fileInfo = gcsFs.getFileInfo(fileObjectUri);

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(getRequestString(bucketName, fileObject));
    }

    assertThat(fileInfo.exists()).isTrue();
    assertThat(fileInfo.getPath()).isEqualTo(fileObjectUri);
  }

  @Test
  public void getDirInfo_objectName_sequential() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(false).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve("/" + dirObject);

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/file1", dirObject + "/file2", dirObject + "/file3");

    FileInfo dirInfo = gcsFs.getFileInfo(new URI("gs://" + bucketName).resolve(dirObject));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null))
          .inOrder();
    }

    assertThat(dirInfo.exists()).isTrue();
    assertThat(dirInfo.getPath().toString()).isEqualTo(dirObjectUri + "/");
  }

  @Test
  public void getDirInfo_dirName_sequential() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(false).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve("/" + dirObject);

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/file1", dirObject + "/file2", dirObject + "/file3");

    FileInfo dirInfo = gcsFs.getFileInfo(new URI("gs://" + bucketName).resolve(dirObject + "/"));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null));

    assertThat(dirInfo.exists()).isTrue();
    assertThat(dirInfo.getPath().toString()).isEqualTo(dirObjectUri + "/");
  }

  @Test
  public void getDirInfo_implicit_objectName_sequential() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(false).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve("/" + dirObject);

    gcsfsIHelper.createObjects(
        bucketName, dirObject + "/file1", dirObject + "/file2", dirObject + "/file3");

    FileInfo dirInfo = gcsFs.getFileInfo(new URI("gs://" + bucketName).resolve(dirObject));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null))
          .inOrder();
    }

    assertThat(dirInfo.exists()).isTrue();
    assertThat(dirInfo.getPath().toString()).isEqualTo(dirObjectUri + "/");
  }

  @Test
  public void getDirInfo_implicit_dirName_sequential() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(false).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve("/" + dirObject);

    gcsfsIHelper.createObjects(
        bucketName, dirObject + "/file1", dirObject + "/file2", dirObject + "/file3");

    FileInfo dirInfo = gcsFs.getFileInfo(new URI("gs://" + bucketName).resolve(dirObject + "/"));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null));

    assertThat(dirInfo.exists()).isTrue();
    assertThat(dirInfo.getPath().toString()).isEqualTo(dirObjectUri + "/");
  }

  @Test
  public void getDirInfo_objectName_parallel() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(true).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve("/" + dirObject);

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/file1", dirObject + "/file2", dirObject + "/file3");

    FileInfo dirInfo = gcsFs.getFileInfo(new URI("gs://" + bucketName).resolve(dirObject));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null));
    }
    assertThat(dirInfo.exists()).isTrue();
    assertThat(dirInfo.getPath().toString()).isEqualTo(dirObjectUri + "/");
  }

  @Test
  public void getDirInfo_dirName_parallel() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(true).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve("/" + dirObject);

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/file1", dirObject + "/file2", dirObject + "/file3");

    FileInfo dirInfo = gcsFs.getFileInfo(new URI("gs://" + bucketName).resolve(dirObject + "/"));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null));

    assertThat(dirInfo.exists()).isTrue();
    assertThat(dirInfo.getPath().toString()).isEqualTo(dirObjectUri + "/");
  }

  @Test
  public void getDirInfo_implicit_objectName_parallel() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(true).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve("/" + dirObject);

    gcsfsIHelper.createObjects(
        bucketName, dirObject + "/file1", dirObject + "/file2", dirObject + "/file3");

    FileInfo dirInfo = gcsFs.getFileInfo(new URI("gs://" + bucketName).resolve(dirObject));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null));
    }

    assertThat(dirInfo.exists()).isTrue();
    assertThat(dirInfo.getPath().toString()).isEqualTo(dirObjectUri + "/");
  }

  @Test
  public void getDirInfo_implicit_dirName_parallel() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(true).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve("/" + dirObject);

    gcsfsIHelper.createObjects(
        bucketName, dirObject + "/file1", dirObject + "/file2", dirObject + "/file3");

    FileInfo dirInfo = gcsFs.getFileInfo(new URI("gs://" + bucketName).resolve(dirObject + "/"));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null));

    assertThat(dirInfo.exists()).isTrue();
    assertThat(dirInfo.getPath().toString()).isEqualTo(dirObjectUri + "/");
  }

  @Test
  public void getFileInfos_sequential() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(false).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/f1", dirObject + "/f2", dirObject + "/subdir/f3");

    List<FileInfo> fileInfos =
        gcsFs.getFileInfos(
            ImmutableList.of(
                bucketUri.resolve(dirObject + "/f1"),
                bucketUri.resolve(dirObject + "/f2"),
                bucketUri.resolve(dirObject + "/subdir/f3")));

    assertThat(fileInfos.stream().map(FileInfo::exists).collect(toList()))
        .containsExactly(true, true, true);
    assertThat(fileInfos.stream().map(FileInfo::getPath).collect(toList()))
        .containsExactly(
            bucketUri.resolve(dirObject + "/f1"),
            bucketUri.resolve(dirObject + "/f2"),
            bucketUri.resolve(dirObject + "/subdir/f3"))
        .inOrder();

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject + "/f1"),
              getRequestString(bucketName, dirObject + "/f2"),
              getRequestString(bucketName, dirObject + "/subdir/f3"))
          .inOrder();
    }
  }

  @Test
  public void getFileInfos_parallel() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(true).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/f1", dirObject + "/f2", dirObject + "/subdir/f3");

    List<FileInfo> fileInfos =
        gcsFs.getFileInfos(
            ImmutableList.of(
                bucketUri.resolve(dirObject + "/f1"),
                bucketUri.resolve(dirObject + "/f2"),
                bucketUri.resolve(dirObject + "/subdir/f3")));

    assertThat(fileInfos.stream().map(FileInfo::exists).collect(toList()))
        .containsExactly(true, true, true);
    assertThat(fileInfos.stream().map(FileInfo::getPath).collect(toList()))
        .containsExactly(
            bucketUri.resolve(dirObject + "/f1"),
            bucketUri.resolve(dirObject + "/f2"),
            bucketUri.resolve(dirObject + "/subdir/f3"));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/f1/", /* maxResults= */ 1, /* pageToken= */ null),
              getRequestString(bucketName, dirObject + "/f1"),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/f2/", /* maxResults= */ 1, /* pageToken= */ null),
              getRequestString(bucketName, dirObject + "/f2"),
              listRequestWithTrailingDelimiter(
                  bucketName,
                  dirObject + "/subdir/f3/",
                  /* maxResults= */ 1,
                  /* pageToken= */ null),
              getRequestString(bucketName, dirObject + "/subdir/f3"));
    }
  }

  @Test
  public void listFileInfo_file() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String fileObject = getTestResource();
    URI fileObjectUri = new URI("gs://" + bucketName + "/").resolve(fileObject);

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, fileObject);

    List<FileInfo> fileInfos = gcsFs.listFileInfo(fileObjectUri);

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, fileObject),
              listRequestWithTrailingDelimiter(
                  bucketName, fileObject + "/", /* pageToken= */ null));
    }

    assertThat(fileInfos).hasSize(1);
    FileInfo fileInfo = fileInfos.get(0);
    assertThat(fileInfo.exists()).isTrue();
    assertThat(fileInfo.getPath()).isEqualTo(fileObjectUri);
  }

  @Test
  public void listFileInfo_file_directoryPath() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String fileObject = getTestResource();
    URI bucketUri = new URI("gs://" + bucketName + "/");

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, fileObject);

    assertThrows(
        FileNotFoundException.class, () -> gcsFs.listFileInfo(bucketUri.resolve(fileObject + "/")));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(bucketName, fileObject + "/", /* pageToken= */ null));
  }

  @Test
  public void listFileInfo_directory() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/file1", dirObject + "/file2");

    List<FileInfo> fileInfos = gcsFs.listFileInfo(bucketUri.resolve(dirObject));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject),
              listRequestWithTrailingDelimiter(bucketName, dirObject + "/", /* pageToken= */ null));
    }

    assertThat(fileInfos.stream().map(FileInfo::exists).collect(toList()))
        .containsExactly(true, true);
    assertThat(fileInfos.stream().map(FileInfo::getPath).collect(toList()))
        .containsExactly(
            bucketUri.resolve(dirObject + "/file1"), bucketUri.resolve(dirObject + "/file2"));
  }

  @Test
  public void listFileInfo_directory_increased_page_size() throws Exception {
    gcsFs =
        newGcsFs(
            newGcsFsOptions()
                .setCloudStorageOptions(
                    getStandardOptionBuilder().setMaxListItemsPerCall(5000).build())
                .build());

    String bucketName = "dataproc-enhanced-list-integ-tests";
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = "listFileInfo_directory";

    List<FileInfo> fileInfos = gcsFs.listFileInfo(bucketUri.resolve(dirObject));

    assertThat(fileInfos).hasSize(5000);

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/", /* maxResults= */ 5000, /* pageToken= */ null));
    }
  }

  @Test
  public void listFileInfo_directory_directoryPath() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/file1", dirObject + "/file2");

    List<FileInfo> fileInfos = gcsFs.listFileInfo(bucketUri.resolve(dirObject + "/"));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(bucketName, dirObject + "/", /* pageToken= */ null));

    assertThat(fileInfos.stream().map(FileInfo::exists).collect(toList()))
        .containsExactly(true, true);
    assertThat(fileInfos.stream().map(FileInfo::getPath).collect(toList()))
        .containsExactly(
            bucketUri.resolve(dirObject + "/file1"), bucketUri.resolve(dirObject + "/file2"));
  }

  @Test
  public void listFileInfo_implicitDirectory() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjects(bucketName, dirObject + "/subdir/file");

    List<FileInfo> fileInfos = gcsFs.listFileInfo(bucketUri.resolve(dirObject));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject),
              listRequestWithTrailingDelimiter(bucketName, dirObject + "/", /* pageToken= */ null));
    }

    assertThat(fileInfos.stream().map(FileInfo::exists).collect(toList())).containsExactly(true);
    assertThat(fileInfos.stream().map(FileInfo::getPath).collect(toList()))
        .containsExactly(bucketUri.resolve(dirObject + "/subdir/"));
  }

  @Test
  public void listFileInfo_emptyDirectoryObject() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/");

    List<FileInfo> fileInfos = gcsFs.listFileInfo(bucketUri.resolve(dirObject));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject),
              listRequestWithTrailingDelimiter(bucketName, dirObject + "/", /* pageToken= */ null));
    }

    assertThat(fileInfos).isEmpty();
  }

  @Test
  public void listFileInfo_notFound() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    assertThrows(
        FileNotFoundException.class, () -> gcsFs.listFileInfo(bucketUri.resolve(dirObject)));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject),
              listRequestWithTrailingDelimiter(bucketName, dirObject + "/", /* pageToken= */ null));
    }
  }

  @Test
  public void listFileInfo_customFields_required() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/object");

    List<FileInfo> fileInfos =
        gcsFs.listFileInfo(
            bucketUri.resolve(dirObject),
            ListFileOptions.DEFAULT.toBuilder().setFields("bucket,name").build());

    FileInfo expectedFileInfo =
        FileInfo.fromItemInfo(
            GoogleCloudStorageItemInfo.createObject(
                StorageResourceId.fromUriPath(
                    bucketUri.resolve(dirObject + "/object"), /* allowEmptyObjectName= */ false),
                /* creationTime= */ 0,
                /* modificationTime= */ 0,
                /* size= */ 0,
                /* contentType= */ null,
                /* contentEncoding= */ null,
                /* metadata= */ null,
                /* contentGeneration= */ 0,
                /* metaGeneration= */ 0,
                new VerificationAttributes(/* md5hash= */ null, /* crc32c= */ null)));

    assertThat(fileInfos).containsExactly(expectedFileInfo);

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject),
              listRequestWithTrailingDelimiter(
                  bucketName,
                  dirObject + "/",
                  /* objectFields= */ "bucket,name",

                  /* pageToken= */ null));
    }
  }

  @Test
  public void listFileInfo_emptyNativeFolder_returnsEmptyList_notFileNotFound() throws Exception {
    gcsFs =
        newGcsFs(
            newGcsFsOptions()
                .setCloudStorageOptions(
                    gcsOptions.toBuilder()
                        .setHnOptimizationEnabled(true)
                        .setHnBucketRenameEnabled(true)
                        .build())
                .build());

    String hnsBucketName = gcsfsIHelper.getUniqueBucketName("hns-list-test");
    gcsFs
        .getGcs()
        .createBucket(
            hnsBucketName,
            CreateBucketOptions.builder().setHierarchicalNamespaceEnabled(true).build());

    URI folderUri = new URI(String.format("gs://%s/%s/", hnsBucketName, getTestResource()));

    gcsFs.mkdir(folderUri);

    FileInfo folderCheck = gcsFs.getFileInfo(folderUri);
    assertThat(folderCheck.exists()).isTrue();
    assertThat(folderCheck.getItemInfo().isNativeHNSFolder()).isTrue();

    List<FileInfo> fileInfos = gcsFs.listFileInfo(folderUri);

    assertThat(fileInfos).isEmpty();
  }

  @Test
  public void listFileInfo_customFields_some() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/object");

    List<FileInfo> fileInfos =
        gcsFs.listFileInfo(
            bucketUri.resolve(dirObject),
            ListFileOptions.DEFAULT.toBuilder().setFields("bucket,name,contentType").build());

    FileInfo expectedFileInfo =
        FileInfo.fromItemInfo(
            GoogleCloudStorageItemInfo.createObject(
                StorageResourceId.fromUriPath(
                    bucketUri.resolve(dirObject + "/object"), /* allowEmptyObjectName= */ false),
                /* creationTime= */ 0,
                /* modificationTime= */ 0,
                /* size= */ 0,
                /* contentType= */ "application/octet-stream",
                /* contentEncoding= */ null,
                /* metadata= */ null,
                /* contentGeneration= */ 0,
                /* metaGeneration= */ 0,
                new VerificationAttributes(/* md5hash= */ null, /* crc32c= */ null)));

    assertThat(fileInfos).containsExactly(expectedFileInfo);

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject),
              listRequestWithTrailingDelimiter(
                  bucketName,
                  dirObject + "/",
                  /* objectFields= */ "bucket,name,contentType",
                  /* pageToken= */ null));
    }
  }

  @Test
  public void move_onEmptyNativeFolder_throwsFileNotFound() throws Exception {
    gcsFs =
        newGcsFs(
            newGcsFsOptions()
                .setCloudStorageOptions(
                    gcsOptions.toBuilder()
                        .setHnOptimizationEnabled(true)
                        .setHnBucketRenameEnabled(true)
                        .build())
                .build());

    String hnsBucketName = gcsfsIHelper.getUniqueBucketName("hns-move-test");
    gcsFs
        .getGcs()
        .createBucket(
            hnsBucketName,
            CreateBucketOptions.builder().setHierarchicalNamespaceEnabled(true).build());

    URI srcFolderUri = new URI(String.format("gs://%s/%s/", hnsBucketName, getTestResource()));
    URI dstFolderUri =
        new URI(String.format("gs://%s/%s/", hnsBucketName, getTestResource() + "-dst"));

    gcsFs.mkdir(srcFolderUri);
    assertThat(gcsFs.getFileInfo(srcFolderUri).getItemInfo().isNativeHNSFolder()).isTrue();

    StorageResourceId srcId = StorageResourceId.fromUriPath(srcFolderUri, true);
    StorageResourceId dstId = StorageResourceId.fromUriPath(dstFolderUri, true);

    assertThrows(
        FileNotFoundException.class, () -> gcsFs.getGcs().move(ImmutableMap.of(srcId, dstId)));

    assertThat(gcsFs.exists(srcFolderUri)).isTrue();
    assertThat(gcsFs.exists(dstFolderUri)).isFalse();
  }

  @Test
  public void renameDirectory_withoutHnsOptimization_leavesEmptyNativeFolderBehind()
      throws Exception {
    String hnsBucketName = gcsfsIHelper.getUniqueBucketName("hns-rename-opposite-copy");
    GoogleCloudStorage gcs = gcsfsIHelper.gcs;
    gcs.createBucket(
        hnsBucketName, CreateBucketOptions.builder().setHierarchicalNamespaceEnabled(true).build());
    GoogleCloudStorageFileSystem hnsAwareGcsFs =
        newGcsFs(
            newGcsFsOptions()
                .setCloudStorageOptions(
                    gcsOptions.toBuilder()
                        .setHnOptimizationEnabled(true)
                        .setHnBucketRenameEnabled(true)
                        .build())
                .build());

    String testDir = getTestResource();
    URI srcDirUri = new URI(String.format("gs://%s/%s/src/", hnsBucketName, testDir));
    URI dstDirUri = new URI(String.format("gs://%s/%s/dst/", hnsBucketName, testDir));

    URI fileInSrcUri = srcDirUri.resolve("file.txt");
    URI emptyNativeSubDirInSrcUri = srcDirUri.resolve("empty-native-subdir/");

    gcsfsIHelper.writeTextFile(hnsBucketName, fileInSrcUri.getPath(), "test-data");
    // Use the HNS-aware client to create the native folder.
    hnsAwareGcsFs.mkdir(emptyNativeSubDirInSrcUri);
    assertThat(
            hnsAwareGcsFs.getFileInfo(emptyNativeSubDirInSrcUri).getItemInfo().isNativeHNSFolder())
        .isTrue();
    hnsAwareGcsFs.close();

    // Perform rename using a client with HNS optimization DISABLED.
    gcsFs =
        newGcsFs(
            newGcsFsOptions()
                .setCloudStorageOptions(
                    gcsOptions.toBuilder()
                        .setHnOptimizationEnabled(false)
                        .setHnBucketRenameEnabled(false)
                        .build())
                .build());
    gcsFs.rename(srcDirUri, dstDirUri);

    assertThat(gcsFs.exists(dstDirUri)).isTrue();
    assertThat(gcsFs.exists(dstDirUri.resolve("file.txt"))).isTrue();
    assertThat(gcsFs.exists(dstDirUri.resolve("empty-native-subdir/"))).isFalse();

    // Use getFolderInfo directly since the hns-optimization flag is off
    assertThat(
            gcsFs
                .getGcs()
                .getFolderInfo(StorageResourceId.fromStringPath(srcDirUri.toString()))
                .exists())
        .isTrue();
    assertThat(gcsFs.exists(fileInSrcUri)).isFalse();
    assertThat(
            gcsFs
                .getGcs()
                .getFolderInfo(
                    StorageResourceId.fromStringPath(emptyNativeSubDirInSrcUri.toString()))
                .exists())
        .isTrue();
  }

  @Test
  public void renameDirectory_withoutHnsOptimization_withMove_leavesEmptyNativeFolderBehind()
      throws Exception {
    String hnsBucketName = gcsfsIHelper.getUniqueBucketName("hns-rename-opposite-move");
    GoogleCloudStorage gcs = gcsfsIHelper.gcs;
    gcs.createBucket(
        hnsBucketName, CreateBucketOptions.builder().setHierarchicalNamespaceEnabled(true).build());
    GoogleCloudStorageFileSystem hnsAwareGcsFs =
        newGcsFs(
            newGcsFsOptions()
                .setCloudStorageOptions(
                    gcsOptions.toBuilder()
                        .setHnOptimizationEnabled(true)
                        .setHnBucketRenameEnabled(true)
                        .build())
                .build());

    String testDir = getTestResource();
    URI srcDirUri = new URI(String.format("gs://%s/%s/src/", hnsBucketName, testDir));
    URI dstDirUri = new URI(String.format("gs://%s/%s/dst/", hnsBucketName, testDir));

    URI fileInSrcUri = srcDirUri.resolve("file.txt");
    URI emptyNativeSubDirInSrcUri = srcDirUri.resolve("empty-native-subdir/");

    gcsfsIHelper.writeTextFile(hnsBucketName, fileInSrcUri.getPath(), "test-data");
    hnsAwareGcsFs.mkdir(emptyNativeSubDirInSrcUri);
    hnsAwareGcsFs.close();

    // Perform rename using a client with HNS optimization DISABLED.
    gcsFs =
        newGcsFs(
            newGcsFsOptions()
                .setCloudStorageOptions(
                    gcsOptions.toBuilder()
                        .setHnOptimizationEnabled(false)
                        .setHnBucketRenameEnabled(false)
                        .setMoveOperationEnabled(true)
                        .build())
                .build());
    gcsFs.rename(srcDirUri, dstDirUri);

    assertThat(gcsFs.exists(dstDirUri)).isTrue();
    assertThat(gcsFs.exists(dstDirUri.resolve("file.txt"))).isTrue();
    assertThat(gcsFs.exists(dstDirUri.resolve("empty-native-subdir/"))).isFalse();

    // Use getFolderInfo directly since the hns-optimization flag is off
    assertThat(
            gcsFs
                .getGcs()
                .getFolderInfo(StorageResourceId.fromStringPath(srcDirUri.toString()))
                .exists())
        .isTrue();
    assertThat(gcsFs.exists(fileInSrcUri)).isFalse();
    assertThat(
            gcsFs
                .getGcs()
                .getFolderInfo(
                    StorageResourceId.fromStringPath(emptyNativeSubDirInSrcUri.toString()))
                .exists())
        .isTrue();
  }

  @Test
  public void listFileInfo_customFields_fails() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/object");

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                gcsFs.listFileInfo(
                    bucketUri.resolve(dirObject),
                    ListFileOptions.DEFAULT.toBuilder().setFields("bucket").build()));

    assertThat(thrown).hasCauseThat().hasCauseThat().isInstanceOf(NullPointerException.class);

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject),
              listRequestWithTrailingDelimiter(
                  bucketName,
                  dirObject + "/",
                  /* objectFields= */ "bucket",
                  /* pageToken= */ null));
    }
  }

  @Test
  public void listFileInfoStartingFrom_customFields_required() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());
    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource() + "/";
    String objectName = dirObject + "I_am_file";
    gcsfsIHelper.createObjects(bucketName, objectName);

    List<FileInfo> fileInfos =
        gcsFs.listFileInfoStartingFrom(
            bucketUri.resolve(dirObject),
            ListFileOptions.DEFAULT.toBuilder().setFields("bucket,name").build());

    // Can't asset that this is the only object we get in response, other object lexicographically
    // higher would also come in response.
    // Only thing we can assert strongly is, list would start with the files created in this
    // directory.
    assertThat(fileInfos.get(0).getPath()).isEqualTo(bucketUri.resolve(objectName));
    // No item info calls are made with offset based api
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithStartOffset(
                bucketName, dirObject, /* pageToken= */ null, /* objectFields */ "bucket,name"));
  }

  @Test
  public void listFileInfoStartingFrom_customFields_fail() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());
    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource() + "/";
    String objectName = dirObject + "I_am_file";
    gcsfsIHelper.createObjects(bucketName, objectName);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            gcsFs.listFileInfoStartingFrom(
                bucketUri.resolve(dirObject),
                ListFileOptions.DEFAULT.toBuilder().setFields("bucket").build()));

    assertTrue(gcsRequestsTracker.getAllRequestStrings().isEmpty());
  }

  @Test
  public void listFileInfoStartingFrom_emptyResponse() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());
    String bucketName = gcsfsIHelper.sharedBucketName1;

    URI bucketUri = new URI("gs://" + bucketName + "/");
    // lexicographically the highest string of length 4
    String dirObject = "~~~~" + "/";
    gcsfsIHelper.createObjects(bucketName, dirObject);

    List<FileInfo> fileInfos = gcsFs.listFileInfoStartingFrom(bucketUri.resolve(dirObject));

    // Can't asset that this is the only object we get in response, other object lexicographically
    // higher would also come in response.
    // Only thing we can assert strongly is, list would start with the files created in this
    // directory.
    assertThat(fileInfos.size()).isEqualTo(0);

    // No item info calls are made with offset based api
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(listRequestWithStartOffset(bucketName, dirObject, /* pageToken= */ null));
  }

  @Test
  public void listFileInfoStartingFrom_sortedLexicographically() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());
    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource() + "/";
    int filesCount = 50;
    List<URI> createdURIs = new ArrayList<>();
    for (int i = 0; i < filesCount; i++) {
      String objectName = dirObject + UUID.randomUUID();
      gcsfsIHelper.createObjects(bucketName, objectName);
      createdURIs.add(bucketUri.resolve(objectName));
    }
    List<URI> sortedURI =
        createdURIs.stream()
            .sorted(Comparator.comparing(URI::getPath))
            .collect(Collectors.toList());

    List<FileInfo> fileInfos = gcsFs.listFileInfoStartingFrom(bucketUri.resolve(dirObject));

    // Can't asset that this is the only object we get in response, other object lexicographically
    // higher would also come in response.
    // Only thing we can assert strongly is, list would start with the files created in this
    // directory.
    for (int i = 0; i < filesCount; i++) {
      assertThat(getURIs(fileInfos).get(i)).isEqualTo(sortedURI.get(i));
    }

    // No item info calls are made with offset based api
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(listRequestWithStartOffset(bucketName, dirObject, /* pageToken= */ null));
  }

  @Test
  public void listFileInfoStartingFrom_filter_directory() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());
    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource() + "/";
    String objectName = dirObject + "I_am_file";
    gcsfsIHelper.createObjects(bucketName, dirObject);
    gcsfsIHelper.createObjects(bucketName, objectName);

    List<FileInfo> fileInfos = gcsFs.listFileInfoStartingFrom(bucketUri.resolve(dirObject));

    // Can't assert that this is the only object we get in response, other object lexicographically
    // higher would also come in response.
    // Only thing we can assert strongly is, list would start with the files created in this
    // directory.
    assertThat(fileInfos.get(0).getPath()).isEqualTo(bucketUri.resolve(objectName));
    // No item info calls are made with offset based api
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(listRequestWithStartOffset(bucketName, dirObject, /* pageToken= */ null));

    // directory object was filtered
    FileInfo dirFileInfo =
        gcsFs.getFileInfoWithHint(bucketUri.resolve(dirObject), PathTypeHint.FILE);
    // directory as an object do exists.
    assertTrue(dirFileInfo.isDirectory());
    // but directory objects are filtered out and only files are returned.
    assertThat(getURIs(fileInfos)).doesNotContain(bucketUri.resolve(dirObject));
  }

  @Test
  public void delete_file_sequential() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(false).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/f1");

    gcsFs.delete(bucketUri.resolve(dirObject + "/f1"), /* recursive= */ false);

    if (isTracingSupported) {
      // This test performs additional POST request to dirObject when run singly, as result
      // generationId for delete operation need to be increased for DELETE request.
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject + "/f1"),
              getRequestString(bucketName, dirObject + "/"),
              deleteRequestString(bucketName, dirObject + "/f1", 1));
    }

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f1"))).isFalse();
  }

  @Test
  public void delete_file_parallel() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(true).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/f1");

    gcsFs.delete(bucketUri.resolve(dirObject + "/f1"), /* recursive= */ false);

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject + "/"),
              getRequestString(bucketName, dirObject + "/f1"),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/f1/", /* maxResults= */ 1, /* pageToken= */ null),
              deleteRequestString(bucketName, dirObject + "/f1", /* generationId= */ 1));
    }

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f1"))).isFalse();
  }

  @Test
  public void delete_directory_sequential() throws Exception {
    delete_directory(/* parallelStatus= */ false);
  }

  @Test
  public void delete_directory_parallel() throws Exception {
    delete_directory(/* parallelStatus= */ true);
  }

  @Test
  public void delete_directory_sequential_withHnsOptimization() throws Exception {
    delete_directory_hns_optimization(/* parallelStatus= */ false);
  }

  @Test
  public void delete_directory_parallel_withHnsOptimization() throws Exception {
    delete_directory_hns_optimization(/* parallelStatus= */ true);
  }

  private void delete_directory(boolean parallelStatus) throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(parallelStatus).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/d1/");

    gcsFs.delete(bucketUri.resolve(dirObject + "/d1/"), /* recursive= */ false);

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject + "/"),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/d1/", /* maxResults= */ 1, /* pageToken= */ null),
              listRequestString(
                  bucketName, true, null, dirObject + "/d1/", "bucket,name,generation", null),
              deleteRequestString(bucketName, dirObject + "/d1/", /* generationId= */ 1));
    }

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/d1"))).isFalse();
  }

  private void delete_directory_hns_optimization(boolean parallelStatus) throws Exception {
    gcsFs =
        newGcsFs(
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(
                    gcsOptions.toBuilder().setHnOptimizationEnabled(true).build())
                .setStatusParallelEnabled(parallelStatus)
                .build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/d1/");
    gcsFs.delete(bucketUri.resolve(dirObject + "/d1/"), /* recursive= */ false);

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsAtLeast(
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/d1/", /* maxResults= */ 1, /* pageToken= */ null),
              listRequestString(
                  bucketName,
                  /* flatList= */ false,
                  /* includeTrailingDelimiter= */ true,
                  dirObject + "/d1/",
                  "bucket,name,generation",
                  /* maxResults= */ 2,
                  /* pageToken= */ null,
                  /* startOffset */ null,
                  /* includeFoldersAsPrefixes= */ true),
              getRequestString(bucketName, dirObject + "/"),
              deleteRequestString(bucketName, dirObject + "/d1/", /* generationId= */ 1));
    }

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/d1"))).isFalse();
  }

  @Test
  public void delete_inferredDirectory() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(false).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjects(bucketName, dirObject + "/");
    gcsfsIHelper.createObjects(bucketName, dirObject + "/d1/f1");

    gcsFs.delete(bucketUri.resolve(dirObject + "/d1/"), /* recursive= */ true);

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/d1/", /* maxResults= */ 1, /* pageToken= */ null),
              listRequestString(
                  bucketName,
                  /* flatList= */ true,
                  /* includeTrailingDelimiter= */ null,
                  dirObject + "/d1/",
                  "bucket,name,generation",

                  /* pageToken= */ null),
              deleteRequestString(bucketName, dirObject + "/d1/f1", /* generationId= */ 1),
              getRequestString(bucketName, dirObject + "/"));
    }

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/d1/"))).isFalse();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/d1/f1"))).isFalse();
  }

  @Test
  public void rename_file_sequential() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(false).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/f1");

    gcsFs.rename(bucketUri.resolve(dirObject + "/f1"), bucketUri.resolve(dirObject + "/f2"));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject + "/f1"),
              getRequestString(bucketName, dirObject + "/f2"),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/f2/", /* maxResults= */ 1, /* pageToken= */ null),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null),
              getRequestString(bucketName, dirObject + "/"),
              moveRequestString(
                  bucketName,
                  dirObject + "/f1",
                  dirObject + "/f2",
                  "moveTo",
                  /* generationId= */ 1,
                  /* sourceGenerationId= */ 1));
    }

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f1"))).isFalse();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f2"))).isTrue();
  }

  @Test
  public void rename_file_withMoveDisabled_usesCopyAndDelete() throws Exception {
    gcsFs =
        newGcsFs(
            newGcsFsOptions()
                .setCloudStorageOptions(
                    gcsOptions.toBuilder().setMoveOperationEnabled(false).build())
                .setStatusParallelEnabled(false)
                .build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/f1");

    gcsFs.rename(bucketUri.resolve(dirObject + "/f1"), bucketUri.resolve(dirObject + "/f2"));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject + "/f1"),
              getRequestString(bucketName, dirObject + "/f2"),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/f2/", /* maxResults= */ 1, /* pageToken= */ null),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null),
              getRequestString(bucketName, dirObject + "/"),
              copyRequestString(
                  bucketName,
                  dirObject + "/f1",
                  bucketName,
                  dirObject + "/f2",
                  "copyTo",
                  /* generationId= */ 1),
              deleteRequestString(bucketName, dirObject + "/f1", 2));
    }

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f1"))).isFalse();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f2"))).isTrue();
  }

  @Test
  public void rename_file_parallel() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(true).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/f1");

    gcsFs.rename(bucketUri.resolve(dirObject + "/f1"), bucketUri.resolve(dirObject + "/f2"));

    if (isTracingSupported) {
      // This test performs additional POST request to dirObject when run singly, as result
      // generationId for delete operation need to be increased for DELETE request.
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject + "/f1"),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/f1/", /* maxResults= */ 1, /* pageToken= */ null),
              getRequestString(bucketName, dirObject + "/f2"),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/f2/", /* maxResults= */ 1, /* pageToken= */ null),
              getRequestString(bucketName, dirObject + "/"),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null),
              moveRequestString(
                  bucketName,
                  dirObject + "/f1",
                  dirObject + "/f2",
                  "moveTo",
                  /* generationId= */ 1,
                  /* sourceGenerationId= */ 1));
    }

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f1"))).isFalse();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f2"))).isTrue();
  }

  @Test
  public void rename_file_shouldFailIfFileExists() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/f1");
    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/f2");

    assertThrows(
        IOException.class,
        () ->
            gcsFs.rename(
                bucketUri.resolve(dirObject + "/f1"), bucketUri.resolve(dirObject + "/f2")));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              getRequestString(bucketName, dirObject + "/f1"),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/f1/", /* maxResults= */ 1, /* pageToken= */ null),
              getRequestString(bucketName, dirObject + "/f2"),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/f2/", /* maxResults= */ 1, /* pageToken= */ null),
              listRequestWithTrailingDelimiter(
                  bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null));
    }

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f1"))).isTrue();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f2"))).isTrue();
  }

  @Test
  public void rename_directory_sequential() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(false).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/srcParent/srcDir/f", dirObject + "/dstParent/");

    gcsFs.rename(
        bucketUri.resolve(dirObject + "/srcParent/srcDir"),
        bucketUri.resolve(dirObject + "/dstParent/dstDir"));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              // Get src info
              getRequestString(bucketName, dirObject + "/srcParent/srcDir"),
              listRequestWithTrailingDelimiter(
                  bucketName,
                  dirObject + "/srcParent/srcDir/",
                  /* maxResults= */ 1,
                  /* pageToken= */ null),
              // Get dst info
              getRequestString(bucketName, dirObject + "/dstParent/dstDir"),
              listRequestWithTrailingDelimiter(
                  bucketName,
                  dirObject + "/dstParent/dstDir/",
                  /* maxResults= */ 1,
                  /* pageToken= */ null),
              // Get dst dir parent info
              listRequestWithTrailingDelimiter(
                  bucketName,
                  dirObject + "/dstParent/",
                  /* maxResults= */ 1,
                  /* pageToken= */ null),
              // Check if src directory parent object exists
              getRequestString(bucketName, dirObject + "/srcParent/"),
              // List files that need to be renamed in src directory
              listRequestString(
                  bucketName,
                  /* flatList= */ true,
                  /* includeTrailingDelimiter= */ null,
                  dirObject + "/srcParent/srcDir/",
                  "bucket,name,generation",

                  /* pageToken= */ null),
              // Move file
              moveRequestString(
                  bucketName,
                  dirObject + "/srcParent/srcDir/f",
                  dirObject + "/dstParent/dstDir/f",
                  "moveTo"),
              // Delete src directory
              deleteRequestString(bucketName, dirObject + "/srcParent/srcDir/", 1));
    }

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/srcParent/srcDir/f"))).isFalse();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/srcParent/srcDir"))).isFalse();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/srcParent"))).isTrue();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/dstParent/dstDir/f"))).isTrue();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/dstParent/dstDir"))).isTrue();
  }

  @Test
  public void rename_onHnsBucket_repairsImplicitDirectory() throws Exception {
    String hnsBucketName = gcsfsIHelper.getUniqueBucketName("hns-bucket");
    gcsFs =
        newGcsFs(
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(
                    gcsOptions.toBuilder().setHnOptimizationEnabled(true).build())
                .build());
    gcsFs
        .getGcs()
        .createBucket(
            hnsBucketName,
            CreateBucketOptions.builder().setHierarchicalNamespaceEnabled(true).build());

    URI parentDirUri = new URI(String.format("gs://%s/explicit-dir-no-repair/", hnsBucketName));
    URI src = parentDirUri.resolve("file.txt");
    URI dst = new URI(String.format("gs://%s/explicit-file.txt", hnsBucketName));

    gcsfsIHelper.writeTextFile(hnsBucketName, src.getPath(), "test-data-hns");
    assertThat(gcsFs.exists(src)).isTrue();

    gcsFs.rename(src, dst);

    assertThat(gcsFs.exists(src)).isFalse();
    assertThat(gcsFs.exists(dst)).isTrue();

    // The parent directory exists since in HNS bucket the parent directories are created
    // automatically
    FileInfo parentInfo = gcsFs.getFileInfo(parentDirUri);
    assertThat(parentInfo.exists()).isTrue();
    assertThat(parentInfo.isDirectory()).isTrue();
  }

  @Test
  public void rename_onNonHnsBucket_withHnsOptimization_repairsImplicitDirectory()
      throws Exception {
    gcsFs =
        newGcsFs(
            newGcsFsOptions()
                .setCloudStorageOptions(
                    gcsOptions.toBuilder().setHnOptimizationEnabled(true).build())
                .build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI parentDirUri = new URI(String.format("gs://%s/implicit-dir-to-repair1/", bucketName));
    URI src = parentDirUri.resolve("file.txt");
    URI dst = new URI(String.format("gs://%s/repaired-file.txt", bucketName));

    gcsfsIHelper.writeTextFile(bucketName, src.getPath(), "test-data");
    assertThat(gcsFs.exists(parentDirUri)).isTrue();
    assertThat(gcsFs.exists(src)).isTrue();

    gcsFs.rename(src, dst);

    assertThat(gcsFs.exists(src)).isFalse();
    assertThat(gcsFs.exists(dst)).isTrue();

    // The parent directory should still exist because the repair logic was triggered
    FileInfo parentInfo = gcsFs.getFileInfo(parentDirUri);
    assertThat(parentInfo.exists()).isTrue();
    assertThat(parentInfo.isDirectory()).isTrue();
  }

  @Test
  public void rename_onNonHnsBucket_withAutoRepairDisabled_doesNotRepairsImplicitDirectory()
      throws Exception {
    gcsFs =
        newGcsFs(
            newGcsFsOptions()
                .setCloudStorageOptions(
                    gcsOptions.toBuilder()
                        .setHnOptimizationEnabled(true)
                        .setAutoRepairImplicitDirectoriesEnabled(false)
                        .build())
                .build());
    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI parentDirUri = new URI(String.format("gs://%s/implicit-dir-to-repair2/", bucketName));
    URI src = parentDirUri.resolve("file.txt");
    URI dst = new URI(String.format("gs://%s/renamed-file.txt", bucketName));

    gcsfsIHelper.writeTextFile(bucketName, src.getPath(), "test-data");
    assertThat(gcsFs.exists(parentDirUri)).isTrue();
    assertThat(gcsFs.exists(src)).isTrue();

    gcsFs.rename(src, dst);

    assertThat(gcsFs.exists(src)).isFalse();
    assertThat(gcsFs.exists(dst)).isTrue();

    // The parent directory does not  exist because the repair logic was not triggered
    FileInfo parentInfo = gcsFs.getFileInfo(parentDirUri);
    assertThat(parentInfo.exists()).isFalse();
  }

  @Test
  public void delete_onNonHnsBucket_withAutoRepairEnabled_repairsImplicitDirectory()
      throws Exception {
    gcsFs =
        newGcsFs(
            newGcsFsOptions()
                .setCloudStorageOptions(
                    gcsOptions.toBuilder().setAutoRepairImplicitDirectoriesEnabled(true).build())
                .build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI parentDirUri = new URI(String.format("gs://%s/implicit-dir-for-delete/", bucketName));
    URI fileToDelete = parentDirUri.resolve("file-to-delete.txt");

    gcsfsIHelper.writeTextFile(bucketName, fileToDelete.getPath(), "test-data");
    assertThat(gcsFs.exists(parentDirUri)).isTrue();
    assertThat(gcsFs.exists(fileToDelete)).isTrue();

    gcsFs.delete(fileToDelete, /* recursive= */ false);

    assertThat(gcsFs.exists(fileToDelete)).isFalse();

    FileInfo parentInfo = gcsFs.getFileInfo(parentDirUri);
    assertThat(parentInfo.exists()).isTrue();
    assertThat(parentInfo.isDirectory()).isTrue();
  }

  @Test
  public void delete_onNonHnsBucket_withAutoRepairDisabled_removesImplicitDirectory()
      throws Exception {
    gcsFs =
        newGcsFs(
            newGcsFsOptions()
                .setCloudStorageOptions(
                    gcsOptions.toBuilder().setAutoRepairImplicitDirectoriesEnabled(false).build())
                .build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI parentDirUri = new URI(String.format("gs://%s/implicit-dir-to-vanish/", bucketName));
    URI fileToDelete = parentDirUri.resolve("file-to-delete.txt");

    gcsfsIHelper.writeTextFile(bucketName, fileToDelete.getPath(), "test-data");
    assertThat(gcsFs.exists(parentDirUri)).isTrue();

    gcsFs.delete(fileToDelete, /* recursive= */ false);

    assertThat(gcsFs.exists(fileToDelete)).isFalse();

    FileInfo parentInfo = gcsFs.getFileInfo(parentDirUri);
    assertThat(parentInfo.exists()).isFalse();
  }

  @Test
  public void delete_onHnsBucket_skipsRepairForNativeFolder() throws Exception {
    String hnsBucketName = gcsfsIHelper.getUniqueBucketName("hns-bucket-delete");
    gcsFs =
        newGcsFs(
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(
                    gcsOptions.toBuilder().setHnOptimizationEnabled(true).build())
                .build());
    gcsFs
        .getGcs()
        .createBucket(
            hnsBucketName,
            CreateBucketOptions.builder().setHierarchicalNamespaceEnabled(true).build());

    URI parentDirUri = new URI(String.format("gs://%s/native-folder-for-delete/", hnsBucketName));
    URI fileToDelete = parentDirUri.resolve("file.txt");

    gcsFs.mkdirs(parentDirUri);
    gcsfsIHelper.writeTextFile(hnsBucketName, fileToDelete.getPath(), "test-data-hns");
    assertThat(gcsFs.exists(fileToDelete)).isTrue();

    gcsFs.delete(fileToDelete, false);

    assertThat(gcsFs.exists(fileToDelete)).isFalse();

    FileInfo parentInfo = gcsFs.getFileInfo(parentDirUri);
    assertThat(parentInfo.exists()).isTrue();
  }

  @Test
  public void rename_directory_parallel() throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().setStatusParallelEnabled(true).build());

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/srcParent/srcDir/f", dirObject + "/dstParent/");

    gcsFs.rename(
        bucketUri.resolve(dirObject + "/srcParent/srcDir"),
        bucketUri.resolve(dirObject + "/dstParent/dstDir"));

    if (isTracingSupported) {
      assertThat(gcsRequestsTracker.getAllRequestStrings())
          .containsExactly(
              // Get src info
              getRequestString(bucketName, dirObject + "/srcParent/srcDir"),
              listRequestWithTrailingDelimiter(
                  bucketName,
                  dirObject + "/srcParent/srcDir/",
                  /* maxResults= */ 1,
                  /* pageToken= */ null),
              // Get dst info
              getRequestString(bucketName, dirObject + "/dstParent/dstDir"),
              listRequestWithTrailingDelimiter(
                  bucketName,
                  dirObject + "/dstParent/dstDir/",
                  /* maxResults= */ 1,
                  /* pageToken= */ null),
              // Get dst dir parent info
              listRequestWithTrailingDelimiter(
                  bucketName,
                  dirObject + "/dstParent/",
                  /* maxResults= */ 1,
                  /* pageToken= */ null),
              // Check if src directory parent object exists
              getRequestString(bucketName, dirObject + "/srcParent/"),
              // List files that need to be renamed in src directory
              listRequestString(
                  bucketName,
                  /* flatList= */ true,
                  /* includeTrailingDelimiter= */ null,
                  dirObject + "/srcParent/srcDir/",
                  "bucket,name,generation",

                  /* pageToken= */ null),
              // Move file
              moveRequestString(
                  bucketName,
                  dirObject + "/srcParent/srcDir/f",
                  dirObject + "/dstParent/dstDir/f",
                  "moveTo"),
              deleteRequestString(bucketName, dirObject + "/srcParent/srcDir/", 1));
    }

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/srcParent/srcDir/f"))).isFalse();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/srcParent/srcDir"))).isFalse();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/srcParent"))).isTrue();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/dstParent/dstDir/f"))).isTrue();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/dstParent/dstDir"))).isTrue();
  }

  @Test
  public void concurrentCreation_newObjet_overwrite_oneSucceeds() throws Exception {
    concurrentCreate_oneSucceeds(/* overwriteExisting= */ false);
  }

  @Test
  public void concurrentCreate_existingObject_overwrite_oneSucceeds() throws Exception {
    concurrentCreate_oneSucceeds(/* overwriteExisting= */ true);
  }

  private void concurrentCreate_oneSucceeds(boolean overwriteExisting) throws Exception {
    gcsFs = newGcsFs(newGcsFsOptions().build());

    GoogleCloudStorageFileSystemIntegrationHelper testHelper =
        new GoogleCloudStorageFileSystemIntegrationHelper(gcsFs);

    URI path = new URI("gs://" + gcsfsIHelper.sharedBucketName1 + "/" + getTestResource());
    assertThat(gcsFs.getFileInfo(path).exists()).isFalse();

    if (overwriteExisting) {
      String text = "Hello World! Existing";
      int numBytesWritten =
          gcsfsIHelper.writeFile(path, text, /* numWrites= */ 1, /* overwrite= */ false);
      assertThat(numBytesWritten).isEqualTo(text.getBytes(UTF_8).length);
    }

    List<String> texts = ImmutableList.of("Hello World!", "World Hello! Long");

    ExecutorService executorService = Executors.newFixedThreadPool(2);
    List<Future<Integer>> futures =
        executorService.invokeAll(
            ImmutableList.of(
                () ->
                    testHelper.writeFile(
                        path, texts.get(0), /* numWrites= */ 1, /* overwrite= */ true),
                () ->
                    testHelper.writeFile(
                        path, texts.get(1), /* numWrites= */ 1, /* overwrite= */ true)));
    executorService.shutdown();

    assertThat(executorService.awaitTermination(1, MINUTES)).isTrue();

    // Verify the final write result is either text1 or text2.
    String readText = testHelper.readTextFile(path);
    assertThat(ImmutableList.of(readText)).containsAnyIn(texts);

    // One future should fail and one succeed
    for (int i = 0; i < futures.size(); i++) {
      Future<Integer> future = futures.get(i);
      String text = texts.get(i);
      if (readText.equals(text)) {
        assertThat(future.get()).isEqualTo(text.length());
      } else {
        assertThrows(ExecutionException.class, future::get);
      }
    }
  }

  private String getTestResource() {
    return name.getMethodName() + "_" + UUID.randomUUID();
  }

  private static GoogleCloudStorageFileSystemOptions.Builder newGcsFsOptions() {
    return GoogleCloudStorageFileSystemOptions.builder().setCloudStorageOptions(gcsOptions);
  }

  private static List<URI> getURIs(List<FileInfo> fileInfos) {
    return fileInfos.stream().map(FileInfo::getPath).collect(toList());
  }

  protected abstract GoogleCloudStorageFileSystem newGcsFs(
      GoogleCloudStorageFileSystemOptions gcsfsOptions) throws IOException;
}
