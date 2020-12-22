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

import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.batchRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.copyRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.deleteRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestWithTrailingDelimiter;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.uploadRequestString;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getStandardOptionBuilder;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertThrows;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link GoogleCloudStorageFileSystem} class. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageFileSystemNewIntegrationTest {

  private static GoogleCloudStorageOptions gcsOptions;
  private static RetryHttpInitializer httpRequestsInitializer;
  private static GoogleCloudStorageFileSystemIntegrationHelper gcsfsIHelper;

  @Rule public TestName name = new TestName();

  @BeforeClass
  public static void before() throws Throwable {
    Credential credential =
        checkNotNull(GoogleCloudStorageTestHelper.getCredential(), "credential must not be null");

    gcsOptions = getStandardOptionBuilder().build();
    httpRequestsInitializer =
        new RetryHttpInitializer(credential, gcsOptions.toRetryHttpInitializerOptions());

    GoogleCloudStorageFileSystem gcsfs =
        new GoogleCloudStorageFileSystem(
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
    GoogleCloudStorageFileSystem gcsfs = gcsfsIHelper.gcsfs;
    assertThat(gcsfs.exists(new URI("gs://" + gcsfsIHelper.sharedBucketName1))).isFalse();
    assertThat(gcsfs.exists(new URI("gs://" + gcsfsIHelper.sharedBucketName2))).isFalse();
  }

  @Test
  public void mkdirs_shouldCreateNewDirectory() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve(dirObject);

    gcsFs.mkdir(dirObjectUri);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            uploadRequestString(bucketName, dirObject + "/", /* generationId= */ null));

    assertThat(gcsFs.exists(dirObjectUri)).isTrue();
    assertThat(gcsFs.getFileInfo(dirObjectUri).isDirectory()).isTrue();
  }

  @Test
  public void getFileInfo_sequential() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs =
        newGcsFs(newGcsFsOptions().setStatusParallelEnabled(false).build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve("/" + dirObject);

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/file1", dirObject + "/file2", dirObject + "/file3");

    FileInfo dirInfo = gcsFs.getFileInfo(dirObjectUri);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject),
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null))
        .inOrder();

    assertThat(dirInfo.exists()).isTrue();
    assertThat(dirInfo.getPath().toString()).isEqualTo(dirObjectUri + "/");
  }

  @Test
  public void getFileInfo_parallel() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions().setStatusParallelEnabled(true).build();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve("/" + dirObject);

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/file1", dirObject + "/file2", dirObject + "/file3");

    FileInfo dirInfo = gcsFs.getFileInfo(dirObjectUri);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject),
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null));

    assertThat(dirInfo.exists()).isTrue();
    assertThat(dirInfo.getPath().toString()).isEqualTo(dirObjectUri + "/");
  }

  @Test
  public void getFileInfo_single_file_sequential() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs =
        newGcsFs(newGcsFsOptions().setStatusParallelEnabled(false).build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String fileObject = getTestResource() + "/f1";
    URI fileObjectUri = new URI("gs://" + bucketName).resolve("/" + fileObject);

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, fileObject);

    FileInfo fileInfo = gcsFs.getFileInfo(fileObjectUri);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(getRequestString(bucketName, fileObject));

    assertThat(fileInfo.exists()).isTrue();
    assertThat(fileInfo.getPath()).isEqualTo(fileObjectUri);
  }

  @Test
  public void getDirInfo_sequential() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs =
        newGcsFs(newGcsFsOptions().setStatusParallelEnabled(false).build(), gcsRequestsTracker);

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
  public void getDirInfo_parallel() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions().setStatusParallelEnabled(true).build();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

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
  public void getFileInfos_sequential() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs =
        newGcsFs(newGcsFsOptions().setStatusParallelEnabled(false).build(), gcsRequestsTracker);

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

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject + "/f1"),
            getRequestString(bucketName, dirObject + "/f2"),
            getRequestString(bucketName, dirObject + "/subdir/f3"))
        .inOrder();
  }

  @Test
  public void getFileInfos_parallel() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs =
        newGcsFs(newGcsFsOptions().setStatusParallelEnabled(true).build(), gcsRequestsTracker);

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

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/f1/", /* maxResults= */ 1, /* pageToken= */ null),
            getRequestString(bucketName, dirObject + "/f1"),
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/f2/", /* maxResults= */ 1, /* pageToken= */ null),
            getRequestString(bucketName, dirObject + "/f2"),
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/subdir/f3/", /* maxResults= */ 1, /* pageToken= */ null),
            getRequestString(bucketName, dirObject + "/subdir/f3"));
  }

  @Test
  public void listFileInfo_file() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String fileObject = getTestResource();
    URI fileObjectUri = new URI("gs://" + bucketName + "/").resolve(fileObject);

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, fileObject);

    List<FileInfo> fileInfos = gcsFs.listFileInfo(fileObjectUri);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, fileObject),
            listRequestWithTrailingDelimiter(
                bucketName, fileObject + "/", /* maxResults= */ 1024, /* pageToken= */ null));

    assertThat(fileInfos).hasSize(1);
    FileInfo fileInfo = fileInfos.get(0);
    assertThat(fileInfo.exists()).isTrue();
    assertThat(fileInfo.getPath()).isEqualTo(fileObjectUri);
  }

  @Test
  public void listFileInfo_file_directoryPath() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String fileObject = getTestResource();
    URI bucketUri = new URI("gs://" + bucketName + "/");
    URI fileObjectUri = bucketUri.resolve(fileObject);

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, fileObject);

    assertThrows(
        FileNotFoundException.class, () -> gcsFs.listFileInfo(bucketUri.resolve(fileObject + "/")));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                bucketName, fileObject + "/", /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listFileInfo_directory() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/file1", dirObject + "/file2");

    List<FileInfo> fileInfos = gcsFs.listFileInfo(bucketUri.resolve(dirObject));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject),
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/", /* maxResults= */ 1024, /* pageToken= */ null));

    assertThat(fileInfos.stream().map(FileInfo::exists).collect(toList()))
        .containsExactly(true, true);
    assertThat(fileInfos.stream().map(FileInfo::getPath).collect(toList()))
        .containsExactly(
            bucketUri.resolve(dirObject + "/file1"), bucketUri.resolve(dirObject + "/file2"));
  }

  @Test
  public void listFileInfo_directory_directoryPath() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/file1", dirObject + "/file2");

    List<FileInfo> fileInfos = gcsFs.listFileInfo(bucketUri.resolve(dirObject + "/"));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/", /* maxResults= */ 1024, /* pageToken= */ null));

    assertThat(fileInfos.stream().map(FileInfo::exists).collect(toList()))
        .containsExactly(true, true);
    assertThat(fileInfos.stream().map(FileInfo::getPath).collect(toList()))
        .containsExactly(
            bucketUri.resolve(dirObject + "/file1"), bucketUri.resolve(dirObject + "/file2"));
  }

  @Test
  public void listFileInfo_implicitDirectory() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjects(bucketName, dirObject + "/subdir/file");

    List<FileInfo> fileInfos = gcsFs.listFileInfo(bucketUri.resolve(dirObject));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject),
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/", /* maxResults= */ 1024, /* pageToken= */ null));

    assertThat(fileInfos.stream().map(FileInfo::exists).collect(toList())).containsExactly(true);
    assertThat(fileInfos.stream().map(FileInfo::getPath).collect(toList()))
        .containsExactly(bucketUri.resolve(dirObject + "/subdir/"));
  }

  @Test
  public void listFileInfo_emptyDirectoryObject() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/");

    List<FileInfo> fileInfos = gcsFs.listFileInfo(bucketUri.resolve(dirObject));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject),
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/", /* maxResults= */ 1024, /* pageToken= */ null));

    assertThat(fileInfos).isEmpty();
  }

  @Test
  public void listFileInfo_notFound() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    assertThrows(
        FileNotFoundException.class, () -> gcsFs.listFileInfo(bucketUri.resolve(dirObject)));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject),
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/", /* maxResults= */ 1024, /* pageToken= */ null));
  }

  @Test
  public void listFileInfo_customFields_required() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

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

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject),
            listRequestWithTrailingDelimiter(
                bucketName,
                dirObject + "/",
                /* objectFields= */ "bucket,name",
                /* maxResults= */ 1024,
                /* pageToken= */ null));
  }

  @Test
  public void listFileInfo_customFields_some() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

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

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject),
            listRequestWithTrailingDelimiter(
                bucketName,
                dirObject + "/",
                /* objectFields= */ "bucket,name,contentType",
                /* maxResults= */ 1024,
                /* pageToken= */ null));
  }

  @Test
  public void listFileInfo_customFields_fails() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

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

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject),
            listRequestWithTrailingDelimiter(
                bucketName,
                dirObject + "/",
                /* objectFields= */ "bucket",
                /* maxResults= */ 1024,
                /* pageToken= */ null));
  }

  @Test
  public void delete_file_sequential() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions().setStatusParallelEnabled(false).build();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/f1");

    gcsFs.delete(bucketUri.resolve(dirObject + "/f1"), /* recursive= */ false);

    // This test performs additional POST request to dirObject when run singly, as result
    // generationId for delete operation need to be increased for DELETE request.
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject + "/f1"),
            getRequestString(bucketName, dirObject + "/"),
            deleteRequestString(bucketName, dirObject + "/f1", 1));

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f1"))).isFalse();
  }

  @Test
  public void delete_file_parallel() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions().setStatusParallelEnabled(true).build();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/f1");

    gcsFs.delete(bucketUri.resolve(dirObject + "/f1"), /* recursive= */ false);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject + "/"),
            getRequestString(bucketName, dirObject + "/f1"),
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/f1/", /* maxResults= */ 1, /* pageToken= */ null),
            deleteRequestString(bucketName, dirObject + "/f1", /* generationId= */ 1));

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

  private void delete_directory(boolean parallelStatus) throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions().setStatusParallelEnabled(parallelStatus).build();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/d1/");

    gcsFs.delete(bucketUri.resolve(dirObject + "/d1/"), /* recursive= */ false);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject + "/"),
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/d1/", /* maxResults= */ 1, /* pageToken= */ null),
            listRequestWithTrailingDelimiter(
                bucketName,
                dirObject + "/d1/",
                "bucket,name,generation",
                /* maxResults= */ 1024,
                /* pageToken= */ null),
            deleteRequestString(bucketName, dirObject + "/d1/", /* generationId= */ 1));

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/d1"))).isFalse();
  }

  @Test
  public void rename_file_sequential() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions().setStatusParallelEnabled(false).build();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/f1");

    gcsFs.rename(bucketUri.resolve(dirObject + "/f1"), bucketUri.resolve(dirObject + "/f2"));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject + "/f1"),
            getRequestString(bucketName, dirObject + "/f2"),
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/f2/", /* maxResults= */ 1, /* pageToken= */ null),
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null),
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/f2/", /* maxResults= */ 1, /* pageToken= */ null),
            getRequestString(bucketName, dirObject + "/"),
            copyRequestString(
                bucketName, dirObject + "/f1", bucketName, dirObject + "/f2", "copyTo"),
            deleteRequestString(bucketName, dirObject + "/f1", 1));

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f1"))).isFalse();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f2"))).isTrue();
  }

  @Test
  public void rename_file_parallel() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions().setStatusParallelEnabled(true).build();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/f1");

    gcsFs.rename(bucketUri.resolve(dirObject + "/f1"), bucketUri.resolve(dirObject + "/f2"));

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
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/f2/", /* maxResults= */ 1, /* pageToken= */ null),
            getRequestString(bucketName, dirObject + "/"),
            listRequestWithTrailingDelimiter(
                bucketName, dirObject + "/", /* maxResults= */ 1, /* pageToken= */ null),
            copyRequestString(
                bucketName, dirObject + "/f1", bucketName, dirObject + "/f2", "copyTo"),
            deleteRequestString(bucketName, dirObject + "/f1", /* generationId= */ 1));

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f1"))).isFalse();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f2"))).isTrue();
  }

  @Test
  public void rename_directory_sequential() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions().setStatusParallelEnabled(false).build();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/srcParent/srcDir/f", dirObject + "/dstParent/");

    gcsFs.rename(
        bucketUri.resolve(dirObject + "/srcParent/srcDir"),
        bucketUri.resolve(dirObject + "/dstParent/dstDir"));

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
                bucketName, dirObject + "/dstParent/", /* maxResults= */ 1, /* pageToken= */ null),
            // Check if src directory parent object exists
            getRequestString(bucketName, dirObject + "/srcParent/"),
            // Create dst directory
            listRequestWithTrailingDelimiter(
                bucketName,
                dirObject + "/dstParent/dstDir/",
                /* maxResults= */ 1,
                /* pageToken= */ null),
            uploadRequestString(
                bucketName, dirObject + "/dstParent/dstDir/", /* generationId= */ null),
            // List files that need to be renamed in src directory
            listRequestString(
                bucketName,
                /* flatList= */ true,
                /* includeTrailingDelimiter= */ null,
                dirObject + "/srcParent/srcDir/",
                "bucket,name,generation",
                /* maxResults= */ 1024,
                /* pageToken= */ null),
            // Copy file
            copyRequestString(
                bucketName,
                dirObject + "/srcParent/srcDir/f",
                bucketName,
                dirObject + "/dstParent/dstDir/f",
                "copyTo"),
            // Delete src directory and file
            batchRequestString(),
            deleteRequestString(bucketName, dirObject + "/srcParent/srcDir/f", 1),
            deleteRequestString(bucketName, dirObject + "/srcParent/srcDir/", 2));

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/srcParent/srcDir/f"))).isFalse();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/srcParent/srcDir"))).isFalse();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/srcParent"))).isTrue();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/dstParent/dstDir/f"))).isTrue();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/dstParent/dstDir"))).isTrue();
  }

  @Test
  public void rename_directory_parallel() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions().setStatusParallelEnabled(true).build();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/srcParent/srcDir/f", dirObject + "/dstParent/");

    gcsFs.rename(
        bucketUri.resolve(dirObject + "/srcParent/srcDir"),
        bucketUri.resolve(dirObject + "/dstParent/dstDir"));

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
                bucketName, dirObject + "/dstParent/", /* maxResults= */ 1, /* pageToken= */ null),
            // Check if src directory parent object exists
            getRequestString(bucketName, dirObject + "/srcParent/"),
            // Create dst directory
            listRequestWithTrailingDelimiter(
                bucketName,
                dirObject + "/dstParent/dstDir/",
                /* maxResults= */ 1,
                /* pageToken= */ null),
            uploadRequestString(
                bucketName, dirObject + "/dstParent/dstDir/", /* generationId= */ null),
            // List files that need to be renamed in src directory
            listRequestString(
                bucketName,
                /* flatList= */ true,
                /* includeTrailingDelimiter= */ null,
                dirObject + "/srcParent/srcDir/",
                "bucket,name,generation",
                /* maxResults= */ 1024,
                /* pageToken= */ null),
            // Copy file
            copyRequestString(
                bucketName,
                dirObject + "/srcParent/srcDir/f",
                bucketName,
                dirObject + "/dstParent/dstDir/f",
                "copyTo"),
            // Delete src directory and file
            batchRequestString(),
            deleteRequestString(bucketName, dirObject + "/srcParent/srcDir/f", 1),
            deleteRequestString(bucketName, dirObject + "/srcParent/srcDir/", 2));

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
    GoogleCloudStorageFileSystem gcsFs =
        newGcsFs(
            newGcsFsOptions().build(), new TrackingHttpRequestInitializer(httpRequestsInitializer));
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

  private static GoogleCloudStorageFileSystem newGcsFs(
      GoogleCloudStorageFileSystemOptions gcsfsOptions,
      TrackingHttpRequestInitializer gcsRequestsTracker)
      throws IOException {
    return new GoogleCloudStorageFileSystem(
        o -> new GoogleCloudStorageImpl(o, gcsRequestsTracker), gcsfsOptions);
  }
}
