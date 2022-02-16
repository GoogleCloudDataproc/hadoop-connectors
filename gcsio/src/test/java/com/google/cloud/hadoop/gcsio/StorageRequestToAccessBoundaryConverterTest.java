/*
 * Copyright 2021 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.storageapi.ObjectsGetMedia;
import com.google.cloud.hadoop.gcsio.storageapi.ObjectsGetMetadata;
import com.google.cloud.hadoop.gcsio.storageapi.StorageRequestFactory;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.AccessBoundary.Action;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for StorageRequestToAccessBoundaryConverter utility class. */
@RunWith(JUnit4.class)
public class StorageRequestToAccessBoundaryConverterTest {

  private static final String BUCKET_NAME = "test-bucket-name";
  private static final String OBJECT_NAME = "/dir/test-object";
  private static final String BUCKET_NAME_DST = "test-bucket-name-dst";
  private static final String OBJECT_NAME_DST = "/dir/test-object-dst";

  private Storage storage;

  @Before
  public void setup() {
    storage =
        new Storage(
            new NetHttpTransport(),
            GsonFactory.getDefaultInstance(),
            /* httpRequestInitializer= */ null);
  }

  @Test
  public void translateObjectListRequest() throws IOException {
    Storage.Objects.List request = storage.objects().list(BUCKET_NAME).setPrefix(OBJECT_NAME);

    List<AccessBoundary> results =
        StorageRequestToAccessBoundaryConverter.fromStorageObjectRequest(request);

    assertThat(results)
        .containsExactly(AccessBoundary.create(BUCKET_NAME, OBJECT_NAME, Action.LIST_OBJECTS));
  }

  @Test
  public void testTranslateObjectListRequestWithNullPrefix() throws IOException {
    Storage.Objects.List request = storage.objects().list(BUCKET_NAME);

    List<AccessBoundary> results =
        StorageRequestToAccessBoundaryConverter.fromStorageObjectRequest(request);

    assertThat(results)
        .containsExactly(AccessBoundary.create(BUCKET_NAME, "/", Action.LIST_OBJECTS));
  }

  @Test
  public void testTranslateObjectInsertRequest() throws IOException {
    StorageObject object = new StorageObject().setName(OBJECT_NAME);
    Storage.Objects.Insert request = storage.objects().insert(BUCKET_NAME, object);

    List<AccessBoundary> results =
        StorageRequestToAccessBoundaryConverter.fromStorageObjectRequest(request);

    assertThat(results)
        .containsExactly(AccessBoundary.create(BUCKET_NAME, OBJECT_NAME, Action.WRITE_OBJECTS));
  }

  @Test
  public void testTranslateObjectComposeRequest() throws IOException {
    List<ComposeRequest.SourceObjects> sources =
        Stream.of(0, 1, 2)
            .map(i -> new ComposeRequest.SourceObjects().setName(OBJECT_NAME + "-" + i))
            .collect(toImmutableList());
    ComposeRequest composeRequest = new ComposeRequest().setSourceObjects(sources);
    Storage.Objects.Compose request =
        storage.objects().compose(BUCKET_NAME, OBJECT_NAME, composeRequest);

    List<AccessBoundary> results =
        StorageRequestToAccessBoundaryConverter.fromStorageObjectRequest(request);

    assertThat(results)
        .containsExactly(
            AccessBoundary.create(BUCKET_NAME, OBJECT_NAME + "-" + 0, Action.READ_OBJECTS),
            AccessBoundary.create(BUCKET_NAME, OBJECT_NAME + "-" + 1, Action.READ_OBJECTS),
            AccessBoundary.create(BUCKET_NAME, OBJECT_NAME + "-" + 2, Action.READ_OBJECTS),
            AccessBoundary.create(BUCKET_NAME, OBJECT_NAME, Action.WRITE_OBJECTS));
  }

  @Test
  public void testTranslateObjectGetMediaRequest() {
    Storage.Objects.Get request =
        new StorageRequestFactory(storage).objectsGetMedia(BUCKET_NAME, OBJECT_NAME);

    List<AccessBoundary> results =
        StorageRequestToAccessBoundaryConverter.fromStorageObjectRequest(request);

    assertThat(results)
        .containsExactly(AccessBoundary.create(BUCKET_NAME, OBJECT_NAME, Action.READ_OBJECTS));
  }

  @Test
  public void testTranslateObjectGetMetadataRequest() {
    Storage.Objects.Get request =
        new StorageRequestFactory(storage).objectsGetMetadata(BUCKET_NAME, OBJECT_NAME);

    List<AccessBoundary> results =
        StorageRequestToAccessBoundaryConverter.fromStorageObjectRequest(request);

    assertThat(results)
        .containsExactly(
            AccessBoundary.create(BUCKET_NAME, OBJECT_NAME, Action.GET_OBJECT_METADATA));
  }

  /**
   * {@link Storage.Objects.Get} is disabled since it gives out permission too wide for getting
   * metadata. Use {@link ObjectsGetMedia} or {@link ObjectsGetMetadata} instead.
   *
   * @throws IOException
   */
  @Test
  public void testDisallowGetRequest() throws IOException {
    Storage.Objects.Get request = storage.objects().get(BUCKET_NAME, OBJECT_NAME);

    assertThrows(
        InvalidParameterException.class,
        () -> StorageRequestToAccessBoundaryConverter.fromStorageObjectRequest(request));
  }

  @Test
  public void testTranslateObjectDeleteRequest() throws IOException {
    Storage.Objects.Delete request = storage.objects().delete(BUCKET_NAME, OBJECT_NAME);
    List<AccessBoundary> results =
        StorageRequestToAccessBoundaryConverter.fromStorageObjectRequest(request);

    assertThat(results)
        .containsExactly(AccessBoundary.create(BUCKET_NAME, OBJECT_NAME, Action.DELETE_OBJECTS));
  }

  @Test
  public void testTranslateObjectRewriteRequest() throws IOException {
    StorageObject object = new StorageObject().setName(OBJECT_NAME);
    Storage.Objects.Rewrite request =
        storage
            .objects()
            .rewrite(BUCKET_NAME, OBJECT_NAME, BUCKET_NAME_DST, OBJECT_NAME_DST, object);

    List<AccessBoundary> results =
        StorageRequestToAccessBoundaryConverter.fromStorageObjectRequest(request);

    assertThat(results)
        .containsExactly(
            AccessBoundary.create(BUCKET_NAME, OBJECT_NAME, Action.READ_OBJECTS),
            AccessBoundary.create(BUCKET_NAME_DST, OBJECT_NAME_DST, Action.WRITE_OBJECTS));
  }

  @Test
  public void testTranslateObjectCopyRequest() throws IOException {
    StorageObject object = new StorageObject().setName(OBJECT_NAME);
    Storage.Objects.Copy request =
        storage.objects().copy(BUCKET_NAME, OBJECT_NAME, BUCKET_NAME_DST, OBJECT_NAME_DST, object);

    List<AccessBoundary> results =
        StorageRequestToAccessBoundaryConverter.fromStorageObjectRequest(request);

    assertThat(results)
        .containsExactly(
            AccessBoundary.create(BUCKET_NAME, OBJECT_NAME, Action.READ_OBJECTS),
            AccessBoundary.create(BUCKET_NAME_DST, OBJECT_NAME_DST, Action.WRITE_OBJECTS));
  }

  @Test
  public void testTranslateObjectPatchRequest() throws IOException {
    StorageObject object = new StorageObject().setName(OBJECT_NAME);
    Storage.Objects.Patch request = storage.objects().patch(BUCKET_NAME, OBJECT_NAME, object);

    List<AccessBoundary> results =
        StorageRequestToAccessBoundaryConverter.fromStorageObjectRequest(request);

    assertThat(results)
        .containsExactly(AccessBoundary.create(BUCKET_NAME, OBJECT_NAME, Action.EDIT_OBJECTS));
  }

  @Test
  public void testTranslateBucketGetRequest() throws IOException {
    Storage.Buckets.Get request = storage.buckets().get(BUCKET_NAME);

    List<AccessBoundary> results =
        StorageRequestToAccessBoundaryConverter.fromStorageObjectRequest(request);

    assertThat(results)
        .containsExactly(AccessBoundary.create(BUCKET_NAME, "/", Action.GET_BUCKETS));
  }

  @Test
  public void testTranslateBucketInsertRequest() throws IOException {
    Bucket bucket = new Bucket().setName(BUCKET_NAME);

    Storage.Buckets.Insert request = storage.buckets().insert("project", bucket);

    List<AccessBoundary> results =
        StorageRequestToAccessBoundaryConverter.fromStorageObjectRequest(request);

    assertThat(results)
        .containsExactly(AccessBoundary.create(BUCKET_NAME, "/", Action.CREATE_BUCKETS));
  }

  @Test
  public void testTranslateBucketDeleteRequest() throws IOException {
    Storage.Buckets.Delete request = storage.buckets().delete(BUCKET_NAME);

    List<AccessBoundary> results =
        StorageRequestToAccessBoundaryConverter.fromStorageObjectRequest(request);

    assertThat(results)
        .containsExactly(AccessBoundary.create(BUCKET_NAME, "/", Action.DELETE_BUCKETS));
  }

  @Test
  public void testTranslateBucketListRequest() throws IOException {
    Storage.Buckets.List request = storage.buckets().list("project");

    List<AccessBoundary> results =
        StorageRequestToAccessBoundaryConverter.fromStorageObjectRequest(request);

    assertThat(results).containsExactly(AccessBoundary.create("", "", Action.LIST_BUCKETS));
  }

  @Test
  public void translateNotRecognizedRequest() throws IOException {
    StorageObject object = new StorageObject().setName(OBJECT_NAME);
    Storage.Objects.Update request = storage.objects().update(BUCKET_NAME, OBJECT_NAME, object);

    InvalidParameterException actualException =
        assertThrows(
            InvalidParameterException.class,
            () -> StorageRequestToAccessBoundaryConverter.fromStorageObjectRequest(request));

    assertThat(actualException)
        .hasMessageThat()
        .isEqualTo(
            "StorageRequest not used by the connector:"
                + " com.google.api.services.storage.Storage.Objects.Update");
  }
}
