package com.google.cloud.hadoop.gcsio.authorization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.StorageObject;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AuthorizationStorageObjectRequestAdapterTest {

  private static final String BUCKET_NAME = "test-bucket-name";
  private static final String OBJECT_NAME = "/dir/test-object";
  private static final String BUCKET_NAME_DST = "test-bucket-name-dst";
  private static final String OBJECT_NAME_DST = "/dir/test-object-dst";

  private Storage storage;

  @Before
  public void setup() {
    storage = new Storage(new NetHttpTransport(), JacksonFactory.getDefaultInstance(), null);
  }

  @Test
  public void testTranslateObjectListRequest() throws IOException {
    Storage.Objects.List request = storage.objects().list(BUCKET_NAME).setPrefix(OBJECT_NAME);
    List<GcsResourceAndActions> results = AuthorizationStorageObjectRequestAdapter
        .fromStorageObjectRequest(request);

    assertEquals(1, results.size());
    GcsResourceAndActions resourceAndActions = results.get(0);

    assertEquals(BUCKET_NAME, resourceAndActions.getBucket());
    assertEquals(OBJECT_NAME, resourceAndActions.getObjectPath());
    assertEquals("read", resourceAndActions.getAction());
  }

  @Test
  public void testTranslateObjectInsertRequest() throws IOException {
    StorageObject object = new StorageObject().setName(OBJECT_NAME);
    Storage.Objects.Insert request = storage.objects().insert(BUCKET_NAME, object);
    List<GcsResourceAndActions> results = AuthorizationStorageObjectRequestAdapter
        .fromStorageObjectRequest(request);

    assertEquals(1, results.size());
    GcsResourceAndActions resourceAndActions = results.get(0);

    assertEquals(BUCKET_NAME, resourceAndActions.getBucket());
    assertEquals(OBJECT_NAME, resourceAndActions.getObjectPath());
    assertEquals("write", resourceAndActions.getAction());
  }

  @Test
  public void testTranslateObjectComposeRequest() throws IOException {
    List<ComposeRequest.SourceObjects> sources = Stream.of(0, 1, 2)
        .map(i -> new ComposeRequest.SourceObjects().setName(OBJECT_NAME + "-" + i)).collect(
            Collectors.toList());
    ComposeRequest composeRequest = new ComposeRequest().setSourceObjects(sources);
    Storage.Objects.Compose request = storage.objects()
        .compose(BUCKET_NAME, OBJECT_NAME, composeRequest);

    List<GcsResourceAndActions> results = AuthorizationStorageObjectRequestAdapter
        .fromStorageObjectRequest(request);

    assertEquals(4, results.size());
    Stream.of(0, 1, 2).forEach(
        i -> assertTrue(results
            .contains(new GcsResourceAndActions(BUCKET_NAME, OBJECT_NAME + "-" + i, "read"))));
    assertTrue(results.contains(new GcsResourceAndActions(BUCKET_NAME, OBJECT_NAME, "write")));
  }

  @Test
  public void testTranslateObjectGetRequest() throws IOException {
    Storage.Objects.Get request = storage.objects().get(BUCKET_NAME, OBJECT_NAME);
    List<GcsResourceAndActions> results = AuthorizationStorageObjectRequestAdapter
        .fromStorageObjectRequest(request);

    assertEquals(1, results.size());
    GcsResourceAndActions resourceAndActions = results.get(0);

    assertEquals(BUCKET_NAME, resourceAndActions.getBucket());
    assertEquals(OBJECT_NAME, resourceAndActions.getObjectPath());
    assertEquals("read", resourceAndActions.getAction());
  }

  @Test
  public void testTranslateObjectDeleteRequest() throws IOException {
    Storage.Objects.Delete request = storage.objects().delete(BUCKET_NAME, OBJECT_NAME);
    List<GcsResourceAndActions> results = AuthorizationStorageObjectRequestAdapter
        .fromStorageObjectRequest(request);

    assertEquals(1, results.size());
    GcsResourceAndActions resourceAndActions = results.get(0);

    assertEquals(BUCKET_NAME, resourceAndActions.getBucket());
    assertEquals(OBJECT_NAME, resourceAndActions.getObjectPath());
    assertEquals("write", resourceAndActions.getAction());
  }

  @Test
  public void testTranslateObjectRewriteRequest() throws IOException {
    StorageObject object = new StorageObject().setName(OBJECT_NAME);
    Storage.Objects.Rewrite request = storage.objects()
        .rewrite(BUCKET_NAME, OBJECT_NAME, BUCKET_NAME_DST, OBJECT_NAME_DST, object);

    List<GcsResourceAndActions> results = AuthorizationStorageObjectRequestAdapter
        .fromStorageObjectRequest(request);

    assertEquals(2, results.size());
    assertTrue(results.contains(new GcsResourceAndActions(BUCKET_NAME, OBJECT_NAME, "read")));
    assertTrue(
        results.contains(new GcsResourceAndActions(BUCKET_NAME_DST, OBJECT_NAME_DST, "write")));
  }

  @Test
  public void testTranslateObjectCopyRequest() throws IOException {
    StorageObject object = new StorageObject().setName(OBJECT_NAME);
    Storage.Objects.Copy request = storage.objects()
        .copy(BUCKET_NAME, OBJECT_NAME, BUCKET_NAME_DST, OBJECT_NAME_DST, object);

    List<GcsResourceAndActions> results = AuthorizationStorageObjectRequestAdapter
        .fromStorageObjectRequest(request);

    assertEquals(2, results.size());
    assertTrue(results.contains(new GcsResourceAndActions(BUCKET_NAME, OBJECT_NAME, "read")));
    assertTrue(
        results.contains(new GcsResourceAndActions(BUCKET_NAME_DST, OBJECT_NAME_DST, "write")));
  }

  @Test
  public void testTranslateObjectPatchRequest() throws IOException {
    StorageObject object = new StorageObject().setName(OBJECT_NAME);
    Storage.Objects.Patch request = storage.objects().patch(BUCKET_NAME, OBJECT_NAME, object);

    List<GcsResourceAndActions> results = AuthorizationStorageObjectRequestAdapter
        .fromStorageObjectRequest(request);

    assertEquals(1, results.size());
    GcsResourceAndActions resourceAndActions = results.get(0);

    assertEquals(BUCKET_NAME, resourceAndActions.getBucket());
    assertEquals(OBJECT_NAME, resourceAndActions.getObjectPath());
    assertEquals("write", resourceAndActions.getAction());
  }

  @Test
  public void testTranslateBucketGetRequest() throws IOException {
    Storage.Buckets.Get request = storage.buckets().get(BUCKET_NAME);

    List<GcsResourceAndActions> results = AuthorizationStorageObjectRequestAdapter
        .fromStorageObjectRequest(request);

    assertEquals(1, results.size());
    GcsResourceAndActions resourceAndActions = results.get(0);

    assertEquals(BUCKET_NAME, resourceAndActions.getBucket());
    assertEquals("/", resourceAndActions.getObjectPath());
    assertEquals("read", resourceAndActions.getAction());
  }

  @Test
  public void testTranslateBucketInsertRequest() throws IOException {
    Bucket bucket = new Bucket().setName(BUCKET_NAME);

    Storage.Buckets.Insert request = storage.buckets().insert("project", bucket);

    List<GcsResourceAndActions> results = AuthorizationStorageObjectRequestAdapter
        .fromStorageObjectRequest(request);

    assertEquals(1, results.size());
    GcsResourceAndActions resourceAndActions = results.get(0);

    assertEquals(BUCKET_NAME, resourceAndActions.getBucket());
    assertEquals("/", resourceAndActions.getObjectPath());
    assertEquals("write", resourceAndActions.getAction());
  }

  @Test
  public void testTranslateBucketDeleteRequest() throws IOException {
    Storage.Buckets.Delete request = storage.buckets().delete(BUCKET_NAME);

    List<GcsResourceAndActions> results = AuthorizationStorageObjectRequestAdapter
        .fromStorageObjectRequest(request);

    assertEquals(1, results.size());
    GcsResourceAndActions resourceAndActions = results.get(0);

    assertEquals(BUCKET_NAME, resourceAndActions.getBucket());
    assertEquals("/", resourceAndActions.getObjectPath());
    assertEquals("write", resourceAndActions.getAction());
  }

  @Test
  public void testTranslateBucketListRequest() throws IOException {
    Storage.Buckets.List request = storage.buckets().list("project");

    List<GcsResourceAndActions> results = AuthorizationStorageObjectRequestAdapter
        .fromStorageObjectRequest(request);

    assertEquals(1, results.size());
    GcsResourceAndActions resourceAndActions = results.get(0);

    assertEquals("", resourceAndActions.getBucket());
    assertEquals("", resourceAndActions.getObjectPath());
    assertEquals("", resourceAndActions.getAction());
  }

  @Test
  public void testTranslateNotRecognizedRequest() throws IOException {
    StorageObject object = new StorageObject().setName(OBJECT_NAME);
    Storage.Objects.Update request = storage.objects().update(BUCKET_NAME, OBJECT_NAME, object);

    try {
      AuthorizationStorageObjectRequestAdapter.fromStorageObjectRequest(request);
      fail();
    } catch (InvalidParameterException e) {
      assertEquals("StorageRequest not used by the connector: Update", e.getMessage());
    }
  }
}
