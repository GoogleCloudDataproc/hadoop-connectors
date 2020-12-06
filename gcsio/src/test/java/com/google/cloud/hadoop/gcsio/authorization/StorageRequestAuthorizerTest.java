package com.google.cloud.hadoop.gcsio.authorization;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests to test StorageRequestAuthorizer class. */
@RunWith(JUnit4.class)
public class StorageRequestAuthorizerTest {

  public static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
  public static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

  private static final String PROJECT = "test-project";
  private static final String BUCKET_NAME = "test-bucket-name";
  private static final String OBJECT_NAME = "/dir/test-object";
  private static final String BUCKET_NAME_DST = "test-bucket-name-dst";
  private static final String OBJECT_NAME_DST = "/dir/test-object-dst";

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock AuthorizationHandler mockHandler;

  @Test
  public void testAuthorizationHandlerIsNull() {
    assertThrows(
        NullPointerException.class,
        () -> new StorageRequestAuthorizer(/* handlerClass= */ null, ImmutableMap.of()));
  }

  @Test
  public void testInitiateAuthorizationHandler() {
    new StorageRequestAuthorizer(
        FakeAuthorizationHandler.class,
        ImmutableMap.of(
            FakeAuthorizationHandler.PROPERTY_KEY, FakeAuthorizationHandler.EXPECTED_VALUE));
  }

  @Test
  public void testAuthorizationHandlerInstantiateException() {
    assertThrows(
        RuntimeException.class,
        () -> new StorageRequestAuthorizer(BrokenAuthorizationHandler.class, ImmutableMap.of()));
  }

  @Test
  public void testAuthorizationAllowed() throws Exception {
    // Any StorageRequest will do
    Storage.Objects.List list = newStorage().objects().list(BUCKET_NAME).setPrefix(OBJECT_NAME);

    doNothing().when(mockHandler).handleListObjects(any(URI.class));

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(list);

    // Nothing should happen when access allowed.
    verify(mockHandler).handleListObjects(any(URI.class));
  }

  @Test
  public void testAccessDenied() throws Exception {
    // Any StorageRequest will do
    Storage.Objects.List list = newStorage().objects().list(BUCKET_NAME).setPrefix(OBJECT_NAME);

    doThrow(AccessDeniedException.class).when(mockHandler).handleListObjects(any(URI.class));

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    assertThrows(AccessDeniedException.class, () -> authorizer.authorize(list));
  }

  @Test
  public void testListObject() throws Exception {
    Storage.Objects.List list = newStorage().objects().list(BUCKET_NAME).setPrefix(OBJECT_NAME);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(list);

    ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);

    verify(mockHandler).handleListObjects(captor.capture());

    assertThat(captor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(captor.getValue().getPath()).isEqualTo(OBJECT_NAME);
  }

  @Test
  public void testInsertObject() throws Exception {
    StorageObject object = new StorageObject().setName(OBJECT_NAME);
    Storage.Objects.Insert insert = newStorage().objects().insert(BUCKET_NAME, object);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(insert);

    ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);

    verify(mockHandler).handleInsertObject(captor.capture());

    assertThat(captor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(captor.getValue().getPath()).isEqualTo(OBJECT_NAME);
  }

  @Test
  public void testComposeObject() throws Exception {
    // Test with three source objects
    List<ComposeRequest.SourceObjects> sourceObjects =
        Stream.of(0, 1, 2)
            .map(x -> new ComposeRequest.SourceObjects().setName(OBJECT_NAME + "-" + x))
            .collect(toImmutableList());
    ComposeRequest request = new ComposeRequest().setSourceObjects(sourceObjects);
    Storage.Objects.Compose compose =
        newStorage().objects().compose(BUCKET_NAME, OBJECT_NAME, request);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(compose);

    ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);
    ArgumentCaptor<List<URI>> listCaptor = ArgumentCaptor.forClass(ArrayList.class);

    verify(mockHandler).handleComposeObject(captor.capture(), listCaptor.capture());

    assertThat(captor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(captor.getValue().getPath()).isEqualTo(OBJECT_NAME);
    for (int i = 0; i < 3; ++i) {
      assertThat(listCaptor.getValue().get(i).getAuthority()).isEqualTo(BUCKET_NAME);
      assertThat(listCaptor.getValue().get(i).getPath()).isEqualTo(OBJECT_NAME + "-" + i);
    }
  }

  @Test
  public void testGetObject() throws Exception {
    Storage.Objects.Get get = newStorage().objects().get(BUCKET_NAME, OBJECT_NAME);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(get);

    ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);

    verify(mockHandler).handleGetObject(captor.capture());

    assertThat(captor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(captor.getValue().getPath()).isEqualTo(OBJECT_NAME);
  }

  @Test
  public void testDeleteObject() throws Exception {
    Storage.Objects.Delete delete = newStorage().objects().delete(BUCKET_NAME, OBJECT_NAME);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(delete);

    ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);

    verify(mockHandler).handleDeleteObject(captor.capture());

    assertThat(captor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(captor.getValue().getPath()).isEqualTo(OBJECT_NAME);
  }

  @Test
  public void testRewriteObject() throws Exception {
    StorageObject object = new StorageObject().setName(OBJECT_NAME);
    Storage.Objects.Rewrite rewrite =
        newStorage()
            .objects()
            .rewrite(BUCKET_NAME, OBJECT_NAME, BUCKET_NAME_DST, OBJECT_NAME_DST, object);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(rewrite);

    ArgumentCaptor<URI> sourceCaptor = ArgumentCaptor.forClass(URI.class);
    ArgumentCaptor<URI> destinationCaptor = ArgumentCaptor.forClass(URI.class);

    verify(mockHandler).handleRewriteObject(sourceCaptor.capture(), destinationCaptor.capture());

    assertThat(sourceCaptor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(sourceCaptor.getValue().getPath()).isEqualTo(OBJECT_NAME);
    assertThat(destinationCaptor.getValue().getAuthority()).isEqualTo(BUCKET_NAME_DST);
    assertThat(destinationCaptor.getValue().getPath()).isEqualTo(OBJECT_NAME_DST);
  }

  @Test
  public void testCopyObject() throws Exception {
    StorageObject object = new StorageObject().setName(OBJECT_NAME);
    Storage.Objects.Copy copy =
        newStorage()
            .objects()
            .copy(BUCKET_NAME, OBJECT_NAME, BUCKET_NAME_DST, OBJECT_NAME_DST, object);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(copy);

    ArgumentCaptor<URI> sourceCaptor = ArgumentCaptor.forClass(URI.class);
    ArgumentCaptor<URI> destinationCaptor = ArgumentCaptor.forClass(URI.class);

    verify(mockHandler).handleCopyObject(sourceCaptor.capture(), destinationCaptor.capture());

    assertThat(sourceCaptor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(sourceCaptor.getValue().getPath()).isEqualTo(OBJECT_NAME);
    assertThat(destinationCaptor.getValue().getAuthority()).isEqualTo(BUCKET_NAME_DST);
    assertThat(destinationCaptor.getValue().getPath()).isEqualTo(OBJECT_NAME_DST);
  }

  @Test
  public void testPatchObject() throws Exception {
    StorageObject object = new StorageObject().setName(OBJECT_NAME);
    Storage.Objects.Patch patch = newStorage().objects().patch(BUCKET_NAME, OBJECT_NAME, object);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(patch);

    ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);

    verify(mockHandler).handlePatchObject(captor.capture());

    assertThat(captor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(captor.getValue().getPath()).isEqualTo(OBJECT_NAME);
  }

  @Test
  public void testListBucket() throws Exception {
    Storage.Buckets.List list = newStorage().buckets().list(PROJECT);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(list);

    verify(mockHandler).handleListBuckets(eq(PROJECT));
  }

  @Test
  public void testInsertBucket() throws Exception {
    Bucket bucket = new Bucket().setName(BUCKET_NAME);

    Storage.Buckets.Insert insert =
        newStorage().buckets().insert(BUCKET_NAME, bucket).setProject(PROJECT);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(insert);

    ArgumentCaptor<String> projectCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);

    verify(mockHandler).handleInsertBucket(projectCaptor.capture(), captor.capture());

    assertThat(projectCaptor.getValue()).isEqualTo(PROJECT);
    assertThat(captor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(captor.getValue().getPath()).isEqualTo("/");
  }

  @Test
  public void testGetBucket() throws Exception {
    Storage.Buckets.Get get = newStorage().buckets().get(BUCKET_NAME);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(get);

    ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);

    verify(mockHandler).handleGetBucket(captor.capture());

    assertThat(captor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(captor.getValue().getPath()).isEqualTo("/");
  }

  @Test
  public void testDeleteBucket() throws Exception {
    Storage.Buckets.Delete delete = newStorage().buckets().delete(BUCKET_NAME);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(delete);

    ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);

    verify(mockHandler).handleDeleteBucket(captor.capture());

    assertThat(captor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(captor.getValue().getPath()).isEqualTo("/");
  }

  private static Storage newStorage() {
    return new Storage(HTTP_TRANSPORT, JSON_FACTORY, /* httpRequestInitializer= */ null);
  }

  // Failing AuthorizationHandler to test instantiate exception. */
  public static class BrokenAuthorizationHandler extends FakeAuthorizationHandler {

    // Non default constructor presence of which fails AuthorizationHandler instantiation
    public BrokenAuthorizationHandler(Object ignore) {}
  }
}
