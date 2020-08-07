package com.google.cloud.hadoop.gcsio.authorization;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
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
  public static final JacksonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  private static final String PROJECT = "test-project";
  private static final String BUCKET_NAME = "test-bucket-name";
  private static final String OBJECT_NAME = "/dir/test-object";
  private static final String BUCKET_NAME_DST = "test-bucket-name-dst";
  private static final String OBJECT_NAME_DST = "/dir/test-object-dst";

  private HttpRequestInitializer trackingHttpRequestInitializer;

  private Storage gcs;

  @Before
  public void setUp() {
    trackingHttpRequestInitializer = null;
    gcs = mockedStorage();
  }

  @Mock AuthorizationHandler mockHandler;

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Test
  public void testAuthorizationHandlerIsNull() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new StorageRequestAuthorizer(null, ImmutableMap.of());
        });
  }

  @Test
  public void testInitiateAuthorizationHandler() throws ReflectiveOperationException {
    new StorageRequestAuthorizer(
        TestConfAuthorizationHandler.class,
        ImmutableMap.of(
            TestConfAuthorizationHandler.PROPERTY_KEY,
            TestConfAuthorizationHandler.EXPECTED_VALUE));
  }

  @Test
  public void testAuthorizationHandlerInstantiateException() {
    assertThrows(
        RuntimeException.class,
        () -> {
          new StorageRequestAuthorizer(
              TestInstantiationExceptionAuthorizationHandler.class, ImmutableMap.of());
        });
  }

  @Test
  public void testAuthorizationAllowed() throws Exception {
    // Any StorageRequest will do
    Storage.Objects.List list = gcs.objects().list(BUCKET_NAME);
    list.setPrefix(OBJECT_NAME);

    doNothing().when(mockHandler).handleListObject(any(URI.class));

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(list, BUCKET_NAME);

    // Nothing should happen when access allowed.
    verify(mockHandler, times(1)).handleListObject(any(URI.class));
  }

  @Test
  public void testAccessDenied() throws Exception {
    // Any StorageRequest will do
    Storage.Objects.List list = gcs.objects().list(BUCKET_NAME);
    list.setPrefix(OBJECT_NAME);

    doThrow(AccessDeniedException.class).when(mockHandler).handleListObject(any(URI.class));

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    assertThrows(
        AccessDeniedException.class,
        () -> {
          authorizer.authorize(list, BUCKET_NAME);
        });
  }

  @Test
  public void testListObject() throws Exception {
    Storage.Objects.List list = gcs.objects().list(BUCKET_NAME);
    list.setPrefix(OBJECT_NAME);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(list, BUCKET_NAME);

    final ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);
    verify(mockHandler).handleListObject(captor.capture());
    assertThat(captor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(captor.getValue().getPath()).isEqualTo(OBJECT_NAME);
  }

  @Test
  public void testInsertObject() throws Exception {
    StorageObject object = new StorageObject().setName(OBJECT_NAME);
    Storage.Objects.Insert insert = gcs.objects().insert(BUCKET_NAME, object);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(insert, BUCKET_NAME);

    final ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);
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
            .collect(Collectors.toList());
    ComposeRequest request = new ComposeRequest().setSourceObjects(sourceObjects);
    Storage.Objects.Compose compose = gcs.objects().compose(BUCKET_NAME, OBJECT_NAME, request);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(compose, BUCKET_NAME);

    final ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);
    final ArgumentCaptor<List<URI>> listCaptor = ArgumentCaptor.forClass(ArrayList.class);
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
    Storage.Objects.Get get = gcs.objects().get(BUCKET_NAME, OBJECT_NAME);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(get, BUCKET_NAME);

    final ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);
    verify(mockHandler).handleGetObject(captor.capture());
    assertThat(captor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(captor.getValue().getPath()).isEqualTo(OBJECT_NAME);
  }

  @Test
  public void testDeleteObject() throws Exception {
    Storage.Objects.Delete delete = gcs.objects().delete(BUCKET_NAME, OBJECT_NAME);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(delete, BUCKET_NAME);

    final ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);
    verify(mockHandler).handleDeleteObject(captor.capture());
    assertThat(captor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(captor.getValue().getPath()).isEqualTo(OBJECT_NAME);
  }

  @Test
  public void testRewriteObject() throws Exception {
    StorageObject object = new StorageObject().setName(OBJECT_NAME);
    Storage.Objects.Rewrite rewrite =
        gcs.objects().rewrite(BUCKET_NAME, OBJECT_NAME, BUCKET_NAME_DST, OBJECT_NAME_DST, object);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(rewrite, BUCKET_NAME);

    final ArgumentCaptor<URI> sourceCaptor = ArgumentCaptor.forClass(URI.class);
    final ArgumentCaptor<URI> destinationCaptor = ArgumentCaptor.forClass(URI.class);
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
        gcs.objects().copy(BUCKET_NAME, OBJECT_NAME, BUCKET_NAME_DST, OBJECT_NAME_DST, object);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(copy, BUCKET_NAME);

    final ArgumentCaptor<URI> sourceCaptor = ArgumentCaptor.forClass(URI.class);
    final ArgumentCaptor<URI> destinationCaptor = ArgumentCaptor.forClass(URI.class);
    verify(mockHandler).handleCopyObject(sourceCaptor.capture(), destinationCaptor.capture());
    assertThat(sourceCaptor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(sourceCaptor.getValue().getPath()).isEqualTo(OBJECT_NAME);
    assertThat(destinationCaptor.getValue().getAuthority()).isEqualTo(BUCKET_NAME_DST);
    assertThat(destinationCaptor.getValue().getPath()).isEqualTo(OBJECT_NAME_DST);
  }

  @Test
  public void testPatchObject() throws Exception {
    StorageObject object = new StorageObject().setName(OBJECT_NAME);
    Storage.Objects.Patch patch = gcs.objects().patch(BUCKET_NAME, OBJECT_NAME, object);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(patch, BUCKET_NAME);

    final ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);
    verify(mockHandler).handlePatchObject(captor.capture());
    assertThat(captor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(captor.getValue().getPath()).isEqualTo(OBJECT_NAME);
  }

  @Test
  public void testListBucket() throws Exception {
    Storage.Buckets.List list = gcs.buckets().list(PROJECT);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(list, BUCKET_NAME);

    verify(mockHandler, times(1)).handleListBucket();
  }

  @Test
  public void testInsertBucket() throws Exception {
    Bucket bucket = new Bucket().setName(BUCKET_NAME);

    Storage.Buckets.Insert insert = gcs.buckets().insert(BUCKET_NAME, bucket);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(insert, BUCKET_NAME);

    final ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);
    verify(mockHandler).handleInsertBucket(captor.capture());
    assertThat(captor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(captor.getValue().getPath()).isEqualTo("/");
  }

  @Test
  public void testGetBucket() throws Exception {
    Storage.Buckets.Get get = gcs.buckets().get(BUCKET_NAME);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(get, BUCKET_NAME);

    final ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);
    verify(mockHandler).handleGetBucket(captor.capture());
    assertThat(captor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(captor.getValue().getPath()).isEqualTo("/");
  }

  @Test
  public void testDeleteBucket() throws Exception {
    Storage.Buckets.Delete delete = gcs.buckets().delete(BUCKET_NAME);

    StorageRequestAuthorizer authorizer = new StorageRequestAuthorizer(mockHandler);

    authorizer.authorize(delete, BUCKET_NAME);

    final ArgumentCaptor<URI> captor = ArgumentCaptor.forClass(URI.class);
    verify(mockHandler).handleDeleteBucket(captor.capture());
    assertThat(captor.getValue().getAuthority()).isEqualTo(BUCKET_NAME);
    assertThat(captor.getValue().getPath()).isEqualTo("/");
  }

  @Test
  public void testCreateGCSURINullObject() {
    URI result = StorageRequestAuthorizer.createGCSURI("test-bucket", null);

    assertThat(result.toString()).isEqualTo("gs://test-bucket/");
  }

  @Test
  public void testCreateGCSURIRelativePath() {
    URI result = StorageRequestAuthorizer.createGCSURI("test-bucket", "path/to/obj");

    assertThat(result.toString()).isEqualTo("gs://test-bucket/path/to/obj");
  }

  @Test
  public void testCreateGCSURI() {
    URI result = StorageRequestAuthorizer.createGCSURI("test-bucket", "/path/to/obj");

    assertThat(result.toString()).isEqualTo("gs://test-bucket/path/to/obj");
  }

  private Storage mockedStorage() {
    return new Storage(HTTP_TRANSPORT, JSON_FACTORY, trackingHttpRequestInitializer);
  }

  /** Fake AuthorizationHandler to test setConf behavior. */
  public static class TestConfAuthorizationHandler implements AuthorizationHandler {
    public static String PROPERTY_KEY = "required.property";
    public static String EXPECTED_VALUE = "required-value";

    @Override
    public void setProperties(Map<String, String> property) {
      assertThat(property.get(PROPERTY_KEY)).isEqualTo(EXPECTED_VALUE);
    }

    @Override
    public void handleListObject(URI resource) throws AccessDeniedException {}

    @Override
    public void handleInsertObject(URI resource) throws AccessDeniedException {}

    @Override
    public void handleComposeObject(URI destinationResource, List<URI> sourceResources)
        throws AccessDeniedException {}

    @Override
    public void handleGetObject(URI resource) throws AccessDeniedException {}

    @Override
    public void handleDeleteObject(URI resource) throws AccessDeniedException {}

    @Override
    public void handleRewriteObject(URI sourceResource, URI destinationResource)
        throws AccessDeniedException {}

    @Override
    public void handleCopyObject(URI sourceResource, URI destinationResource)
        throws AccessDeniedException {}

    @Override
    public void handlePatchObject(URI resource) throws AccessDeniedException {}

    @Override
    public void handleListBucket() throws AccessDeniedException {}

    @Override
    public void handleInsertBucket(URI resource) throws AccessDeniedException {}

    @Override
    public void handleGetBucket(URI resource) throws AccessDeniedException {}

    @Override
    public void handleDeleteBucket(URI resource) throws AccessDeniedException {}
  }

  /** Fake AuthorizationHandler to test instantiate exception. */
  public static class TestInstantiationExceptionAuthorizationHandler
      implements AuthorizationHandler {
    // This class is missing a default constructor
    TestInstantiationExceptionAuthorizationHandler(String argument) {}

    @Override
    public void setProperties(Map<String, String> property) {}

    @Override
    public void handleListObject(URI resource) throws AccessDeniedException {}

    @Override
    public void handleInsertObject(URI resource) throws AccessDeniedException {}

    @Override
    public void handleComposeObject(URI destinationResource, List<URI> sourceResources)
        throws AccessDeniedException {}

    @Override
    public void handleGetObject(URI resource) throws AccessDeniedException {}

    @Override
    public void handleDeleteObject(URI resource) throws AccessDeniedException {}

    @Override
    public void handleRewriteObject(URI sourceResource, URI destinationResource)
        throws AccessDeniedException {}

    @Override
    public void handleCopyObject(URI sourceResource, URI destinationResource)
        throws AccessDeniedException {}

    @Override
    public void handlePatchObject(URI resource) throws AccessDeniedException {}

    @Override
    public void handleListBucket() throws AccessDeniedException {}

    @Override
    public void handleInsertBucket(URI resource) throws AccessDeniedException {}

    @Override
    public void handleGetBucket(URI resource) throws AccessDeniedException {}

    @Override
    public void handleDeleteBucket(URI resource) throws AccessDeniedException {}
  }
}
