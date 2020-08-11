package com.google.cloud.hadoop.gcsio.authorization;

import static com.google.common.truth.Truth.assertThat;

import java.net.URI;
import java.util.List;
import java.util.Map;

/** Fake {@link AuthorizationHandler} for testing */
public class FakeAuthorizationHandler implements AuthorizationHandler {
  public static String PROPERTY_KEY = "required.property";
  public static String EXPECTED_VALUE = "required-value";

  @Override
  public void setProperties(Map<String, String> properties) {
    assertThat(properties).containsExactly(PROPERTY_KEY, EXPECTED_VALUE);
  }

  @Override
  public void handleListObjects(URI resource) {}

  @Override
  public void handleInsertObject(URI resource) {}

  @Override
  public void handleComposeObject(URI destinationResource, List<URI> sourceResources) {}

  @Override
  public void handleGetObject(URI resource) {}

  @Override
  public void handleDeleteObject(URI resource) {}

  @Override
  public void handleRewriteObject(URI sourceResource, URI destinationResource) {}

  @Override
  public void handleCopyObject(URI sourceResource, URI destinationResource) {}

  @Override
  public void handlePatchObject(URI resource) {}

  @Override
  public void handleListBuckets(String project) {}

  @Override
  public void handleInsertBucket(String project, URI resource) {}

  @Override
  public void handleGetBucket(URI resource) {}

  @Override
  public void handleDeleteBucket(URI resource) {}
}
