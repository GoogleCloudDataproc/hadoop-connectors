package com.google.cloud.hadoop.gcsio.authorization;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableSet;
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
  public String handleListObjects(URI resource) {
    return "";
  }

  @Override
  public String handleInsertObject(URI resource)  {
    return "";
  }

  @Override
  public String handleComposeObject(URI destinationResource, List<URI> sourceResources)  {
    return "";
  }

  @Override
  public String handleGetObject(URI resource)  {
    return "";
  }

  @Override
  public String handleDeleteObject(URI resource)  {
    return "";
  }

  @Override
  public String handleRewriteObject(URI sourceResource, URI destinationResource)  {
    return "";
  }

  @Override
  public String handleCopyObject(URI sourceResource, URI destinationResource)  {
    return "";
  }

  @Override
  public String handlePatchObject(URI resource)  {
    return "";
  }

  @Override
  public String handleListBuckets(String project)  {
    return "";
  }

  @Override
  public String handleInsertBucket(String project, URI resource)  {
    return "";
  }

  @Override
  public String handleGetBucket(URI resource)  {
    return "";
  }

  @Override
  public String handleDeleteBucket(URI resource)  {
    return "";
  }

  @Override
  public AuthorizationRequest getAuthorizationRequestByAccessToken(String accessToken) {
    return new AuthorizationRequest( "test-bucket", "test-object", "test-user", ImmutableSet.of("test-group"));
  }
}
