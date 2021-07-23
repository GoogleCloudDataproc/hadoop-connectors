package com.google.cloud.hadoop.gcsio.authorization;

/**
 * The Authorization mode defines how to obtain the Access Token.
 * BASIC means the generated Access Token works for any GCS request.
 * REQUEST_CONTEXT_RELATED means the generated Access Token works for the objects/actions
 * specified in the request
 */
public enum AuthorizationMode {
  BASIC("BASIC"),
  REQUEST_CONTEXT_RELATED("REQUEST_CONTEXT_RELATED");

  private final String authorizationTypeName;

  AuthorizationMode(String name) {
    this.authorizationTypeName = name;
  }
}