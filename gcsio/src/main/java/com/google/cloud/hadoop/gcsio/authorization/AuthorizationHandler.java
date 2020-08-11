package com.google.cloud.hadoop.gcsio.authorization;

import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.List;
import java.util.Map;

/**
 * All custom authorization handler implementation should implement this interface.
 *
 * <p>The class is created by reflection. All customized AuthorizationHandler should implement this
 * interface and read arguments through hadoop's Configurable interface.
 */
public interface AuthorizationHandler {

  /**
   * This method is called after instantiation and before calling handle. All properties with the
   * prefix "fs.gs.authorization.handler.properties." will be set to the properties map.
   *
   * @param properties Immutable map of properties.
   */
  void setProperties(Map<String, String> properties);

  /**
   * Handles <a href=https://cloud.google.com/storage/docs/json_api/v1/objects/list>list object
   * request</a>.
   *
   * @param resource A GCS object URI.
   * @throws AccessDeniedException Thrown when access denied.
   */
  void handleListObjects(URI resource) throws AccessDeniedException;

  /**
   * Handles <a href=https://cloud.google.com/storage/docs/json_api/v1/objects/insert>insert object
   * request</a>.
   *
   * @param resource A GCS object URI.
   * @throws AccessDeniedException Thrown when access denied.
   */
  void handleInsertObject(URI resource) throws AccessDeniedException;

  /**
   * Handles <a href=https://cloud.google.com/storage/docs/json_api/v1/objects/compose>compose
   * object request</a>.
   *
   * @param destinationResource URI of destination GCS object.
   * @param sourceResources A list of source GCS objects' URI.
   * @throws AccessDeniedException Thrown when access denied.
   */
  void handleComposeObject(URI destinationResource, List<URI> sourceResources)
      throws AccessDeniedException;

  /**
   * Handles <a href=https://cloud.google.com/storage/docs/json_api/v1/objects/get>get object
   * request</a>.
   *
   * @param resource A GCS object URI.
   * @throws AccessDeniedException Thrown when access denied.
   */
  void handleGetObject(URI resource) throws AccessDeniedException;

  /**
   * Handles <a href=https://cloud.google.com/storage/docs/json_api/v1/objects/delete>delete object
   * request</a>.
   *
   * @param resource A GCS object URI.
   * @throws AccessDeniedException Thrown when access denied.
   */
  void handleDeleteObject(URI resource) throws AccessDeniedException;

  /**
   * Handles <a href=https://cloud.google.com/storage/docs/json_api/v1/objects/rewrite>rewrite
   * object request</a>.
   *
   * @param sourceResource The source GCS object URI.
   * @param destinationResource The destination GCS object URI.
   * @throws AccessDeniedException Thrown when access denied.
   */
  void handleRewriteObject(URI sourceResource, URI destinationResource)
      throws AccessDeniedException;

  /**
   * Handles <a href=https://cloud.google.com/storage/docs/json_api/v1/objects/copy>copy object
   * request</a>.
   *
   * @param sourceResource The source GCS object URI.
   * @param destinationResource The destination GCS object URI.
   * @throws AccessDeniedException Thrown when access denied.
   */
  void handleCopyObject(URI sourceResource, URI destinationResource) throws AccessDeniedException;

  /**
   * Handles <a href=https://cloud.google.com/storage/docs/json_api/v1/objects/patch>patch object
   * request</a>.
   *
   * @param resource A GCS object URI.
   * @throws AccessDeniedException Thrown when access denied.
   */
  void handlePatchObject(URI resource) throws AccessDeniedException;

  /**
   * Handles <a href=https://cloud.google.com/storage/docs/json_api/v1/buckets/list>list bucket
   * request</a>.
   *
   * @param project A GCP project ID in which buckets are listed.
   * @throws AccessDeniedException Thrown when access denied.
   */
  void handleListBuckets(String project) throws AccessDeniedException;

  /**
   * Handles <a href=https://cloud.google.com/storage/docs/json_api/v1/buckets/insert>insert bucket
   * request</a>.
   *
   * @param project A GCP project ID where bucket created.
   * @param resource A GCS bucket URI.
   * @throws AccessDeniedException Thrown when access denied.
   */
  void handleInsertBucket(String project, URI resource) throws AccessDeniedException;

  /**
   * Handles <a href=https://cloud.google.com/storage/docs/json_api/v1/buckets/get>get bucket
   * request</a>.
   *
   * @param resource A GCS bucket URI.
   * @throws AccessDeniedException Thrown when access denied.
   */
  void handleGetBucket(URI resource) throws AccessDeniedException;

  /**
   * Handles <a href=https://cloud.google.com/storage/docs/json_api/v1/buckets/delete>delete bucket
   * request</a>.
   *
   * @param resource A GCS bucket URI.
   * @throws AccessDeniedException Thrown when access denied.
   */
  void handleDeleteBucket(URI resource) throws AccessDeniedException;
}
