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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.GOOGLEAPIS_ENDPOINT;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class TrackingHttpRequestInitializer implements HttpRequestInitializer {

  private static final String OBJECT_FIELDS =
      "bucket,name,timeCreated,updated,generation,metageneration,size,contentType,contentEncoding"
          + ",md5Hash,crc32c,metadata";

  private static final String GET_REQUEST_FORMAT =
      "GET:" + GOOGLEAPIS_ENDPOINT + "/storage/v1/b/%s/o/%s%s";

  private static final String GET_MEDIA_REQUEST_FORMAT =
      "GET:" + GOOGLEAPIS_ENDPOINT + "/download/storage/v1/b/%s/o/%s?alt=media";

  private static final String GET_BUCKET_REQUEST_FORMAT =
      "GET:" + GOOGLEAPIS_ENDPOINT + "/storage/v1/b/%s";

  private static final String POST_REQUEST_FORMAT =
      "POST:" + GOOGLEAPIS_ENDPOINT + "/storage/v1/b/%s/o/%s";

  private static final String POST_COPY_REQUEST_FORMAT =
      "POST:" + GOOGLEAPIS_ENDPOINT + "/storage/v1/b/%s/o/%s/%s/b/%s/o/%s";

  private static final String UPLOAD_REQUEST_FORMAT =
      "POST:"
          + GOOGLEAPIS_ENDPOINT
          + "/upload/storage/v1/b/%s/o?ifGenerationMatch=%s&uploadType=multipart:%s";

  private static final String RESUMABLE_UPLOAD_REQUEST_FORMAT =
      "POST:"
          + GOOGLEAPIS_ENDPOINT
          + "/upload/storage/v1/b/%s/o?ifGenerationMatch=%s&name=%s&uploadType=resumable";

  private static final String RESUMABLE_UPLOAD_CHUNK_REQUEST_FORMAT =
      "PUT:"
          + GOOGLEAPIS_ENDPOINT
          + "/upload/storage/v1/b/%s/o?%sname=%s&uploadType=resumable&upload_id=upload_%s";

  private static final String UPDATE_METADATA_REQUEST_FORMAT =
      "POST:" + GOOGLEAPIS_ENDPOINT + "/storage/v1/b/%s/o/%s?ifMetagenerationMatch=%d";

  private static final String DELETE_BUCKET_REQUEST_FORMAT =
      "DELETE:" + GOOGLEAPIS_ENDPOINT + "/storage/v1/b/%s";

  private static final String DELETE_META_REQUEST_FORMAT =
      "DELETE:" + GOOGLEAPIS_ENDPOINT + "/storage/v1/b/%s/o/%s?ifMetagenerationMatch=%d";

  private static final String DELETE_REQUEST_FORMAT =
      "DELETE:" + GOOGLEAPIS_ENDPOINT + "/storage/v1/b/%s/o/%s?ifGenerationMatch=%s";

  private static final String LIST_BUCKETS_REQUEST_FORMAT =
      "GET:" + GOOGLEAPIS_ENDPOINT + "/storage/v1/b?maxResults=1024&project=%s";

  private static final String LIST_REQUEST_FORMAT =
      "GET:"
          + GOOGLEAPIS_ENDPOINT
          + "/storage/v1/b/%s/o?%sfields=items(%s),prefixes,nextPageToken"
          + "%s&maxResults=%d%s";

  private static final String LIST_SIMPLE_REQUEST_FORMAT =
      "GET:" + GOOGLEAPIS_ENDPOINT + "/storage/v1/b/%s/o?maxResults=%d&prefix=%s";

  private static final String BATCH_REQUEST = "POST:" + GOOGLEAPIS_ENDPOINT + "/batch/storage/v1";

  private static final String COMPOSE_REQUEST_FORMAT =
      "POST:" + GOOGLEAPIS_ENDPOINT + "/storage/v1/b/%s/o/%s/compose";

  private static final String CREATE_BUCKET_REQUEST_FORMAT =
      "POST:" + GOOGLEAPIS_ENDPOINT + "/storage/v1/b?project=%s";

  private static final String PAGE_TOKEN_PARAM_PATTERN = "pageToken=[^&]+";

  private static final String REWRITE_TOKEN_PARAM_PATTERN = "rewriteToken=[^&]+";

  private static final String GENERATION_MATCH_TOKEN_PARAM_PATTERN = "ifGenerationMatch=[^&]+";

  private static final String UPLOAD_ID_PARAM_PATTERN = "upload_id=[^&]+";

  private final HttpRequestInitializer delegate;

  private final List<HttpRequest> requests = Collections.synchronizedList(new ArrayList<>());

  private final boolean replaceRequestParams;

  public TrackingHttpRequestInitializer() {
    this(/* delegate= */ null);
  }

  public TrackingHttpRequestInitializer(HttpRequestInitializer delegate) {
    this(delegate, /* replaceRequestParams= */ true);
  }

  public TrackingHttpRequestInitializer(boolean replaceRequestParams) {
    this(/* delegate= */ null, replaceRequestParams);
  }

  public TrackingHttpRequestInitializer(
      HttpRequestInitializer delegate, boolean replaceRequestParams) {
    this.delegate = delegate;
    this.replaceRequestParams = replaceRequestParams;
  }

  @Override
  public void initialize(HttpRequest request) throws IOException {
    if (delegate != null) {
      delegate.initialize(request);
    }
    HttpExecuteInterceptor executeInterceptor = request.getInterceptor();
    request.setInterceptor(
        r -> {
          if (executeInterceptor != null) {
            executeInterceptor.intercept(r);
          }
          requests.add(r);
        });
  }

  public ImmutableList<HttpRequest> getAllRequests() {
    return ImmutableList.copyOf(requests);
  }

  public ImmutableList<String> getAllRequestStrings() {
    AtomicLong pageTokenId = new AtomicLong();
    AtomicLong rewriteTokenId = new AtomicLong();
    AtomicLong generationMatchId = new AtomicLong();
    AtomicLong resumableUploadId = new AtomicLong();
    return requests.stream()
        .map(GoogleCloudStorageIntegrationHelper::requestToString)
        // Replace randomized pageToken with predictable value so it could be asserted in tests
        .map(r -> replacePageTokenWithId(r, pageTokenId))
        .map(r -> replaceRewriteTokenWithId(r, rewriteTokenId))
        .map(r -> replaceGenerationMatchWithId(r, generationMatchId))
        .map(r -> replaceResumableUploadIdWithId(r, resumableUploadId))
        .collect(toImmutableList());
  }

  public ImmutableList<String> getAllRawRequestStrings() {
    return requests.stream()
        .map(GoogleCloudStorageIntegrationHelper::requestToString)
        .collect(toImmutableList());
  }

  private String replacePageTokenWithId(String request, AtomicLong pageTokenId) {
    return replaceRequestParams
        ? replaceWithId(request, PAGE_TOKEN_PARAM_PATTERN, "pageToken=token_", pageTokenId)
        : request;
  }

  private String replaceRewriteTokenWithId(String request, AtomicLong rewriteTokenId) {
    return replaceRequestParams
        ? replaceWithId(request, REWRITE_TOKEN_PARAM_PATTERN, "rewriteToken=token_", rewriteTokenId)
        : request;
  }

  private String replaceGenerationMatchWithId(String request, AtomicLong generationId) {
    String idPrefix = "ifGenerationMatch=generationId_";
    return replaceRequestParams
        ? replaceWithId(request, GENERATION_MATCH_TOKEN_PARAM_PATTERN, idPrefix, generationId)
        : request;
  }

  private static String replaceResumableUploadIdWithId(String request, AtomicLong uploadId) {
    return replaceWithId(request, UPLOAD_ID_PARAM_PATTERN, "upload_id=upload_", uploadId);
  }

  private static String replaceWithId(
      String request, String pattern, String idPrefix, AtomicLong id) {
    long nextId = id.get() + 1;
    String replacedRequest = request.replaceAll(pattern, idPrefix + nextId);
    if (!request.equals(replacedRequest)) {
      id.incrementAndGet();
    }
    return replacedRequest;
  }

  public void reset() {
    requests.clear();
  }

  public static String getRequestString(String bucketName, String object) {
    return getRequestString(bucketName, object, OBJECT_FIELDS);
  }

  public static String getRequestString(String bucketName, String object, String fields) {
    String queryParameters = fields == null ? "" : "?fields=" + fields;
    return String.format(GET_REQUEST_FORMAT, bucketName, urlEncode(object), queryParameters);
  }

  public static String getMediaRequestString(String bucketName, String object) {
    return getMediaRequestString(bucketName, object, /* generationId= */ null);
  }

  public static String getMediaRequestString(String bucketName, String object, Long generationId) {
    return String.format(GET_MEDIA_REQUEST_FORMAT, bucketName, urlEncode(object))
        + (generationId == null ? "" : "&generation=" + generationId);
  }

  public static String getBucketRequestString(String bucketName) {
    return String.format(GET_BUCKET_REQUEST_FORMAT, bucketName);
  }

  public static String postRequestString(String bucketName, String object) {
    return String.format(POST_REQUEST_FORMAT, bucketName, urlEncode(object));
  }

  public static String rewriteRequestString(
      String srcBucket,
      String srcObject,
      String dstBucket,
      String dstObject,
      Integer maxBytesRewrittenPerCall,
      Integer rewriteTokenId) {
    String rewriteParams =
        (maxBytesRewrittenPerCall == null
                ? ""
                : "?maxBytesRewrittenPerCall=" + maxBytesRewrittenPerCall)
            + (rewriteTokenId == null ? "" : "&rewriteToken=token_" + rewriteTokenId);
    return copyRequestString(srcBucket, srcObject, dstBucket, dstObject, "rewriteTo")
        + rewriteParams;
  }

  public static String copyRequestString(
      String srcBucket, String srcObject, String dstBucket, String dstObject, String requestType) {
    return String.format(
        POST_COPY_REQUEST_FORMAT,
        srcBucket,
        urlEncode(srcObject),
        requestType,
        dstBucket,
        urlEncode(dstObject));
  }

  public static String uploadRequestString(String bucketName, String object, Integer generationId) {
    return uploadRequestString(bucketName, object, generationId, true);
  }

  public static String uploadRequestString(
      String bucketName, String object, Integer generationId, boolean replaceGenerationId) {
    String request =
        String.format(
            UPLOAD_REQUEST_FORMAT,
            bucketName,
            replaceGenerationId ? "generationId_" + generationId : generationId,
            object);
    return generationId == null ? request.replaceAll("ifGenerationMatch=[^&]+&", "") : request;
  }

  public static String resumableUploadRequestString(
      String bucketName, String object, Integer generationId, boolean replaceGenerationId) {
    String request =
        String.format(
            RESUMABLE_UPLOAD_REQUEST_FORMAT,
            bucketName,
            replaceGenerationId ? "generationId_" + generationId : generationId,
            object);
    return generationId == null ? request.replaceAll("ifGenerationMatch=[^&]+&", "") : request;
  }

  public static String resumableUploadChunkRequestString(
      String bucketName, String object, Integer uploadId) {
    return resumableUploadChunkRequestString(
        bucketName, object, /* generationId= */ null, uploadId);
  }

  public static String resumableUploadChunkRequestString(
      String bucketName, String object, Integer generationId, Integer uploadId) {
    final String generationIdParameter =
        generationId == null ? "" : "ifGenerationMatch=generationId_" + generationId + "&";
    return String.format(
        RESUMABLE_UPLOAD_CHUNK_REQUEST_FORMAT, bucketName, generationIdParameter, object, uploadId);
  }

  public static String updateMetadataRequestString(
      String bucketName, String object, int metaGenerationId) {
    return String.format(
        UPDATE_METADATA_REQUEST_FORMAT, bucketName, urlEncode(object), metaGenerationId);
  }

  public static String deleteBucketRequestString(String bucketName) {
    return String.format(DELETE_BUCKET_REQUEST_FORMAT, bucketName);
  }

  public static String deleteRequestString(String bucketName, String object, long generationId) {
    return deleteRequestString(bucketName, object, generationId, /* replaceGenerationId */ true);
  }

  public static String deleteRequestString(
      String bucketName, String object, long generationId, boolean replaceGenerationId) {
    return String.format(
        DELETE_REQUEST_FORMAT,
        bucketName,
        urlEncode(object),
        replaceGenerationId ? "generationId_" + generationId : generationId);
  }

  public static String deleteMatchMetaGenerationRequestString(
      String bucketName, String object, int metaGenerationId) {
    return String.format(
        DELETE_META_REQUEST_FORMAT, bucketName, urlEncode(object), metaGenerationId);
  }

  public static String batchRequestString() {
    return BATCH_REQUEST;
  }

  public static String composeRequestString(
      String bucketName, String object, Integer generationId) {
    return composeRequestString(bucketName, object, generationId, /* replaceGenerationId= */ true);
  }

  public static String composeRequestString(
      String bucketName, String object, Integer generationId, boolean replaceGenerationId) {
    String request = String.format(COMPOSE_REQUEST_FORMAT, bucketName, urlEncode(object));
    return generationId == null
        ? request
        : String.format(
            "%s?ifGenerationMatch=%s",
            request, replaceGenerationId ? "generationId_" + generationId : generationId);
  }

  public static String listBucketsRequestString(String projectId) {
    return String.format(LIST_BUCKETS_REQUEST_FORMAT, projectId);
  }

  public static String listRequestString(String bucket, String prefix, int maxResults) {
    return String.format(LIST_SIMPLE_REQUEST_FORMAT, bucket, maxResults, prefix);
  }

  public static String listRequestString(
      String bucket, String prefix, int maxResults, String pageToken) {
    return listRequestString(
        bucket,
        /* includeTrailingDelimiter= */ false,
        prefix,
        /* objectFields= */ OBJECT_FIELDS,
        maxResults,
        pageToken);
  }

  public static String listRequestWithTrailingDelimiter(
      String bucket, String prefix, int maxResults, String pageToken) {
    return listRequestWithTrailingDelimiter(
        bucket, prefix, /* objectFields= */ OBJECT_FIELDS, maxResults, pageToken);
  }

  public static String listRequestWithTrailingDelimiter(
      String bucket, String prefix, String objectFields, int maxResults, String pageToken) {
    return listRequestString(
        bucket, /* includeTrailingDelimiter= */ true, prefix, objectFields, maxResults, pageToken);
  }

  public static String listRequestString(
      String bucket,
      Boolean includeTrailingDelimiter,
      String prefix,
      int maxResults,
      String pageToken) {
    return listRequestString(
        bucket,
        includeTrailingDelimiter,
        prefix,
        /* objectFields= */ OBJECT_FIELDS,
        maxResults,
        pageToken);
  }

  public static String listRequestString(
      String bucket,
      Boolean includeTrailingDelimiter,
      String prefix,
      String objectFields,
      int maxResults,
      String pageToken) {
    return listRequestString(
        bucket,
        /* flatList= */ false,
        includeTrailingDelimiter,
        prefix,
        objectFields,
        maxResults,
        pageToken);
  }

  public static String listRequestString(
      String bucket,
      boolean flatList,
      Boolean includeTrailingDelimiter,
      String prefix,
      String objectFields,
      int maxResults,
      String pageToken) {
    String extraParams = pageToken == null ? "" : "&pageToken=" + pageToken;
    extraParams += prefix == null ? "" : "&prefix=" + prefix;
    return String.format(
        LIST_REQUEST_FORMAT,
        bucket,
        flatList ? "" : "delimiter=/&",
        objectFields,
        includeTrailingDelimiter == null
            ? ""
            : "&includeTrailingDelimiter=" + includeTrailingDelimiter,
        maxResults,
        extraParams);
  }

  public static String createBucketRequestString(String projectId) {
    return String.format(CREATE_BUCKET_REQUEST_FORMAT, projectId);
  }

  private static String urlEncode(String string) {
    try {
      return URLEncoder.encode(string, UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Failed to url encode string: " + string, e);
    }
  }
}
