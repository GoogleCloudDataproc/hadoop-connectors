/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.http.GenericUrl;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LoggingUrlRedactorTest {

  @Test
  public void redactUrl_nullReturnsNullLiteral() {
    assertThat(LoggingUrlRedactor.redactUrl(null)).isEqualTo("null");
  }

  @Test
  public void redactUrl_preservesAllAllowlistedQueryParamsCaseInsensitively() {
    GenericUrl url =
        new GenericUrl(
            "https://storage.googleapis.com/storage/v1/b/my-bucket/o"
                + "?alt=media"
                + "&delimiter=/"
                + "&endOffset=z"
                + "&fields=items(name,size)"
                + "&generation=42"
                + "&ifGenerationMatch=0"
                + "&ifGenerationNotMatch=1"
                + "&ifMetaGenerationMatch=2"
                + "&ifMetaGenerationNotMatch=3"
                + "&ifSourceGenerationMatch=4"
                + "&ifSourceGenerationNotMatch=5"
                + "&ifSourceMetagenerationMatch=6"
                + "&ifSourceMetagenerationNotMatch=7"
                + "&includeFoldersAsPrefixes=true"
                + "&includeTrailingDelimiter=true"
                + "&matchGlob=*.parquet"
                + "&maxBytesRewrittenPerCall=1048576"
                + "&maxResults=100"
                + "&name=dir/obj"
                + "&pageToken=next-page"
                + "&prefix=dir/"
                + "&project=my-gcp-proj"
                + "&projection=noAcl"
                + "&softDeleted=false"
                + "&sourceGeneration=9"
                + "&startOffset=a"
                + "&uploadType=resumable"
                + "&userProject=my-proj"
                + "&versions=true");

    String redacted = LoggingUrlRedactor.redactUrl(url);

    assertThat(redacted).isEqualTo(url.build());
    assertThat(redacted).doesNotContain(LoggingUrlRedactor.REDACTED);
  }

  @Test
  public void redactUrl_redactsNonAllowlistedParamsWithoutMutatingOriginal() {
    String rawUrl =
        "https://storage.googleapis.com/upload/storage/v1/b/my-bucket/o"
            + "?uploadType=resumable"
            + "&name=path/file.txt"
            + "&upload_id=SECRET_UPLOAD_TOKEN"
            + "&X-Goog-Signature=SECRET_V4_SIG"
            + "&X-Goog-Credential=SECRET_CRED"
            + "&X-Goog-Security-Token=SECRET_SEC_TOK"
            + "&Signature=SECRET_V2_SIG"
            + "&GoogleAccessId=SECRET_ACCESS_ID"
            + "&access_token=SECRET_OAUTH_TOKEN"
            + "&oauth_token=SECRET_LEGACY_OAUTH"
            + "&key=SECRET_API_KEY"
            + "&customParam=SECRET_CUSTOM";
    GenericUrl url = new GenericUrl(rawUrl);
    String originalBuilt = url.build();

    String redacted = LoggingUrlRedactor.redactUrl(url);

    assertThat(redacted).contains("uploadType=resumable");
    assertThat(redacted).contains("name=path/file.txt");
    assertThat(redacted).contains("upload_id=REDACTED");
    assertThat(redacted).contains("X-Goog-Signature=REDACTED");
    assertThat(redacted).contains("X-Goog-Credential=REDACTED");
    assertThat(redacted).contains("X-Goog-Security-Token=REDACTED");
    assertThat(redacted).contains("Signature=REDACTED");
    assertThat(redacted).contains("GoogleAccessId=REDACTED");
    assertThat(redacted).contains("access_token=REDACTED");
    assertThat(redacted).contains("oauth_token=REDACTED");
    assertThat(redacted).contains("key=REDACTED");
    assertThat(redacted).contains("customParam=REDACTED");
    assertThat(redacted).doesNotContain("SECRET_");
    assertThat(url.build()).isEqualTo(originalBuilt);
    assertThat(url.getFirst("upload_id")).isEqualTo("SECRET_UPLOAD_TOKEN");
  }

  @Test
  public void redactUrl_stripsUserInfoWithoutMutatingOriginal() {
    GenericUrl url =
        new GenericUrl(
            "https://alice:topsecret@storage.googleapis.com/upload/storage/v1/b/bkt/o"
                + "?uploadType=resumable&upload_id=SECRET_ID");

    String redacted = LoggingUrlRedactor.redactUrl(url);

    assertThat(redacted)
        .isEqualTo(
            "https://storage.googleapis.com/upload/storage/v1/b/bkt/o"
                + "?uploadType=resumable&upload_id=REDACTED");
    assertThat(redacted).doesNotContain("alice");
    assertThat(redacted).doesNotContain("topsecret");
    assertThat(url.getUserInfo()).isEqualTo("alice:topsecret");
  }

  @Test
  public void redactUrl_handlesMultiValueSingleValueAndNullValuesSafely() {
    GenericUrl url = new GenericUrl("https://storage.googleapis.com/upload/storage/v1/b/bkt/o");
    url.set("upload_id", Arrays.asList("secret-1", "secret-2"));
    url.set("token", Arrays.asList("secret-3", null));
    url.set("api_key", "secret-4");
    url.put("null_param", null);
    url.set("uploadType", "resumable");

    String redacted = LoggingUrlRedactor.redactUrl(url);

    assertThat(redacted).contains("uploadType=resumable");
    assertThat(redacted).contains("upload_id=REDACTED&upload_id=REDACTED");
    assertThat(redacted).contains("token=REDACTED&token=REDACTED");
    assertThat(redacted).contains("api_key=REDACTED");
    assertThat(redacted).doesNotContain("secret-1");
    assertThat(redacted).doesNotContain("secret-2");
    assertThat(redacted).doesNotContain("secret-3");
    assertThat(redacted).doesNotContain("secret-4");
    assertThat(url.get("upload_id")).isEqualTo(Arrays.asList("secret-1", "secret-2"));
    assertThat(url.get("api_key")).isEqualTo("secret-4");
  }

  @Test
  public void redactUrl_handlesImmutableCollectionsAndStringBuilderValuesWithoutMutating() {
    GenericUrl url =
        new GenericUrl("https://alice:pass@storage.googleapis.com:8443/upload/b/bkt/o#frag");
    url.set("upload_id", List.of("secret-a", "secret-b"));
    url.set("unmod_token", Collections.unmodifiableList(Arrays.asList("secret-c")));
    url.set("empty_list", Collections.emptyList());
    url.set("prefix", new StringBuilder("my-dir/"));
    url.set("name", ImmutableList.of("obj"));
    url.setPathParts(List.of("", "upload", "b", "bkt", "o"));

    String redacted = LoggingUrlRedactor.redactUrl(url);

    assertThat(redacted)
        .isEqualTo(
            "https://storage.googleapis.com:8443/upload/b/bkt/o"
                + "?upload_id=REDACTED&upload_id=REDACTED"
                + "&unmod_token=REDACTED"
                + "&prefix=my-dir/&name=obj#REDACTED");
    assertThat(redacted).doesNotContain("alice");
    assertThat(redacted).doesNotContain("pass");
    assertThat(redacted).doesNotContain("secret-");
    assertThat(url.getUserInfo()).isEqualTo("alice:pass");
    assertThat(url.get("upload_id")).isEqualTo(List.of("secret-a", "secret-b"));
  }

  @Test
  public void redactUrl_handlesIncompleteUrlAndNullElementsWithoutThrowing() {
    GenericUrl url = new GenericUrl();
    url.setPathParts(Arrays.asList("", "upload", null, "o"));
    url.set("prefix", Arrays.asList("dir/", null));
    url.set("upload_id", "SECRET_INCOMPLETE");

    String redacted = LoggingUrlRedactor.redactUrl(url);

    assertThat(redacted).isEqualTo("/upload//o?prefix=dir/&upload_id=REDACTED");
    assertThat(redacted).doesNotContain("SECRET_INCOMPLETE");
  }

  @Test
  public void redactUrl_preservesVerbatimRawPathWithoutDoubleEncoding() {
    GenericUrl url =
        new GenericUrl(
            "https://storage.googleapis.com/storage/v1/b/bkt/o/dir%2Ffile%2B1"
                + "?uploadType=resumable&upload_id=SECRET_VERBATIM#raw-secret",
            /* verbatim= */ true);

    String redacted = LoggingUrlRedactor.redactUrl(url);

    assertThat(redacted)
        .isEqualTo(
            "https://storage.googleapis.com/storage/v1/b/bkt/o/dir%2Ffile%2B1"
                + "?uploadType=resumable&upload_id=REDACTED#REDACTED");
    assertThat(redacted).doesNotContain("%252F");
    assertThat(redacted).doesNotContain("SECRET_VERBATIM");
    assertThat(redacted).doesNotContain("raw-secret");
  }

  @Test
  public void redactUrl_replacesAnyFragmentWithRedactedWithoutMutatingOriginal() {
    GenericUrl commaSecretFragment =
        new GenericUrl(
            "https://storage.googleapis.com/upload/storage/v1/b/bkt/o"
                + "?uploadType=resumable#upload_id=SECRET_A,SECRET_B;oauth_token=SECRET_C");
    GenericUrl hashBeforeQuery =
        new GenericUrl(
            "https://storage.googleapis.com/upload/storage/v1/b/bkt/o/file"
                + "#1?uploadType=resumable&upload_id=SECRET_HASH_UPLOAD_ID");
    GenericUrl bareSecretFragment =
        new GenericUrl(
            "https://storage.googleapis.com/upload/storage/v1/b/bkt/o"
                + "?uploadType=resumable#SECRET_RAW_FRAGMENT_TOKEN");

    String redactedComma = LoggingUrlRedactor.redactUrl(commaSecretFragment);
    String redactedHash = LoggingUrlRedactor.redactUrl(hashBeforeQuery);
    String redactedBare = LoggingUrlRedactor.redactUrl(bareSecretFragment);

    assertThat(redactedComma)
        .isEqualTo(
            "https://storage.googleapis.com/upload/storage/v1/b/bkt/o"
                + "?uploadType=resumable#REDACTED");
    assertThat(redactedComma).doesNotContain("SECRET_");
    assertThat(commaSecretFragment.getFragment())
        .isEqualTo("upload_id=SECRET_A,SECRET_B;oauth_token=SECRET_C");

    assertThat(redactedHash)
        .isEqualTo("https://storage.googleapis.com/upload/storage/v1/b/bkt/o/file#REDACTED");
    assertThat(redactedHash).doesNotContain("SECRET_HASH_UPLOAD_ID");

    assertThat(redactedBare)
        .isEqualTo(
            "https://storage.googleapis.com/upload/storage/v1/b/bkt/o"
                + "?uploadType=resumable#REDACTED");
    assertThat(redactedBare).doesNotContain("SECRET_RAW_FRAGMENT_TOKEN");
  }

  @Test
  public void redactUrlString_redactsValidRelativeAndMalformedUrls() {
    assertThat(LoggingUrlRedactor.redactUrlString(null)).isEqualTo("null");
    assertThat(LoggingUrlRedactor.redactUrlString("")).isEmpty();

    String full =
        "https://storage.googleapis.com/upload/storage/v1/b/bkt/o/a+b"
            + "?uploadType=resumable&upload_id=SECRET_FULL_1";
    assertThat(LoggingUrlRedactor.redactUrlString(full))
        .isEqualTo(
            "https://storage.googleapis.com/upload/storage/v1/b/bkt/o/a%2Bb"
                + "?uploadType=resumable&upload_id=REDACTED");

    String relative = "/upload/storage/v1/b/bkt/o/a+b?upload_id=SECRET_REL#frag_secret";
    String redactedRelative = LoggingUrlRedactor.redactUrlString(relative);
    assertThat(redactedRelative).isEqualTo("/upload/storage/v1/b/bkt/o/a+b?REDACTED#REDACTED");
    assertThat(redactedRelative).doesNotContain("SECRET_REL");
    assertThat(redactedRelative).doesNotContain("frag_secret");

    String malformedWithUserInfo = "customscheme://alice:pw@host/path?upload_id=SECRET_BAD";
    String redactedMalformed = LoggingUrlRedactor.redactUrlString(malformedWithUserInfo);
    assertThat(redactedMalformed).isEqualTo("customscheme://host/path?REDACTED");
    assertThat(redactedMalformed).doesNotContain("alice");
    assertThat(redactedMalformed).doesNotContain("pw");
    assertThat(redactedMalformed).doesNotContain("SECRET_BAD");
  }

  @Test
  public void formatThrowableForLog_redactsUrlsAcrossCausalChain() {
    assertThat(LoggingUrlRedactor.formatThrowableForLog(null)).isEqualTo("null");

    IOException root =
        new IOException(
            "503 Service Unavailable\n"
                + "PUT https://storage.googleapis.com/upload/storage/v1/b/bkt/o"
                + "?uploadType=resumable&upload_id=SECRET_CAUSE_ID\n"
                + "backend error");
    IOException outer = new IOException("Upload failed", root);

    String summary = LoggingUrlRedactor.formatThrowableForLog(outer);

    assertThat(summary).contains("Upload failed");
    assertThat(summary).contains("503 Service Unavailable");
    assertThat(summary).contains("uploadType=resumable");
    assertThat(summary).contains("upload_id=REDACTED");
    assertThat(summary).doesNotContain("SECRET_CAUSE_ID");
  }

  @Test
  public void digestToken_handlesNullEmptyAndDeterministicHash() {
    assertThat(LoggingUrlRedactor.digestToken(null)).isEqualTo("null");
    assertThat(LoggingUrlRedactor.digestToken("")).isEmpty();

    String token = "AB2u-sample-upload-session-token";
    String digest1 = LoggingUrlRedactor.digestToken(token);
    String digest2 = LoggingUrlRedactor.digestToken(token);
    String otherDigest = LoggingUrlRedactor.digestToken(token + "-other");

    assertThat(digest1).matches("sha256:[0-9a-f]{16}");
    assertThat(digest1).isEqualTo(digest2);
    assertThat(digest1).isNotEqualTo(otherDigest);
    assertThat(digest1).doesNotContain(token);
  }
}
