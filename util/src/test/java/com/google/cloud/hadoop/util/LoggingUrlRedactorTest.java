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
                + "&fields=items(name,size)"
                + "&generation=42"
                + "&ifGenerationMatch=0"
                + "&ifGenerationNotMatch=1"
                + "&ifMetaGenerationMatch=2"
                + "&ifMetaGenerationNotMatch=3"
                + "&includeFoldersAsPrefixes=true"
                + "&includeTrailingDelimiter=true"
                + "&maxResults=100"
                + "&matchGlob=*.parquet"
                + "&name=dir/obj"
                + "&pageToken=next-page"
                + "&prefix=dir/"
                + "&projection=noAcl"
                + "&softDeleted=false"
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
  public void redactUrl_handlesNonPublicImmutableCollectionsWithoutMutating() {
    GenericUrl url =
        new GenericUrl("https://alice:pass@storage.googleapis.com:8443/upload/b/bkt/o#frag");
    url.set("upload_id", List.of("secret-a", "secret-b"));
    url.set("empty_list", Collections.emptyList());
    url.set("prefix", ImmutableList.of("dir/"));

    String redacted = LoggingUrlRedactor.redactUrl(url);

    assertThat(redacted)
        .isEqualTo(
            "https://storage.googleapis.com:8443/upload/b/bkt/o"
                + "?upload_id=REDACTED&upload_id=REDACTED&prefix=dir/#frag");
    assertThat(redacted).doesNotContain("alice");
    assertThat(redacted).doesNotContain("pass");
    assertThat(redacted).doesNotContain("secret-a");
    assertThat(redacted).doesNotContain("secret-b");
    assertThat(url.getUserInfo()).isEqualTo("alice:pass");
    assertThat(url.get("upload_id")).isEqualTo(List.of("secret-a", "secret-b"));
  }

  @Test
  public void redactUrl_redactsSensitiveKeyValuesInFragmentsWithoutMutatingOriginal() {
    GenericUrl tokenFragmentUrl =
        new GenericUrl(
            "https://storage.googleapis.com/upload/storage/v1/b/bkt/o"
                + "?uploadType=resumable#access_token=SECRET_FRAG_TOKEN&section=intro");
    GenericUrl hashBeforeQueryUrl =
        new GenericUrl(
            "https://storage.googleapis.com/upload/storage/v1/b/bkt/o/file"
                + "#1?uploadType=resumable&upload_id=SECRET_HASH_UPLOAD_ID");
    GenericUrl benignFragmentUrl =
        new GenericUrl(
            "https://storage.googleapis.com/upload/storage/v1/b/bkt/o?uploadType=resumable#frag");

    String redactedTokenFragment = LoggingUrlRedactor.redactUrl(tokenFragmentUrl);
    String redactedHashBeforeQuery = LoggingUrlRedactor.redactUrl(hashBeforeQueryUrl);
    String redactedBenignFragment = LoggingUrlRedactor.redactUrl(benignFragmentUrl);

    assertThat(redactedTokenFragment).contains("uploadType=resumable");
    assertThat(redactedTokenFragment).contains("#access_token=REDACTED&section=intro");
    assertThat(redactedTokenFragment).doesNotContain("SECRET_FRAG_TOKEN");
    assertThat(tokenFragmentUrl.getFragment())
        .isEqualTo("access_token=SECRET_FRAG_TOKEN&section=intro");

    assertThat(redactedHashBeforeQuery).contains("#1?uploadType=resumable&upload_id=REDACTED");
    assertThat(redactedHashBeforeQuery).doesNotContain("SECRET_HASH_UPLOAD_ID");
    assertThat(hashBeforeQueryUrl.getFragment())
        .isEqualTo("1?uploadType=resumable&upload_id=SECRET_HASH_UPLOAD_ID");

    assertThat(redactedBenignFragment)
        .isEqualTo(
            "https://storage.googleapis.com/upload/storage/v1/b/bkt/o?uploadType=resumable#frag");
  }

  @Test
  public void redactUrl_redactsSensitiveFragmentInNonPublicCollectionFallbackWithoutMutating() {
    GenericUrl url =
        new GenericUrl(
            "https://storage.googleapis.com/upload/storage/v1/b/bkt/o"
                + "#access_token=SECRET_FALLBACK_FRAG&upload_id=SECRET_FALLBACK_UPLOAD_ID");
    url.set("upload_id", List.of("secret-a"));

    String redacted = LoggingUrlRedactor.redactUrl(url);

    assertThat(redacted).contains("upload_id=REDACTED");
    assertThat(redacted).contains("#access_token=REDACTED&upload_id=REDACTED");
    assertThat(redacted).doesNotContain("SECRET_FALLBACK_");
    assertThat(redacted).doesNotContain("secret-a");
    assertThat(url.getFragment())
        .isEqualTo("access_token=SECRET_FALLBACK_FRAG&upload_id=SECRET_FALLBACK_UPLOAD_ID");
    assertThat(url.get("upload_id")).isEqualTo(List.of("secret-a"));
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
