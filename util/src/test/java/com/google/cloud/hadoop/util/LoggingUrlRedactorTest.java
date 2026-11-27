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

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.SocketTimeoutException;
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
                + "&contentEncoding=gzip"
                + "&delimiter=/"
                + "&destinationKmsKeyName=projects/p/locations/l/keyRings/r/cryptoKeys/k"
                + "&destinationPredefinedAcl=private"
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
                + "&kmsKeyName=projects/p/locations/l/keyRings/r/cryptoKeys/k"
                + "&matchGlob=*.parquet"
                + "&maxBytesRewrittenPerCall=1048576"
                + "&maxResults=100"
                + "&name=dir/obj"
                + "&pageToken=next-page"
                + "&predefinedAcl=private"
                + "&predefinedDefaultObjectAcl=projectPrivate"
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
            + "&customParam=SECRET_CUSTOM"
            + "&quotaUser=SECRET_QUOTA_USER";
    GenericUrl url = new GenericUrl(rawUrl);
    String originalBuilt = url.build();

    String redacted = LoggingUrlRedactor.redactUrl(url);

    assertThat(redacted).contains("uploadType=resumable");
    assertThat(redacted).contains("name=path/file.txt");
    assertThat(redacted)
        .contains("upload_id=" + LoggingUrlRedactor.digestToken("SECRET_UPLOAD_TOKEN"));
    assertThat(redacted).contains("X-Goog-Signature=REDACTED");
    assertThat(redacted).contains("X-Goog-Credential=REDACTED");
    assertThat(redacted).contains("X-Goog-Security-Token=REDACTED");
    assertThat(redacted).contains("Signature=REDACTED");
    assertThat(redacted).contains("GoogleAccessId=REDACTED");
    assertThat(redacted).contains("access_token=REDACTED");
    assertThat(redacted).contains("oauth_token=REDACTED");
    assertThat(redacted).contains("key=REDACTED");
    assertThat(redacted).contains("customParam=REDACTED");
    assertThat(redacted).contains("quotaUser=REDACTED");
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
                + "?uploadType=resumable&upload_id="
                + LoggingUrlRedactor.digestToken("SECRET_ID"));
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
    assertThat(redacted)
        .contains(
            "upload_id="
                + LoggingUrlRedactor.digestToken("secret-1")
                + "&upload_id="
                + LoggingUrlRedactor.digestToken("secret-2"));
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
                + "?upload_id="
                + LoggingUrlRedactor.digestToken("secret-a")
                + "&upload_id="
                + LoggingUrlRedactor.digestToken("secret-b")
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

    assertThat(redacted)
        .isEqualTo(
            "/upload//o?prefix=dir/&upload_id="
                + LoggingUrlRedactor.digestToken("SECRET_INCOMPLETE"));
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
                + "?uploadType=resumable&upload_id="
                + LoggingUrlRedactor.digestToken("SECRET_VERBATIM")
                + "#REDACTED");
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
                + "?uploadType=resumable&upload_id="
                + LoggingUrlRedactor.digestToken("SECRET_FULL_1"));

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
  public void redactUrlsInText_keepsParenthesesQuotesAndIpv6HostsInsideUrls() {
    String id = "SECRET_TEXT_UPLOAD_ID";
    String digest = LoggingUrlRedactor.digestToken(id);
    String base = "https://storage.googleapis.com/upload/storage/v1/b/bkt/o";

    assertThat(
            LoggingUrlRedactor.redactUrlsInText(
                "PUT " + base + "?name=report(1).csv&upload_id=" + id))
        .isEqualTo("PUT " + base + "?name=report(1).csv&upload_id=" + digest);
    assertThat(
            LoggingUrlRedactor.redactUrlsInText("PUT " + base + "?name=user's.csv&upload_id=" + id))
        .isEqualTo("PUT " + base + "?name=user's.csv&upload_id=" + digest);
    assertThat(LoggingUrlRedactor.redactUrlsInText("PUT http://[::1]:8080/upload?upload_id=" + id))
        .isEqualTo("PUT http://[::1]:8080/upload?upload_id=" + digest);
  }

  @Test
  public void redactUrlsInText_keepsWrappingPunctuationOutsideUrls() {
    String id = "SECRET_WRAPPED_TEXT_UPLOAD_ID";
    String digest = LoggingUrlRedactor.digestToken(id);

    assertThat(
            LoggingUrlRedactor.redactUrlsInText(
                "failed (see 'https://host.example/o?upload_id=" + id + "'), retrying."))
        .isEqualTo("failed (see 'https://host.example/o?upload_id=" + digest + "'), retrying.");
    assertThat(
            LoggingUrlRedactor.redactUrlsInText(
                "(https://host.example/o?name=a(1)&upload_id=" + id + ")"))
        .isEqualTo("(https://host.example/o?name=a(1)&upload_id=" + digest + ")");
    assertThat(LoggingUrlRedactor.redactUrlsInText("[https://host.example/o?upload_id=" + id + "]"))
        .isEqualTo("[https://host.example/o?upload_id=" + digest + "]");
    assertThat(LoggingUrlRedactor.redactUrlsInText("[http://[::1]:8080/o?upload_id=" + id + "]"))
        .isEqualTo("[http://[::1]:8080/o?upload_id=" + digest + "]");
  }

  @Test
  public void redactThrowable_keepsHttpErrorTypeStatusDetailsAndStackTrace() {
    String id = "SECRET_JSON_ERROR_UPLOAD_ID";
    String digest = LoggingUrlRedactor.digestToken(id);
    HttpHeaders headers = new HttpHeaders();
    GoogleJsonError details = new GoogleJsonError();
    details.setCode(503);
    GoogleJsonResponseException original =
        new GoogleJsonResponseException(
            new HttpResponseException.Builder(503, "Service Unavailable", headers)
                .setMessage(
                    "503 Service Unavailable\nPUT https://storage.googleapis.com/upload/storage/v1"
                        + "/b/bkt/o?uploadType=resumable&upload_id="
                        + id)
                .setContent("{\"error\":{\"code\":503}}")
                .setAttemptCount(2),
            details);
    original.addSuppressed(new IOException("retry of https://host.example/o?upload_id=" + id));

    Throwable redacted = LoggingUrlRedactor.redactThrowable(original);

    assertThat(redacted).isInstanceOf(GoogleJsonResponseException.class);
    GoogleJsonResponseException redactedJson = (GoogleJsonResponseException) redacted;
    assertThat(redactedJson.getStatusCode()).isEqualTo(503);
    assertThat(redactedJson.getStatusMessage()).isEqualTo("Service Unavailable");
    assertThat(redactedJson.getHeaders()).isSameInstanceAs(headers);
    assertThat(redactedJson.getDetails()).isSameInstanceAs(details);
    assertThat(redactedJson.getContent()).isEqualTo("{\"error\":{\"code\":503}}");
    assertThat(redactedJson.getAttemptCount()).isEqualTo(2);
    assertThat(redactedJson.getMessage()).contains("uploadType=resumable&upload_id=" + digest);
    assertThat(redactedJson.getStackTrace()).isEqualTo(original.getStackTrace());
    assertThat(redactedJson.getSuppressed()).hasLength(1);
    assertThat(Throwables.getStackTraceAsString(redacted)).doesNotContain(id);
    assertThat(original.getMessage()).contains(id);
    assertThat(LoggingUrlRedactor.redactThrowable(redacted)).isSameInstanceAs(redacted);

    HttpResponseException plainHttp =
        new HttpResponseException.Builder(410, "Gone", new HttpHeaders())
            .setMessage("410 Gone\nPUT https://host.example/o?upload_id=" + id)
            .build();
    Throwable redactedHttp = LoggingUrlRedactor.redactThrowable(plainHttp);
    assertThat(redactedHttp.getClass()).isEqualTo(HttpResponseException.class);
    assertThat(((HttpResponseException) redactedHttp).getStatusCode()).isEqualTo(410);
    assertThat(redactedHttp.getMessage())
        .isEqualTo("410 Gone\nPUT https://host.example/o?upload_id=" + digest);
  }

  @Test
  public void redactThrowable_replacesOtherTypesKeepingClassNameCauseAndStackTrace() {
    String id = "SECRET_WRAPPED_UPLOAD_ID";
    IOException cause = new IOException("PUT https://host.example/o?upload_id=" + id);
    IllegalStateException outer = new IllegalStateException("upload failed", cause);

    Throwable redacted = LoggingUrlRedactor.redactThrowable(outer);

    assertThat(redacted.toString()).isEqualTo("java.lang.IllegalStateException: upload failed");
    assertThat(redacted.getStackTrace()).isEqualTo(outer.getStackTrace());
    assertThat(redacted.getCause().toString())
        .isEqualTo(
            "java.io.IOException: PUT https://host.example/o?upload_id="
                + LoggingUrlRedactor.digestToken(id));
    assertThat(redacted.getCause().getStackTrace()).isEqualTo(cause.getStackTrace());
    assertThat(Throwables.getStackTraceAsString(redacted)).doesNotContain(id);
  }

  @Test
  public void redactThrowable_returnsSameInstanceWhenNothingNeedsRedaction() {
    assertThat(LoggingUrlRedactor.redactThrowable(null)).isNull();
    IOException plain = new IOException("Read timed out", new SocketTimeoutException("timeout"));
    assertThat(LoggingUrlRedactor.redactThrowable(plain)).isSameInstanceAs(plain);
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
    assertThat(LoggingUrlRedactor.digestToken(digest1)).isEqualTo(digest1);
  }
}
