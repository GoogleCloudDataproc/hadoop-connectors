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

import com.google.api.client.http.GenericUrl;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Utility for redacting sensitive components from URLs, query parameters, and tokens prior to
 * logging without mutating caller objects.
 */
public final class LoggingUrlRedactor {

  public static final String REDACTED = "REDACTED";

  private static final ImmutableSet<String> SAFE_QUERY_PARAMS =
      ImmutableSet.of(
          "alt",
          "delimiter",
          "fields",
          "generation",
          "ifgenerationmatch",
          "ifgenerationnotmatch",
          "ifmetagenerationmatch",
          "ifmetagenerationnotmatch",
          "includefoldersasprefixes",
          "includetrailingdelimiter",
          "maxresults",
          "matchglob",
          "name",
          "pagetoken",
          "prefix",
          "projection",
          "softdeleted",
          "uploadtype",
          "userproject",
          "versions");

  private static final Pattern SENSITIVE_QUERY_PARAM_PATTERN =
      Pattern.compile(
          "(?<![A-Za-z0-9_-])"
              + "(upload_id|X-Goog-Signature|X-Goog-Credential|X-Goog-Security-Token"
              + "|Signature|GoogleAccessId|access_token|key)"
              + "=([^&\\s'\"<>()\\[\\]{},;#]+)",
          Pattern.CASE_INSENSITIVE);

  private LoggingUrlRedactor() {}

  /**
   * Returns a log-safe string representation of {@code url} with user info removed, all
   * non-allowlisted query parameter values replaced with {@link #REDACTED}, and sensitive key-value
   * pairs in fragments redacted. Does not mutate {@code url}.
   */
  public static String redactUrl(@Nullable GenericUrl url) {
    if (url == null) {
      return "null";
    }
    GenericUrl sanitized;
    try {
      sanitized = url.clone();
      sanitized.setUserInfo(null);
      if (sanitized.getFragment() != null) {
        sanitized.setFragment(redactSensitiveKeyValues(sanitized.getFragment()));
      }
      for (Map.Entry<String, Object> entry : new ArrayList<>(sanitized.entrySet())) {
        String key = entry.getKey();
        if (key == null || isSafeQueryParam(key)) {
          continue;
        }
        Object value = entry.getValue();
        if (value == null) {
          continue;
        }
        sanitized.set(key, redactValue(value));
      }
    } catch (IllegalArgumentException e) {
      // GenericUrl.clone() uses reflective Data.clone(), which rejects non-public Collection
      // implementations such as List.of(), ImmutableList, and Collections.emptyList().
      sanitized = copyAndRedactWithoutClone(url);
    }
    return sanitized.build();
  }

  private static GenericUrl copyAndRedactWithoutClone(GenericUrl url) {
    GenericUrl copy = new GenericUrl();
    if (url.getScheme() != null) {
      copy.setScheme(url.getScheme());
    }
    if (url.getHost() != null) {
      copy.setHost(url.getHost());
    }
    copy.setPort(url.getPort());
    if (url.getPathParts() != null) {
      copy.setPathParts(new ArrayList<>(url.getPathParts()));
    }
    if (url.getFragment() != null) {
      copy.setFragment(redactSensitiveKeyValues(url.getFragment()));
    }
    for (Map.Entry<String, Object> entry : url.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      if (key == null || value == null) {
        continue;
      }
      if (isSafeQueryParam(key)) {
        copy.set(
            key, value instanceof Collection<?> ? new ArrayList<>((Collection<?>) value) : value);
      } else {
        copy.set(key, redactValue(value));
      }
    }
    return copy;
  }

  private static Object redactValue(Object value) {
    if (value instanceof Collection<?>) {
      Collection<?> values = (Collection<?>) value;
      List<String> redactedValues = new ArrayList<>(values.size());
      for (int i = 0; i < values.size(); i++) {
        redactedValues.add(REDACTED);
      }
      return redactedValues;
    }
    return REDACTED;
  }

  /**
   * Returns a deterministic non-reversible digest of {@code token} suitable for log correlation, or
   * {@code "null"} / {@code ""} for null / empty inputs.
   */
  public static String digestToken(@Nullable String token) {
    if (token == null) {
      return "null";
    }
    if (token.isEmpty()) {
      return "";
    }
    return "sha256:"
        + Hashing.sha256().hashString(token, StandardCharsets.UTF_8).toString().substring(0, 16);
  }

  private static boolean isSafeQueryParam(String key) {
    return SAFE_QUERY_PARAMS.contains(key.toLowerCase(Locale.US));
  }

  private static String redactSensitiveKeyValues(String text) {
    return SENSITIVE_QUERY_PARAM_PATTERN.matcher(text).replaceAll("$1=" + REDACTED);
  }
}
