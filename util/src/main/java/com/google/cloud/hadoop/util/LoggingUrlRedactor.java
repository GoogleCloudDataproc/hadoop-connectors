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
import com.google.api.client.util.escape.CharEscapers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
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
          "endoffset",
          "fields",
          "generation",
          "ifgenerationmatch",
          "ifgenerationnotmatch",
          "ifmetagenerationmatch",
          "ifmetagenerationnotmatch",
          "ifsourcegenerationmatch",
          "ifsourcegenerationnotmatch",
          "ifsourcemetagenerationmatch",
          "ifsourcemetagenerationnotmatch",
          "includefoldersasprefixes",
          "includetrailingdelimiter",
          "matchglob",
          "maxbytesrewrittenpercall",
          "maxresults",
          "name",
          "pagetoken",
          "prefix",
          "project",
          "projection",
          "softdeleted",
          "sourcegeneration",
          "startoffset",
          "uploadtype",
          "userproject",
          "versions");

  private static final Pattern URL_IN_TEXT_PATTERN =
      Pattern.compile("https?://[^\\s'\"<>()\\[\\]{}]+", Pattern.CASE_INSENSITIVE);

  private LoggingUrlRedactor() {}

  /**
   * Returns a log-safe string representation of {@code url} with user info removed, all
   * non-allowlisted query parameter values replaced with {@link #REDACTED}, and any fragment
   * replaced with {@link #REDACTED}. Does not mutate {@code url}.
   */
  public static String redactUrl(@Nullable GenericUrl url) {
    if (url == null) {
      return "null";
    }
    StringBuilder out = new StringBuilder();
    appendAuthorityWithoutUserInfo(out, url);
    appendPath(out, url);
    appendQueryParams(out, url);
    if (url.getFragment() != null) {
      out.append('#').append(REDACTED);
    }
    return out.toString();
  }

  /**
   * Returns a log-safe version of a raw URL string. Parses {@code urlString} via {@link GenericUrl}
   * and delegates to {@link #redactUrl(GenericUrl)}; if parsing fails (for example, relative or
   * malformed URLs), strips user info and replaces query/fragment components with {@link
   * #REDACTED}.
   */
  public static String redactUrlString(@Nullable String urlString) {
    if (urlString == null) {
      return "null";
    }
    if (urlString.isEmpty()) {
      return "";
    }
    try {
      return redactUrl(new GenericUrl(urlString));
    } catch (IllegalArgumentException e) {
      return stripSensitivePartsFromRawUrl(urlString);
    }
  }

  /**
   * Returns a single-line log-safe summary of {@code throwable} and its causal chain with any
   * embedded HTTP(S) URLs redacted via {@link #redactUrlString(String)}.
   */
  public static String formatThrowableForLog(@Nullable Throwable throwable) {
    if (throwable == null) {
      return "null";
    }
    StringBuilder out = new StringBuilder();
    List<Throwable> chain = Throwables.getCausalChain(throwable);
    for (int i = 0; i < chain.size(); i++) {
      if (i > 0) {
        out.append(" caused by: ");
      }
      Throwable current = chain.get(i);
      out.append(current.getClass().getName());
      String message = current.getMessage();
      if (message != null) {
        out.append(": ").append(redactUrlsInText(message));
      }
    }
    return out.toString();
  }

  /**
   * Returns a deterministic non-reversible digest of {@code token} suitable for log correlation, or
   * {@code "null"} / {@code ""} for null / empty inputs. Intended for high-entropy server-issued
   * session tokens (such as resumable upload IDs), not low-entropy identifiers.
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

  private static void appendAuthorityWithoutUserInfo(StringBuilder out, GenericUrl url) {
    String scheme = url.getScheme();
    if (scheme != null) {
      out.append(scheme).append("://");
    }
    String host = url.getHost();
    if (host != null) {
      out.append(host);
    }
    int port = url.getPort();
    if (port != -1) {
      out.append(':').append(port);
    }
  }

  private static void appendPath(StringBuilder out, GenericUrl url) {
    List<String> pathParts = url.getPathParts();
    if (pathParts == null) {
      return;
    }
    if (!hasNullElement(pathParts)) {
      String rawPath = url.getRawPath();
      if (rawPath != null) {
        out.append(rawPath);
      }
      return;
    }
    for (int i = 0; i < pathParts.size(); i++) {
      if (i > 0) {
        out.append('/');
      }
      String part = pathParts.get(i);
      if (part != null && !part.isEmpty()) {
        out.append(CharEscapers.escapeUriPath(part));
      }
    }
  }

  private static boolean hasNullElement(List<String> list) {
    for (String element : list) {
      if (element == null) {
        return true;
      }
    }
    return false;
  }

  private static void appendQueryParams(StringBuilder out, GenericUrl url) {
    boolean first = true;
    for (Map.Entry<String, Object> entry : url.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      if (key != null && value != null) {
        first = appendQueryEntry(out, first, key, value);
      }
    }
  }

  private static boolean appendQueryEntry(
      StringBuilder out, boolean first, String key, Object value) {
    String escapedName = CharEscapers.escapeUriQuery(key);
    boolean safe = isSafeQueryParam(key);
    if (value instanceof Collection<?>) {
      boolean nextFirst = first;
      for (Object item : (Collection<?>) value) {
        if (safe) {
          if (item != null) {
            nextFirst =
                appendQueryParam(
                    out, nextFirst, escapedName, CharEscapers.escapeUriQuery(item.toString()));
          }
        } else {
          nextFirst = appendQueryParam(out, nextFirst, escapedName, REDACTED);
        }
      }
      return nextFirst;
    }
    String formattedValue = safe ? CharEscapers.escapeUriQuery(value.toString()) : REDACTED;
    return appendQueryParam(out, first, escapedName, formattedValue);
  }

  private static boolean appendQueryParam(
      StringBuilder out, boolean first, String escapedName, String formattedValue) {
    out.append(first ? '?' : '&').append(escapedName);
    if (!formattedValue.isEmpty()) {
      out.append('=').append(formattedValue);
    }
    return false;
  }

  private static String stripSensitivePartsFromRawUrl(String urlString) {
    int hashIdx = urlString.indexOf('#');
    boolean hasFragment = hashIdx >= 0;
    String beforeFragment = hasFragment ? urlString.substring(0, hashIdx) : urlString;

    int queryIdx = beforeFragment.indexOf('?');
    boolean hasQuery = queryIdx >= 0;
    String base = hasQuery ? beforeFragment.substring(0, queryIdx) : beforeFragment;

    int schemeIdx = base.indexOf("://");
    if (schemeIdx >= 0) {
      int authorityStart = schemeIdx + 3;
      int pathStart = base.indexOf('/', authorityStart);
      int authorityEnd = pathStart >= 0 ? pathStart : base.length();
      int atIdx = base.lastIndexOf('@', authorityEnd - 1);
      if (atIdx >= authorityStart) {
        base = base.substring(0, authorityStart) + base.substring(atIdx + 1);
      }
    }

    StringBuilder out = new StringBuilder(base);
    if (hasQuery) {
      out.append('?').append(REDACTED);
    }
    if (hasFragment) {
      out.append('#').append(REDACTED);
    }
    return out.toString();
  }

  private static String redactUrlsInText(String text) {
    Matcher matcher = URL_IN_TEXT_PATTERN.matcher(text);
    StringBuffer result = new StringBuffer();
    while (matcher.find()) {
      matcher.appendReplacement(result, Matcher.quoteReplacement(redactUrlString(matcher.group())));
    }
    matcher.appendTail(result);
    return result.toString();
  }

  private static boolean isSafeQueryParam(String key) {
    return SAFE_QUERY_PARAMS.contains(key.toLowerCase(Locale.US));
  }
}
