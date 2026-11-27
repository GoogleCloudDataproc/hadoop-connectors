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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.util.escape.CharEscapers;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Utility for redacting sensitive components from URLs, query parameters, tokens, and exception
 * messages prior to logging without mutating caller objects.
 */
public final class LoggingUrlRedactor {

  public static final String REDACTED = "REDACTED";

  private static final String UPLOAD_ID_QUERY_PARAM = "upload_id";

  private static final ImmutableSet<String> SAFE_QUERY_PARAMS =
      ImmutableSet.of(
          "alt",
          "contentencoding",
          "delimiter",
          "destinationkmskeyname",
          "destinationpredefinedacl",
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
          "kmskeyname",
          "matchglob",
          "maxbytesrewrittenpercall",
          "maxresults",
          "name",
          "pagetoken",
          "predefinedacl",
          "predefineddefaultobjectacl",
          "prefix",
          "project",
          "projection",
          "softdeleted",
          "sourcegeneration",
          "startoffset",
          "uploadtype",
          "userproject",
          "versions");

  // URLs built by google-http-client keep ' ( ) unescaped and IPv6 hosts use [ ], so these
  // characters can be part of a URL embedded in text. Trailing wrappers are trimmed separately.
  private static final Pattern URL_IN_TEXT_PATTERN =
      Pattern.compile("https?://[^\\s\"<>{}]+", Pattern.CASE_INSENSITIVE);

  private static final String TRAILING_URL_PUNCTUATION = ".,;:!?";

  private static final Pattern DIGEST_PATTERN = Pattern.compile("sha256:[0-9a-f]{16}");

  private LoggingUrlRedactor() {}

  /**
   * Returns a log-safe string representation of {@code url} with user info removed, {@code
   * upload_id} values replaced with {@link #digestToken(String)}, all other non-allowlisted query
   * parameter values replaced with {@link #REDACTED}, and any fragment replaced with {@link
   * #REDACTED}. Does not mutate {@code url}.
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
   * Returns a copy of {@code throwable} that is safe to log or rethrow: HTTP(S) URLs in the
   * messages of the throwable, its causes, and its suppressed exceptions are redacted via {@link
   * #redactUrlString(String)}, and stack traces are preserved. {@link HttpResponseException} and
   * {@link GoogleJsonResponseException} keep their type, status, headers, and error details so that
   * callers such as {@link ApiErrorExtractor} keep working. Other throwables that need redaction
   * are replaced with a copy that reports the original class name. Returns {@code throwable} itself
   * if nothing needs to be redacted.
   */
  @Nullable
  public static Throwable redactThrowable(@Nullable Throwable throwable) {
    if (throwable == null) {
      return null;
    }
    return redactThrowable(throwable, Collections.newSetFromMap(new IdentityHashMap<>()));
  }

  /**
   * Returns a deterministic non-reversible digest of {@code token} suitable for log correlation, or
   * {@code "null"} / {@code ""} for null / empty inputs. Intended for high-entropy server-issued
   * session tokens (such as resumable upload IDs), not low-entropy identifiers. Values that are
   * already digests are returned unchanged, so redacting the same text twice is safe.
   */
  public static String digestToken(@Nullable String token) {
    if (token == null) {
      return "null";
    }
    if (token.isEmpty() || DIGEST_PATTERN.matcher(token).matches()) {
      return token;
    }
    return "sha256:"
        + Hashing.sha256().hashString(token, StandardCharsets.UTF_8).toString().substring(0, 16);
  }

  /**
   * Returns {@code text} with every HTTP(S) URL redacted via {@link #redactUrlString(String)}.
   * Trailing punctuation, and closing brackets or quotes that wrap a URL, are kept as plain text.
   */
  @VisibleForTesting
  static String redactUrlsInText(String text) {
    Matcher matcher = URL_IN_TEXT_PATTERN.matcher(text);
    StringBuilder out = new StringBuilder(text.length());
    int copiedUpTo = 0;
    while (matcher.find()) {
      int urlStart = matcher.start();
      int urlEnd = trimUrlEnd(text, urlStart, matcher.end());
      out.append(text, copiedUpTo, urlStart)
          .append(redactUrlString(text.substring(urlStart, urlEnd)));
      copiedUpTo = urlEnd;
    }
    return out.append(text, copiedUpTo, text.length()).toString();
  }

  private static Throwable redactThrowable(Throwable throwable, Set<Throwable> visited) {
    visited.add(throwable);
    String message = throwable.getMessage();
    String redactedMessage = message == null ? null : redactUrlsInText(message);
    Throwable cause = throwable.getCause();
    Throwable redactedCause =
        cause == null || visited.contains(cause) ? null : redactThrowable(cause, visited);
    boolean changed = !Objects.equals(message, redactedMessage) || redactedCause != cause;
    Throwable[] suppressed = throwable.getSuppressed();
    Throwable[] redactedSuppressed = new Throwable[suppressed.length];
    for (int i = 0; i < suppressed.length; i++) {
      redactedSuppressed[i] =
          visited.contains(suppressed[i]) ? null : redactThrowable(suppressed[i], visited);
      changed |= redactedSuppressed[i] != suppressed[i];
    }
    if (!changed) {
      return throwable;
    }
    Throwable copy = copyWithMessage(throwable, redactedMessage);
    if (redactedCause != null) {
      copy.initCause(redactedCause);
    }
    copy.setStackTrace(throwable.getStackTrace());
    for (Throwable redactedSuppressedThrowable : redactedSuppressed) {
      if (redactedSuppressedThrowable != null) {
        copy.addSuppressed(redactedSuppressedThrowable);
      }
    }
    return copy;
  }

  private static Throwable copyWithMessage(Throwable throwable, @Nullable String message) {
    if (throwable instanceof HttpResponseException) {
      HttpResponseException httpException = (HttpResponseException) throwable;
      String content = httpException.getContent();
      HttpResponseException.Builder builder =
          new HttpResponseException.Builder(
                  httpException.getStatusCode(),
                  httpException.getStatusMessage(),
                  httpException.getHeaders())
              .setMessage(message)
              .setContent(content == null ? null : redactUrlsInText(content))
              .setAttemptCount(httpException.getAttemptCount());
      return httpException instanceof GoogleJsonResponseException
          ? new GoogleJsonResponseException(
              builder, ((GoogleJsonResponseException) httpException).getDetails())
          : builder.build();
    }
    return new RedactedThrowable(throwable.getClass().getName(), message);
  }

  /**
   * Returns the end of the URL that starts at {@code start}, dropping trailing punctuation and any
   * closing parenthesis, bracket, or quote that is not part of the URL itself.
   */
  private static int trimUrlEnd(String text, int start, int end) {
    int unmatchedParens = countChar(text, start, end, ')') - countChar(text, start, end, '(');
    int unmatchedBrackets = countChar(text, start, end, ']') - countChar(text, start, end, '[');
    boolean openQuote =
        (start > 0 && text.charAt(start - 1) == '\'') || countChar(text, start, end, '\'') % 2 != 0;
    int urlEnd = end;
    while (urlEnd > start) {
      char c = text.charAt(urlEnd - 1);
      if (TRAILING_URL_PUNCTUATION.indexOf(c) >= 0) {
        urlEnd--;
      } else if (c == ')' && unmatchedParens > 0) {
        unmatchedParens--;
        urlEnd--;
      } else if (c == ']' && unmatchedBrackets > 0) {
        unmatchedBrackets--;
        urlEnd--;
      } else if (c == '\'' && openQuote) {
        openQuote = false;
        urlEnd--;
      } else {
        break;
      }
    }
    return urlEnd;
  }

  private static int countChar(String text, int start, int end, char target) {
    int count = 0;
    for (int i = start; i < end; i++) {
      if (text.charAt(i) == target) {
        count++;
      }
    }
    return count;
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
    if (!(value instanceof Collection<?>)) {
      return appendQueryParam(out, first, escapedName, formatQueryValue(key, value));
    }
    boolean safe = isSafeQueryParam(key);
    boolean nextFirst = first;
    for (Object item : (Collection<?>) value) {
      if (item != null) {
        nextFirst = appendQueryParam(out, nextFirst, escapedName, formatQueryValue(key, item));
      } else if (!safe) {
        nextFirst = appendQueryParam(out, nextFirst, escapedName, REDACTED);
      }
    }
    return nextFirst;
  }

  private static String formatQueryValue(String key, Object value) {
    if (isSafeQueryParam(key)) {
      return CharEscapers.escapeUriQuery(value.toString());
    }
    // Upload IDs are digested so that request URLs can be correlated with upload ID headers.
    return UPLOAD_ID_QUERY_PARAM.equalsIgnoreCase(key) ? digestToken(value.toString()) : REDACTED;
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

  private static boolean isSafeQueryParam(String key) {
    return SAFE_QUERY_PARAMS.contains(key.toLowerCase(Locale.US));
  }

  /** Log-safe stand-in for a throwable whose message had to be redacted. */
  private static final class RedactedThrowable extends Exception {
    private static final long serialVersionUID = 1L;

    private final String originalClassName;

    RedactedThrowable(String originalClassName, @Nullable String message) {
      super(message);
      this.originalClassName = originalClassName;
    }

    @Override
    public String toString() {
      String message = getLocalizedMessage();
      return message == null ? originalClassName : originalClassName + ": " + message;
    }
  }
}
