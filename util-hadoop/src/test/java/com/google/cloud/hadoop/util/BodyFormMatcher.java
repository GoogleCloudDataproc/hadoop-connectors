package com.google.cloud.hadoop.util;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class BodyFormMatcher extends TypeSafeMatcher<String> {

  private Map<String, String> expectedBodyParams;

  public BodyFormMatcher(Map<String, String> expectedBodyParams) {
    this.expectedBodyParams = expectedBodyParams;
  }

  @Override
  protected boolean matchesSafely(String requestBody) {
    Map<String, String> bodyParams =
        Arrays.asList(requestBody.split("&")).stream()
            .map(fieldAndValue -> fieldAndValue.split("="))
            .collect(Collectors.toMap(v -> v[0], v -> v[1]));

    return bodyParams.entrySet().containsAll(expectedBodyParams.entrySet());
  }

  @Override
  public void describeTo(Description description) {
    description.appendText(
        expectedBodyParams.entrySet().stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.joining(", ", "{", "}")));
  }

  @Override
  protected void describeMismatchSafely(String item, Description mismatchDescription) {
    super.describeMismatchSafely(
        Arrays.asList(item.split("&")).stream()
            .map(fieldAndValue -> fieldAndValue.split("="))
            .map(v -> v[0] + "=" + v[1])
            .collect(Collectors.joining(", ", "{", "}")),
        mismatchDescription);
  }
}
