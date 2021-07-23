package com.google.cloud.hadoop.gcsio.authorization;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GcsResourceAndActionTest {

  @Test
  public void defaultConstructor_emptryStringValues() {
    GcsResourceAndAction actualObject = new GcsResourceAndAction();

    assertThat(actualObject.getBucket()).isEqualTo("");
    assertThat(actualObject.getObjectPath()).isEqualTo("");
    assertThat(actualObject.getAction()).isEqualTo("");
  }

  @Test
  public void equals_sameObject() {
    GcsResourceAndAction actualObject = new GcsResourceAndAction();

    assertThat(actualObject.equals(actualObject)).isEqualTo(true);
  }

  @Test
  public void equals_notEqualsNull() {
    GcsResourceAndAction actualObject = new GcsResourceAndAction();

    assertThat(actualObject.equals(null)).isEqualTo(false);
  }

  @Test
  public void hashCode_expectedValue() {
    GcsResourceAndAction actualObject = new GcsResourceAndAction("testBucket", "testObject", "read");

    // hard coding the expected hash value to detect any changes in the hashcode function
    assertThat(actualObject.hashCode()).isEqualTo(1357278144);
  }
}
