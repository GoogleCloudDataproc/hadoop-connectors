package com.google.cloud.hadoop.gcsio.authorization;

import java.util.Objects;

public class GcsResourceAndActions {

  private final String bucket;
  private final String objectPath;
  private final String action;

  public GcsResourceAndActions() {
    this("", "", "");
  }

  public GcsResourceAndActions(String bucket, String objectPath, String action) {
    this.bucket = bucket;
    this.objectPath = objectPath;
    this.action = action;
  }

  public String getBucket() {
    return bucket;
  }

  public String getObjectPath() {
    return objectPath;
  }

  public String getAction() {
    return action;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GcsResourceAndActions that = (GcsResourceAndActions) o;
    return Objects.equals(getBucket(), that.getBucket()) && Objects
        .equals(getObjectPath(), that.getObjectPath()) && Objects
        .equals(getAction(), that.getAction());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getBucket(), getObjectPath(), getAction());
  }
}
