package com.google.cloud.hadoop.io.bigquery.output;

import java.io.IOException;
import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TimePartitioning;

/**
 * Wrapper for BigQuery {@link TimePartitioning}.
 *
 * <p>This class is used to avoid client code to depend on BigQuery API classes, so that there is no
 * potential conflict between different versions of BigQuery API libraries in the client.
 *
 * @see TimePartitioning.
 */
public class BigQueryTimePartitioning {
  private final TimePartitioning timePartitioning;

  public BigQueryTimePartitioning() {
    this.timePartitioning = new TimePartitioning();
  }

  public BigQueryTimePartitioning(TimePartitioning timePartitioning) {
    this.timePartitioning = timePartitioning;
  }

  public String getType() {
    return timePartitioning.getType();
  }

  public void setType(String type) {
    timePartitioning.setType(type);
  }

  public String getField() {
    return timePartitioning.getField();
  }

  public void setField(String field) {
    timePartitioning.setField(field);
  }

  public long getExpirationMs() {
    return timePartitioning.getExpirationMs();
  }

  public void setExpirationMs(long expirationMs) {
    timePartitioning.setExpirationMs(expirationMs);
  }

  public Boolean getRequirePartitionFilter() {
    return timePartitioning.getRequirePartitionFilter();
  }

  public void setRequirePartitionFilter(Boolean requirePartitionFilter) {
    timePartitioning.setRequirePartitionFilter(requirePartitionFilter);
  }

  @Override
  public int hashCode() {
    return timePartitioning.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof BigQueryTimePartitioning)) {
      return false;
    }
    BigQueryTimePartitioning other = (BigQueryTimePartitioning) obj;
    return timePartitioning.equals(other.timePartitioning);
  }

  TimePartitioning get() {
    return timePartitioning;
  }

  static TimePartitioning getFromJson(String json) throws IOException {
    JsonParser parser = JacksonFactory.getDefaultInstance().createJsonParser(json);
    return parser.parseAndClose(TimePartitioning.class);
  }

  public String getAsJson() throws IOException {
    return JacksonFactory.getDefaultInstance().toString(timePartitioning);
  }

  static BigQueryTimePartitioning wrap(TimePartitioning tableTimePartitioning) {
    return new BigQueryTimePartitioning(tableTimePartitioning);
  }
}
