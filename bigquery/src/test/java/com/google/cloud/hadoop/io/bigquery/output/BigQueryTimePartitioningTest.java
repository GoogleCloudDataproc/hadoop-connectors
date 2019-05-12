package com.google.cloud.hadoop.io.bigquery.output;

import static org.junit.Assert.*;
import java.io.IOException;
import org.junit.Test;

public class BigQueryTimePartitioningTest {

  public static final String TIME_PARTITIONING_JSON =
      "{\"expirationMs\":\"1000\",\"field\":\"ingestDate\",\"requirePartitionFilter\":true,\"type\":\"day\"}";

  @Test
  public void testConvertToJson() throws IOException {
    BigQueryTimePartitioning bigQueryTimePartitioning = new BigQueryTimePartitioning();
    bigQueryTimePartitioning.setType("day");
    bigQueryTimePartitioning.setExpirationMs(1000L);
    bigQueryTimePartitioning.setField("ingestDate");
    bigQueryTimePartitioning.setRequirePartitionFilter(true);
    assertEquals(TIME_PARTITIONING_JSON, bigQueryTimePartitioning.getAsJson());
  }

  @Test
  public void testConvertFromJson() throws IOException {
    BigQueryTimePartitioning bigQueryTimePartitioning = new BigQueryTimePartitioning();
    bigQueryTimePartitioning.setType("day");
    bigQueryTimePartitioning.setExpirationMs(1000L);
    bigQueryTimePartitioning.setField("ingestDate");
    bigQueryTimePartitioning.setRequirePartitionFilter(true);

    assertEquals(
        bigQueryTimePartitioning.get(),
        BigQueryTimePartitioning.getFromJson(TIME_PARTITIONING_JSON));
  }

  @Test
  public void testConversion_OnlyTypeIsPresent() throws IOException {
    BigQueryTimePartitioning bigQueryTimePartitioning = new BigQueryTimePartitioning();
    bigQueryTimePartitioning.setType("DAY");
    String json = bigQueryTimePartitioning.getAsJson();
    assertEquals("{\"type\":\"DAY\"}", json);
    assertEquals("DAY", BigQueryTimePartitioning.getFromJson(json).getType());
  }
}
