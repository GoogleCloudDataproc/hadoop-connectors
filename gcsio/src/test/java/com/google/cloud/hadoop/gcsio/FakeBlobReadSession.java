package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.core.ApiFutures;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadAsFutureBytes;
import com.google.cloud.storage.ReadProjectionConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FakeBlobReadSession implements BlobReadSession {

  private static final String TEST_STRING =
      "Lorem ipsum dolor sit amet. Qui esse voluptatum qui tempora quia quo maiores galisum. Et officia cum";

  public static final String SUBSTRING_20_10 = TEST_STRING.substring(20, 30);
  public static final String SUBSTRING_50_7 = TEST_STRING.substring(50, 57);
  public static final String SUBSTRING_65_17 = TEST_STRING.substring(65, 82);

  @Override
  public BlobInfo getBlobInfo() {
    return null;
  }

  @Override
  public <Projection> Projection readAs(ReadProjectionConfig<Projection> readProjectionConfig) {
    assertThat(readProjectionConfig).isInstanceOf(ReadAsFutureBytes.class);
    RangeSpec range = ((ReadAsFutureBytes) readProjectionConfig).getRange();
    return (Projection)
        ApiFutures.immediateFuture(getSubString(range).getBytes(StandardCharsets.UTF_8));
  }

  private String getSubString(RangeSpec range) {
    return TEST_STRING.substring(
        (int) range.begin(), (int) (range.begin() + range.maxLength().getAsLong()));
  }

  @Override
  public void close() throws IOException {}
}
