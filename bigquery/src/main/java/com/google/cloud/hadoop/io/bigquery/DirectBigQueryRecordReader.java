package com.google.cloud.hadoop.io.bigquery;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1beta1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1beta1.Storage.StreamPosition;
import com.google.cloud.hadoop.io.bigquery.DirectBigQueryInputFormat.DirectBigQueryInputSplit;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/** */
public class DirectBigQueryRecordReader extends RecordReader<NullWritable, GenericRecord> {

  private Schema schema;
  private Stream stream;
  private Parser parser = new Parser();
  private GenericRecord current;
  private boolean finalized;
  private long limit;
  private long idx;
  private BigQueryStorageClient client;
  private Iterator<ReadRowsResponse> responseIterator;
  private Iterator<GenericRecord> recordIterator;

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    DirectBigQueryInputSplit split =
        (DirectBigQueryInputSplit) genericSplit;
    schema = parser.parse(checkNotNull(split.getSchema(), "schema"));

    stream = Stream.newBuilder().setName(checkNotNull(split.getName(), "name")).build();
    ReadRowsRequest request =
        ReadRowsRequest.newBuilder()
            .setReadPosition(StreamPosition.newBuilder().setStream(stream).build())
            .build();

    client = getClient(context.getConfiguration());
    responseIterator = client.readRowsCallable().call(request).iterator();
    recordIterator = Collections.emptyIterator();

    limit = split.getLimit();
    idx = 0;
    finalized = false;
  }

  @Override
  public boolean nextKeyValue() {
    // Finalize reader once we hit limit. We must at that point keep reading until BigQuery stops
    // sending records.
    if (idx >= limit && !finalized) {
      client.finalizeStream(stream);
      finalized = true;
    }
    idx++;

    // TODO: unwrap runtime exceptions thrown by responseIterator:
    // RE(InterruptedException) -> InterruptedException
    // RE(StatusException) -> IOException
    // See ClientCalls.BlockingResponseStream.hasNext
    if (responseIterator.hasNext() && !recordIterator.hasNext()) {
      recordIterator =
          new AvroRecordIterator(
              schema, responseIterator.next().getAvroRows().getSerializedBinaryRows());
    }
    if (recordIterator.hasNext()) {
      current = recordIterator.next();
      return true;
    }
    current = null;
    return false;
  }

  @Override
  public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  @Override
  public GenericRecord getCurrentValue() {
    return current;
  }

  @Override
  public float getProgress() {
    // TODO: report progress
    return -1;
  }

  @Override
  public void close() {}

  /**
   * Helper method to override for testing.
   *
   * @return Bigquery.
   * @throws IOException on IO Error.
   */
  protected BigQueryStorageClient getClient(Configuration conf) throws IOException {
    return BigQueryStorageClient.create();
  }

  private static class AvroRecordIterator implements Iterator<GenericRecord> {

    private final BinaryDecoder in;
    private final GenericDatumReader<GenericRecord> reader;

    // TODO: replace nulls with reusable objects
    public AvroRecordIterator(Schema schema, ByteString bytes) {
      reader = new GenericDatumReader<>(schema);
      in = new DecoderFactory().binaryDecoder(bytes.toByteArray(), null);
    }

    @Override
    public boolean hasNext() {
      try {
        return !in.isEnd();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public GenericRecord next() {
      try {
        return reader.read(null, in);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
