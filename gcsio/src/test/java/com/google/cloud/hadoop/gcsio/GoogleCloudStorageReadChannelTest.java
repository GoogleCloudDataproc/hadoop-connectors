/*
 * Copyright 2018 Google LLC
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

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTest.newStorageObject;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.BUCKET_NAME;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.HTTP_TRANSPORT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.OBJECT_NAME;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.createReadChannel;
import static com.google.cloud.hadoop.gcsio.MockGoogleCloudStorageImplFactory.mockedGcsImpl;
import static com.google.cloud.hadoop.gcsio.StorageResourceId.UNKNOWN_GENERATION_ID;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getMediaRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getRequestString;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.dataRangeResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.dataResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.inputStreamResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonDataResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonErrorResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.truth.Truth.assertThat;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertThrows;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.client.util.DateTime;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.ErrorResponses;
import com.google.common.collect.ImmutableMap;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GoogleCloudStorageReadChannel} class. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageReadChannelTest {

  @Test
  public void metadataInitialization_eager() throws IOException {
    StorageObject object = newStorageObject(BUCKET_NAME, OBJECT_NAME);
    MockHttpTransport transport = mockTransport(jsonDataResponse(object));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(true).build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);

    assertThat(requests).hasSize(1);
    assertThat(readChannel.size()).isEqualTo(object.getSize().longValue());
    assertThat(requests).hasSize(1);
  }

  @Test
  public void metadataInitialization_lazy() throws IOException {
    StorageObject object = newStorageObject(BUCKET_NAME, OBJECT_NAME);
    MockHttpTransport transport = mockTransport(jsonDataResponse(object));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);

    assertThat(requests).isEmpty();
    assertThat(readChannel.size()).isEqualTo(object.getSize().longValue());
    assertThat(requests).hasSize(1);
  }

  @Test
  public void fadviseAuto_onForwardRead_switchesToRandom() throws IOException {
    int seekPosition = 5;
    byte[] testData = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09};
    byte[] testData2 = Arrays.copyOfRange(testData, seekPosition, testData.length);

    MockHttpTransport transport =
        mockTransport(
            // 1st read request response
            dataRangeResponse(Arrays.copyOfRange(testData, 1, testData.length), 1, testData.length),
            // 2nd read request response
            dataRangeResponse(testData2, seekPosition, testData2.length));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), requests::add);

    GoogleCloudStorageReadOptions options =
        newLazyReadOptionsBuilder()
            .setFadvise(Fadvise.AUTO)
            .setMinRangeRequestSize(1)
            .setInplaceSeekLimit(2)
            .build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);

    byte[] readBytes = new byte[1];

    readChannel.position(1);
    assertThat(readChannel.read(ByteBuffer.wrap(readBytes))).isEqualTo(1);
    assertThat(readBytes).isEqualTo(new byte[] {testData[1]});

    readChannel.position(seekPosition);
    assertThat(readChannel.randomAccessStatus()).isFalse();

    assertThat(readChannel.read(ByteBuffer.wrap(readBytes))).isEqualTo(1);
    assertThat(readBytes).isEqualTo(new byte[] {testData[seekPosition]});
    assertThat(readChannel.randomAccessStatus()).isTrue();

    List<String> rangeHeaders =
        requests.stream().map(r -> r.getHeaders().getRange()).collect(toList());

    assertThat(rangeHeaders).containsExactly("bytes=1-", "bytes=5-5").inOrder();
  }

  @Test
  public void fadviseAuto_onBackwardRead_switchesToRandom() throws IOException {
    int seekPosition = 5;
    byte[] testData = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09};
    byte[] testData2 = Arrays.copyOfRange(testData, seekPosition, testData.length);

    MockHttpTransport transport =
        mockTransport(
            // 1st read request response
            dataRangeResponse(testData2, seekPosition, testData2.length),
            // 2nd read request response
            dataRangeResponse(testData, 0, testData.length));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), requests::add);

    GoogleCloudStorageReadOptions options =
        newLazyReadOptionsBuilder().setFadvise(Fadvise.AUTO).setMinRangeRequestSize(1).build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);

    byte[] readBytes = new byte[1];

    readChannel.position(seekPosition);

    assertThat(readChannel.read(ByteBuffer.wrap(readBytes))).isEqualTo(1);
    assertThat(readBytes).isEqualTo(new byte[] {testData[seekPosition]});

    readChannel.position(0);
    assertThat(readChannel.randomAccessStatus()).isFalse();

    assertThat(readChannel.read(ByteBuffer.wrap(readBytes))).isEqualTo(1);
    assertThat(readBytes).isEqualTo(new byte[] {testData[0]});
    assertThat(readChannel.randomAccessStatus()).isTrue();

    List<String> rangeHeaders =
        requests.stream().map(r -> r.getHeaders().getRange()).collect(toList());

    assertThat(rangeHeaders).containsExactly("bytes=5-", "bytes=0-0").inOrder();
  }

  @Test
  public void fadviseAutoRandom_onSequentialRead_switchToSequential() throws IOException {
    int blockSize = 3;
    byte[] testData = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09};
    List<MockLowLevelHttpResponse> lowLevelHttpResponses = new ArrayList<>();

    GoogleCloudStorageReadOptions options =
        newLazyReadOptionsBuilder()
            .setFadvise(Fadvise.AUTO_RANDOM)
            .setMinRangeRequestSize(1)
            .setBlockSize(blockSize)
            .build();

    int requestLength = (int) options.getMinRangeRequestSize();
    int rangeStartIndex = 0;
    for (int i = 0; i <= options.getFadviseRequestTrackCount(); i++) {
      int rangeEndIndex = rangeStartIndex + requestLength;
      if (i == options.getFadviseRequestTrackCount()) {
        rangeEndIndex = rangeStartIndex + blockSize;
      }
      MockLowLevelHttpResponse request =
          dataRangeResponse(
              Arrays.copyOfRange(testData, rangeStartIndex, rangeEndIndex),
              rangeStartIndex,
              testData.length);
      lowLevelHttpResponses.add(request);
      rangeStartIndex = rangeEndIndex;
    }

    MockHttpTransport transport =
        mockTransport(
            lowLevelHttpResponses.toArray(
                new MockLowLevelHttpResponse[lowLevelHttpResponses.size()]));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), requests::add);

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);

    byte[] readBytes = new byte[requestLength];
    rangeStartIndex = 0;
    for (int i = 0; i <= options.getFadviseRequestTrackCount(); i++) {
      readChannel.position(rangeStartIndex);

      assertThat(readChannel.read(ByteBuffer.wrap(readBytes))).isEqualTo(requestLength);
      assertThat(readBytes).isEqualTo(new byte[] {testData[rangeStartIndex]});
      if (i == options.getFadviseRequestTrackCount()) {
        assertThat(readChannel.randomAccessStatus()).isFalse();
      } else {
        assertThat(readChannel.randomAccessStatus()).isTrue();
      }
      rangeStartIndex += requestLength;
    }

    List<String> rangeHeaders =
        requests.stream().map(r -> r.getHeaders().getRange()).collect(toList());

    // bytes=3-5 instead of opening channel till end as in AUTO_RANDOM adapting to sequential read
    // will result in opening channel till blockSize.
    assertThat(rangeHeaders)
        .containsExactly("bytes=0-0", "bytes=1-1", "bytes=2-2", "bytes=3-5")
        .inOrder();
  }

  @Test
  public void footerPrefetch_reused() throws IOException {
    int footerSize = 2;
    byte[] testData = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09};
    int footerStart = testData.length - footerSize;
    byte[] footer = Arrays.copyOfRange(testData, footerStart, testData.length);

    MockHttpTransport transport =
        mockTransport(
            // Footer prefetch response
            dataRangeResponse(footer, footerStart, testData.length),
            // Footer read miss request response
            dataResponse(new byte[] {testData[footerStart - 1]}));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), requests::add);

    GoogleCloudStorageReadOptions options =
        newLazyReadOptionsBuilder()
            .setFadvise(Fadvise.RANDOM)
            .setMinRangeRequestSize(footerSize)
            .build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);
    assertThat(requests).isEmpty();

    byte[] readBytes = new byte[2];

    // Force lazy footer prefetch
    readChannel.position(footerStart);
    assertThat(readChannel.read(ByteBuffer.wrap(readBytes))).isEqualTo(2);
    assertThat(readChannel.size()).isEqualTo(testData.length);
    assertThat(readBytes).isEqualTo(Arrays.copyOfRange(testData, footerStart, testData.length));

    readChannel.position(footerStart - 1);

    assertThat(readChannel.read(ByteBuffer.wrap(readBytes))).isEqualTo(2);
    assertThat(readBytes)
        .isEqualTo(Arrays.copyOfRange(testData, footerStart - 1, testData.length - 1));

    List<String> rangeHeaders =
        requests.stream().map(r -> r.getHeaders().getRange()).collect(toList());

    assertThat(rangeHeaders).containsExactly("bytes=8-9", "bytes=7-7").inOrder();
  }

  @Test
  public void read_onlyRequestedRange() throws IOException {
    int rangeSize = 2;
    int seekPosition = 0;
    byte[] testData = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09};
    byte[] requestedContent = Arrays.copyOfRange(testData, seekPosition, seekPosition + rangeSize);

    MockHttpTransport transport =
        mockTransport(
            // Footer prefetch response
            dataRangeResponse(requestedContent, 0, testData.length));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), requests::add);

    GoogleCloudStorageReadOptions options =
        newLazyReadOptionsBuilder()
            .setFadvise(Fadvise.SEQUENTIAL)
            .setMinRangeRequestSize(4)
            .setReadExactRequestedBytesEnabled(true)
            .build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);
    assertThat(requests).isEmpty();

    byte[] readBytes = new byte[rangeSize];

    assertThat(readChannel.read(ByteBuffer.wrap(readBytes))).isEqualTo(readBytes.length);
    assertThat(readChannel.size()).isEqualTo(testData.length);
    assertThat(readBytes).isEqualTo(requestedContent);

    List<String> rangeHeaders =
        requests.stream().map(r -> r.getHeaders().getRange()).collect(toList());

    assertThat(rangeHeaders).containsExactly("bytes=0-1").inOrder();
  }

  @Test
  public void read_whenBufferIsEmpty() throws IOException {
    ByteBuffer emptyBuffer = ByteBuffer.wrap(new byte[0]);

    Storage storage = new Storage(HTTP_TRANSPORT, GsonFactory.getDefaultInstance(), r -> {});

    GoogleCloudStorageReadChannel readChannel =
        createReadChannel(storage, newLazyReadOptionsBuilder().build());

    assertThat(readChannel.read(emptyBuffer)).isEqualTo(0);
  }

  @Test
  public void read_whenPositionIsEqualToSize() throws IOException {
    ByteBuffer readBuffer = ByteBuffer.wrap(new byte[1]);
    StorageObject object = newStorageObject(BUCKET_NAME, OBJECT_NAME);

    MockHttpTransport transport = mockTransport(jsonDataResponse(object.setSize(BigInteger.ZERO)));

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), r -> {});

    GoogleCloudStorageReadChannel readChannel =
        createReadChannel(storage, GoogleCloudStorageReadOptions.DEFAULT);

    assertThat(readChannel.position()).isEqualTo(readChannel.size());
    assertThat(readChannel.read(readBuffer)).isEqualTo(-1);
  }

  @Test
  public void size_whenObjectIsGzipEncoded_shouldBeSetToMaxLongValue() throws IOException {
    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(
                newStorageObject(BUCKET_NAME, OBJECT_NAME).setContentEncoding("gzip")));
    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), r -> {});

    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setGzipEncodingSupportEnabled(true).build();
    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, readOptions);

    assertThat(readChannel.size()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void initMetadata_throwsException_whenReadConsistencyEnabledAndGenerationIsNull()
      throws IOException {
    Storage storage = new Storage(HTTP_TRANSPORT, GsonFactory.getDefaultInstance(), r -> {});

    GoogleCloudStorageReadOptions options = newLazyReadOptionsBuilder().build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                readChannel.initMetadata("gzip", /* sizeFromMetadata= */ 1, UNKNOWN_GENERATION_ID));

    assertThat(e).hasMessageThat().contains("Generation parameter of -1 is invalid");
  }

  @Test
  public void initMetadata_succeeds_whenReadConsistencyEnabledAndGenerationIsValid()
      throws IOException {
    Storage storage = new Storage(HTTP_TRANSPORT, GsonFactory.getDefaultInstance(), r -> {});

    GoogleCloudStorageReadOptions options =
        newLazyReadOptionsBuilder().setGzipEncodingSupportEnabled(true).build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);

    readChannel.initMetadata("gzip", /* sizeFromMetadata= */ 1, /* generation= */ 1234L);
  }

  @Test
  public void initGeneration_hasGenerationId() throws IOException {
    StorageObject storageObject = newStorageObject(BUCKET_NAME, OBJECT_NAME);
    MockHttpTransport transport = mockTransport(jsonDataResponse(storageObject));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);
    // initialize metadata
    readChannel.size();
    assertThat(readChannel.generation()).isEqualTo(storageObject.getGeneration());
  }

  @Test
  public void lazyInitGeneration_succeeds_whenReadConsistencyStrict() throws IOException {
    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(newStorageObject(BUCKET_NAME, OBJECT_NAME).setGeneration(5L)));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);
    // initialize metadata
    readChannel.size();
    assertThat(readChannel.generation()).isEqualTo(5L);
  }

  @Test
  public void lazyReadFileAtSpecificGeneration_fails_ifGenerationChanged() throws IOException {
    long requestedGeneration = 5L;
    long actualGeneration = 342L;

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(
                newStorageObject(BUCKET_NAME, OBJECT_NAME).setGeneration(actualGeneration)));

    Storage storage =
        new Storage(transport, GsonFactory.getDefaultInstance(), new ArrayList<>()::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();

    GoogleCloudStorageReadChannel readChannel =
        createReadChannel(storage, options, requestedGeneration);

    IllegalStateException exception = assertThrows(IllegalStateException.class, readChannel::size);

    assertThat(exception)
        .hasMessageThat()
        .contains(
            String.format(
                "Provided generation (%d) should be equal to fetched generation (%d)",
                requestedGeneration, actualGeneration));
  }

  @Test
  public void eagerReadFileAtSpecificGeneration_fails_ifGenerationChanged() throws IOException {
    long requestedGeneration = 5L;
    long actualGeneration = 342L;

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(
                newStorageObject(BUCKET_NAME, OBJECT_NAME).setGeneration(actualGeneration)));

    Storage storage =
        new Storage(transport, GsonFactory.getDefaultInstance(), new ArrayList<>()::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(true).build();

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> createReadChannel(storage, options, requestedGeneration));

    assertThat(exception)
        .hasMessageThat()
        .contains(
            String.format(
                "Provided generation (%d) should be equal to fetched generation (%d)",
                requestedGeneration, actualGeneration));
  }

  @Test
  public void lazyReadFileAtSpecificGeneration_succeeds_whenReadConsistencyStrict()
      throws IOException {
    long generation = 5L;
    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(newStorageObject(BUCKET_NAME, OBJECT_NAME).setGeneration(generation)));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options, generation);
    // initialize metadata
    readChannel.size();
    assertThat(readChannel.generation()).isEqualTo(generation);
  }

  @Test
  public void eagerReadFileAtSpecificGeneration_succeeds_whenReadConsistencyStrict()
      throws IOException {
    long generation = 5L;
    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(newStorageObject(BUCKET_NAME, OBJECT_NAME).setGeneration(generation)));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(true).build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options, generation);
    assertThat(readChannel.generation()).isEqualTo(generation);
  }

  @Test
  public void lazyReadFileAtSpecificGeneration_fails_whenReadConsistencyStrict()
      throws IOException {
    long generation = 5L;
    MockHttpTransport transport = mockTransport(jsonErrorResponse(ErrorResponses.NOT_FOUND));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();
    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options, generation);
    assertThrows(FileNotFoundException.class, readChannel::size);
  }

  @Test
  public void eagerReadFileAtSpecificGeneration_fails_whenReadConsistencyStrict()
      throws IOException {
    long generation = 5L;
    MockHttpTransport transport = mockTransport(jsonErrorResponse(ErrorResponses.NOT_FOUND));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(true).build();
    assertThrows(
        FileNotFoundException.class, () -> createReadChannel(storage, options, generation));
  }

  @Test
  public void afterRetry_subsequentReads_succeed() throws IOException {
    long generation = 5L;
    MockHttpTransport transport =
        mockTransport(
            jsonErrorResponse(ErrorResponses.NOT_FOUND),
            jsonDataResponse(newStorageObject(BUCKET_NAME, OBJECT_NAME).setGeneration(generation)));

    Storage storage =
        new Storage(transport, GsonFactory.getDefaultInstance(), new ArrayList<>()::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();
    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options, generation);

    assertThrows(FileNotFoundException.class, readChannel::size);

    assertThat(readChannel.size()).isNotEqualTo(0);
    assertThat(readChannel.generation()).isEqualTo(generation);
  }

  @Test
  public void retired_request_same_invocationId() throws IOException {
    long generation = 5L;
    MockHttpTransport transport =
        mockTransport(
            jsonErrorResponse(ErrorResponses.RATE_LIMITED),
            jsonDataResponse(newStorageObject(BUCKET_NAME, OBJECT_NAME).setGeneration(generation)));
    TrackingHttpRequestInitializer requestsTracker =
        new TrackingHttpRequestInitializer(
            new RetryHttpInitializer(null, RetryHttpInitializerOptions.builder().build()));
    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), requestsTracker);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();
    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options, generation);

    assertThat(readChannel.size()).isNotEqualTo(0);
    List<String> invocationIds = requestsTracker.getAllRequestInvocationIds();
    // Request is retired only once, making total request count to be 2.
    assertThat(invocationIds.size()).isEqualTo(2);
    Set<String> uniqueInvocationIds = Set.copyOf(invocationIds);
    // For retried request invocationId remains same causing the set to contain only one element
    assertThat(uniqueInvocationIds.size()).isEqualTo(1);
  }

  @Test
  public void read_gzipEncoded_shouldReadAllBytes() throws IOException {
    byte[] testData = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(newStorageObject(BUCKET_NAME, OBJECT_NAME).setContentEncoding("gzip")),
            dataRangeResponse(testData, 0, testData.length));

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), r -> {});

    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setGzipEncodingSupportEnabled(true).build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, readOptions);

    assertThat(readChannel.size()).isEqualTo(Long.MAX_VALUE);
    assertThat(readChannel.read(ByteBuffer.wrap(new byte[testData.length + 1])))
        .isEqualTo(testData.length);
    assertThat(readChannel.size()).isEqualTo(testData.length);
  }

  @Test
  public void read_withPositiveGeneration_usesGenerationMatchPrecondition() throws IOException {
    long generation = 12345L;
    byte[] testData = {0x01, 0x02, 0x03};
    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(newStorageObject(BUCKET_NAME, OBJECT_NAME).setGeneration(generation)),
            dataResponse(testData));
    List<HttpRequest> requests = new ArrayList<>();
    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), requests::add);
    GoogleCloudStorageReadChannel readChannel =
        createReadChannel(storage, GoogleCloudStorageReadOptions.builder().build(), generation);

    readChannel.read(ByteBuffer.allocate(testData.length));

    // The first request is for metadata, the second is for data.
    // Verify the data request URL contains the generation parameter.
    String mediaRequestUrl = getMediaRequestString(BUCKET_NAME, OBJECT_NAME, generation);
    assertThat("GET:" + requests.get(1).getUrl().toString()).isEqualTo(mediaRequestUrl);
  }

  @Test
  public void read_withZeroGeneration_doesNotUseGenerationMatchPrecondition() throws IOException {
    long requestedGeneration = 0L;
    // The actual object has a real, positive generation ID.
    long actualGeneration = 54321L;
    byte[] testData = {0x04, 0x05, 0x06};
    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(
                newStorageObject(BUCKET_NAME, OBJECT_NAME).setGeneration(actualGeneration)),
            dataResponse(testData));
    List<HttpRequest> requests = new ArrayList<>();
    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), requests::add);
    GoogleCloudStorageReadChannel readChannel =
        createReadChannel(
            storage,
            GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build(),
            requestedGeneration);

    readChannel.read(ByteBuffer.allocate(testData.length));

    // The data request URL should not contain a generation parameter,
    // because the requested generation was 0. The generation parameter in getMediaRequestString
    // will be null when the check for generation > 0 fails.
    String mediaRequestUrl = getMediaRequestString(BUCKET_NAME, OBJECT_NAME);
    assertThat("GET:" + requests.get(0).getUrl().toString()).isEqualTo(mediaRequestUrl);
  }

  @Test
  public void open_gzipContentEncoding_succeeds_whenContentEncodingSupported() throws Exception {
    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(
                newStorageObject(BUCKET_NAME, OBJECT_NAME).setContentEncoding("gzip")));

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), r -> {});

    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setGzipEncodingSupportEnabled(true).build();

    try (GoogleCloudStorageReadChannel channel = createReadChannel(storage, readOptions)) {
      channel.position();
    }
  }

  @Test
  public void open_gzipContentEncoding_throwsIOException_ifContentEncodingNotSupported()
      throws Exception {
    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(
                newStorageObject(BUCKET_NAME, OBJECT_NAME).setContentEncoding("gzip")));

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), r -> {});

    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setGzipEncodingSupportEnabled(false).build();

    IOException e = assertThrows(IOException.class, () -> createReadChannel(storage, readOptions));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Cannot read GZIP encoded files - content encoding support is disabled.");
  }

  /**
   * Helper for test cases involving {@code GoogleCloudStorage.open(StorageResourceId)} to set up
   * the shared sleeper/clock/backoff mocks and set {@code maxRetries}. Also checks basic invariants
   * of a fresh readChannel, such as its position() and isOpen().
   */
  private void setUpAndValidateReadChannelMocksAndSetMaxRetries(
      GoogleCloudStorageReadChannel readChannel, int maxRetries) throws IOException {
    readChannel.setMaxRetries(maxRetries);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);
  }

  /** Test error handling of {@link GoogleCloudStorageReadChannel#skipInPlace(long)} */
  @Test
  public void testReadWithFailedInplaceSeekSucceeds() throws IOException {
    byte[] testData = {0x01, 0x02, 0x03, 0x05, 0x08};
    byte[] testData2 = Arrays.copyOfRange(testData, 3, testData.length);

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(
                new StorageObject()
                    .setBucket(BUCKET_NAME)
                    .setName(OBJECT_NAME)
                    .setTimeCreated(new DateTime(11L))
                    .setUpdated(new DateTime(12L))
                    .setSize(BigInteger.valueOf(testData.length))
                    .setContentEncoding(null)
                    .setGeneration(1L)
                    .setMetageneration(1L)),
            inputStreamResponse(
                CONTENT_LENGTH,
                testData.length,
                new InputStream() {
                  private int current = 0;

                  @Override
                  public synchronized int read() throws IOException {
                    if (current == 0) {
                      current += 1;
                      return 1;
                    }
                    throw new IOException("In-place seek IOException");
                  }
                }),
            dataResponse(ImmutableMap.of("Content-Length", testData2.length), testData2));
    GoogleCloudStorage gcs = mockedGcsImpl(transport);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel)
            gcs.open(
                new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                GoogleCloudStorageReadOptions.builder()
                    .setInplaceSeekLimit(3)
                    .setFadvise(Fadvise.SEQUENTIAL)
                    .build());

    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 3);

    // Read once then skip to trigger a call to method under testing.
    assertThat(readChannel.read(ByteBuffer.wrap(new byte[1]))).isEqualTo(1);
    readChannel.position(3);

    // IOException thrown. Trigger lazy-seek behavior.
    byte[] byte3 = new byte[1];
    assertThat(readChannel.read(ByteBuffer.wrap(byte3))).isEqualTo(1);
    assertThat(byte3).isEqualTo(new byte[] {testData[3]});
  }

  @Test
  public void read_gzipped_withExceptionThrownDuringRead() throws IOException {
    byte[] testDataBatch = new byte[1024];
    new Random().nextBytes(testDataBatch);
    // Throw exception after 2GiB byte that caused data duplication before
    long exceptionByte = 2L * 1024 * 1024 * 1024 + testDataBatch.length;
    long maxLength = exceptionByte + testDataBatch.length;

    long generation = 3419;

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(
                newStorageObject(BUCKET_NAME, OBJECT_NAME)
                    .setContentEncoding("gzip")
                    // gzipped object size is smaller than uncompressed data
                    .setSize(BigInteger.valueOf(maxLength / 2))
                    .setGeneration(generation)),
            inputStreamResponse(
                    new InputStream() {
                      long bytesRead = 0;
                      boolean streamFailed;

                      @Override
                      public int read() throws IOException {
                        if (streamFailed) {
                          return -1;
                        }
                        if (bytesRead == exceptionByte) {
                          throw new IOException(
                              String.format("Read exception at %d byte", exceptionByte));
                        }
                        return testDataBatch[(int) (bytesRead++ % testDataBatch.length)] & 255;
                      }
                    })
                .addHeader("Content-Encoding", "gzip")
                // gzipped object size is smaller than uncompressed data
                .addHeader("Content-Length", String.valueOf(maxLength / 2)),
            inputStreamResponse(
                    new InputStream() {
                      long bytesRead = 0;

                      @Override
                      public int read() {
                        return bytesRead < maxLength
                            ? testDataBatch[(int) (bytesRead++ % testDataBatch.length)] & 255
                            : -1;
                      }
                    })
                .addHeader("Content-Encoding", "gzip")
                // gzipped object size is smaller than uncompressed data
                .addHeader("Content-Length", String.valueOf(maxLength / 2)));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, GsonFactory.getDefaultInstance(), requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setFadvise(Fadvise.SEQUENTIAL)
            .setGzipEncodingSupportEnabled(true)
            .build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);

    assertThat(readChannel.size()).isEqualTo(Long.MAX_VALUE);

    byte[] readBytes = new byte[testDataBatch.length];
    long totalBytesRead = 0;
    long bytesRead;
    while ((bytesRead = readChannel.read(ByteBuffer.wrap(readBytes))) > 0) {
      totalBytesRead += bytesRead;
      assertThat(bytesRead).isEqualTo(testDataBatch.length);
      assertThat(readBytes).isEqualTo(testDataBatch);
      readBytes = new byte[testDataBatch.length];
    }
    assertThat(totalBytesRead).isEqualTo(maxLength);

    List<String> rangeHeaders =
        requests.stream().map(r -> r.getHeaders().getRange()).collect(toList());
    assertThat(rangeHeaders).containsExactly(null, null, null).inOrder();

    List<String> requestStrings =
        requests.stream().map(r -> r.getRequestMethod() + ":" + r.getUrl()).collect(toList());
    assertThat(requestStrings)
        .containsExactly(
            getRequestString(
                BUCKET_NAME, OBJECT_NAME, /* fields= */ "contentEncoding,generation,size"),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, generation),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, generation))
        .inOrder();
  }

  @Test
  public void openStream_gzipped_onPrematureEofInSkip_throwsEofException() throws IOException {
    // Setup: Mock transport to return an InputStream that immediately signals EOF.
    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(
                newStorageObject(BUCKET_NAME, OBJECT_NAME)
                    .setContentEncoding("gzip")
                    .setSize(BigInteger.valueOf(100))),
            inputStreamResponse(
                new InputStream() {
                  @Override
                  public int read() {
                    return -1;
                  }
                }));

    Storage storage = new Storage(transport, new GsonFactory(), r -> {});
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setGzipEncodingSupportEnabled(true).build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, readOptions);

    // Position the channel to a non-zero offset to trigger a skip.
    readChannel.position(10);

    // Attempting to read should trigger the skip, which should then fail with EOFException.
    EOFException e =
        assertThrows(EOFException.class, () -> readChannel.read(ByteBuffer.allocate(1)));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(
            "Unexpected end of stream trying to skip 10 bytes to seek to position 10,"
                + " size: 9223372036854775807 for 'gs://foo-bucket/bar-object'");
  }

  private static GoogleCloudStorageReadOptions.Builder newLazyReadOptionsBuilder() {
    return GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false);
  }
}
