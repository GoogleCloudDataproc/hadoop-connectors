/*
 * Copyright 2013 Google Inc.
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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.encodeMetadata;

import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.util.AbstractGoogleAsyncWriteChannel;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.hadoop.util.LoggingMediaHttpUploaderProgressListener;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/** Implements WritableByteChannel to provide write access to GCS. */
public class GoogleCloudStorageWriteChannel extends AbstractGoogleAsyncWriteChannel<StorageObject>
    implements GoogleCloudStorageItemInfo.Provider {

  private static final Duration MIN_LOGGING_INTERVAL = Duration.ofMinutes(1);

  private final Storage gcs;
  private final StorageResourceId resourceId;
  private final CreateObjectOptions createOptions;
  private final ObjectWriteConditions writeConditions;
  // ClientRequestHelper to be used instead of calling final methods in client requests.
  private final ClientRequestHelper<StorageObject> clientRequestHelper;

  private GoogleCloudStorageItemInfo completedItemInfo = null;

  private final Hasher cumulativeCrc32c;
  private long totalLength;

  private static String actualCrc32c;

  private GoogleCloudStorage gcsimpl;

  /**
   * Constructs an instance of GoogleCloudStorageWriteChannel.
   *
   * @param gcs storage object instance
   * @param uploadThreadPool thread pool to use for running the upload operation
   * @param requestHelper a ClientRequestHelper to set extra headers
   * @param channelOptions write channel options
   * @param resourceId object to create
   * @param createOptions object creation options
   * @param writeConditions conditions on which write should be allowed to continue
   */
  public GoogleCloudStorageWriteChannel(
      Storage gcs,
      ClientRequestHelper<StorageObject> requestHelper,
      ExecutorService uploadThreadPool,
      AsyncWriteChannelOptions channelOptions,
      StorageResourceId resourceId,
      CreateObjectOptions createOptions,
      ObjectWriteConditions writeConditions,
      GoogleCloudStorage gcsimpl) {
    super(uploadThreadPool, channelOptions);
    this.clientRequestHelper = requestHelper;
    this.gcs = gcs;
    this.resourceId = resourceId;
    this.createOptions = createOptions;
    this.writeConditions = writeConditions;
    this.totalLength = 0;
    this.cumulativeCrc32c = Hashing.crc32c().newHasher();
    this.actualCrc32c = "";
    this.gcsimpl = gcsimpl;
    System.out.println("In GoogleCloudStorageWriteChannel constructor");
  }

  @Override
  public synchronized int write(ByteBuffer src) throws IOException {
    System.out.println("Animesh: write call");
    System.out.println(src.toString().length());
    ByteBuffer dup = src.duplicate();
    int written = super.write(src);
    hash(dup, written);
    return written;
  }

  private long hash(ByteBuffer src, long written) {
    ByteBuffer buffer = src.slice();
    int remaining = buffer.remaining();
    int consumed = remaining;
    if (written < remaining) {
      int intExact = Math.toIntExact(written);
      buffer.limit(intExact);
      consumed = intExact;
    }
    totalLength += remaining;
    cumulativeCrc32c.putBytes(buffer);
    src.position(src.position() + consumed);
    return consumed;
  }

  @Override
  public void close() throws IOException {
    System.out.println("In GoogleCloudStorageWriteChannel , closing channel");
    if (super.isOpen()) {
      super.close();
      closeInteral();
    }
  }

  private void closeInteral() throws IOException {
    // String srcCrc = cumulativeCrc32c.hash().toString();
    String srcCrc = BaseEncoding.base64().encode(Ints.toByteArray(cumulativeCrc32c.hash().asInt()));
    // String srcCrc = Base64.getEncoder().encodeToString(cumulativeCrc32c.hash().asBytes());
    String destCrc = this.actualCrc32c;

    // byte[] srcbytes = cumulativeCrc32c.hash().asBytes();
    // byte[] destbytes = hexStringToByteArray(this.actualCrc32c);

    logger.atSevere().log("Src CRC32: '%s'. dest CRC32: %s", srcCrc, destCrc);
    if (srcCrc.equals(destCrc)) {
      System.out.println("Restoring object...");
      deleteOrRestoreObject();
      throw new IOException("");
    }
  }

  private void deleteOrRestoreObject() {
    // GoogleCloudStorageImpl gcsUtil = new
    // GoogleCloudStorageImpl(options.getCloudStorageOptions());
    // GoogleCloudStorageImpl gcsUtil = GoogleCloudStorageImpl.builder()
    //     .setOptions(options.getCloudStorageOptions())
    //     .setCredentials(credentials)
    //     .setDownscopedAccessTokenFn(downscopedAccessTokenFn)
    //     .build();
    try {

      SeekableByteChannel readchannel = this.gcsimpl.open(this.resourceId);
      byte[] buffer = new byte[8000]; // 8KB
      readchannel.read(ByteBuffer.wrap(buffer));
      System.out.println(
          ".....File before restore: " + new String(buffer, Charset.defaultCharset()));

      ListObjectOptions options = ListObjectOptions.builder().setVersionEnabled(true).build();
      List<GoogleCloudStorageItemInfo> itemList =
          this.gcsimpl.listObjectInfo(
              this.resourceId.getBucketName(), this.resourceId.getObjectName(), options);

      System.out.println(".....version count: " + itemList.size());

      GoogleCloudStorageItemInfo last = null;
      GoogleCloudStorageItemInfo secondToLast = null;
      int len = itemList.size();
      if (len >= 1) {
        last = itemList.get(len - 1);
      }
      if (len >= 2) {
        secondToLast = itemList.get(len - 2);
      }
      List<StorageResourceId> objectsTodelete = new ArrayList<StorageResourceId>();

      if (secondToLast != null && last != null) {

        StorageResourceId src = secondToLast.getResourceId();
        StorageResourceId dest = last.getResourceId();

        Map<StorageResourceId, StorageResourceId> sourceToDestinationObjectsMap = new HashMap<>(1);
        sourceToDestinationObjectsMap.put(src, dest);

        this.gcsimpl.copy(sourceToDestinationObjectsMap);

        objectsTodelete.add(secondToLast.getResourceId());
        this.gcsimpl.deleteObjects(objectsTodelete);

        // reading after restore
        readchannel = this.gcsimpl.open(this.resourceId);
        buffer = new byte[8000]; // 8KB
        readchannel.read(ByteBuffer.wrap(buffer));
        System.out.println(
            ".....File after restore: " + new String(buffer, Charset.defaultCharset()));

      } else if (last != null) {
        // deleting last object as it was the only one
        objectsTodelete.add(last.getResourceId());
        this.gcsimpl.deleteObjects(objectsTodelete);
        System.out.println(
            ".....File after delete: " + new String(buffer, Charset.defaultCharset()));
      }

    } catch (Exception e) {
      System.out.println("Failed to restore. Possible corrupt state " + e.getMessage());
    }
  }

  // public static byte[] hexStringToByteArray(String s) {
  //   int len = s.length();
  //   byte[] data = new byte[len / 2];
  //   for (int i = 0; i < len; i += 2) {
  //     data[i / 2] =
  //         (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1),
  // 16));
  //   }
  //   return data;
  // }

  @Override
  public void startUpload(InputStream pipeSource) throws IOException {

    System.out.println("In GoogleCloudStorageWriteChannel , starting upload 1");
    // Connect pipe-source to the stream used by uploader.
    InputStreamContent objectContentStream =
        new InputStreamContent(getContentType(), pipeSource)
            // Indicate that we do not know length of file in advance.
            .setLength(-1)
            .setCloseInputStream(false);

    Storage.Objects.Insert request = createRequest(objectContentStream);
    request.setDisableGZipContent(true);

    // Change chunk size from default value (10MB) to one that yields higher performance.
    clientRequestHelper.setChunkSize(request, channelOptions.getUploadChunkSize());

    // Given that the two ends of the pipe must operate asynchronous relative
    // to each other, we need to start the upload operation on a separate thread.
    uploadOperation = threadPool.submit(new UploadOperation(request, pipeSource));
  }

  Storage.Objects.Insert createRequest(InputStreamContent inputStream) throws IOException {
    // Create object with the given name and metadata.
    StorageObject object =
        new StorageObject()
            .setContentEncoding(createOptions.getContentEncoding())
            .setMetadata(encodeMetadata(createOptions.getMetadata()))
            .setName(resourceId.getObjectName());
    Storage.Objects.Insert insert =
        gcs.objects()
            .insert(resourceId.getBucketName(), object, inputStream)
            .setName(resourceId.getObjectName())
            .setKmsKeyName(createOptions.getKmsKeyName());
    writeConditions.apply(insert);
    insert
        .getMediaHttpUploader()
        .setDirectUploadEnabled(isDirectUploadEnabled())
        .setProgressListener(
            new LoggingMediaHttpUploaderProgressListener(
                resourceId.getObjectName(), MIN_LOGGING_INTERVAL.toMillis()));
    return insert;
  }

  @Override
  public void handleResponse(StorageObject response) {
    completedItemInfo = GoogleCloudStorageImpl.createItemInfoForStorageObject(resourceId, response);
  }

  /**
   * Derived classes may optionally intercept an IOException thrown from the {@code execute()}
   * method of a prepared request that came from {@link #createRequest}, and return a reconstituted
   * "response" object if the IOException can be handled as a success; for example, if the caller
   * already has an identifier for an object, and the response is used solely for obtaining the same
   * identifier, and the IOException is a handled "409 Already Exists" type of exception, then the
   * derived class may override this method to return the expected "identifier" response. Return
   * null to let the exception propagate through correctly.
   */
  public StorageObject createResponseFromException(IOException e) {
    return null;
  }

  protected String getContentType() {
    return completedItemInfo == null
        ? createOptions.getContentType()
        : completedItemInfo.getContentType();
  }

  @Override
  protected String getResourceString() {
    return resourceId.toString();
  }

  /**
   * Returns non-null only if close() has been called and the underlying object has been
   * successfully committed.
   */
  @Override
  public GoogleCloudStorageItemInfo getItemInfo() {
    return completedItemInfo;
  }

  class UploadOperation implements Callable<StorageObject> {
    // Object to be uploaded. This object declared final for safe object publishing.
    private final Storage.Objects.Insert uploadObject;

    // Read end of the pipe. This object declared final for safe object publishing.
    private final InputStream pipeSource;

    /** Constructs an instance of UploadOperation. */
    public UploadOperation(Storage.Objects.Insert uploadObject, InputStream pipeSource) {
      this.uploadObject = uploadObject;
      this.pipeSource = pipeSource;
    }

    @Override
    public StorageObject call() throws Exception {
      // Try-with-resource will close this end of the pipe so that
      // the writer at the other end will not hang indefinitely.
      try (InputStream ignore = pipeSource) {
        StorageObject resp = uploadObject.execute();
        actualCrc32c = resp.getCrc32c();
        System.out.println("crc from server" + resp.getCrc32c());
        return resp;
      } catch (IOException e) {
        GoogleCloudStorageEventBus.postOnException();
        StorageObject response = createResponseFromException(e);
        if (response == null) {
          throw e;
        }
        logger.atWarning().withCause(e).log(
            "Received IOException during '%s' upload, but successfully converted to response: '%s'.",
            resourceId, response);
        return response;
      }
    }
  }
}
