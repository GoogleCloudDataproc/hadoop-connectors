/*
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.api.services.storage.Storage.Objects.Insert;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.util.AbstractGoogleAsyncWriteChannel;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.cloud.hadoop.util.LoggingMediaHttpUploaderProgressListener;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

/** Implements WritableByteChannel to provide write access to GCS. */
public class GoogleCloudStorageWriteChannel
    extends AbstractGoogleAsyncWriteChannel<Insert, StorageObject>
    implements GoogleCloudStorageItemInfo.Provider {

  private static final long MIN_LOGGING_INTERVAL_MS = 60000L;

  private final Storage gcs;
  private final StorageResourceId resourceId;
  private final CreateObjectOptions createOptions;
  private final ObjectWriteConditions writeConditions;

  private GoogleCloudStorageItemInfo completedItemInfo = null;

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
      ObjectWriteConditions writeConditions) {
    super(requestHelper, uploadThreadPool, channelOptions);
    this.gcs = gcs;
    this.resourceId = resourceId;
    this.createOptions = createOptions;
    this.writeConditions = writeConditions;
  }

  @Override
  public Insert createRequest(InputStreamContent inputStream) throws IOException {
    // Create object with the given name and metadata.
    StorageObject object =
        new StorageObject()
            .setContentEncoding(createOptions.getContentEncoding())
            .setMetadata(encodeMetadata(createOptions.getMetadata()))
            .setName(resourceId.getObjectName());

    Insert insert =
        gcs.objects()
            .insert(resourceId.getBucketName(), object, inputStream)
            .setName(resourceId.getObjectName())
            .setKmsKeyName(createOptions.getKmsKeyName());
    writeConditions.apply(insert);
    if (insert.getMediaHttpUploader() != null) {
      insert
          .getMediaHttpUploader()
          .setDirectUploadEnabled(isDirectUploadEnabled())
          .setProgressListener(
              new LoggingMediaHttpUploaderProgressListener(
                  resourceId.getObjectName(), MIN_LOGGING_INTERVAL_MS));
    }
    return insert;
  }

  @Override
  public void handleResponse(StorageObject response) {
    this.completedItemInfo = GoogleCloudStorageImpl.createItemInfoForStorageObject(
        new StorageResourceId(bucketName, objectName), response);
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
}
