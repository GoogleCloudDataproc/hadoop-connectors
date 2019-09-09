/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.ChunkFailed;
import com.google.cloud.hadoop.util.ClientRequestHelper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Implements WritableByteChannel to provide write access to GCS with retry for resumable upload
 * session.
 */
public class GoogleCloudStorageWriteChannelRetryWrapper implements WritableByteChannel {
  public GoogleCloudStorageWriteChannel channel;
  private final String bucket;
  private final String object;
  private final ExecutorService backgroundTasksThreadPool;
  private final Storage gcs;
  private final ClientRequestHelper<StorageObject> clientRequestHelper;
  private final StorageResourceId resourceId;
  private final CreateObjectOptions options;
  private final GoogleCloudStorageOptions storageOptions;
  private final ObjectWriteConditions writeConditions;
  private final Map<String, String> rewrittenMetadata;

  public GoogleCloudStorageWriteChannelRetryWrapper(
      CreateObjectOptions options,
      GoogleCloudStorageOptions storageOptions,
      StorageResourceId resourceId,
      ExecutorService backgroundTasksThreadPool,
      Storage gcs,
      ClientRequestHelper<StorageObject> clientRequestHelper,
      String bucketName,
      String objectName,
      String contentType,
      String contentEncoding,
      String kmsKeyName,
      AsyncWriteChannelOptions writeChannelOptions,
      ObjectWriteConditions writeConditions,
      Map<String, String> rewrittenMetadata)
      throws IOException {
    this.bucket = bucketName;
    this.object = objectName;
    this.backgroundTasksThreadPool = backgroundTasksThreadPool;
    this.gcs = gcs;
    this.clientRequestHelper = clientRequestHelper;
    this.resourceId = resourceId;
    this.options = options;
    this.storageOptions = storageOptions;
    this.writeConditions = writeConditions;
    this.rewrittenMetadata = rewrittenMetadata;
    this.channel =
        new GoogleCloudStorageWriteChannel(
            backgroundTasksThreadPool,
            gcs,
            clientRequestHelper,
            resourceId.getBucketName(),
            resourceId.getObjectName(),
            contentType,
            contentEncoding,
            /* kmsKeyName= */ kmsKeyName,
            writeChannelOptions,
            writeConditions,
            rewrittenMetadata);
  }

  public void initialize() throws IOException {

    this.channel.initialize();
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return this.channel.write(src);
  }

  @Override
  public boolean isOpen() {
    return this.channel.isOpen();
  }

  @Override
  public void close() throws IOException {
    try {
      this.channel.close();
    } catch (IOException e) {
      if (e.getCause().getClass() == ChunkFailed.class) {
        initialize();
      }
      this.channel.close();
    }
  }
}
