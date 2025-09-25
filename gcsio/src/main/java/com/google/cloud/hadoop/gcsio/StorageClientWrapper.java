/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import com.google.api.core.ApiFuture;
import com.google.api.core.BetaApi;
import com.google.api.gax.paging.Page;
import com.google.cloud.Policy;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.HmacKey;
import com.google.cloud.storage.Notification;
import com.google.cloud.storage.NotificationInfo;
import com.google.cloud.storage.PostPolicyV4;
import com.google.cloud.storage.ServiceAccount;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper around the {@link Storage} client that delegates all calls to the underlying client,
 * except for the {@link #close()} method, which notifies the {@link StorageClientProvider}.
 */
public class StorageClientWrapper implements Storage {

  private final Storage storage;
  private final StorageClientProvider storageClientProvider;

  /**
   * Constructs a new StorageClientWrapper.
   *
   * @param storage the underlying {@link Storage} client to wrap.
   * @param storageClientProvider the provider that created this client.
   */
  public StorageClientWrapper(Storage storage, StorageClientProvider storageClientProvider) {
    this.storage = storage;
    this.storageClientProvider = storageClientProvider;
  }

  /**
   * Gets the underlying wrapped {@link Storage} instance.
   *
   * @return The wrapped {@link Storage} client.
   */
  Storage getStorage() {
    return storage;
  }

  /**
   * Custom close logic that notifies the {@link StorageClientProvider} that this client is being
   * closed.
   */
  @Override
  public void close() {
    storageClientProvider.close(this);
  }

  @Override
  public Bucket create(BucketInfo bucketInfo, BucketTargetOption... options) {
    return storage.create(bucketInfo, options);
  }

  @Override
  public Blob create(BlobInfo blobInfo, BlobTargetOption... options) {
    return storage.create(blobInfo, options);
  }

  @Override
  public Blob create(BlobInfo blobInfo, byte[] content, BlobTargetOption... options) {
    return storage.create(blobInfo, content, options);
  }

  @Override
  public Blob create(
      BlobInfo blobInfo, byte[] content, int offset, int length, BlobTargetOption... options) {
    return storage.create(blobInfo, content, offset, length, options);
  }

  /**
   * @deprecated
   */
  @Override
  @Deprecated
  public Blob create(BlobInfo blobInfo, InputStream content, BlobWriteOption... options) {
    return storage.create(blobInfo, content, options);
  }

  @Override
  public Blob createFrom(BlobInfo blobInfo, Path path, BlobWriteOption... options)
      throws IOException {
    return storage.createFrom(blobInfo, path, options);
  }

  @Override
  public Blob createFrom(BlobInfo blobInfo, Path path, int bufferSize, BlobWriteOption... options)
      throws IOException {
    return storage.createFrom(blobInfo, path, bufferSize, options);
  }

  @Override
  public Blob createFrom(BlobInfo blobInfo, InputStream content, BlobWriteOption... options)
      throws IOException {
    return storage.createFrom(blobInfo, content, options);
  }

  @Override
  public Blob createFrom(
      BlobInfo blobInfo, InputStream content, int bufferSize, BlobWriteOption... options)
      throws IOException {
    return storage.createFrom(blobInfo, content, bufferSize, options);
  }

  @Override
  public Bucket get(String bucket, BucketGetOption... options) {
    return storage.get(bucket, options);
  }

  @Override
  public Bucket lockRetentionPolicy(BucketInfo bucketInfo, BucketTargetOption... options) {
    return storage.lockRetentionPolicy(bucketInfo, options);
  }

  @Override
  public Blob get(String bucket, String blob, BlobGetOption... options) {
    return storage.get(bucket, blob, options);
  }

  @Override
  public Blob get(BlobId blobId, BlobGetOption... options) {
    return storage.get(blobId, options);
  }

  @Override
  public Blob get(BlobId blobId) {
    return storage.get(blobId);
  }

  @Override
  public Blob restore(BlobId blobId, BlobRestoreOption... options) {
    return storage.restore(blobId, options);
  }

  @Override
  public Page<Bucket> list(BucketListOption... options) {
    return storage.list(options);
  }

  @Override
  public Page<Blob> list(String bucket, BlobListOption... options) {
    return storage.list(bucket, options);
  }

  @Override
  public Bucket update(BucketInfo bucketInfo, BucketTargetOption... options) {
    return storage.update(bucketInfo, options);
  }

  @Override
  public Blob update(BlobInfo blobInfo, BlobTargetOption... options) {
    return storage.update(blobInfo, options);
  }

  @Override
  public Blob update(BlobInfo blobInfo) {
    return storage.update(blobInfo);
  }

  @Override
  public boolean delete(String bucket, BucketSourceOption... options) {
    return storage.delete(bucket, options);
  }

  @Override
  public boolean delete(String bucket, String blob, BlobSourceOption... options) {
    return storage.delete(bucket, blob, options);
  }

  @Override
  public boolean delete(BlobId blobId, BlobSourceOption... options) {
    return storage.delete(blobId, options);
  }

  @Override
  public boolean delete(BlobId blobId) {
    return storage.delete(blobId);
  }

  @Override
  public Blob compose(ComposeRequest composeRequest) {
    return storage.compose(composeRequest);
  }

  @Override
  public CopyWriter copy(CopyRequest copyRequest) {
    return storage.copy(copyRequest);
  }

  @Override
  public byte[] readAllBytes(String bucket, String blob, BlobSourceOption... options) {
    return storage.readAllBytes(bucket, blob, options);
  }

  @Override
  public byte[] readAllBytes(BlobId blob, BlobSourceOption... options) {
    return storage.readAllBytes(blob, options);
  }

  @Override
  public StorageBatch batch() {
    return storage.batch();
  }

  @Override
  public ReadChannel reader(String bucket, String blob, BlobSourceOption... options) {
    return storage.reader(bucket, blob, options);
  }

  @Override
  public ReadChannel reader(BlobId blob, BlobSourceOption... options) {
    return storage.reader(blob, options);
  }

  @Override
  public void downloadTo(BlobId blob, Path path, BlobSourceOption... options) {
    storage.downloadTo(blob, path, options);
  }

  @Override
  public void downloadTo(BlobId blob, OutputStream outputStream, BlobSourceOption... options) {
    storage.downloadTo(blob, outputStream, options);
  }

  @Override
  public WriteChannel writer(BlobInfo blobInfo, BlobWriteOption... options) {
    return storage.writer(blobInfo, options);
  }

  @Override
  public WriteChannel writer(URL signedUrl) {
    return storage.writer(signedUrl);
  }

  @Override
  public URL signUrl(BlobInfo blobInfo, long duration, TimeUnit unit, SignUrlOption... options) {
    return storage.signUrl(blobInfo, duration, unit, options);
  }

  @Override
  public PostPolicyV4 generateSignedPostPolicyV4(
      BlobInfo blobInfo,
      long duration,
      TimeUnit unit,
      PostPolicyV4.PostFieldsV4 fields,
      PostPolicyV4.PostConditionsV4 conditions,
      PostPolicyV4Option... options) {
    return storage.generateSignedPostPolicyV4(
        blobInfo, duration, unit, fields, conditions, options);
  }

  @Override
  public PostPolicyV4 generateSignedPostPolicyV4(
      BlobInfo blobInfo,
      long duration,
      TimeUnit unit,
      PostPolicyV4.PostFieldsV4 fields,
      PostPolicyV4Option... options) {
    return storage.generateSignedPostPolicyV4(blobInfo, duration, unit, fields, options);
  }

  @Override
  public PostPolicyV4 generateSignedPostPolicyV4(
      BlobInfo blobInfo,
      long duration,
      TimeUnit unit,
      PostPolicyV4.PostConditionsV4 conditions,
      PostPolicyV4Option... options) {
    return storage.generateSignedPostPolicyV4(blobInfo, duration, unit, conditions, options);
  }

  @Override
  public PostPolicyV4 generateSignedPostPolicyV4(
      BlobInfo blobInfo, long duration, TimeUnit unit, PostPolicyV4Option... options) {
    return storage.generateSignedPostPolicyV4(blobInfo, duration, unit, options);
  }

  @Override
  public List<Blob> get(BlobId... blobIds) {
    return storage.get(blobIds);
  }

  @Override
  public List<Blob> get(Iterable<BlobId> blobIds) {
    return storage.get(blobIds);
  }

  @Override
  public List<Blob> update(BlobInfo... blobInfos) {
    return storage.update(blobInfos);
  }

  @Override
  public List<Blob> update(Iterable<BlobInfo> blobInfos) {
    return storage.update(blobInfos);
  }

  @Override
  public List<Boolean> delete(BlobId... blobIds) {
    return storage.delete(blobIds);
  }

  @Override
  public List<Boolean> delete(Iterable<BlobId> blobIds) {
    return storage.delete(blobIds);
  }

  @Override
  public Acl getAcl(String bucket, Acl.Entity entity, BucketSourceOption... options) {
    return storage.getAcl(bucket, entity, options);
  }

  @Override
  public Acl getAcl(String bucket, Acl.Entity entity) {
    return storage.getAcl(bucket, entity);
  }

  @Override
  public boolean deleteAcl(String bucket, Acl.Entity entity, BucketSourceOption... options) {
    return storage.deleteAcl(bucket, entity, options);
  }

  @Override
  public boolean deleteAcl(String bucket, Acl.Entity entity) {
    return storage.deleteAcl(bucket, entity);
  }

  @Override
  public Acl createAcl(String bucket, Acl acl, BucketSourceOption... options) {
    return storage.createAcl(bucket, acl, options);
  }

  @Override
  public Acl createAcl(String bucket, Acl acl) {
    return storage.createAcl(bucket, acl);
  }

  @Override
  public Acl updateAcl(String bucket, Acl acl, BucketSourceOption... options) {
    return storage.updateAcl(bucket, acl, options);
  }

  @Override
  public Acl updateAcl(String bucket, Acl acl) {
    return storage.updateAcl(bucket, acl);
  }

  @Override
  public List<Acl> listAcls(String bucket, BucketSourceOption... options) {
    return storage.listAcls(bucket, options);
  }

  @Override
  public List<Acl> listAcls(String bucket) {
    return storage.listAcls(bucket);
  }

  @Override
  public Acl getDefaultAcl(String bucket, Acl.Entity entity) {
    return storage.getDefaultAcl(bucket, entity);
  }

  @Override
  public boolean deleteDefaultAcl(String bucket, Acl.Entity entity) {
    return storage.deleteDefaultAcl(bucket, entity);
  }

  @Override
  public Acl createDefaultAcl(String bucket, Acl acl) {
    return storage.createDefaultAcl(bucket, acl);
  }

  @Override
  public Acl updateDefaultAcl(String bucket, Acl acl) {
    return storage.updateDefaultAcl(bucket, acl);
  }

  @Override
  public List<Acl> listDefaultAcls(String bucket) {
    return storage.listDefaultAcls(bucket);
  }

  @Override
  public Acl getAcl(BlobId blob, Acl.Entity entity) {
    return storage.getAcl(blob, entity);
  }

  @Override
  public boolean deleteAcl(BlobId blob, Acl.Entity entity) {
    return storage.deleteAcl(blob, entity);
  }

  @Override
  public Acl createAcl(BlobId blob, Acl acl) {
    return storage.createAcl(blob, acl);
  }

  @Override
  public Acl updateAcl(BlobId blob, Acl acl) {
    return storage.updateAcl(blob, acl);
  }

  @Override
  public List<Acl> listAcls(BlobId blob) {
    return storage.listAcls(blob);
  }

  @Override
  public HmacKey createHmacKey(ServiceAccount serviceAccount, CreateHmacKeyOption... options) {
    return storage.createHmacKey(serviceAccount, options);
  }

  @Override
  public Page<HmacKey.HmacKeyMetadata> listHmacKeys(ListHmacKeysOption... options) {
    return storage.listHmacKeys(options);
  }

  @Override
  public HmacKey.HmacKeyMetadata getHmacKey(String accessId, GetHmacKeyOption... options) {
    return storage.getHmacKey(accessId, options);
  }

  @Override
  public void deleteHmacKey(HmacKey.HmacKeyMetadata metadata, DeleteHmacKeyOption... options) {
    storage.deleteHmacKey(metadata, options);
  }

  @Override
  public HmacKey.HmacKeyMetadata updateHmacKeyState(
      HmacKey.HmacKeyMetadata metadata,
      HmacKey.HmacKeyState state,
      UpdateHmacKeyOption... options) {
    return storage.updateHmacKeyState(metadata, state, options);
  }

  @Override
  public Policy getIamPolicy(String bucket, BucketSourceOption... options) {
    return storage.getIamPolicy(bucket, options);
  }

  @Override
  public Policy setIamPolicy(String bucket, Policy policy, BucketSourceOption... options) {
    return storage.setIamPolicy(bucket, policy, options);
  }

  @Override
  public List<Boolean> testIamPermissions(
      String bucket, List<String> permissions, BucketSourceOption... options) {
    return storage.testIamPermissions(bucket, permissions, options);
  }

  @Override
  public ServiceAccount getServiceAccount(String projectId) {
    return storage.getServiceAccount(projectId);
  }

  @Override
  public Notification createNotification(String bucket, NotificationInfo notificationInfo) {
    return storage.createNotification(bucket, notificationInfo);
  }

  @Override
  public Notification getNotification(String bucket, String notificationId) {
    return storage.getNotification(bucket, notificationId);
  }

  @Override
  public List<Notification> listNotifications(String bucket) {
    return storage.listNotifications(bucket);
  }

  @Override
  public boolean deleteNotification(String bucket, String notificationId) {
    return storage.deleteNotification(bucket, notificationId);
  }

  @Override
  @BetaApi
  public BlobWriteSession blobWriteSession(BlobInfo blobInfo, BlobWriteOption... options) {
    return storage.blobWriteSession(blobInfo, options);
  }

  @Override
  public Blob moveBlob(MoveBlobRequest request) {
    return storage.moveBlob(request);
  }

  @Override
  @BetaApi
  public ApiFuture<BlobReadSession> blobReadSession(BlobId id, BlobSourceOption... options) {
    return storage.blobReadSession(id, options);
  }

  @Override
  @BetaApi
  public BlobAppendableUpload blobAppendableUpload(
      BlobInfo blobInfo, BlobAppendableUploadConfig uploadConfig, BlobWriteOption... options) {
    return storage.blobAppendableUpload(blobInfo, uploadConfig, options);
  }

  @Override
  public StorageOptions getOptions() {
    return storage.getOptions();
  }
}
