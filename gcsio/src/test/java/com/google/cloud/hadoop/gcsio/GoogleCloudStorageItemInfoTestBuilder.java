/*
 * Copyright 2019 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;

@AutoValue
public abstract class GoogleCloudStorageItemInfoTestBuilder {

  /** Default StorageResourceId. */
  public static final StorageResourceId STORAGE_RESOURCE_ID_DEFAULT =
      new StorageResourceId("testBucket", "testObject");

  /** Default creation time. */
  public static final long CREATION_TIME_DEFAULT = 10L;

  /** Default modification time. */
  public static final long MODIFICATION_TIME_DEFAULT = 10L;

  /** Default size. */
  public static final long SIZE_DEFAULT = 200L;

  /** Default location. */
  public static final String LOCATION_DEFAULT = "location";

  /** Default storage class. */
  public static final String STORAGE_CLASS_DEFAULT = "storage class";

  /** Default content type. */
  public static final String CONTENT_TYPE_DEFAULT = "storage class";

  /** Default content encoding. */
  public static final String CONTENT_ENCODING_DEFAULT = null;

  /** Default metadata. */
  public static final Map<String, byte[]> METADATA_DEFAULT =
      ImmutableMap.of("foo", new byte[2]);

  /** Default content generation. */
  public static final long CONTENT_GENERATION_DEFAULT = 0L;

  /** Default meta generation. */
  public static final long META_GENERATION_DEFAULT = 0L;

  /** Default VerificationAttributes. */
  public static final VerificationAttributes VERIFICATION_ATTRIBUTES_DEFAULT = new VerificationAttributes(null, null);

  static Builder builder() {
    return new AutoValue_GoogleCloudStorageItemInfoTestBuilder.Builder()
        .setStorageResourceId(STORAGE_RESOURCE_ID_DEFAULT)
        .setCreationTime(CREATION_TIME_DEFAULT)
        .setModificationTime(MODIFICATION_TIME_DEFAULT)
        .setSize(SIZE_DEFAULT)
        .setLocation(LOCATION_DEFAULT)
        .setStorageClass(STORAGE_CLASS_DEFAULT)
        .setContentType(CONTENT_TYPE_DEFAULT)
        .setContentEncoding(CONTENT_ENCODING_DEFAULT)
        .setMetadata(METADATA_DEFAULT)
        .setContentGeneration(CONTENT_GENERATION_DEFAULT)
        .setMetaGeneration(META_GENERATION_DEFAULT)
        .setVerificationAttributes(VERIFICATION_ATTRIBUTES_DEFAULT);
  }

  public abstract StorageResourceId getStorageResourceId();

  public abstract long getCreationTime();

  public abstract long getModificationTime();

  public abstract long getSize();

  @Nullable
  public abstract String getLocation();

  @Nullable
  public abstract String getStorageClass();

  @Nullable
  public abstract String getContentType();

  @Nullable
  public abstract String getContentEncoding();

  public abstract Map<String, byte[]> getMetadata();

  public abstract long getContentGeneration();

  public abstract long getMetaGeneration();

  public abstract VerificationAttributes getVerificationAttributes();

  /** Mutable builder for the GoogleCloudStorageItemInfo class. */
  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setStorageResourceId(StorageResourceId storageResourceId);

    abstract Builder setCreationTime(long creationTime);

    abstract Builder setModificationTime(long modificationTime);

    abstract Builder setSize(long size);

    @Nullable
    abstract Builder setLocation(String location);

    @Nullable
    abstract Builder setStorageClass(String storageClass);

    @Nullable
    abstract Builder setContentType(String contentType);

    @Nullable
    abstract Builder setContentEncoding(String contentEncoding);

    abstract Builder setMetadata(Map<String, byte[]> metadata);

    abstract Builder setContentGeneration(long contentGeneration);

    abstract Builder setMetaGeneration(long metaGeneration);

    abstract Builder setVerificationAttributes(VerificationAttributes verificationAttributes);

    abstract GoogleCloudStorageItemInfoTestBuilder autoBuild();

    public GoogleCloudStorageItemInfo build() {

      GoogleCloudStorageItemInfoTestBuilder instance = autoBuild();

      return new GoogleCloudStorageItemInfo(
          instance.getStorageResourceId(),
          /* creationTime= */ instance.getCreationTime(),
          /* modificationTime= */ instance.getModificationTime(),
          /* size= */ instance.getSize(),
          instance.getLocation(),
          instance.getStorageClass(),
          instance.getContentType(),
          /* contentEncoding= */ instance.getContentEncoding(),
          instance.getMetadata(),
          instance.getContentGeneration(),
          instance.getMetaGeneration(),
          instance.getVerificationAttributes());
    }
  }
}
