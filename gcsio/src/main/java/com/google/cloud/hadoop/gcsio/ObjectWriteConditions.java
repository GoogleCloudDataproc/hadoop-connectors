/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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
import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/**
 * Conditions on which a object write should be allowed to continue. Corresponds to setting {@code
 * IfGenerationMatch} and {@code IfMetaGenerationMatch} in API requests.
 */
@AutoValue
public abstract class ObjectWriteConditions {

  /** No conditions for completing the write. */
  public static final ObjectWriteConditions NONE = builder().build();

  public static Builder builder() {
    return new AutoValue_ObjectWriteConditions.Builder()
        .setMetaGenerationMatch(null)
        .setContentGenerationMatch(null);
  }

  public abstract Builder toBuilder();

  @Nullable
  public abstract Long getMetaGenerationMatch();

  public boolean hasMetaGenerationMatch() {
    return getMetaGenerationMatch() != null;
  }

  @Nullable
  public abstract Long getContentGenerationMatch();

  public boolean hasContentGenerationMatch() {
    return getContentGenerationMatch() != null;
  }

  /** Apply the conditions represented by this object to an Insert operation. */
  public void apply(Storage.Objects.Insert objectToInsert) {
    if (hasContentGenerationMatch()) {
      objectToInsert.setIfGenerationMatch(getContentGenerationMatch());
    }

    if (hasMetaGenerationMatch()) {
      objectToInsert.setIfMetagenerationMatch(getMetaGenerationMatch());
    }
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setMetaGenerationMatch(Long metaGenerationMatch);

    public abstract Builder setContentGenerationMatch(Long contentGenerationMatch);

    public abstract ObjectWriteConditions build();
  }
}
